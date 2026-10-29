/* BSD 2-Clause License
 *
 * Copyright (c) 2023, Andrea Giacomo Baldan All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "acl.h"
#include "config.h"
#include "ev.h"
#include "handlers.h"
#include "logging.h"
#include "memory.h"
#include "memorypool.h"
#include "network.h"
#include "persistence.h"
#include "sol_internal.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

pthread_mutex_t mutex;

/*
 * Per peer address connection accounting for max_connections_per_ip. The
 * table is only touched under the global mutex: the accept path inserts and
 * increments, client_deactivate_locked() decrements, and an entry disappears
 * when its count reaches zero so a scan of random addresses cannot grow the
 * table forever
 */
struct ip_conn {
    char ip[INET6_ADDRSTRLEN + 8];
    unsigned count;
    UT_hash_handle hh;
};

static struct ip_conn *ip_conns = NULL;

/*
 * The connection keeps the peer as "address:port" (or "[v6]:port") because
 * that is what makes a log line useful, but the quota is per address: every
 * new connection comes from a different source port, so keying the table on
 * the whole string would never count two connections together. The last ':'
 * is the separator in both forms (the IPv6 address is bracketed)
 */
static void ip_key(const char *ip, char *out, size_t outsz)
{
    const char *colon = strrchr(ip, ':');
    size_t len        = colon ? (size_t)(colon - ip) : strlen(ip);
    if (len >= outsz)
        len = outsz - 1;
    memcpy(out, ip, len);
    out[len] = '\0';
}

/* Caller holds the global mutex. False when the peer is over its limit */
static bool ip_conn_acquire(struct server *server, const char *ip)
{
    if (server->conf->max_connections_per_ip == 0)
        return true;

    char key[INET6_ADDRSTRLEN + 8];
    ip_key(ip, key, sizeof(key));

    struct ip_conn *entry = NULL;
    HASH_FIND_STR(ip_conns, key, entry);
    if (entry && entry->count >= server->conf->max_connections_per_ip)
        return false;
    if (!entry) {
        entry = try_alloc(sizeof(*entry));
        if (!entry)
            return false;
        snprintf(entry->ip, sizeof(entry->ip), "%s", key);
        entry->count = 0;
        HASH_ADD_STR(ip_conns, ip, entry);
    }
    entry->count++;
    return true;
}

/* Caller holds the global mutex */
static void ip_conn_release(struct server *server, const char *ip)
{
    if (server->conf->max_connections_per_ip == 0 || !ip || !ip[0])
        return;
    char key[INET6_ADDRSTRLEN + 8];
    ip_key(ip, key, sizeof(key));

    struct ip_conn *entry = NULL;
    HASH_FIND_STR(ip_conns, key, entry);
    if (!entry)
        return;
    if (entry->count > 0)
        entry->count--;
    if (entry->count == 0) {
        HASH_DEL(ip_conns, entry);
        free_memory(entry);
    }
}

/*
 * Auxiliary structure to be used as init argument for eventloop, fd is the
 * listening socket we want to share between multiple instances, cronjobs is
 * just a flag to signal if we want to register cronjobs on that particular
 * instance or not (to not repeat useless cron jobs on multiple threads)
 */
struct listen_payload {
    int fd;
    bool cronjobs;
    struct server *server;
    /*
     * Slot this loop owns in the `loops` array below. Every thread gets a
     * payload of its own: they used to share one struct, and a worker could
     * read `cronjobs` after the main thread had already set it, registering a
     * second (and third) copy of every periodic task
     */
    int index;
};

/*
 * The event contexts of every loop, filled in when each thread starts and
 * disposed of once every thread has left its own (see eventloop_start and the
 * end of start_server): a loop that is stopping can still be the target of
 * another one (a delivery, a takeover), so nothing may be released until all
 * of them are done
 */
static struct ev_ctx *loops[THREADSNR + 1];

/* Seconds in a Sol, easter egg */
static const double SOL_SECONDS = 88775.24;

/*
 * General informations of the broker, all fields will be published
 * periodically to internal topics
 */
struct sol_info info;

/*
 * TCP server, based on I/O multiplexing abstraction called ev_ctx. Each thread
 * (if any) should have his own ev_ctx and thus being responsible of a subset
 * of clients.
 * At the init of the server, the ev_ctx will be instructed to run some
 * periodic tasks and to run a callback on accept on new connections. From now
 * on start a simple juggling of callbacks to be scheduled on the event loop,
 * typically after being accepted a connection his handle (fd) will be added to
 * the backend of the loop (this case we're using EPOLL as a backend but also
 * KQUEUE or SELECT/POLL should be easy to plug-in) and read_callback will be
 * run every time there's new data incoming. If a complete packet is received
 * and correctly parsed it will be processed by calling the right handler from
 * the handler module, based on the command it carries and a response will be
 * fired back.
 *
 *                             MAIN THREAD
 *                              [EV_CTX]
 *
 *    ACCEPT_CALLBACK         READ_CALLBACK         WRITE_CALLBACK
 *  -------------------    ------------------    --------------------
 *        |                        |                       |
 *      ACCEPT                     |                       |
 *        | ---------------------> |                       |
 *        |                  READ AND DECODE               |
 *        |                        |                       |
 *        |                        |                       |
 *        |                     PROCESS                    |
 *        |                        |                       |
 *        |                        |                       |
 *        |                        | --------------------> |
 *        |                        |                     WRITE
 *      ACCEPT                     |                       |
 *        | ---------------------> | <-------------------- |
 *        |                        |                       |
 *
 * The whole method could be easily distributed across a threadpool, by paying
 * attention to the shared critical parts on handler module.
 * The access to shared data strucures could be guarded by a mutex or a
 * spinlock, and being generally fast operations it shouldn't suffer high
 * contentions by the threads and thus being really fast.
 * The drawback of this approach is that the concurrency is not equally
 * distributed across all threads, each thread has it's own eventloop and thus
 * is responsible for a subset of connections, without any possibility of
 * cooperation from other threads. This should be mitigated by the fact that
 * this application mainly deals with short-lived connections so there's
 * frequent turnover of monitored FDs, increasing the number of connections
 * for each different thread.
 */

static void client_init(struct client *);

/*
 * Deactivate a connection, releasing its pool slot: returns true when a live
 * client was actually released. client_close() wraps it together with the
 * connection counter, and is the entry point every close path must use
 */
static bool client_deactivate(struct client *);

static void client_close(struct client *);

/* Human readable form of the internal error codes, defined below */
static const char *solerr(int rc);

// CALLBACKS for the eventloop
static void accept_callback(struct ev_ctx *, void *);

static void read_callback(struct ev_ctx *, void *);

static void write_callback(struct ev_ctx *, void *);

/*
 * Deliver the will (if the CONNECT carried one) and drop the connection.
 * Shared by the read error path and the keepalive timeout below: both close
 * a connection for the same protocol reason, "the client is gone and never
 * sent a DISCONNECT", and MQTT v3.1.1 §3.1.4 requires the Will Message to
 * be published in exactly those cases. The caller must already run on the
 * loop that owns `c` and must not hold any lock: publish_lwt() takes the
 * global one itself
 */
static void client_shutdown(struct ev_ctx *ctx, struct client *c, int rc)
{
    log_error("Closing connection with %s (%s): %s", c->client_id,
              c->conn.ip, solerr(rc));
    /*
     * Deliver the LWT, if any: publish_lwt() runs the same path a normal
     * PUBLISH takes (exact topic key, wildcard subscriber attach) and
     * takes the locks it needs itself. Doing the
     * lookup while holding the global mutex and then calling
     * publish_message(), which acquires that same non-recursive mutex
     * on entry, would deadlock this very thread and freeze the broker
     * (and make a later SIGTERM hang on the thread join).
     *
     * The session pointer is read exactly once, under the client mutex:
     * server_kick_duplicate() runs on the loop that accepted the
     * duplicate id, clears has_lwt and NULLs session under this very
     * lock, and reading c->session twice (once in the condition, once in
     * the argument) opened a window where the takeover landed in between
     * and the dereference below crashed. Claiming a reference while still
     * locked also keeps the session alive for the whole publish even when
     * the duplicate CONNECT tears the old one down on its own thread a
     * moment later
     */
    struct client_session *lwt_session = NULL;
#if THREADSNR > 0
    pthread_mutex_lock(&c->mutex);
#endif
    if (c->has_lwt && c->session) {
        lwt_session = c->session;
        INCREF(lwt_session, struct client_session);
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    if (lwt_session) {
        publish_lwt(c->server, &lwt_session->lwt_msg);
        DECREF(lwt_session, struct client_session);
    }
    // Clean resources
    ev_del_fd(ctx, c->conn.fd);
    /*
     * No subscription cleanup here: client_deactivate() already drops
     * the topic subscriptions of a clean session, while the ones of a
     * persistent session (clean_session = false) must survive this
     * abrupt disconnect or the reconnected client would have a session
     * with no subscription left on the broker side
     */
    client_close(c);
}

/*
 * Periodic routine to check for incomplete transactions on QoS > 0 to be
 * concluded
 */
static void inflight_msg_check(struct ev_ctx *, void *);

/*
 * Processing message function, will be applied on fully formed mqtt packet
 * received on read_callback callback
 */
static void process_message(struct ev_ctx *, struct client *);

/* Periodic routine to publish general stats about the broker on $SOL topics */
static void publish_stats(struct ev_ctx *, void *);

/*
 * Statistics topics, published every N seconds defined by configuration
 * interval
 */
#define SYS_TOPICS 18

/*
 * Utility struct for information topics. Just the name of the topic and his
 * length, the length is computed at compile time by the SYS_T macro so that
 * it can never drift from the actual strlen() of the topic name (a wrong
 * length would make the broker publish a topic nobody is subscribed to)
 */
struct sys_topic {
    const char *name;
    int len;
};

/*
 * Note: every statistics topic must end with the hierarchical separator '/',
 * the same normalization rule applied to the topics and filters published or
 * subscribed by clients, otherwise a subscriber to $SOL/broker/# would never
 * match it
 */
#define SYS_T(t)                                                               \
    {                                                                          \
        .name = t, .len = (int)(sizeof(t) - 1)                                 \
    }

/* Information topics mapping, accessed by publish_stats periodic routine */
static const struct sys_topic sys_topics[SYS_TOPICS] = {
    SYS_T("$SOL/broker/clients/"),
    SYS_T("$SOL/broker/messages/"),
    SYS_T("$SOL/broker/uptime/"),
    SYS_T("$SOL/broker/uptime/sol/"),
    SYS_T("$SOL/broker/clients/connected/"),
    SYS_T("$SOL/broker/clients/disconnected/"),
    SYS_T("$SOL/broker/bytes/sent/"),
    SYS_T("$SOL/broker/bytes/received/"),
    SYS_T("$SOL/broker/messages/sent/"),
    SYS_T("$SOL/broker/messages/received/"),
    SYS_T("$SOL/broker/memory/used/"),
    SYS_T("$SOL/broker/sessions/"),
    SYS_T("$SOL/broker/topics/"),
    SYS_T("$SOL/broker/subscriptions/"),
    SYS_T("$SOL/broker/messages/inflight/"),
    SYS_T("$SOL/broker/messages/retained/"),
    SYS_T("$SOL/broker/messages/dropped/"),
    SYS_T("$SOL/broker/memory/limit/")};

/* Simple error_code to string function, to be refined */
static const char *solerr(int rc)
{
    switch (rc) {
    case -ERRCLIENTDC:
        return "Client disconnected";
    case -ERRSOCKETERR:
        return strerror(errno);
    case -ERRPACKETERR:
        return "Error reading packet";
    case -ERRMAXREQSIZE:
        return "Packet sent exceeds max size accepted";
    case -ERREAGAIN:
        return "Socket FD EAGAIN";
    case -ERRNOMEM:
        return "Out of memory";
    case -ERRKEEPALIVE:
        return "Keepalive timeout";
    case MQTT_UNACCEPTABLE_PROTOCOL_VERSION:
        return "[MQTT] Unknown protocol version";
    case MQTT_IDENTIFIER_REJECTED:
        return "[MQTT] Wrong identifier";
    case MQTT_SERVER_UNAVAILABLE:
        return "[MQTT] Server unavailable";
    case MQTT_BAD_USERNAME_OR_PASSWORD:
        return "[MQTT] Bad username or password";
    case MQTT_NOT_AUTHORIZED:
        return "[MQTT] Not authorized";
    default:
        return "Unknown error";
    }
}

/*
 * ====================================================
 *  Cron tasks, to be repeated at fixed time intervals
 * ====================================================
 */

/*
 * Publish statistics periodic task, it will be called once every N config
 * defined seconds, it publishes some informations on predefined topics
 */

/*
 * Store lookups performed by the stats routine must hold the global mutex:
 * publish_message() is called with the lock released (it acquires it
 * itself), so the lookup and the publish cannot share one critical section
 */
static struct topic *stats_topic(struct server *server, const char *name)
{
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    struct topic *t = topic_store_get(server->store, name);
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif
    return t;
}

/* Topic store counters, collected in one walk under the global mutex */
struct store_stats {
    size_t topics;
    size_t retained;
};

static void collect_store_stats(struct trie_node *node, void *arg)
{
    struct topic *t        = node->data;
    struct store_stats *st = arg;
    if (!t)
        return;
    st->topics++;
    if (t->retained)
        st->retained++;
}

static void publish_stats(struct ev_ctx *ctx, void *data)
{
    struct server *server = ctx->data;
    struct config *conf   = server->conf;
    (void)data;
    (void)ctx;

    /*
     * Snapshot the mutable counters once: reading them twice could mix two
     * different instants and make the "disconnected" delta inconsistent with
     * the "connected" one published just before
     */
    size_t active = info.active_connections;
    size_t total  = info.total_connections;
    size_t msent  = info.messages_sent;
    size_t mrecv  = info.messages_recv;

    char cclients[21];
    snprintf(cclients, 21, "%lu", (unsigned long)active);

    /* Number of connections accepted since the broker start */
    char ctotal[21];
    snprintf(ctotal, 21, "%lu", (unsigned long)total);

    /* Connections accepted and then dropped (clamped, snapshots race) */
    char cdisconn[21];
    snprintf(cdisconn, 21, "%lu",
             (unsigned long)(total > active ? total - active : 0));

    char bsent[21];
    snprintf(bsent, 21, "%lu", (unsigned long)info.bytes_sent);

    char brecv[21];
    snprintf(brecv, 21, "%lu", (unsigned long)info.bytes_recv);

    char sent[21];
    snprintf(sent, 21, "%lu", (unsigned long)msent);

    char recv[21];
    snprintf(recv, 21, "%lu", (unsigned long)mrecv);

    /* Total messages handled, incoming plus outgoing */
    char mtotal[21];
    snprintf(mtotal, 21, "%lu", (unsigned long)(msent + mrecv));

    unsigned long long uptime = (unsigned long long)(time(NULL) -
                                                     info.start_time);
    char utime[21];
    snprintf(utime, 21, "%llu", uptime);

    double sol_uptime = (double)uptime / SOL_SECONDS;
    char sutime[16];
    snprintf(sutime, 16, "%.4f", sol_uptime);

    long long memory = memory_used();
    char mem[21];
    snprintf(mem, 21, "%lld", memory);

    /*
     * Store and session counters: one pass over the session map and one over
     * the topic trie, both under the global mutex, so the numbers describe a
     * single instant. They are only refreshed at stats_publish_interval,
     * which is why a full walk is affordable here
     */
    size_t sessions = 0, subscriptions = 0, inflight = 0;
    struct store_stats st = {0, 0};
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    for (struct client_session *s = server->sessions; s; s = s->hh.next) {
        sessions++;
        subscriptions += list_size(s->subscriptions);
        inflight += s->inflights;
    }
    topic_store_map(server->store, NULL, collect_store_stats, &st);
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif

    char csessions[21], ctopics[21], csubs[21], cinflight[21], cretained[21],
        cdropped[21], climit[21];
    snprintf(csessions, 21, "%lu", (unsigned long)sessions);
    snprintf(ctopics, 21, "%lu", (unsigned long)st.topics);
    snprintf(csubs, 21, "%lu", (unsigned long)subscriptions);
    snprintf(cinflight, 21, "%lu", (unsigned long)inflight);
    snprintf(cretained, 21, "%lu", (unsigned long)st.retained);
    snprintf(cdropped, 21, "%lu",
             (unsigned long)info.messages_dropped);
    snprintf(climit, 21, "%lu", (unsigned long)conf->max_memory);

    /*
     * One single packet reused for every statistics topic: the broker always
     * publishes them at QoS 0 (header 0x30, pkt_id 0), so no subscriber ever
     * keeps a reference to it and its contents can safely be swapped between
     * the publishes below
     */
    struct mqtt_packet p = {.header  = (union mqtt_header){.byte = PUBLISH_B},
                            .publish = (struct mqtt_publish){.pkt_id = 0}};

    /*
     * Publish `val` on the statistics topic at index `i`: every topic
     * declared in sys_topics[] must be published here, a declared but never
     * published topic would make subscribers to $SOL/broker/# silently miss
     * part of the advertised feed
     */
#define PUBLISH_STAT(i, val)                                                   \
    do {                                                                       \
        p.publish.topiclen   = sys_topics[i].len;                              \
        p.publish.topic      = (unsigned char *)sys_topics[i].name;            \
        p.publish.payloadlen = strlen(val);                                    \
        p.publish.payload    = (unsigned char *)(val);                         \
        publish_message(server, &p, stats_topic(server, sys_topics[i].name)); \
    } while (0)

    PUBLISH_STAT(0, ctotal);   /* $SOL/broker/clients/           */
    PUBLISH_STAT(1, mtotal);   /* $SOL/broker/messages/          */
    PUBLISH_STAT(2, utime);    /* $SOL/broker/uptime/            */
    PUBLISH_STAT(3, sutime);   /* $SOL/broker/uptime/sol/        */
    PUBLISH_STAT(4, cclients); /* $SOL/broker/clients/connected/ */
    PUBLISH_STAT(5, cdisconn); /* $SOL/broker/clients/disconnected/ */
    PUBLISH_STAT(6, bsent);    /* $SOL/broker/bytes/sent/        */
    PUBLISH_STAT(7, brecv);    /* $SOL/broker/bytes/received/    */
    PUBLISH_STAT(8, sent);     /* $SOL/broker/messages/sent/     */
    PUBLISH_STAT(9, recv);     /* $SOL/broker/messages/received/ */
    PUBLISH_STAT(10, mem);     /* $SOL/broker/memory/used/       */
    PUBLISH_STAT(11, csessions);  /* $SOL/broker/sessions/          */
    PUBLISH_STAT(12, ctopics);    /* $SOL/broker/topics/            */
    PUBLISH_STAT(13, csubs);      /* $SOL/broker/subscriptions/     */
    PUBLISH_STAT(14, cinflight);  /* $SOL/broker/messages/inflight/ */
    PUBLISH_STAT(15, cretained);  /* $SOL/broker/messages/retained/ */
    PUBLISH_STAT(16, cdropped);   /* $SOL/broker/messages/dropped/  */
    PUBLISH_STAT(17, climit);     /* $SOL/broker/memory/limit/      */

#undef PUBLISH_STAT
}

/*
 * Check for inflight messages in the ingoing and outgoing maps (actually
 * arrays), each position between 0-65535 contains either NULL or a pointer
 * to an inflight stucture with a timestamp of the sending action, the
 * target file descriptor (e.g. the client) and the payload to be sent
 * unserialized, this way it's possible to set the DUP flag easily at the cost
 * of additional packing before re-sending it out
 */
static void inflight_msg_check(struct ev_ctx *ctx, void *data)
{
    struct server *server = ctx->data;
    struct config *conf   = server->conf;
    (void)data;
    (void)ctx;
    size_t size    = 0;
    time_t now     = time(NULL);
    struct client *c, *tmp;
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    HASH_ITER(hh, server->clients_map, c, tmp)
    {
        if (!c || !c->connected || !c->session || !has_inflight(c->session))
            continue;
        struct client_session *s = c->session;
#if THREADSNR > 0
        pthread_mutex_lock(&c->mutex);
#endif
        /*
         * Walk only the packet identifiers actually pending (typically a
         * handful) instead of all 65535 slots of the tables: this loop runs
         * once per second per client while holding both the global and the
         * client mutex, the full scan used to dominate the periodic cost of
         * the broker with many connections
         */
        bool dirty = false;
        for (unsigned i = 0; i < s->pending_len; ++i) {
            unsigned short mid = s->pending_mids[i];
            struct inflight_msg *imsg = session_inflight(s, mid);
            if (!imsg)
                continue;
            // Messages: re-send with the DUP flag set
            if (imsg->packet &&
                (now - imsg->seen) > (time_t)conf->retransmit_interval) {
                struct mqtt_packet *p = imsg->packet;
                p->header.bits.qos    = imsg->qos;
                /*
                 * The packet is shared by every subscriber of the original
                 * publish, each with its own identifier: restore the one of
                 * this session or the client would acknowledge an identifier
                 * it never received
                 */
                p->publish.pkt_id = mid;
                // Set DUP flag to 1
                mqtt_set_dup(p);
                size = mqtt_size(p, NULL);
                if (!outgoing_reserve(c, size)) {
                    /*
                     * The subscriber is not draining its buffer yet, keep
                     * the original timestamp so the message is retried on
                     * the next tick instead of piling up more bytes
                     */
                    continue;
                }
                log_debug("Re-sending message to %s", c->client_id);
                // Serialize the packet and send it out again
                mqtt_pack(p, c->wbuf + c->towrite);
                c->towrite += size;
                dirty = true;
                // Update information stats
                info.messages_sent++;
                /*
                 * Refresh the timestamp: without it the message would be
                 * re-sent on every tick (once per second) instead of every
                 * retransmission interval, flooding the client
                 */
                imsg->seen = now;
            }
            // Acks: re-send the PUBREL still awaiting a PUBCOMP
            if (imsg->ack_sent > 0 &&
                (now - imsg->ack_sent) > (time_t)conf->retransmit_interval) {
                /*
                 * A well formed PUBREL: the fixed header byte must be set
                 * explicitly (mqtt_ack() only fills the packet identifier)
                 * and the packet must be packed from its own structure,
                 * packing `p` here used to dereference a NULL pointer on
                 * every retransmission
                 */
                struct mqtt_packet ack = {
                    .header = (union mqtt_header){.byte = PUBREL_B}};
                mqtt_ack(&ack, mid);
                size = mqtt_size(&ack, NULL);
                if (!outgoing_reserve(c, size))
                    continue;
                log_debug("Re-sending ack to %s", c->client_id);
                // Serialize the packet and send it out again
                mqtt_pack(&ack, c->wbuf + c->towrite);
                c->towrite += size;
                dirty = true;
                // Update information stats
                info.messages_sent++;
                /* Same as above: wait another interval before re-sending */
                imsg->ack_sent = now;
            }
        }
        /*
         * One single write event for everything enqueued above, the event
         * loop coalesces the descriptor in any case
         */
        if (dirty)
            enqueue_event_write(c);
#if THREADSNR > 0
        pthread_mutex_unlock(&c->mutex);
#endif
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif
}

/*
 * Runs on the loop that owns the connection: ev_fire_event() only replaces
 * the write callback of the slot, so the actual teardown has to be scheduled
 * here rather than done straight from the cron's loop, the same cross-thread
 * primitive a duplicate-id takeover (kick_callback) uses
 */
static void keepalive_timeout_callback(struct ev_ctx *ctx, void *arg)
{
    struct client *c = arg;
    /* The connection may have dropped on its own before this ran */
    if (c->online == false)
        return;
    client_shutdown(ctx, c, -ERRKEEPALIVE);
}

/*
 * MQTT v3.1.1 §3.1.2.10: when the Keep Alive is not zero and no Control
 * Packet arrives within one and a half times that period the Server MUST
 * close the connection. Keep Alive 0 means "no keepalive processing" and is
 * skipped.
 *
 * last_seen is written by the loop that owns the connection (read_callback,
 * on every complete packet) and read here: time_t is a single aligned word
 * on every supported target, so a stale or torn value can only delay the
 * disconnect by one tick, never misplace it. Note that partial reads do not
 * refresh it on purpose, only a *complete* Control Packet does, exactly as
 * the spec words it
 */
static void keepalive_check(struct ev_ctx *ctx, void *data)
{
    struct server *server = ctx->data;
    (void)data;
    /*
     * The one second tick is also where a SIGHUP requested log reopen is
     * serviced: the signal handler cannot call fopen() itself
     */
    sol_log_maybe_reopen();
    time_t now      = time(NULL);
    struct client *c, *tmp;
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    HASH_ITER(hh, server->clients_map, c, tmp)
    {
        if (!c || !c->connected || c->keepalive == 0)
            continue;
        /*
         * One and a half times the negotiated period, as the spec requires.
         * The margin keeps a client that sends a PINGREQ right on the edge
         * from being dropped by a cron that happens to fire a hair early
         */
        time_t limit = (time_t)c->keepalive + (time_t)c->keepalive / 2;
        if (now - c->last_seen <= limit)
            continue;
        log_info("Keepalive expired for %s (last packet %lds ago, limit "
                 "%lds): dropping the connection (MQTT v3.1.1 §3.1.2.10)",
                 c->client_id, (long)(now - c->last_seen), (long)limit);
        /*
         * Hand the teardown to the loop that owns this descriptor: closing a
         * fd (and freeing the client) from here would race with the callback
         * that thread is about to run on it
         */
        ev_fire_event(c->ctx, c->conn.fd, EV_WRITE, keepalive_timeout_callback,
                      (void *)c);
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif
}

/*
 * The max_memory policy, in the order the configuration promises: first the
 * topics nobody holds anymore are released, then the oldest messages queued
 * for the clients that are away, and when even that is not enough new
 * connections are refused until the broker comes back under its limit. Under
 * the limit the reclamation still runs, on its own (much slower) interval;
 * every level of the policy is logged when it acts
 */
static void memory_check(struct ev_ctx *ctx, void *data)
{
    struct server *server = ctx->data;
    struct config *conf   = server->conf;
    (void)ctx;
    (void)data;

    bool limited = conf->max_memory > 0;
    bool over    = limited && memory_used() > conf->max_memory;

    /* Under pressure every check acts, otherwise the walk runs on its own
       (much slower) interval */
    static time_t last_reclaim = 0;
    time_t now                 = time(NULL);
    bool reclaim = over || now - last_reclaim >= (time_t)conf->reclaim_interval;
    if (reclaim)
        last_reclaim = now;

    /*
     * The state of the broker is evaluated on every check, even when there is
     * nothing to reclaim: a broker that came back under its limit (a
     * subscriber went away, a client acknowledged its backlog) has to accept
     * connections again, and waiting for the next reclamation interval would
     * keep it refusing them for up to `reclaim_interval` seconds
     */
    if (!reclaim && !server->over_memory)
        return;

#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif

    /* Level one: the topics nothing refers to anymore */
    if (reclaim)
        topic_store_reclaim(server->store);

    /* Level two: the oldest queued messages, until the limit is honoured */
    over = limited && memory_used() > conf->max_memory;
    if (over) {
        struct client_session *s, *tmp;
        HASH_ITER(hh, server->sessions, s, tmp)
        {
            while (limited && memory_used() > conf->max_memory &&
                   session_drop_oldest_queued(s))
                log_warning("Memory over the limit: dropped the oldest "
                            "message queued for %s",
                            s->session_id);
        }
    }

    /*
     * Level three: nothing left to give back. New connections are refused
     * until it drops, which is the only lever left once every reclaimable
     * byte has been reclaimed
     */
    bool still_over = limited && memory_used() > conf->max_memory;
    if (still_over && !server->over_memory)
        log_warning("Memory over the limit (%lu bytes used, %lu allowed): "
                    "refusing new connections until it drops",
                    (unsigned long)memory_used(),
                    (unsigned long)conf->max_memory);
    else if (!still_over && server->over_memory)
        log_info("Memory back under the limit, accepting connections again");
    server->over_memory = still_over;

#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif
}

/*
 * Periodic snapshot of everything the broker must not lose on a restart:
 * the retained messages and the persistent sessions, with their subscriptions
 * and the messages queued for their away clients. The whole snapshot runs
 * under the global mutex, which makes it a consistent point in time (and
 * blocks the publishers for the few milliseconds a file of any teaching size
 * takes to write); `persistence_interval` decides how often it is even
 * considered, and the dirty flag makes sure an idle broker never touches the
 * file at all
 */
static void persistence_check(struct ev_ctx *ctx, void *data)
{
    struct server *server = ctx->data;
    struct config *conf   = server->conf;
    (void)ctx;
    (void)data;

    if (!conf->persistence_file[0])
        return;

    static time_t last_check = 0;
    time_t now               = time(NULL);
    if (now - last_check < (time_t)conf->persistence_interval)
        return;
    last_check = now;

#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    if (persistence_is_dirty())
        persistence_save(server, conf->persistence_file);
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif
}

/*
 * ======================================================
 *  Private functions and callbacks for server behaviour
 * ======================================================
 */

/*
 * All clients are pre-allocated at the start of the server, but their buffers
 * (read and write) are not, they're lazily allocated with this function, meant
 * to be called on the accept callback
 */
static void client_init(struct client *client)
{
    struct config *conf   = client->server->conf;
    client->online        = true;
    client->connected     = false;
    client->clean_session = true;
    client->client_id[0]  = '\0';
    client->username[0]   = '\0';
    client->status        = WAITING_HEADER;
    client->rc            = 0;
    client->rpos          = ATOMIC_VAR_INIT(0);
    client->read          = ATOMIC_VAR_INIT(0);
    client->toread        = ATOMIC_VAR_INIT(0);
    /*
     * The buffers start at INITIAL_BUFFER_SIZE and grow on demand (see
     * recv_reserve() and outgoing_reserve()): an idle connection costs a few
     * KB instead of two full max_request_size blocks, which at the 2MB of the
     * shipped configuration was 4MB per accepted socket. A pool slot reused
     * after a previous connection keeps whatever it grew to, so a busy
     * subscriber does not pay the realloc again
     */
    if (!client->rbuf) {
        client->rbuf_cap = conf->max_request_size < INITIAL_BUFFER_SIZE
                               ? conf->max_request_size
                               : INITIAL_BUFFER_SIZE;
        client->rbuf = try_calloc(client->rbuf_cap, sizeof(unsigned char));
    }
    client->wrote   = ATOMIC_VAR_INIT(0);
    client->towrite = ATOMIC_VAR_INIT(0);
    if (!client->wbuf) {
        client->wbuf_cap = conf->max_request_size < INITIAL_BUFFER_SIZE
                               ? conf->max_request_size
                               : INITIAL_BUFFER_SIZE;
        client->wbuf = try_calloc(client->wbuf_cap, sizeof(unsigned char));
    }
    client->last_seen = time(NULL);
    client->keepalive = 0;
    /*
     * Start with a full bucket so a client may send up to max_publish_rate
     * messages right after connecting instead of being throttled from the
     * first one
     */
    client->rate_tokens = (double)conf->max_publish_rate;
    client->rate_last   = 0.0;
    client->has_lwt   = false;
    client->close_after_write = false;
    client->kick_pending      = false;
    client->session   = NULL;
    pthread_mutex_init(&client->mutex, NULL);
}

/*
 * As we really don't want to completely de-allocate a client in favor of
 * making it reusable by another connection we simply deactivate it according
 * to its state (e.g. if it's a clean_session connected client or not) and we
 * allow the clients memory pool to reclaim it. The caller must already hold
 * the global server mutex (see client_deactivate below)
 */
static bool client_deactivate_locked(struct client *client)
{
    struct server *server = client->server;

    /*
     * Canonical lock order: the global mutex is always taken before the
     * per-client one. publish_message() and inflight_msg_check() acquire the
     * global lock first and a client lock second, so taking them in the
     * opposite order here would create the ABBA cycle that freezes the whole
     * broker when a SUBSCRIBE or a disconnect races with a PUBLISH
     */
#if THREADSNR > 0
    pthread_mutex_lock(&client->mutex);
#endif
    if (client->online == false) {
#if THREADSNR > 0
        pthread_mutex_unlock(&client->mutex);
#endif
        return false;
    }

    client->rpos = client->toread = client->read = 0;
    client->wrote = client->towrite = 0;
    close_connection(&client->conn);

    client->online = false;

    /* The peer address slot taken at accept time is free again */
    ip_conn_release(server, client->conn.ip);

    /*
     * The clients map indexes live connections only: drop the entry no
     * matter the session type. Leaving it behind for a persistent session
     * (as this used to) meant the next CONNECT with the same id inserted a
     * second entry with the same key, and every later lookup (a publish
     * towards that session) could keep hitting the deactivated one:
     * messages then went to the offline queue or were dropped while the
     * reconnected client was sitting right there
     */
    if (client->connected == true)
        HASH_DEL(server->clients_map, client);

    if (client->clean_session == true && client->session) {
        /* The same teardown a CONNECT with clean_session over a persistent
           session performs, and the one the log records as session-del */
        session_delete(server, client->client_id);
        client->session = NULL;
    }
    /*
     * All accesses to the client must happen before the pool can recycle
     * the slot: touching the struct after memorypool_free() would corrupt
     * whatever connection reuses it
     */
    client->connected    = false;
    client->client_id[0] = '\0';
#if THREADSNR > 0
    pthread_mutex_unlock(&client->mutex);
    /*
     * Destroying while the global mutex is still held is safe: every other
     * thread that locks a client mutex does so only after acquiring the
     * global one, so no waiter can exist on this mutex right now
     */
    pthread_mutex_destroy(&client->mutex);
#endif
    /*
     * Return the slot unconditionally: with the clients map entry gone
     * nothing references the struct anymore (a persistent session only
     * points at its own data). The old condition on clean_session leaked
     * one pool slot per persistent client disconnect, and the slot kept
     * its rbuf/wbuf alive with it forever
     */
    memorypool_free(server->pool, client);
    return true;
}

/*
 * Deactivate a connection taking the global mutex first (canonical order),
 * true when a live client was actually released
 */
static bool client_deactivate(struct client *client)
{
    bool released;
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    released = client_deactivate_locked(client);
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif
    return released;
}

/*
 * The one entry point of the close paths (read, write and protocol errors):
 * drops the connection and updates the statistics, unless the client was
 * already released by a racing close. A victim of a duplicate id is only
 * released by its own loop (kick_callback): clearing the flag here is safe
 * because the takeover fired its event on this very loop, and an event
 * cannot survive this descriptor being removed from the loop a moment
 * before, so this side owns the release
 */
static void client_close(struct client *client)
{
    client->kick_pending = false;
    if (client_deactivate(client) == false)
        return;
    // Update stats
    info.active_connections--;
}

/*
 * Runs on the event loop that owns the connection being dropped: removing
 * another thread's descriptor (and freeing its client) could race with the
 * callback that thread is about to run, so a takeover schedules this on the
 * victim's own loop instead, the same cross-thread primitive the publish
 * delivery already uses
 */
static void kick_callback(struct ev_ctx *ctx, void *arg)
{
    struct client *client = arg;
    /* The connection may have dropped on its own before this ran */
    if (client->online == false)
        return;
    log_info("Dropping connection with %s (%s): the client id was taken "
             "over by a newer connection (MQTT v3.1.1 §3.1.4)",
             client->client_id, client->conn.ip);
    ev_del_fd(ctx, client->conn.fd);
    client->kick_pending = false;
    if (client_deactivate(client) == false)
        return; // Already released, the stats were updated there
    // Update stats: the kicked connection was counted on accept
    info.active_connections--;
}

void server_kick_duplicate(const char *client_id, const struct client *keep,
                           struct mqtt_packet *will)
{
    struct server *server = keep->server;
    struct client *old    = NULL;
    HASH_FIND_STR(server->clients_map, client_id, old);
    /*
     * The map holds one entry per live connection: a duplicate key can only
     * be a client that is still connected while a new CONNECT arrives with
     * the same id. The caller must hold the global mutex, the lookup and
     * the detach below rely on it
     */
    if (!old || old == keep)
        return;
    log_info("Kicking out already connected client %s (%s): the id just "
             "reconnected (MQTT v3.1.1 §3.1.4)",
             client_id, old->conn.ip);
    /*
     * Detach the victim from everything shared right here, before the
     * takeover below takes over (or destroys) the session: the global mutex
     * excludes every handler that takes it first, the victim's own one
     * excludes the ACK handlers that touch the session with it alone, so no
     * handler of the old connection can be inside a critical section on the
     * session while it is handed over. Afterwards the old connection holds
     * no session, no will and no map entry: whichever close path runs next
     * finds nothing to dereference, and the struct is released by its own
     * loop (kick_callback)
     */
#if THREADSNR > 0
    pthread_mutex_lock(&old->mutex);
#endif
    /*
     * MQTT v3.1.1 §3.1.2.5: the victim's connection is closed by the server
     * without a DISCONNECT packet, so its will has to be published. The copy
     * is taken here, while the session is still reachable, and handed back to
     * the caller which delivers it once the global mutex is released
     * (publish_lwt() locks that very mutex and would deadlock this thread)
     */
    if (will && old->has_lwt && old->session &&
        old->session->lwt_msg.publish.topic)
        mqtt_packet_copy(will, &old->session->lwt_msg);
    HASH_DEL(server->clients_map, old);
    old->connected = false;
    /*
     * The victim publishes nothing on its own from now on: the will was
     * claimed above and its close path must not deliver a second copy
     */
    old->has_lwt   = false;
    old->session   = NULL;
    /*
     * Claim the release: the close paths of the old connection (and only
     * its own loop thread runs them) must leave the struct alone from now
     * on, this callback owns it
     */
    old->kick_pending = true;
#if THREADSNR > 0
    pthread_mutex_unlock(&old->mutex);
#endif
    /*
     * The descriptor is closed on the loop that owns the connection (the
     * same cross-thread primitive the publish delivery uses): removing it
     * from here could race with the callback that loop is already about to
     * run
     */
    ev_fire_event(old->ctx, old->conn.fd, EV_WRITE, kick_callback,
                  (void *)old);
}

/*
 * Make room for `len` bytes in the read buffer, growing it by doubling up to
 * max_request_size. False when the request does not fit the configured limit,
 * which the caller reports as -ERRMAXREQSIZE exactly like the old fixed-size
 * buffer did
 */
static bool recv_reserve(struct client *c, size_t len)
{
    struct config *conf = c->server->conf;
    if (len > conf->max_request_size)
        return false;
    if (len <= c->rbuf_cap)
        return true;
    size_t cap = c->rbuf_cap ? c->rbuf_cap : INITIAL_BUFFER_SIZE;
    while (cap < len)
        cap *= 2;
    if (cap > conf->max_request_size)
        cap = conf->max_request_size;
    c->rbuf     = try_realloc(c->rbuf, cap);
    c->rbuf_cap = cap;
    return true;
}

/*
 * Parse packet header, it is required at least the Fixed Header of each
 * packed, which is contained in the first 2 bytes in order to read packet
 * type and total length that we need to recv to complete the packet.
 *
 * This function accept a socket fd, a buffer to read incoming streams of
 * bytes and a pointer to the decoded fixed header that will be set in the
 * final parsed packet.
 *
 * - c: A struct client pointer, contains the FD of the requesting client
 *      as well as his SSL context in case of TLS communication. Also it store
 *      the reading buffer to be used for incoming byte-streams, tracking
 *      read, to be read and reading position taking into account the bytes
 *      required to encode the packet length.
 */
static ssize_t recv_packet(struct client *c)
{
    struct config *conf = c->server->conf;
    ssize_t nread   = 0;
    unsigned opcode = 0, pos = 0;
    usize pktlen    = 0;

    /*
     * The fixed header alone needs up to five bytes (type + four Remaining
     * Length bytes), and the whole packet is later checked against
     * max_request_size before its size is used
     */
    if (!recv_reserve(c, MQTT_MAX_LEN_BYTES + 1))
        return -ERRMAXREQSIZE;

    // Base status, we have read 0 to 2 bytes
    if (c->status == WAITING_HEADER) {

        /*
         * Read the first two bytes, the first should contain the message type
         * code
         */
        /*
         * recv_bytes() returns 0 both on EOF and on "EAGAIN with no data",
         * telling them apart only through errno. recv() returning 0 (EOF)
         * does NOT set errno, so a stale EAGAIN from an earlier partial
         * read would mislabel a disconnect as EAGAIN and spin the event
         * loop forever: clear it so the checks below see only what this
         * call did
         */
        errno           = 0;
        nread = recv_data(&c->conn, c->rbuf + c->read, 2 - c->read);

        if (errno != EAGAIN && errno != EWOULDBLOCK && nread <= 0)
            return nread == -1 ? -ERRSOCKETERR : -ERRCLIENTDC;

        c->read += nread;

        if (errno == EAGAIN && c->read < 2)
            return -ERREAGAIN;

        c->status = WAITING_LENGTH;
    }

    /*
     * We have already read the packet HEADER, thus we know what packet we're
     * dealing with, we're between bytes 2-4, as after the 1st byte, the
     * remaining 3 can be all used to store the packet length, or, in case of
     * ACK type packet or PINGREQ/PINGRESP and DISCONNECT, the entire packet
     */
    if (c->status == WAITING_LENGTH) {

        if (c->read == 2) {
            opcode = *c->rbuf >> 4;

            /*
             * Check for OPCODE, if an unknown OPCODE is received return an
             * error
             */
            if (DISCONNECT < opcode || CONNECT > opcode)
                return -ERRPACKETERR;

            /*
             * We have a PINGRESP/PINGREQ or a DISCONNECT packet, we're done
             * here
             */
            if (opcode > UNSUBSCRIBE) {
                /*
                 * MQTT v3.1.1 §3.12.1, §3.13.1 and §3.14.1: those packets are
                 * the fixed header alone, their Remaining Length is 0 by
                 * definition. A byte there used to be ignored while the
                 * payload it declared stayed in the stream, so the connection
                 * desynchronized and those bytes were parsed as the header of
                 * the next packet. Refuse the packet instead
                 */
                if (c->rbuf[1] != 0x00)
                    return -ERRPACKETERR;
                c->rpos   = 2;
                c->toread = c->read;
                goto exit;
            }
        }

        /*
         * Fill up to the 4 bytes the Remaining Length field may occupy on
         * top of the header byte, how many are really needed is only known
         * once the continuation bits are decoded
         */
        if (c->read < 4) {
            errno   = 0;
            nread = recv_data(&c->conn, c->rbuf + c->read, 4 - c->read);

            if (errno != EAGAIN && errno != EWOULDBLOCK && nread <= 0)
                return nread == -1 ? -ERRSOCKETERR : -ERRCLIENTDC;

            c->read += nread;

            if (errno == EAGAIN && c->read < 4)
                return -ERREAGAIN;
        }

        /*
         * Decode the Remaining Length from the bytes actually received so
         * far: decoding past them would take stale buffer contents for a
         * length. When the field continues beyond them (and is still within
         * the 4 bytes allowed by MQTT v3.1.1 §2.2.3) fetch the missing
         * bytes before decoding again, a 5th byte means malformed
         */
        for (;;) {
            pktlen = mqtt_decode_length(c->rbuf + 1, &pos,
                                        (unsigned)(c->read - 1));
            if (pktlen != (usize)-1)
                break;
            if (pos >= MQTT_MAX_LEN_BYTES)
                return -ERRPACKETERR;
            errno   = 0;
            nread = recv_data(&c->conn, c->rbuf + c->read, 5 - c->read);
            if (nread <= 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return -ERREAGAIN;
                return nread == -1 ? -ERRSOCKETERR : -ERRCLIENTDC;
            }
            c->read += nread;
        }

        /*
         * Set return code to -ERRMAXREQSIZE in case the total packet len
         * exceeds the configuration limit `max_request_size`: the check is
         * made on the whole packet (fixed header included) because the read
         * buffer is allocated exactly that big and `toread` below would
         * push the read a few bytes past its end otherwise
         */
        if (pktlen + pos + 1 > conf->max_request_size)
            return -ERRMAXREQSIZE;

        /*
         * Update the toread field for the client with the entire length of
         * the current packet, which is comprehensive of packet length,
         * bytes used to encode it and 1 byte for the header
         * We've already tracked the bytes we read so far, we just need to
         * read toread-read bytes.
         */
        c->rpos   = pos + 1;
        c->toread = pktlen + pos + 1; // pos = bytes used to store length
        /* The whole packet has to fit the read buffer at once */
        if (!recv_reserve(c, c->toread))
            return -ERRMAXREQSIZE;

        /*
         * Looks like we got a packet of 4 bytes or less (ACKs, PINGREQ,
         * DISCONNECT), we're done reading: the test is made on the total
         * packet size and not on the remaining length, as a 5 bytes packet
         * (e.g. a PUBLISH on a 1 char topic) still needs one more byte
         */
        if (c->toread <= 4)
            goto exit;

        c->status = WAITING_DATA;
    }

    /*
     * Last status, we have access to the length of the packet and we know for
     * sure that it's not a PINGREQ/PINGRESP/DISCONNECT packet.
     */
    errno   = 0;
    nread = recv_data(&c->conn, c->rbuf + c->read, c->toread - c->read);

    if (errno != EAGAIN && errno != EWOULDBLOCK && nread <= 0)
        return nread == -1 ? -ERRSOCKETERR : -ERRCLIENTDC;

    c->read += nread;
    if (errno == EAGAIN && c->read < c->toread)
        return -ERREAGAIN;

exit:

    return SOL_OK;
}

/*
 * Handle incoming requests, after being accepted or after a reply, under the
 * hood it calls recv_packet and return an error code according to the outcome
 * of the operation
 */
static inline int read_data(struct client *c)
{

    /*
     * We must read all incoming bytes till an entire packet is received. This
     * is achieved by following the MQTT protocol specifications, which
     * send the size of the remaining packet as the second byte. By knowing it
     * we know if the packet is ready to be deserialized and used.
     */
    int err = recv_packet(c);

    /*
     * Looks like we got a client disconnection or If a not correct packet
     * received, we must free the buffer and reset the handler to the request
     * again, setting EPOLL to EPOLLIN
     *
     * TODO: Set a error_handler for ERRMAXREQSIZE instead of dropping client
     *       connection, explicitly returning an informative error code to the
     *       client connected.
     */
    if (err < 0)
        goto err;

    if (c->read < c->toread)
        return -ERREAGAIN;

    info.bytes_recv += c->read;

    return SOL_OK;

    // Disconnect packet received

err:

    return err;
}

/*
 * Write stream of bytes to a client represented by a connection object, till
 * all bytes to be written is exhausted, tracked by towrite field or if an
 * EAGAIN (socket descriptor must be in non-blocking mode) error is raised,
 * meaning we cannot write anymore for the current cycle.
 */
static inline int write_data(struct client *c)
{
#if THREADSNR > 0
    pthread_mutex_lock(&c->mutex);
#endif
    ssize_t wrote =
        send_data(&c->conn, c->wbuf + c->wrote, c->towrite - c->wrote);
    if (errno != EAGAIN && errno != EWOULDBLOCK && wrote < 0)
        goto clientdc;
    c->wrote += wrote > 0 ? wrote : 0;
    if (c->wrote < c->towrite && errno == EAGAIN)
        goto eagain;
    // Update information stats
    info.bytes_sent += c->towrite;
    // Reset client written bytes track fields
    c->towrite = c->wrote = 0;
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    return SOL_OK;

clientdc:
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    return -ERRSOCKETERR;

eagain:
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    return -ERREAGAIN;
}

/*
 * ===========
 *  Callbacks
 * ===========
 */

/*
 * Callback dedicated to client replies, try to send as much data as possible
 * epmtying the client buffer and rearming the socket descriptor for reading
 * after
 */
static void write_callback(struct ev_ctx *ctx, void *arg)
{
    struct client *client = arg;
    int err               = write_data(client);
    switch (err) {
    case SOL_OK:
        /*
         * A refused connection (CONNACK with a non-zero return code) is
         * flushed as soon as it is written: the read side is never rearmed
         * for it
         */
        if (client->close_after_write) {
            log_info("Closing connection with %s (%s): refused (%i)",
                     client->client_id, client->conn.ip, client->rc);
            ev_del_fd(ctx, client->conn.fd);
            client_close(client);
            break;
        }
        /*
         * Rearm descriptor making it ready to receive input,
         * read_callback will be the callback to be used; also reset the
         * read buffer status for the client.
         */
        client->status = WAITING_HEADER;
        ev_fire_event(ctx, client->conn.fd, EV_READ, read_callback, client);
        break;
    case -ERREAGAIN:
        /*
         * We have an EAGAIN error, which is really just signaling that
         * for some reasons the kernel is not ready to write more bytes at
         * the moment and it would block, so we just want to re-try some
         * time later, re-enqueuing a new write event
         */
        enqueue_event_write(client);
        break;
    default:
        log_info("Closing connection with %s (%s): %s %i", client->client_id,
                 client->conn.ip, solerr(client->rc), err);
        ev_del_fd(ctx, client->conn.fd);
        client_close(client);
        break;
    }
}

/*
 * Handle incoming connections, create a a fresh new struct client structure
 * and link it to the fd, ready to be set in EV_READ event, then schedule a
 * call to the read_callback to handle incoming streams of bytes
 */
static void accept_callback(struct ev_ctx *ctx, void *data)
{
    struct server *server = ctx->data;
    struct config *conf   = server->conf;
    int serverfd          = *((int *)data);
    while (1) {

        /*
         * Accept a new incoming connection assigning ip address
         * and socket descriptor to the connection structure
         * pointer passed as argument
         */
        struct connection conn;
        connection_init(&conn, conf->tls ? server->ssl_ctx : NULL,
                        conf->socket_family);
        int fd = accept_connection(&conn, serverfd);
        /*
         * A negative descriptor is a failed accept (the TLS handshake could
         * have been rejected): leave it to close_connection() above, wait
         * for the next EPOLLIN. Note that 0 is a perfectly valid descriptor,
         * it is only seen when stdin has been closed, and must not be
         * mistaken for a failure
         */
        if (fd < 0) {
            close_connection(&conn);
            /*
             * The listening socket is level triggered: when the descriptor
             * table is exhausted (EMFILE/ENFILE) every accept fails and
             * epoll reports the socket ready at once, over and over, spinning
             * the loop at full CPU until the backlog drains. Yield here so a
             * transient fd shortage cannot monopolize the thread; a plain
             * EAGAIN just ends the loop as usual
             */
            if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS ||
                errno == ENOMEM)
                usleep(1000);
            break;
        }

        /*
         * Create a client structure to handle his context
         * connection
         */
        /*
         * The memory policy refuses new connections while the broker is over
         * its limit and has nothing left to reclaim (see memory_check). The
         * flag is atomic and read before taking the global mutex: the refusal
         * path leaves through a `break`, and breaking with the lock held
         * would abandon it for good. The listening socket is level triggered,
         * so a small nap keeps the loop from spinning on a refusal
         */
        if (server->over_memory) {
            log_debug("Refusing connection from %s: memory over the limit",
                      conn.ip);
            close_connection(&conn);
            usleep(1000);
            break;
        }

#if THREADSNR > 0
        pthread_mutex_lock(&mutex);
#endif
        struct client *c = memorypool_alloc(server->pool);
        /*
         * Take the peer address slot in the same critical section: the
         * table is global, and a client that fails the per IP quota must
         * give its pool slot back before the lock is released
         */
        bool ip_ok = c != NULL && ip_conn_acquire(server, conn.ip);
        if (c && !ip_ok)
            memorypool_free(server->pool, c);
#if THREADSNR > 0
        pthread_mutex_unlock(&mutex);
#endif
        /*
         * No free slot left in the pool: refuse the connection rather than
         * dereference a NULL client
         */
        if (!c) {
            log_error("No free client slot for %s, connection refused",
                      conn.ip);
            close_connection(&conn);
            break;
        }
        if (!ip_ok) {
            log_warning("Refusing connection from %s: max_connections_per_ip "
                        "(%u) reached",
                        conn.ip, conf->max_connections_per_ip);
            close_connection(&conn);
            break;
        }
        c->conn   = conn;
        c->server = server;
        client_init(c);
        c->ctx = ctx;

        /*
         * Count the connection before arming it: client_close() below (and
         * the error paths it serves) always decrements active_connections,
         * so a failure to register the descriptor has to find the counter
         * already incremented or the statistics underflow instead
         */
        info.active_connections++;
        info.total_connections++;

        /* Add it to the epoll loop */
        if (ev_register_event(ctx, fd, EV_READ, read_callback, c) < 0) {
            /*
             * The descriptor could not be armed (an epoll_ctl failure, or a
             * select build rejecting a fd beyond FD_SETSIZE): close both the
             * client and the socket, or the connection would hang forever
             * with no event registered against it
             */
            log_error("Cannot register fd %d with the event loop", fd);
            ev_del_fd(ctx, fd);
            client_close(c);
            break;
        }

        log_info("[%p] Connection from %s", (void *)pthread_self(), conn.ip);
    }
}

/*
 * Reading packet callback, it's the main function that will be called every
 * time a connected client has some data to be read, notified by the eventloop
 * context.
 */
static void read_callback(struct ev_ctx *ctx, void *data)
{
    struct client *c = data;
    if (c->status == SENDING_DATA)
        return;
    /*
     * Received a bunch of data from a client, after the creation
     * of an IO event we need to read the bytes and encoding the
     * content according to the protocol
     */
    int rc = read_data(c);
    switch (rc) {
    case SOL_OK:
        /*
         * All is ok, raise an event to the worker poll EPOLL and
         * link it with the IO event containing the decode payload
         * ready to be processed
         */
        /* Record last action as of now */
        c->last_seen = time(NULL);
        c->status    = SENDING_DATA;
        process_message(ctx, c);
        break;
    case -ERRCLIENTDC:
    case -ERRSOCKETERR:
    case -ERRPACKETERR:
    case -ERRMAXREQSIZE:
        // TODO move to default branch
        /*
         * We got an unexpected error or a disconnection from the
         * client side: deliver the will and release the connection,
         * the LWT/close sequence is shared with the keepalive timeout
         */
        client_shutdown(ctx, c, rc);
        break;
    case -ERREAGAIN:
        /*
         * We have an EAGAIN error, which is really just signaling that
         * for some reasons the kernel is not ready to read more bytes at
         * the moment and it would block, so we just want to re-try some
         * time later, re-enqueuing a new read event
         */
        ev_fire_event(ctx, c->conn.fd, EV_READ, read_callback, c);
        break;
    }
}

/*
 * This function is called only if the client has sent a full stream of bytes
 * consisting of a complete packet as expected by the MQTT protocol and by the
 * declared length of the packet.
 * It uses eventloop APIs to react accordingly to the packet type received,
 * validating it before proceed to call handlers. Depending on the handler
 * called and its outcome, it'll enqueue an event to write a reply or just
 * reset the client state to allow reading some more packets.
 */
static void process_message(struct ev_ctx *ctx, struct client *c)
{
    struct server *server = c->server;
    struct io_event io    = {.client = c, .server = server};
    /*
     * Unpack received bytes into a mqtt_packet structure and execute the
     * correct handler based on the type of the operation.
     */
    int rc = mqtt_unpack(c->rbuf + c->rpos, &io.data, *c->rbuf,
                         c->read - c->rpos);
    c->toread = c->read = c->rpos = 0;
    if (rc == MQTT_UNACCEPTABLE_PROTOCOL_VERSION ||
        rc == MQTT_IDENTIFIER_REJECTED) {
        /*
         * MQTT v3.1.1 §3.1.2.1 and §3.1.3.1: a CONNECT with a wrong protocol
         * name/level or an unacceptable client id must be answered with a
         * CONNACK carrying the return code, and §3.2.2.3 mandates closing
         * the network connection right after: the flag makes write_callback
         * drop the connection as soon as the CONNACK below has been flushed
         */
        log_error("Rejecting CONNECT from %s: return code %i",
                  c->client_id[0] ? c->client_id : "unknown", rc);
        struct mqtt_packet connack = {
            .header = (union mqtt_header){.byte = CONNACK_B}};
        mqtt_connack(&connack, 0, (u8)rc);
#if THREADSNR > 0
        pthread_mutex_lock(&c->mutex);
#endif
        /* Never let a refusal overflow the write buffer */
        if (outgoing_reserve(c, MQTT_ACK_LEN)) {
            mqtt_pack(&connack, c->wbuf + c->towrite);
            c->towrite += MQTT_ACK_LEN;
        }
        c->close_after_write = true;
        c->rc                = rc;
#if THREADSNR > 0
        pthread_mutex_unlock(&c->mutex);
#endif
        enqueue_event_write(c);
        if (io.data.header.bits.type != PUBLISH)
            mqtt_packet_destroy(&io.data);
        return;
    }
    if (rc != MQTT_OK) {
        /*
         * The packet is malformed or it's a type a client must never send
         * (CONNACK, SUBACK, UNSUBACK, PINGRESP or a reserved one): there's
         * nothing decoded beside the fixed header, so no handler can be run
         * safely, report it as a protocol error and let the switch below
         * close the connection. mqtt_unpack() has already released whatever
         * a failed CONNECT had allocated
         */
        log_error("Protocol error: undecodable packet type %u sent by %s",
                  io.data.header.bits.type, c->client_id);
        c->rc = -ERRPACKETERR;
    } else {
        c->rc = handle_command(io.data.header.bits.type, &io);
    }
    switch (c->rc) {
    case REPLY:
    case MQTT_NOT_AUTHORIZED:
    case MQTT_BAD_USERNAME_OR_PASSWORD:
    case MQTT_IDENTIFIER_REJECTED:
        /*
         * Write out to client, after a request has been processed in
         * worker thread routine. Just send out all bytes stored in the
         * reply buffer to the reply file descriptor.
         */
        enqueue_event_write(c);
        /* Free resource, ACKs will be free'd closing the server */
        if (io.data.header.bits.type != PUBLISH)
            mqtt_packet_destroy(&io.data);
        break;
    case -ERRCLIENTDC:
        /*
         * The client sent a DISCONNECT: the decoded packet is deliberately
         * released here, a handler that bails out early (a refused CONNECT)
         * leaves behind the credentials or tuples that mqtt_unpack()
         * allocated, and dropping the connection without this call leaked
         * all of it on every refused packet. A PUBLISH that failed to
         * decode already released its own buffers, so destroying it here is
         * a no-op. No will is delivered: §3.14.4 makes a DISCONNECT a clean
         * close
         */
        mqtt_packet_destroy(&io.data);
        ev_del_fd(ctx, c->conn.fd);
        client_close(io.client);
        break;
    case -ERRPACKETERR:
        /*
         * The client broke the protocol (a second CONNECT, a packet sent
         * before CONNECT, a type only the server may send). Same release as
         * above, but the connection goes through client_shutdown(): §3.1.2.5
         * wants the will published whenever the network connection is closed
         * without a DISCONNECT, and this is such a close
         */
        mqtt_packet_destroy(&io.data);
        client_shutdown(ctx, c, c->rc);
        break;
    case -ERRNOMEM:
        log_error("%s", solerr(c->rc));
        /*
         * The client state is left as it was: reset it to the reading state
         * and keep the connection alive, otherwise the client would hang
         * forever in SENDING_DATA with the EPOLLIN interest still armed
         */
        c->status = WAITING_HEADER;
        break;
    default:
        c->status = WAITING_HEADER;
        if (io.data.header.bits.type != PUBLISH)
            mqtt_packet_destroy(&io.data);
        break;
    }
}

/*
 * Eventloop stop callback, will be triggered by an EV_CLOSEFD event and stop
 * the running loop, unblocking the call.
 */
static void stop_handler(struct ev_ctx *ctx, void *arg)
{
    (void)arg;
    ev_stop(ctx);
}

/*
 * IO worker function, wait for events on a dedicated epoll descriptor owned
 * by this thread, following the normal EPOLL semantic, EPOLLIN for incoming
 * bytes to be unpacked and processed by a worker thread, EPOLLOUT for bytes
 * incoming from a worker thread, ready to be delivered out.
 */
static void eventloop_start(void *args)
{
    struct listen_payload *loop_data = args;
    struct server *server            = loop_data->server;
    struct config *conf              = server->conf;
    struct ev_ctx *ctx               = try_alloc(sizeof(*ctx));
    int sfd                          = loop_data->fd;
    loops[loop_data->index]          = ctx;
    ev_init(ctx, EVENTLOOP_MAX_EVENTS);
    ctx->data = server;
    /*
     * Register the shared run descriptor: on Linux it is the eventfd, on
     * other platforms the *read* end of the pipe (index 0). The write end
     * (index 1) is what the signal handler writes to, and registering it
     * here meant the loops waited for a write end to become readable, so
     * the shutdown event never arrived and the broker had to be killed
     */
#ifdef __linux__
    ev_register_event(ctx, conf->run, EV_CLOSEFD | EV_READ, stop_handler,
                      NULL);
#else
    ev_register_event(ctx, conf->run[0], EV_CLOSEFD | EV_READ, stop_handler,
                      NULL);
#endif
    // Register listening FD with accept callback
    ev_register_event(ctx, sfd, EV_READ, accept_callback, &sfd);
    /*
     * Periodic tasks run on one loop only, the one start_server() starts on
     * its own thread: every thread has its own payload now, so a worker can
     * no longer catch a flag the main thread set after starting it
     */
    if (loop_data->cronjobs == true) {
        ev_register_cron(ctx, publish_stats, NULL, conf->stats_pub_interval,
                         0);
        ev_register_cron(ctx, inflight_msg_check, NULL, 1, 0);
        ev_register_cron(ctx, keepalive_check, NULL, 1, 0);
        ev_register_cron(ctx, persistence_check, NULL, 1, 0);
        ev_register_cron(ctx, memory_check, NULL, 1, 0);
    }
    // Start the loop, blocking call
    ev_run(ctx);
    /*
     * The context is deliberately *not* destroyed here: another loop may
     * still be about to fire an event at it (a delivery to one of its
     * clients, a takeover), and walking a freed array is exactly the
     * use-after-free the sanitizer found at shutdown. start_server() disposes
     * of every context once all the loops have returned
     */
}

/*
 * ===================
 *  Main APIs exposed
 * ===================
 */

/*
 * Fire a write callback to reply after a client request, under the hood it
 * schedules an EV_WRITE event with a client pointer set to write carried
 * contents out on the socket descriptor.
 */
void enqueue_event_write(const struct client *c)
{
    ev_fire_event(c->ctx, c->conn.fd, EV_WRITE, write_callback, (void *)c);
}

/*
 * Main entry point for the server, to be called with an address and a port
 * to start listening. The function may fail only in the case of Out of memory
 * error occurs or listen call fails, on the other cases it should just log
 * unexpected errors.
 */
int start_server(struct server *server, const char *addr, const char *port)
{
    struct config *conf = server->conf;

    INIT_INFO;

    /* Initialize global Sol instance */
    server->store = topic_store_new();
    server->auths = NULL;
    server->pool  = memorypool_new(BASE_CLIENTS_NUM, sizeof(struct client));
    if (!server->pool)
        log_fatal("Failed to allocate %d sized memory pool for clients",
                  BASE_CLIENTS_NUM);
    log_info("Client pool: %d blocks of %zu bytes, grown on demand",
             BASE_CLIENTS_NUM, sizeof(struct client));
    server->clients_map = NULL;
    server->sessions    = NULL;
    server->over_memory = false;
    pthread_mutex_init(&mutex, NULL);

    if (conf->allow_anonymous == false)
        if (!config_read_passwd_file(conf->password_file, &server->auths))
            log_error("Failed to read password file");

    /* Generate stats topics */
    for (int i = 0; i < SYS_TOPICS; i++) {
        struct topic *t = topic_new(try_strdup(sys_topics[i].name));
        if (!t)
            log_fatal("start_server failed: Out of memory");
        topic_store_put(server->store, t);
    }

    /*
     * Topic level authorization: an access control list is read before the
     * listening socket exists, so no client can be served before the rules
     * are in place
     */
    acl_load(conf->acl_file);

    /*
     * Rebuild what a previous run left behind: the retained messages and the
     * persistent sessions. This happens before the listening socket exists,
     * so no client can race it, and persistence_load() takes no lock for the
     * same reason
     */
    persistence_load(server, conf->persistence_file);

    /*
     * A limit below what the broker already holds would refuse every new
     * connection from the first second, which looks like a hang from the
     * outside: warn about it rather than let it surprise anybody
     */
    if (conf->max_memory > 0 && conf->max_memory <= memory_used())
        log_warning("max_memory (%zu bytes) is below the memory the broker "
                    "already uses (%zu bytes): raise it, or new connections "
                    "will be refused",
                    conf->max_memory, memory_used());

    /* Start listening for new connections */
    int sfd = make_listen(addr, port, conf->socket_family, conf);

    /* Setup SSL in case of flag true */
    if (conf->tls == true) {
        openssl_init();
        server->ssl_ctx = create_ssl_context(conf);
        load_certificates(server->ssl_ctx, conf->cafile, conf->certfile,
                          conf->keyfile, conf->require_client_cert);
    }

    log_info("Server start");
    info.start_time                  = time(NULL);

    struct listen_payload main_loop = {.fd      = sfd,
                                       .cronjobs = true,
                                       .server   = server,
                                       .index    = THREADSNR};

#if THREADSNR > 0
    pthread_t thrs[THREADSNR];
    /* One payload per thread, alive until the joins below */
    struct listen_payload worker_loops[THREADSNR];
    for (int i = 0; i < THREADSNR; ++i) {
        worker_loops[i] = (struct listen_payload){.fd      = sfd,
                                                  .cronjobs = false,
                                                  .server   = server,
                                                  .index    = i};
        pthread_create(&thrs[i], NULL, (void *(*)(void *)) & eventloop_start,
                       &worker_loops[i]);
    }
#endif
    // start eventloop, could be spread on multiple threads
    eventloop_start(&main_loop);

#if THREADSNR > 0
    for (int i = 0; i < THREADSNR; ++i)
        pthread_join(thrs[i], NULL);
#endif

    /*
     * Every loop has returned by now, so no other thread can reach these
     * contexts anymore: this is the point where they can be released
     */
    for (int i = 0; i <= THREADSNR; ++i) {
        if (!loops[i])
            continue;
        ev_destroy(loops[i]);
        free_memory(loops[i]);
        loops[i] = NULL;
    }

    /*
     * Final snapshot, catching whatever the last periodic run did not: every
     * loop has stopped by now, so there is no lock left to take
     */
    if (conf->persistence_file[0] && persistence_is_dirty())
        persistence_save(server, conf->persistence_file);

    close(sfd);
    AUTH_DESTROY(server->auths);
    acl_destroy();
    topic_store_destroy(server->store);

    /* Destroy SSL context, if any present */
    if (conf->tls == true) {
        SSL_CTX_free(server->ssl_ctx);
        openssl_cleanup();
    }
    pthread_mutex_destroy(&mutex);

    log_info("Sol v%s exiting", VERSION);

    return SOL_OK;
}

/*
 * Make the entire process a daemon running in background
 */
void daemonize(void)
{

    int fd;

    if (fork() != 0)
        exit(0);

    setsid();

    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO)
            close(fd);
    }
}
