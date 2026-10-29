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

/* Periodic broker jobs: keepalive, retries, stats, persistence, and memory. */
#include "server_internal.h"
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

#include <time.h>

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

static const double SOL_SECONDS = 88775.24;

void cron_topics_init(struct server *server)
{
    for (int i = 0; i < SYS_TOPICS; i++) {
        struct topic *t = topic_new(try_strdup(sys_topics[i].name));
        if (!t)
            log_fatal("start_server failed: Out of memory");
        topic_store_put(server->store, t);
    }
}

/*
 * ====================================================
 *  Cron tasks, to be repeated at fixed time intervals
 * ====================================================
 */

/*
 * Publish statistics once per configured interval on predefined $SOL topics.
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

void cron_publish_stats(struct ev_ctx *ctx, void *data)
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
void cron_inflight_msg_check(struct ev_ctx *ctx, void *data)
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
void cron_keepalive_check(struct ev_ctx *ctx, void *data)
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
void cron_memory_check(struct ev_ctx *ctx, void *data)
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
void cron_persistence_check(struct ev_ctx *ctx, void *data)
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
