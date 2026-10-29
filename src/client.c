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

/* Socket callbacks and the lifecycle of an accepted client connection. */
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


static void client_init(struct client *);
static bool client_deactivate(struct client *);
static void client_close(struct client *);
static const char *solerr(int rc);
static void read_callback(struct ev_ctx *, void *);
static void process_message(struct ev_ctx *, struct client *);

/*
 * Deliver the will (if the CONNECT carried one) and drop the connection.
 * Shared by the read error path and the keepalive timeout below: both close
 * a connection for the same protocol reason, "the client is gone and never
 * sent a DISCONNECT", and MQTT v3.1.1 §3.1.4 requires the Will Message to
 * be published in exactly those cases. The caller must already run on the
 * loop that owns `c` and must not hold any lock: publish_lwt() takes the
 * global one itself
 */
void client_shutdown(struct ev_ctx *ctx, struct client *c, int rc)
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

    /* recv_packet() buffers a full MQTT packet using its variable-length
     * Remaining Length field before the parser is called.
     */
    int err = recv_packet(c);

    /* A disconnect, malformed packet, or oversized packet is handled by the
     * caller; incomplete packets return EAGAIN and stay buffered.
     */
    if (err < 0)
        goto err;

    if (c->read < c->toread)
        return -ERREAGAIN;

    info.bytes_recv += c->read;

    return SOL_OK;

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
void client_accept_callback(struct ev_ctx *ctx, void *data)
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
    /* Read until a full packet is buffered, then dispatch it for processing. */
    int rc = read_data(c);
    switch (rc) {
    case SOL_OK:
        /* Refresh keepalive state before processing the packet. */
        c->last_seen = time(NULL);
        c->status    = SENDING_DATA;
        process_message(ctx, c);
        break;
    case -ERRCLIENTDC:
    case -ERRSOCKETERR:
    case -ERRPACKETERR:
    case -ERRMAXREQSIZE:
        /*
         * Close on I/O, malformed-packet or size-limit errors. The protocol
         * has no response code for an oversized packet; client_shutdown()
         * handles the will and connection cleanup.
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
 * Fire a write callback to reply after a client request, under the hood it
 * schedules an EV_WRITE event with a client pointer set to write carried
 * contents out on the socket descriptor.
 */
void enqueue_event_write(const struct client *c)
{
    ev_fire_event(c->ctx, c->conn.fd, EV_WRITE, write_callback, (void *)c);
}
