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

#include "handlers.h"
#include "acl.h"
#include "config.h"
#include "logging.h"
#include "memory.h"
#include "mqtt.h"
#include "persistence.h"
#include "server.h"
#include "sol_internal.h"
#include <stdio.h>

/* Prototype for a command handler */
typedef int handler(struct io_event *);

/* Command handler, each one have responsibility over a defined command packet
 */
static int connect_handler(struct io_event *);
static int disconnect_handler(struct io_event *);
static int subscribe_handler(struct io_event *);
static int unsubscribe_handler(struct io_event *);
static int publish_handler(struct io_event *);
static int puback_handler(struct io_event *);
static int pubrec_handler(struct io_event *);
static int pubrel_handler(struct io_event *);
static int pubcomp_handler(struct io_event *);
static int pingreq_handler(struct io_event *);

/*
 * Rejects the packet types a client must never send to the broker, i.e.
 * CONNACK, SUBACK, UNSUBACK, PINGRESP and the reserved ones: they have no
 * handler of their own and used to be NULL slots, dereferenced blindly by
 * handle_command.
 */
static int protocol_violation_handler(struct io_event *);

static void session_init(struct client_session *, const char *);

/* Keep the session list of the topics it holds a subscriber entry on in sync */
static void session_track_topic(struct client_session *, struct topic *);
static void session_untrack_topic(struct client_session *, struct topic *);

/* Drop the first nodes of a list without touching the data they point to */
static void list_drop_prefix(List *, unsigned long);

static struct client_session *client_session_alloc(const char *);

static unsigned short next_free_mid(struct client_session *);

static void inflight_msg_init(struct inflight_msg *, struct mqtt_packet *);

/*
 * Reserve `len` bytes at the end of the client's write buffer, returns true
 * when the packet fits. Every reply is packed straight into that buffer, of
 * exactly conf->max_request_size bytes: the periodic retransmission scan can
 * fill it close to the brim while the client keeps sending commands, and an
 * unguarded pack would then write past the end of the heap block. Callers
 * must skip the reply when this returns false (a dropped reply is recovered
 * by the sender retransmitting, an overflow is not)
 */
static bool outgoing_reserve(struct client *c, size_t len)
{
    if (c->towrite + len <= conf->max_request_size)
        return true;
    log_error("Cannot send reply to %s: write buffer full (towrite = %zu, "
              "packet = %zu bytes)",
              c->client_id, c->towrite, len);
    return false;
}

/*
 * Duplicate `len` bytes into a fresh NUL terminated buffer. The MQTT strings
 * this is used on (the will topic and message) are binary: an application
 * message may carry any byte, NUL included, so the copy is driven by the
 * decoded length and never by strlen(), which would truncate it
 */
static unsigned char *dup_bytes(const unsigned char *src, size_t len)
{
    unsigned char *dst = try_alloc(len + 1);
    if (src && len > 0)
        memcpy(dst, src, len);
    dst[len] = '\0';
    return dst;
}

/*
 * Deep copy of a PUBLISH packet: the copy owns its own topic and payload
 * buffers and comes with a single reference held by the caller (DECREF
 * releases it). The retained message of a topic needs exactly this, a packet
 * that outlives the one travelling to the current subscribers, so that every
 * later subscription can be served from it
 */
static struct mqtt_packet *mqtt_publish_copy(const struct mqtt_packet *src)
{
    struct mqtt_packet *copy = mqtt_packet_alloc(src->header.byte);

    mqtt_packet_copy(copy, src);
    /* The caller owns the copy from here on */
    INCREF(copy, struct mqtt_packet);
    return copy;
}

/*
 * Command handler mapped usign their position paired with their type, one
 * slot for each of the 16 possible values of the 4 bits packet type, so that
 * an unexpected type can never index past the end of the table. Slots that
 * don't hold a real handler point to protocol_violation_handler instead of
 * NULL.
 */
static handler *handlers[16] = {
    protocol_violation_handler, /* 0:  reserved */
    connect_handler,            /* 1:  CONNECT    (client to server) */
    protocol_violation_handler, /* 2:  CONNACK    (server to client) */
    publish_handler,            /* 3:  PUBLISH    (client to server) */
    puback_handler,             /* 4:  PUBACK     (client to server) */
    pubrec_handler,             /* 5:  PUBREC     (client to server) */
    pubrel_handler,             /* 6:  PUBREL     (client to server) */
    pubcomp_handler,            /* 7:  PUBCOMP    (client to server) */
    subscribe_handler,          /* 8:  SUBSCRIBE  (client to server) */
    protocol_violation_handler, /* 9:  SUBACK     (server to client) */
    unsubscribe_handler,        /* 10: UNSUBSCRIBE(client to server) */
    protocol_violation_handler, /* 11: UNSUBACK   (server to client) */
    pingreq_handler,            /* 12: PINGREQ    (client to server) */
    protocol_violation_handler, /* 13: PINGRESP   (server to client) */
    disconnect_handler,         /* 14: DISCONNECT (client to server) */
    protocol_violation_handler  /* 15: reserved */
};

/*
 * =========================
 *  Internal module helpers
 * =========================
 */

static void session_free(const struct ref *refcount)
{
    struct client_session *session =
        container_of(refcount, struct client_session, refcount);
    list_destroy(session->subscriptions, 0);
    /*
     * Deep: the queue nodes hold struct queued_msg wrappers allocated here,
     * while the packets themselves are released by the loop below (their
     * reference belongs to the inflight entries, not to the queue)
     */
    list_destroy(session->outgoing_msgs, 1);
    /*
     * Release the packets still awaiting an acknowledgement: the pending
     * list mirrors the i_msgs table, so there is no need to walk all 65535
     * slots of it
     */
    for (unsigned i = 0; i < session->pending_len; ++i) {
        struct inflight_msg *imsg = &session->i_msgs[session->pending_mids[i]];
        if (imsg->packet) {
            DECREF(imsg->packet, struct mqtt_packet);
            imsg->packet = NULL;
        }
    }
    free_memory(session->pending_mids);
    free_memory(session->q2_recv_mids);
    free_memory(session->i_acks);
    free_memory(session->i_msgs);
    /*
     * Release the strings of the will (delivered or not): the packet is
     * embedded in the session so only its members are heap owned
     */
    mqtt_packet_destroy(&session->lwt_msg);
    free_memory(session);
    /* A session that goes away is a record the file must not keep */
    persistence_mark_dirty();
}

static void session_init(struct client_session *session, const char *session_id)
{
    session->inflights     = ATOMIC_VAR_INIT(0);
    session->next_free_mid = 1;
    session->subscriptions = list_new(NULL);
    session->outgoing_msgs = list_new(NULL);
    snprintf(session->session_id, MQTT_CLIENT_ID_LEN, "%s", session_id);
    session->i_acks = try_calloc(MAX_INFLIGHT_MSGS, sizeof(time_t));
    session->i_msgs =
        try_calloc(MAX_INFLIGHT_MSGS, sizeof(struct inflight_msg));
    session->pending_mids = NULL;
    session->pending_len  = 0;
    session->pending_cap  = 0;
    session->q2_recv_mids = NULL;
    session->q2_recv_len  = 0;
    session->q2_recv_cap  = 0;
    /* Well defined (all zeros) until a CONNECT carrying a will fills it */
    session->lwt_msg = (struct mqtt_packet){0};
    session->refcount = (struct ref){session_free, 0};
}

static struct client_session *client_session_alloc(const char *session_id)
{
    struct client_session *session = try_alloc(sizeof(*session));
    session_init(session, session_id);
    return session;
}

/*
 * Create a persistent session and register it in the server map: the entry
 * point the persistence loader uses to rebuild a session it read back from
 * disk. It runs before the event loops exist, so it takes no lock, and the
 * session starts exactly like the one a clean_session=false CONNECT leaves
 * behind when its client disconnects
 */
struct client_session *session_restore(const char *session_id)
{
    struct client_session *session = client_session_alloc(session_id);
    session->clean_session         = false;
    INCREF(session, struct client_session);
    HASH_ADD_STR(server.sessions, session_id, session);
    return session;
}

/*
 * Keep the pending list in sync with the i_msgs/i_acks tables: the periodic
 * retransmission scan walks it once per second per client while holding both
 * the global and the client mutex, the old code walked all 65535 slots of
 * the tables instead. The list only ever holds a handful of identifiers in
 * practice, so every operation below is O(pending)
 */
void inflight_track_mid(struct client_session *session, unsigned short mid)
{
    for (unsigned i = 0; i < session->pending_len; ++i)
        if (session->pending_mids[i] == mid)
            return;
    if (session->pending_len == session->pending_cap) {
        unsigned newcap = session->pending_cap ? session->pending_cap * 2 : 16;
        session->pending_mids =
            try_realloc(session->pending_mids, newcap * sizeof(u16));
        session->pending_cap = newcap;
    }
    session->pending_mids[session->pending_len++] = mid;
}

void inflight_untrack_mid(struct client_session *session, unsigned short mid)
{
    for (unsigned i = 0; i < session->pending_len; ++i) {
        if (session->pending_mids[i] == mid) {
            /*
             * Shift the tail down instead of swapping the last element in:
             * the array is walked in order by the retransmission scan, so
             * reusing an hole in the middle would scramble the order in which
             * unacknowledged messages are sent again (they must leave in
             * publication order, §4.6)
             */
            for (unsigned j = i; j + 1 < session->pending_len; ++j)
                session->pending_mids[j] = session->pending_mids[j + 1];
            session->pending_len--;
            return;
        }
    }
}

/*
 * Inbound QoS 2 bookkeeping: while a mid sits in this set a PUBREC has been
 * sent for it and the PUBREL has not landed yet, so the identifier belongs to
 * an exactly-once flow already delivered. Lookups are O(q2_recv_len), which
 * is a handful of entries in practice (one per message the publisher still
 * owes a PUBREL for)
 */
static bool inbound_q2_find(const struct client_session *session,
                            unsigned short mid)
{
    for (unsigned i = 0; i < session->q2_recv_len; ++i)
        if (session->q2_recv_mids[i] == mid)
            return true;
    return false;
}

static void inbound_q2_track(struct client_session *session, unsigned short mid)
{
    /* Idempotent: a mid already open must never appear twice in the array */
    if (inbound_q2_find(session, mid))
        return;
    if (session->q2_recv_len == session->q2_recv_cap) {
        unsigned newcap = session->q2_recv_cap ? session->q2_recv_cap * 2 : 8;
        session->q2_recv_mids =
            try_realloc(session->q2_recv_mids, newcap * sizeof(u16));
        session->q2_recv_cap = newcap;
    }
    session->q2_recv_mids[session->q2_recv_len++] = mid;
}

static void inbound_q2_untrack(struct client_session *session,
                               unsigned short mid)
{
    for (unsigned i = 0; i < session->q2_recv_len; ++i) {
        if (session->q2_recv_mids[i] == mid) {
            /* Swap with the last element: the order is not significant */
            session->q2_recv_mids[i] =
                session->q2_recv_mids[--session->q2_recv_len];
            return;
        }
    }
}

/*
 * Return a packet identifier not currently awaiting an acknowledgement.
 * Reusing one that is still in flight would overwrite its inflight entry,
 * losing the reference to the message and its retransmission state
 */
static inline unsigned short next_free_mid(struct client_session *session)
{
    for (unsigned tries = 0; tries < MAX_INFLIGHT_MSGS - 1; ++tries) {
        unsigned mid = session->next_free_mid;
        session->next_free_mid =
            (mid + 1 >= MAX_INFLIGHT_MSGS) ? 1 : (unsigned)(mid + 1);
        if (!session->i_msgs[mid].packet && session->i_acks[mid] <= 0)
            return (unsigned short)mid;
    }
    /* Identifier space exhausted, the client is not acknowledging */
    return 0;
}

static inline void inflight_msg_init(struct inflight_msg *imsg,
                                     struct mqtt_packet *p)
{
    imsg->seen   = time(NULL);
    imsg->packet = p;
    imsg->qos    = p->header.bits.qos;
}

/*
 * Queue a message for a session that is not connected: the queue entry keeps
 * the packet pointer and the identifier it was given, the inflight entry owns
 * the reference that keeps the packet alive and the pending list is kept in
 * sync. Deliberately leaves i_acks alone: that table holds the PUBREL pending
 * state of a QoS 2 delivery, arming it for a plain PUBLISH leaked a PUBREL
 * into every QoS 1 flow
 */
static void queue_offline_msg(struct client_session *s, struct mqtt_packet *pkt,
                              u16 mid)
{
    struct queued_msg *qm = try_alloc(sizeof(*qm));
    /*
     * The identifier travels with the entry and has to be in the packet too:
     * it is what the reconnect packs into the replayed PUBLISH
     */
    pkt->publish.pkt_id = mid;
    qm->packet          = pkt;
    qm->mid             = mid;
    /* Appended at the back, drained in order on reconnect */
    list_push_back(s->outgoing_msgs, qm);
    inflight_msg_init(&s->i_msgs[mid], pkt);
    inflight_track_mid(s, mid);
    ++s->inflights;
    persistence_mark_dirty();
}

/*
 * Queue a message for a disconnected session, the entry point the
 * persistence loader uses to put back the messages it read from disk: the
 * packet is built here (it owns its copies of the topic and of the payload)
 * and takes an identifier of its own, one the reconnect will replay it with
 */
void session_queue_message(struct client_session *s, const char *topic,
                           unsigned char qos, const unsigned char *payload,
                           size_t len)
{
    if (!s || !topic)
        return;

    unsigned short mid = next_free_mid(s);
    if (mid == 0) {
        log_warning("No free packet identifier for %s, dropping a restored "
                    "message",
                    s->session_id);
        return;
    }

    struct mqtt_packet *pkt = mqtt_packet_alloc(PUBLISH_B);
    pkt->header.bits.qos    = qos;
    pkt->publish            = (struct mqtt_publish){
        .topiclen   = strlen(topic),
        .topic      = dup_bytes((const unsigned char *)topic, strlen(topic)),
        .payloadlen = len,
        .payload    = dup_bytes(payload, len)};
    /* Owned by the inflight entry the queue call below fills in */
    INCREF(pkt, struct mqtt_packet);
    queue_offline_msg(s, pkt, (u16)mid);
}

/*
 * Drop the oldest message queued for a disconnected session: what the
 * max_memory policy does to give memory back. The packet is released through
 * the inflight entry that owns it, the identifier is dropped from the pending
 * list so the retransmission scan never sends a message that does not exist
 * anymore, and the queue slot goes away. False when there is nothing left to
 * drop
 */
bool session_drop_oldest_queued(struct client_session *s)
{
    if (!s || !s->outgoing_msgs || !s->outgoing_msgs->head)
        return false;

    struct queued_msg *qm = s->outgoing_msgs->head->data;
    if (qm && qm->packet) {
        struct inflight_msg *imsg = &s->i_msgs[qm->mid];
        if (imsg->packet) {
            inflight_msg_clear(imsg);
            imsg->packet = NULL;
        }
        s->i_acks[qm->mid] = -1;
    }
    if (s->inflights > 0)
        --s->inflights;
    if (qm)
        inflight_untrack_mid(s, qm->mid);
    /* Releases the queue node and its wrapper, not the packet */
    list_drop_prefix(s->outgoing_msgs, 1);
    persistence_mark_dirty();
    return true;
}

/*
 * Deliver the retained message of `t` to the client that has just subscribed,
 * at the QoS granted to that subscription and never above the one the message
 * was retained with, as §4.4 requires. Unlike a normal publish this targets a
 * single subscriber, so the packet is packed here for it: a QoS > 0 delivery
 * takes an identifier of its own and is tracked in the session inflight
 * tables the same way publish_message() does, which also puts it under the
 * periodic retransmission. The caller holds the global mutex and the client
 * mutex, so nothing is locked here
 */
static void deliver_retained(struct client *c, struct topic *t,
                             unsigned char granted_qos)
{
    struct mqtt_packet *stored = t->retained;
    struct client_session *s   = c->session;

    if (!stored || !s)
        return;

    unsigned char qos = stored->header.bits.qos < granted_qos
                            ? stored->header.bits.qos
                            : granted_qos;

    struct mqtt_packet *out = mqtt_publish_copy(stored);
    out->header             = (union mqtt_header){.byte = PUBLISH_B};
    out->header.bits.qos    = qos;
    /* §3.3.1.2: a message delivered because of a new subscription keeps the
     * RETAIN flag, it is the one case where the receiver may rely on it */
    out->header.bits.retain = 1;

    if (qos > AT_MOST_ONCE) {
        unsigned short mid = next_free_mid(s);
        if (mid == 0) {
            /* Every identifier is taken, drop it like the publish path does */
            log_warning("No free packet identifier for %s, dropping retained "
                        "message on %s",
                        s->session_id, t->name);
            DECREF(out, struct mqtt_packet);
            return;
        }
        out->publish.pkt_id = mid;
        /* One more reference, owned by the inflight entry until it is acked */
        INCREF(out, struct mqtt_packet);
        inflight_msg_init(&s->i_msgs[mid], out);
        inflight_track_mid(s, mid);
        ++s->inflights;
    }

    size_t len = mqtt_size(out, NULL);
    if (c->towrite + len > conf->max_request_size) {
        /* A QoS 0 delivery is lost, a QoS > 0 one is retried by the scan */
        log_warning("Write buffer full (%lu bytes pending) for %s, dropping "
                    "retained message",
                    (unsigned long)c->towrite, c->client_id);
        DECREF(out, struct mqtt_packet);
        return;
    }
    mqtt_pack(out, c->wbuf + c->towrite);
    c->towrite += len;
    info.messages_sent++;
    log_debug("Sending retained PUBLISH to %s (q%u, m%u, %s, ... (%u bytes))",
              c->client_id, qos, out->publish.pkt_id, out->publish.topic,
              out->publish.payloadlen);
    /* Released either here or when the last acknowledgement lands */
    DECREF(out, struct mqtt_packet);
}

/*
 * One of the two exposed functions of the module, it's also needed on server
 * module to publish periodic messages (e.g. $SOL stats). It's responsible
 * of the normal publish but also taking care of disconnected clients, enqueuing
 * packets and setting up inflight messages for QoS > 0.
 * Returns the number of publish done or an error code in case of conditions
 * that requires de-allocation of the pkt argument occurs.
 */
int publish_message(struct mqtt_packet *pkt, const struct topic *t)
{

    bool all_at_most_once = true;
    size_t len            = 0;
    unsigned short mid    = 0;
    unsigned char qos     = pkt->header.bits.qos;
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    /*
     * MQTT v3.1.1 §3.3.1-3: the RETAIN flag is cleared for every delivery
     * that goes out to an *established* subscription, whatever the publisher
     * set on the incoming packet. It only stays set on the path that feeds a
     * brand new subscription, i.e. the retained flush in subscribe_handler(),
     * which copies the stored packet straight into the write buffer and never
     * comes through here (§3.3.1-2).
     *
     * Clearing it for the whole call rather than per subscriber also covers
     * the QoS > 0 inflight copies: those are handed to i_msgs[] and it is
     * their bytes the retransmission cron sends later, and a retransmission
     * is by definition a delivery to an established subscription. The caller
     * stores any retained copy *before* calling this, so the will, the
     * $SOL stats and the retained message in the store are unaffected
     */
    pkt->header.bits.retain = 0;
    int count = HASH_COUNT(t->subscribers);

    if (count == 0) {
        INCREF(pkt, struct mqtt_packet);
        goto exit;
    }

    // first run check
    struct subscriber *sub, *dummy;
    HASH_ITER(hh, t->subscribers, sub, dummy)
    {
        struct client_session *s = sub->session;
        struct client *sc        = NULL;
        HASH_FIND_STR(server.clients_map, s->session_id, sc);
        /*
         * Update QoS according to subscriber's one, following MQTT
         * rules: The min between the original QoS and the subscriber
         * QoS
         */
        pkt->header.bits.qos = qos >= sub->granted_qos ? sub->granted_qos : qos;
        len = mqtt_size(pkt, NULL); // override len, no ID set in QoS 0
        /*
         * if QoS 0
         *
         * Set the correct size of the output packet and set the
         * correct QoS value (0) and packet identifier to (0) as
         * specified by MQTT specs
         */
        pkt->publish.pkt_id = 0;

        /*
         * if QoS > 0 we set packet identifier and track the inflight
         * message, proceed with the publish towards online subscriber.
         */
        if (pkt->header.bits.qos > AT_MOST_ONCE) {
            mid = next_free_mid(s);
            if (mid == 0) {
                /*
                 * Every identifier is still awaiting an acknowledgement:
                 * without a free one this message could never be tracked or
                 * acknowledged, drop it instead of overwriting an inflight
                 * entry (which would leak the previous packet reference)
                 */
                log_warning("No free packet identifier for %s, dropping PUBLISH",
                            s->session_id);
                continue;
            }
            pkt->publish.pkt_id = mid;
            /*
             * If offline, we must enqueue messages in the inflight queue
             * of the client, they will be sent out only in case of a
             * clean_session == false connection
             */
            if (!sc || sc->online == false) {
                if (s->clean_session == false) {
                    /*
                     * One single reference, owned by the inflight entry: the
                     * queue entry only borrows the pointer and is dropped
                     * without releasing anything when the session flushes it
                     */
                    INCREF(pkt, struct mqtt_packet);
                    queue_offline_msg(s, pkt, (u16)mid);
                    all_at_most_once = false;
                    persistence_mark_dirty();
                }
                continue;
            }
            /* Reference owned by the inflight entry, released on PUBACK */
            INCREF(pkt, struct mqtt_packet);
#if THREADSNR > 0
            pthread_mutex_lock(&sc->mutex);
#endif
            /*
             * The subscriber client is marked as online, so we proceed to
             * set the inflight messages according to the QoS level required
             * and write back the payload. The pending list is mutated under
             * the client mutex, the very lock the acknowledgement handlers
             * and the periodic retransmission check hold
             */
            inflight_msg_init(&sc->session->i_msgs[mid], pkt);
            /*
             * The retransmission of the PUBLISH itself is driven by the
             * inflight entry above: i_acks is reserved for the PUBREL a
             * QoS 2 subscriber acknowledges with a PUBREC (see
             * pubrec_handler), arming it here leaked a PUBREL into every
             * QoS 1 flow
             */
            inflight_track_mid(sc->session, mid);
            ++sc->session->inflights;
#if THREADSNR > 0
            pthread_mutex_unlock(&sc->mutex);
#endif
            all_at_most_once = false;
        }
        /*
         * A QoS 0 message has nowhere to go if the subscriber is offline
         * (or gone): packing it into a deactivated client would lock a
         * destroyed mutex and enqueue a write on a closed descriptor
         */
        if (!sc || sc->online == false)
            continue;
#if THREADSNR > 0
        pthread_mutex_lock(&sc->mutex);
#endif
        /*
         * Never write past the end of the subscriber write buffer: it is
         * drained asynchronously by the event loop, a fast publisher
         * outrunning a slow subscriber would otherwise corrupt the heap.
         * When full, a QoS > 0 message is retried by the periodic
         * retransmission check once the buffer drained, a QoS 0 one is
         * dropped as the spec allows
         */
        if (sc->towrite + len > conf->max_request_size) {
#if THREADSNR > 0
            pthread_mutex_unlock(&sc->mutex);
#endif
            log_warning("Write buffer full (%lu bytes pending) for %s, "
                        "dropping PUBLISH",
                        (unsigned long)sc->towrite, sc->client_id);
            continue;
        }
        mqtt_pack(pkt, sc->wbuf + sc->towrite);
        sc->towrite += len;
#if THREADSNR > 0
        pthread_mutex_unlock(&sc->mutex);
#endif

        // Schedule a write for the current subscriber on the next event cycle
        enqueue_event_write(sc);

        info.messages_sent++;

        log_debug(
            "Sending PUBLISH to %s (d%i, q%u, r%i, m%u, %s, ... (%i bytes))",
            sc->client_id, pkt->header.bits.dup, pkt->header.bits.qos,
            pkt->header.bits.retain, pkt->publish.pkt_id, pkt->publish.topic,
            pkt->publish.payloadlen);
    }

    // add return code
    if (all_at_most_once == true) {
        count = 0;
        /*
         * Returning 0 asks the caller to DECREF the packet: when every
         * delivery was QoS 0 no reference was ever taken, so take the one
         * the caller's DECREF balances to release the packet (and the topic
         * and payload strings only it owns, io.data is left untouched for
         * PUBLISH) exactly once instead of leaking it on every message.
         * Callers that do not DECREF (stats, LWT) are unaffected: they
         * leave the counter above 0
         */
        if (pkt->refcount.count == 0)
            INCREF(pkt, struct mqtt_packet);
    }

exit:

#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif
    return count;
}

/*
 * Check if a topic matches a subscription filter, supporting the single level
 * wildcard '+' and the multilevel wildcard '#'.
 *
 * Both the topic and the filter are normalized by the caller to end with a
 * trailing '/', i.e. "foo/bar" becomes "foo/bar/", so levels are simply the
 * sequences of characters separated by '/'.
 */
static inline int match_subscription(const char *topic, const char *wtopic,
                                     bool multilevel)
{
    const char *w = wtopic;
    const char *t = topic;

    /*
     * MQTT v3.1.1 §4.7.2: a filter starting with a wildcard must never match
     * a topic name beginning with '$' (the broker's own $SOL statistics live
     * there), only a filter spelling the '$' explicitly is allowed to
     */
    if (topic[0] == '$' && (w[0] == '#' || w[0] == '+'))
        return -SOL_ERR;

    /* A bare '#' subscription matches every topic */
    if (w[0] == '#' && (w[1] == '\0' || w[1] == '/'))
        return SOL_OK;

    while (*w) {
        const char *wend = strchr(w, '/');
        const char *tend = strchr(t, '/');
        size_t wlen      = wend ? (size_t)(wend - w) : strlen(w);
        size_t tlen      = tend ? (size_t)(tend - t) : strlen(t);

        /* '#' at any other position matches the rest of the topic */
        if (wlen == 1 && w[0] == '#')
            return SOL_OK;

        /* The topic has less levels than the filter: no match */
        if (tlen == 0 && !tend)
            return -SOL_ERR;

        /*
         * '+' consumes exactly one level, whatever it contains, every other
         * level is a literal that must match the topic one character by
         * character
         */
        if (!(wlen == 1 && w[0] == '+') &&
            (wlen != tlen || strncmp(w, t, wlen) != 0))
            return -SOL_ERR;

        w = wend ? wend + 1 : w + wlen;
        t = tend ? tend + 1 : t + tlen;
    }

    /*
     * The filter is exhausted: the topic must be exhausted as well, unless
     * the filter ends with a multilevel wildcard consuming the remaining
     * levels
     */
    return (*t == '\0' || multilevel) ? SOL_OK : -SOL_ERR;
}

/*
 * Command handlers
 */

/*
 * Drop the first `n` entries of the offline queue: both the list nodes and
 * the struct queued_msg wrappers they hold are released. The packets are left
 * alone on purpose, they stay owned by the session's inflight table until the
 * subscriber acknowledges them
 */
static void list_drop_prefix(List *l, unsigned long n)
{
    while (n-- > 0 && l->head) {
        struct list_node *node = l->head;
        l->head                = node->next;
        if (!l->head)
            l->tail = NULL;
        l->len--;
        free_memory(node->data);
        free_memory(node);
    }
}

static void set_connack(struct client *c, unsigned char rc, unsigned sp)
{
    unsigned char connect_flags = 0 | (sp & 0x1) << 0;

    struct mqtt_packet response = {
        .header  = {.byte = CONNACK_B},
        .connack = (struct mqtt_connack){.byte = connect_flags, .rc = rc}};

    /*
     * The client is already published in the clients map by the time the
     * CONNACK is built, so another thread's publish_message() may be packing
     * into this very write buffer under the client mutex: taking it here is
     * the same protocol every other writer of wbuf/towrite follows, and the
     * global mutex is not held at this point so the canonical order is kept
     */
#if THREADSNR > 0
    pthread_mutex_lock(&c->mutex);
#endif

    /*
     * The write buffer is exactly max_request_size bytes big: the CONNACK
     * must never be packed past its end (a fresh connection has an empty
     * buffer, so this only trips on an absurdly small configuration). Drop
     * the connection instead of leaving the client waiting forever for a
     * CONNACK that was never written
     */
    if (c->towrite + MQTT_ACK_LEN > conf->max_request_size) {
        log_error("Cannot send CONNACK to %s: write buffer too small "
                  "(max_request_size = %zu)",
                  c->client_id, conf->max_request_size);
        c->close_after_write = true;
#if THREADSNR > 0
        pthread_mutex_unlock(&c->mutex);
#endif
        return;
    }
    mqtt_pack(&response, c->wbuf + c->towrite);
    c->towrite += MQTT_ACK_LEN;

    /*
     * If a session was present and the connected client have disabled the
     * clean session flag, we have to take care of the outgoing messages
     * pending, strictly after the CONNACK encoding
     */
    if (rc == MQTT_CONNECTION_ACCEPTED && c->clean_session == false &&
        sp == 1) {
        log_info("Resuming session for %s", c->client_id);
        unsigned long packed = 0;
        list_foreach(item, c->session->outgoing_msgs)
        {
            struct queued_msg *qm = item->data;
            size_t len            = mqtt_size(qm->packet, NULL);
            /*
             * Stop at the first message that no longer fits the write
             * buffer: it stays in the queue with its packet id still
             * tracked, so the retransmission cron delivers it once the
             * connection has drained. Packing unconditionally (as this
             * used to) wrote past the end of the write buffer on a large
             * backlog, corrupting whatever followed it
             */
            if (c->towrite + len > conf->max_request_size)
                break;
            mqtt_pack(qm->packet, c->wbuf + c->towrite);
            c->towrite += len;
            ++packed;
        }
        // Forget just the messages flushed above
        list_drop_prefix(c->session->outgoing_msgs, packed);
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
}

static int connect_handler(struct io_event *e)
{

    unsigned session_present = 0;
    struct mqtt_connect *c   = &e->data.connect;
    struct client *cc        = e->client;

    if (cc->connected == true) {
        /*
         * Already connected client, a second CONNECT packet is a violation of
         * the protocol (§3.1.4). Reported as a protocol error and not as a
         * plain disconnection: the connection is closed without a DISCONNECT
         * packet, so the will carried by the first CONNECT has to be
         * delivered (§3.1.2.5)
         */
        log_info("Received double CONNECT from %s, disconnecting client",
                 c->payload.client_id);
        return -ERRPACKETERR;
    }

    /*
     * If allow_anonymous is false we need to check for an existing
     * username:password pair match in the authentications table
     */
    if (conf->allow_anonymous == false) {
        if (c->bits.username == 0 || c->bits.password == 0)
            goto bad_auth;
        else {
            struct authentication *auth = NULL;
            HASH_FIND_STR(server.auths, (char *)c->payload.username, auth);
            if (!auth || !check_passwd((char *)c->payload.password, auth->salt))
                goto bad_auth;
        }
    }

    /*
     * MQTT v3.1.1 §3.1.3.1: a zero-length client id is only acceptable when
     * the client asks for a clean session (the server then assigns an id).
     * With clean_session == false the server has no way to name the session
     * and must answer with the Identifier rejected return code (0x02), not
     * with Not authorized (0x05)
     */
    if (!c->payload.client_id[0] && c->bits.clean_session == false)
        goto id_rejected;

    /*
     * Check for client ID, if not present generate a random ID, otherwise add
     * the client to the sessions map if not already present
     */
    if (!c->payload.client_id[0])
        generate_random_id((char *)c->payload.client_id);
    /*
     * Add the new connected client to the global map, if it is already
     * connected, kick him out accordingly to the MQTT v3.1.1 specs.
     */
    snprintf(cc->client_id, MQTT_CLIENT_ID_LEN, "%s", c->payload.client_id);
    /*
     * Keep the username for the access control list: rules can name a user
     * instead of a client id, and the CONNECT payload is gone by the time the
     * first SUBSCRIBE or PUBLISH has to be checked
     */
    if (c->bits.username == 1 && c->payload.username)
        snprintf(cc->username, MQTT_CLIENT_ID_LEN, "%s", c->payload.username);

    /*
     * The will is a publication the broker makes on the client's behalf, so
     * it needs the same permission a PUBLISH would: a client that cannot
     * write a topic must not be able to write it through its will either
     */
    if (c->bits.will &&
        !acl_check(cc->client_id, cc->username,
                   (const char *)c->payload.will_topic, ACL_WRITE)) {
        log_warning("ACL: %s is not allowed to publish its will to %s",
                    cc->client_id, c->payload.will_topic);
        goto not_authorized;
    }

    /*
     * Will of the connection this one is about to take over, if any: it is
     * copied under the lock (the victim's session is still reachable there)
     * and published right after, once the global mutex is released
     */
    struct mqtt_packet kicked_will = {0};

#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    /*
     * MQTT v3.1.1 §3.1.4-2: when the id already belongs to a live
     * connection the server must disconnect the older one. Done before the
     * session lookup so the state below is the one left by the kick
     */
    server_kick_duplicate(cc->client_id, cc, &kicked_will);

    // First we check if a session is present
    HASH_FIND_STR(server.sessions, cc->client_id, cc->session);
    if (cc->session && c->bits.clean_session == true) {
        /*
         * A clean session is requested over a session left by a previous
         * persistent connection: drop it completely — the map entry, the
         * subscribers still attached to the topics and the session itself,
         * the very teardown a clean disconnect performs. Clearing only the
         * map entry (as this used to) leaked the session and left its
         * subscribers on the topics, routing every message of the fresh
         * session to a dead one
         */
        topic_store_remove_wildcard(server.store, cc->client_id);
        list_foreach(item, cc->session->subscriptions)
        {
            topic_del_subscriber(item->data, cc);
        }
        HASH_DEL(server.sessions, cc->session);
        DECREF(cc->session, struct client_session);
        cc->session = NULL;
    } else if (cc->session) {
        session_present = 1;
    }

    cc->connected = true;
    /*
     * Remember the Keep Alive the client asked for: the periodic scan below
     * is what enforces it, the value lives on the connection and not on the
     * session because it is negotiated on every CONNECT (§3.1.2.10)
     */
    cc->keepalive = c->payload.keepalive;
    /*
     * The configured keepalive is a maximum: a client asking for a longer
     * period, or for none at all (Keep Alive 0), is still held to the broker
     * policy, otherwise a single client could opt out of the check entirely
     * and leave a dead connection around forever. 0 in the configuration
     * disables the cap and lets the client decide
     */
    if (conf->keepalive > 0 &&
        (cc->keepalive == 0 || cc->keepalive > conf->keepalive))
        cc->keepalive = conf->keepalive;

    log_info("New client connected as %s (c%i, k%u)", c->payload.client_id,
             c->bits.clean_session, c->payload.keepalive);

    /*
     * If no session was found or the client is a new connecting client or an
     * anonymous one, we create a session here
     */
    if (c->bits.clean_session == true || !cc->session) {
        cc->session = client_session_alloc(cc->client_id);
        INCREF(cc->session, struct client_session);
        HASH_ADD_STR(server.sessions, session_id, cc->session);
    }

    cc->session->clean_session = c->bits.clean_session;

    // Let's track client on the global map to be used on publish
    HASH_ADD_STR(server.clients_map, client_id, cc);
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif

    /*
     * §3.1.2.5: the connection taken over above was closed by the server
     * without a DISCONNECT, so the will it had registered goes out now, and
     * it goes out *before* the will of this new connection replaces it in
     * the session. publish_lwt() takes the global mutex itself, which is why
     * this happens here and not inside the critical section
     */
    if (kicked_will.publish.topic) {
        log_info("Delivering the will of the taken over connection (%s)",
                 kicked_will.publish.topic);
        publish_lwt(&kicked_will);
        /* The packet owns its strings, the struct itself is on this stack */
        mqtt_packet_destroy(&kicked_will);
    }

    // Add LWT topic and message if present
    if (c->bits.will) {
        cc->has_lwt = true;
        unsigned char *will_topic   = c->payload.will_topic;
        unsigned char *will_message = c->payload.will_message;
        /*
         * Both are guaranteed non-NULL, non-empty and wildcard free here:
         * unpack_mqtt_connect() refuses a CONNECT whose will topic fails
         * those checks. The lengths come from the wire, an application
         * message being binary, the will payload included, so strlen() would
         * silently truncate it at the first NUL byte it carries
         */
        size_t tpc_len = c->payload.will_topic_len;
        size_t msg_len = c->payload.will_message_len;

        /*
         * The will topic is indexed under its normalized name (trailing '/',
         * like every other topic in the store) so that the delivery finds
         * the very same node subscriptions are attached to. The insert runs
         * with the global mutex held, a concurrent SUBSCRIBE or PUBLISH
         * mutates the very same trie from its own critical section
         */
        char will_store_name[tpc_len + 2];
        if (will_topic[tpc_len - 1] != '/')
            snprintf(will_store_name, tpc_len + 2, "%s/", will_topic);
        else
            snprintf(will_store_name, tpc_len + 1, "%s", will_topic);
#if THREADSNR > 0
        pthread_mutex_lock(&mutex);
#endif
        struct topic *t = topic_store_get_or_put(server.store, will_store_name);
#if THREADSNR > 0
        pthread_mutex_unlock(&mutex);
#endif

        /*
         * A session kept across a reconnect still holds the strings of the
         * previous will: release them before installing the new one (a
         * fresh session is all zeros, destroy is a no-op on it).
         *
         * While an inflight message still points at the embedded packet its
         * strings must stay alive and, more importantly, the packet itself
         * must not be overwritten: the retransmission scan would otherwise
         * send the *new* will in place of the one the QoS flow is still
         * delivering, and the old strings would be leaked on every
         * reconnect. Keeping the previous will for that rare case is the
         * lesser evil and is what session_free() eventually releases
         */
        if (has_inflight(cc->session)) {
            log_warning("Reconnecting %s with inflight messages: keeping the "
                        "previous LWT, the new one takes effect once the "
                        "session drains",
                        cc->client_id);
        } else {
            mqtt_packet_destroy(&cc->session->lwt_msg);

            cc->session->lwt_msg = (struct mqtt_packet){
                .header  = (union mqtt_header){.byte = PUBLISH_B},
                .publish = (struct mqtt_publish){
                    .pkt_id     = 0, // placeholder
                    .topiclen   = tpc_len,
                    .topic      = dup_bytes(will_topic, tpc_len),
                    .payloadlen = msg_len,
                    .payload    = dup_bytes(will_message, msg_len)}};

            cc->session->lwt_msg.header.bits.qos = c->bits.will_qos;
            /*
             * The packet is embedded in the session: park its reference
             * count at 1 so that the bookkeeping INCREFs publish_message()
             * makes for QoS > 0 inflight tracking can never bring it back
             * to 0, which would call a free function on the middle of the
             * session struct
             */
            cc->session->lwt_msg.refcount.count = 1;

            /*
             * §3.3.1.3: a will carrying the retain flag is stored as the
             * retained message of its topic, replacing whatever was there
             * (the previous packet is released here). Stored unpacked, like
             * the one a PUBLISH leaves behind, so every later subscriber is
             * served at the QoS its own subscription was granted. Note this
             * only runs when the will was really replaced above: with
             * messages still in flight the session keeps the previous one
             */
            if (c->bits.will_retain == 1) {
#if THREADSNR > 0
                pthread_mutex_lock(&mutex);
#endif
                if (t->retained) {
                    DECREF(t->retained, struct mqtt_packet);
                    t->retained = NULL;
                }
                /* A zero length payload clears the retained message instead
                 * of storing an empty one (§3.3.1.3) */
                if (msg_len > 0)
                    t->retained = mqtt_publish_copy(&cc->session->lwt_msg);
                persistence_mark_dirty();
#if THREADSNR > 0
                pthread_mutex_unlock(&mutex);
#endif
            }
        }
        log_info("Will message specified (%lu bytes)",
                 cc->session->lwt_msg.publish.payloadlen);
        log_info("\t%s", cc->session->lwt_msg.publish.payload);
    }

    /*
     * Session presence is decided above: a persistent session carried over
     * from a previous connection sets session_present, a clean session over
     * a stale one tears the old one down first
     */
    cc->clean_session = c->bits.clean_session;

    set_connack(cc, MQTT_CONNECTION_ACCEPTED, session_present);

    log_debug("Sending CONNACK to %s (%u, %u)", cc->client_id, session_present,
              MQTT_CONNECTION_ACCEPTED);

    return REPLY;

bad_auth:
    log_debug("Sending CONNACK to %s (%u, %u)", cc->client_id, session_present,
              MQTT_BAD_USERNAME_OR_PASSWORD);
    set_connack(cc, MQTT_BAD_USERNAME_OR_PASSWORD, session_present);
    /*
     * MQTT v3.1.1 §3.2.2.3: a CONNACK with a non-zero return code is
     * followed by closing the network connection, otherwise the client
     * would keep talking on a connection the broker considers refused
     */
    cc->close_after_write = true;

    return MQTT_BAD_USERNAME_OR_PASSWORD;

not_authorized:
    log_debug("Sending CONNACK to %s (%u, %u)",
              cc->client_id[0] ? cc->client_id : "unknown", session_present,
              MQTT_NOT_AUTHORIZED);
    set_connack(cc, MQTT_NOT_AUTHORIZED, session_present);
    /* Same §3.2.2.3 rule as above: flush then close */
    cc->close_after_write = true;
    return MQTT_NOT_AUTHORIZED;

id_rejected:
    log_debug("Sending CONNACK to %s (%u, %u)",
              cc->client_id[0] ? cc->client_id : "unknown", session_present,
              MQTT_IDENTIFIER_REJECTED);
    set_connack(cc, MQTT_IDENTIFIER_REJECTED, session_present);
    /* Same §3.2.2.3 rule as above: flush then close */
    cc->close_after_write = true;

    return MQTT_IDENTIFIER_REJECTED;
}

static int disconnect_handler(struct io_event *e)
{
    log_debug("Received DISCONNECT from %s", e->client->client_id);
    return -ERRCLIENTDC;
}

static inline void add_wildcard(const char *topic, struct subscriber *s,
                                bool wildcard)
{
    struct subscription *subscription = try_alloc(sizeof(*subscription));
    subscription->subscriber          = s;
    subscription->topic               = try_strdup(topic);
    subscription->multilevel          = wildcard;
    INCREF(s, struct subscriber);
    topic_store_add_wildcard(server.store, subscription);
}

static void recursive_sub(struct trie_node *node, void *arg)
{
    if (!node || !node->data)
        return;
    struct topic *t      = node->data;
    /*
     * We need to make a copy of the subscriber cause UTHASH needs a proper
     * handle to work correctly, otherwise we'll end up freeing the same
     * refernce on disconnect and break the table
     */
    struct subscriber *s = subscriber_clone(arg), *tmp;
    HASH_FIND_STR(t->subscribers, s->id, tmp);
    if (!tmp) {
        s->origins = SUBSCRIBER_WILD;
        INCREF(s, struct subscriber);
        HASH_ADD_STR(t->subscribers, id, s);
        log_debug("Adding subscriber %s to topic %s", s->session->session_id,
                  t->name);
        session_track_topic(s->session, t);
    } else {
        /*
         * Already attached to this very topic (typically by an exact
         * SUBSCRIBE): remember that a wildcard subscription covers it too so
         * that an UNSUBSCRIBE of the filter keeps the exact one alive, the
         * unused copy is dropped instead of being leaked
         */
        tmp->origins |= SUBSCRIBER_WILD;
        session_track_topic(tmp->session, t);
        free_memory(s);
    }
}

/*
 * One filter of the SUBSCRIBE packet, kept until the retained messages have
 * been flushed right after the SUBACK
 */
struct retained_delivery {
    const char *filter;
    bool multilevel;
    unsigned char qos; /* QoS granted to this filter, caps the delivery */
};

/*
 * Normalize a subscription filter the way the store keys every topic: filters
 * ending in "/#" are stripped to their prefix, keeping only the multilevel
 * semantics in the returned flag, a bare "#" is kept as is and anything else
 * gains a trailing '/'. Shared by SUBSCRIBE and UNSUBSCRIBE so both look up
 * the very same trie node and wildcard index entry. `dst` must be distinct
 * from `src` and hold strlen(src) + 2 bytes, returns true when the filter is
 * multilevel ('#')
 */
static bool normalize_filter(char *dst, const char *src)
{
    size_t len = strlen(src);

    if (len >= 2 && src[len - 1] == '#' && src[len - 2] == '/') {
        snprintf(dst, len + 1, "%s", src);
        dst[len - 1] = '\0';
        return true;
    }
    if (len == 1 && src[0] == '#') {
        snprintf(dst, 2, "%s", src);
        return true;
    }
    if (len > 0 && src[len - 1] == '/')
        snprintf(dst, len + 1, "%s", src);
    else
        snprintf(dst, len + 2, "%s/", src);
    return false;
}

/*
 * MQTT v3.1.1 §4.7.1: a topic filter must be well formed before it is
 * indexed in the store, otherwise the trie ends up holding patterns that can
 * never match, and the wildcard matching rules would be silently violated.
 *
 *  - §4.7.1.1: a filter carries at least one character (so "" is refused)
 *  - §4.7.1.2: '#' is only ever the *entire* last level, so it must be the
 *    final character and be preceded by '/' (or be the whole filter). The
 *    old code accepted "a#", "a/#/b" and "#/x" and happily stored them, and
 *    normalize_filter() then either kept a pattern that matched nothing or
 *    stripped the '#' from the middle of the filter
 *  - §4.7.1.3: '+' is only ever an *entire* level, so it must be surrounded
 *    by '/' (or sit at either end of the filter): "a+b" and "+x" were stored
 *    as literal keys that no publish to "a/b" would ever reach
 *
 * Returns true when the filter is well formed
 */
static bool valid_topic_filter(const char *f, size_t len)
{
    if (len == 0)
        return false;

    for (size_t i = 0; i < len; i++) {
        /*
         * §1.5.3: U+0000 is forbidden in every UTF-8 string of the protocol.
         * An embedded NUL is also what makes the filter and its normalized
         * copy disagree in length (every helper here is strlen driven), so a
         * filter like "a\0b" would be stored as if it read "a"
         */
        if (f[i] == '\0')
            return false;
        if (f[i] == '#') {
            if (i + 1 != len) // must be the last character
                return false;
            if (i > 0 && f[i - 1] != '/') // must be its own level
                return false;
        } else if (f[i] == '+') {
            if (i > 0 && f[i - 1] != '/')
                return false;
            if (i + 1 < len && f[i + 1] != '/')
                return false;
        }
    }
    return true;
}

/*
 * Register one topic filter for a session: validation, normalization and the
 * whole store dance (the '+' filters land in the wildcard index, a trailing
 * "/#" is stripped to its prefix and flagged multilevel, an exact filter gets
 * its subscriber entry on the node). It is the very body of a SUBSCRIBE
 * tuple, shared with the persistence loader so a session rebuilt from disk
 * ends up registered exactly like one that subscribed over the wire. The
 * caller must hold the global mutex; false means the filter is malformed
 */
bool session_subscribe(struct client_session *session, const char *filter,
                       unsigned char qos)
{
    if (!session || !valid_topic_filter(filter, strlen(filter)))
        return false;

    char topic[strlen(filter) + 2];
    snprintf(topic, strlen(filter) + 1, "%s", filter);

    /*
     * Recursive subscribe to all children topics if the filter ends with
     * "/#" (or is a bare "#"): the wildcard part is stripped from the stored
     * filter and flagged as multilevel, see normalize_filter()
     */
    bool wildcard = normalize_filter(topic, filter);

    /*
     * Let's explore two possible scenarios:
     * 1. Normal topic (no single level wildcard '+') which can end with
     *    multilevel wildcard '#'
     * 2. A topic contaning one or more single level wildcard '+'
     */
    struct topic *t = topic_store_get_or_put(server.store, topic);
    if (!index(topic, '+')) {
        struct subscriber *tmp;
        HASH_FIND_STR(t->subscribers, session->session_id, tmp);
        if (!tmp) {
            tmp = topic_add_subscriber(t, session, qos);
            /* Keep the entry alive for the subscriber map */
            INCREF(tmp, struct subscriber);
            /*
             * A fresh entry takes its origin from the filter that made it,
             * not from a generic EXACT default: the entry a multilevel
             * filter creates on its own node (the "a" node of "a/#") is the
             * record of that filter, and marking it exact as well made a
             * later UNSUBSCRIBE of "a/#" keep delivering the bare "a" as if
             * it were still subscribed. The WILD branch below completes the
             * filter registration
             */
            tmp->origins = wildcard ? 0 : SUBSCRIBER_EXACT;
            /*
             * Track the topic once, on the first subscription of this node:
             * a repeated SUBSCRIBE used to append the same topic again,
             * growing the list (and the teardown loop) with every duplicate
             */
            session_track_topic(session, t);
        } else {
            /*
             * MQTT v3.1.1 §3.8.4: a SUBSCRIBE for a filter already
             * subscribed replaces the subscription, only the granted QoS
             * may differ. The wildcard index shares this very subscriber
             * record, so the new QoS applies there too
             */
            tmp->granted_qos = qos;
        }
        if (wildcard == true) {
            /*
             * Register the filter even when an entry for this client already
             * lives on the node (e.g. the bare topic was subscribed before:
             * "a" and "a/#" share the very same node): skipping it then used
             * to silently drop the filter, no message under the prefix would
             * ever match it. The WILD origin marks the registration as done,
             * so a repeated SUBSCRIBE does not duplicate the index record
             */
            if (!(tmp->origins & SUBSCRIBER_WILD)) {
                /*
                 * The entry covers the filter (and its own topic, as "a/#"
                 * matches "a"): remember it so that UNSUBSCRIBE of the
                 * wildcard drops the record too
                 */
                tmp->origins |= SUBSCRIBER_WILD;
                add_wildcard(topic, tmp, wildcard);
                topic_store_map(server.store, topic, recursive_sub, tmp);
            }
        } else {
            /*
             * The entry now doubles as an explicit subscription of this very
             * topic: remembering the origin lets a concurrent wildcard
             * covering it survive its UNSUBSCRIBE (and vice versa)
             */
            tmp->origins |= SUBSCRIBER_EXACT;
        }
    } else {
        /*
         * Here we encountered at least 1 single level wildcard '+', we add
         * the subscription to the wildcard index as we can't know at this
         * point which topic it will match
         *
         * MQTT v3.1.1 §3.8.4: a SUBSCRIBE for a filter already subscribed
         * replaces the previous subscription, only the granted QoS may
         * differ. Indexing a fresh record on every repeat grew the wildcard
         * index (and with it the client teardown loop) without bound while
         * delivery kept deduplicating anyway, so a repeat now re-grants the
         * QoS on the existing entry
         */
        struct subscription *ex = topic_store_find_wildcard_filter(
            server.store, topic, session->session_id);
        if (ex) {
            ex->subscriber->granted_qos = qos;
        } else {
            struct subscriber *sub = subscriber_new(session, qos);
            add_wildcard(topic, sub, wildcard);
        }
    }
    persistence_mark_dirty();
    return true;
}

/*
 * Does a topic name match a filter? Used by the access control list, which
 * speaks the same wildcard language a SUBSCRIBE does: the name and the filter
 * are normalized the way the store keys them and matched with the very rules
 * a subscription follows
 */
bool topic_access_match(const char *topic, const char *filter)
{
    if (!topic || !filter)
        return false;

    size_t tlen = strlen(topic);
    char *name  = try_alloc(tlen + 2);
    if (tlen > 0 && topic[tlen - 1] == '/')
        snprintf(name, tlen + 1, "%s", topic);
    else
        snprintf(name, tlen + 2, "%s/", topic);

    size_t flen   = strlen(filter);
    char *pattern = try_alloc(flen + 2);
    snprintf(pattern, flen + 1, "%s", filter);
    bool multilevel = normalize_filter(pattern, filter);

    bool match = match_subscription(name, pattern, multilevel) == SOL_OK;
    free_memory(name);
    free_memory(pattern);
    return match;
}

/*
 * Argument passed to retained_collect while scanning the whole topic store
 */
struct retained_collection {
    struct topic **topics; /* Topics holding a retained message, in store order */
    size_t len;
    size_t cap;
};

/*
 * Callback applied to every topic of the store: collects the topics holding a
 * retained message so that all the filters of the SUBSCRIBE can be matched
 * against them after the SUBACK has been packed, as required by the MQTT
 * v3.1.1 spec when a subscription is made after a message has been retained
 */
static void retained_collect(struct trie_node *node, void *arg)
{
    if (!node || !node->data)
        return;
    struct topic *t = node->data;
    if (!t->retained)
        return;
    struct retained_collection *rc = arg;
    if (rc->len == rc->cap) {
        rc->cap    = rc->cap ? rc->cap * 2 : 16;
        rc->topics = try_realloc(rc->topics, rc->cap * sizeof(*rc->topics));
    }
    rc->topics[rc->len++] = t;
}

static int subscribe_handler(struct io_event *e)
{
    struct mqtt_subscribe *s = &e->data.subscribe;

    /*
     * We respond to the subscription request with SUBACK and a list of QoS in
     * the same exact order of reception
     */
    unsigned char rcs[s->tuples_len];
    struct client *c = e->client;

    /*
     * Subscription filters, kept to flush the retained messages matching them
     * right after the SUBACK has been sent
     */
    struct retained_delivery rd[s->tuples_len];
    unsigned rd_len = 0;

    /* Subscribe packets contains a list of topics and QoS tuples */
    for (unsigned i = 0; i < s->tuples_len; i++) {

        log_debug("Received SUBSCRIBE from %s", c->client_id);

        /*
         * MQTT v3.1.1 §3.8.4: a malformed filter is not an error for the
         * whole packet, it is answered with the failure code 0x80 in the
         * SUBACK slot reserved for this very tuple, leaving the other filters
         * of the same SUBSCRIBE untouched. Indexing it instead (as this used
         * to) installed patterns like "a/#/b" or "a+b" in the store that no
         * PUBLISH could ever match
         */
        if (!valid_topic_filter((const char *)s->tuples[i].topic,
                                s->tuples[i].topic_len)) {
            log_warning("Rejecting malformed topic filter from %s",
                        c->client_id);
            rcs[i] = MQTT_SUBACK_FAILURE;
            continue;
        }

        /*
         * A subscription the access control list does not allow is answered
         * with the failure code of §3.8.4, like a malformed filter: the rest
         * of the packet is served normally
         */
        if (!acl_check(c->client_id, c->username,
                       (const char *)s->tuples[i].topic, ACL_READ)) {
            log_warning("ACL: %s is not allowed to subscribe to %s",
                        c->client_id, s->tuples[i].topic);
            rcs[i] = MQTT_SUBACK_FAILURE;
            continue;
        }

        /*
         * Register the filter: validation, normalization and the store
         * mutation all live in session_subscribe(), the very code the
         * persistence loader reuses to rebuild a session read back from
         * disk. The store is always mutated with the global mutex held first
         * and the client mutex second, the canonical order shared with
         * publish_message() delivery, otherwise an ABBA deadlock between a
         * SUBSCRIBE and a concurrent PUBLISH would be possible
         */
#if THREADSNR > 0
        pthread_mutex_lock(&mutex);
        pthread_mutex_lock(&c->mutex);
#endif
        bool registered = session_subscribe(e->client->session,
                                            (const char *)s->tuples[i].topic,
                                            s->tuples[i].qos);
        if (!registered) {
            log_warning("Rejecting malformed topic filter from %s",
                        c->client_id);
            rcs[i] = MQTT_SUBACK_FAILURE;
#if THREADSNR > 0
            pthread_mutex_unlock(&mutex);
            pthread_mutex_unlock(&c->mutex);
#endif
            continue;
        }

        /*
         * The filter is kept the way the store keys it (trailing '/', the
         * '#' of a multilevel filter stripped) to match the retained
         * messages against it after the SUBACK: the same normalization
         * session_subscribe() just applied
         */
        char topic[s->tuples[i].topic_len + 2];
        snprintf(topic, s->tuples[i].topic_len + 1, "%s", s->tuples[i].topic);
        bool wildcard =
            normalize_filter(topic, (const char *)s->tuples[i].topic);
        log_debug("\t%s (QoS %i)", topic, s->tuples[i].qos);

        /*
         * Retained messages matching the subscriptions must be flushed after
         * the SUBACK, keep a copy of every filter to scan the store for them
         * once the SUBACK has been packed
         */
        rd[rd_len].filter     = try_strdup(topic);
        rd[rd_len].multilevel = wildcard;
        rd[rd_len].qos        = s->tuples[i].qos;
        rd_len++;
#if THREADSNR > 0
        pthread_mutex_unlock(&mutex);
        pthread_mutex_unlock(&c->mutex);
#endif
        rcs[i] = s->tuples[i].qos;
    }

    struct mqtt_packet pkt = {.header = (union mqtt_header){.byte = SUBACK_B}};
    mqtt_suback(&pkt, s->pkt_id, rcs, s->tuples_len);

#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
    pthread_mutex_lock(&c->mutex);
#endif
    size_t len = mqtt_size(&pkt, NULL);
    if (outgoing_reserve(c, len)) {
        mqtt_pack(&pkt, c->wbuf + c->towrite);
        c->towrite += len;
    }

    /*
     * Retained messages matching the subscriptions are published right after
     * the SUBACK: the store is walked a single time collecting the topics
     * holding a retained message and every filter is then matched against
     * them. The output is identical to scanning the whole store once per
     * filter, but the cost no longer grows with filters x topics
     */
    struct retained_collection rc = {NULL, 0, 0};
    topic_store_map(server.store, NULL, retained_collect, &rc);
    /*
     * A filter set covering the same topic twice ("a/b" and "a/+") must still
     * deliver its retained message once, exactly like the live path does,
     * where a session holds a single subscriber entry per topic
     */
    bool *sent = try_calloc(rc.len ? rc.len : 1, sizeof(*sent));
    for (unsigned i = 0; i < rd_len; i++) {
        for (size_t j = 0; j < rc.len; j++) {
            if (sent[j])
                continue;
            struct topic *t = rc.topics[j];
            if (match_subscription(t->name, rd[i].filter,
                                   rd[i].multilevel) != SOL_OK)
                continue;
            deliver_retained(c, t, rd[i].qos);
            sent[j] = true;
        }
        free_memory((char *)rd[i].filter);
    }
    free_memory(sent);
    free_memory(rc.topics);
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
    pthread_mutex_unlock(&c->mutex);
#endif

    log_debug("Sending SUBACK to %s", c->client_id);

    mqtt_packet_destroy(&pkt);

    return REPLY;
}

/*
 * The session keeps the list of the topics it holds a subscriber entry on:
 * it is what the teardown walks to detach them all, and what tells the topic
 * store which nodes are still referenced. Both helpers keep it a *set*,
 * exactly mirroring the entries that exist at any moment
 */
static void session_track_topic(struct client_session *s, struct topic *t)
{
    list_foreach(item, s->subscriptions)
    {
        if (item->data == t)
            return;
    }
    list_push(s->subscriptions, t);
}

/* Comparator for list_remove_node(): 0 marks the node to unlink */
static int topic_ptr_cmp(const void *node_ptr, const void *topic_ptr)
{
    return ((const struct list_node *)node_ptr)->data == topic_ptr ? 0 : 1;
}

static void session_untrack_topic(struct client_session *s, struct topic *t)
{
    struct list_node *node = list_remove_node(s->subscriptions, t, topic_ptr_cmp);
    free_memory(node);
}

/*
 * Drop the explicit (exact topic) subscription of a client from a topic: the
 * entry survives when a wildcard subscription also covers the same topic, so
 * that UNSUBSCRIBE stops only what it names
 */
static void detach_exact(struct topic *t, struct client_session *session)
{
    struct subscriber *sub = NULL;

    if (!session)
        return;
    HASH_FIND_STR(t->subscribers, session->session_id, sub);
    if (!sub)
        return;
    sub->origins &= ~SUBSCRIBER_EXACT;
    if (sub->origins != 0)
        return;
    HASH_DEL(t->subscribers, sub);
    DECREF(sub, struct subscriber);
    session_untrack_topic(session, t);
}

/*
 * Stop the wildcard deliveries a filter drives: every session topic matching
 * it loses its WILD origin (the filter record on its own trie node included),
 * entries still held by an exact subscription stay alive and keep delivering
 */
static void detach_wild_attached(struct client_session *session,
                                 const char *filter, bool multilevel)
{
    if (!session || !session->subscriptions)
        return;
    /*
     * The walk is done by hand instead of with list_foreach: an entry that
     * loses its last origin is removed from this very list, and the macro
     * reads the next pointer of the node it just visited, which would be
     * freed memory by then
     */
    struct list_node *item = session->subscriptions->head;
    while (item) {
        struct list_node *next = item->next;
        struct topic *t        = item->data;
        if (match_subscription(t->name, filter, multilevel) == SOL_OK) {
            struct subscriber *sub = NULL;
            HASH_FIND_STR(t->subscribers, session->session_id, sub);
            if (sub && (sub->origins & SUBSCRIBER_WILD)) {
                sub->origins &= ~SUBSCRIBER_WILD;
                if (sub->origins == 0) {
                    HASH_DEL(t->subscribers, sub);
                    DECREF(sub, struct subscriber);
                    session_untrack_topic(session, t);
                }
            }
        }
        item = next;
    }
}

static int unsubscribe_handler(struct io_event *e)
{

    struct client *c = e->client;

    log_debug("Received UNSUBSCRIBE from %s", c->client_id);

#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
    pthread_mutex_lock(&c->mutex);
#endif
    for (int i = 0; i < e->data.unsubscribe.tuples_len; ++i) {
        const char *raw = (const char *)e->data.unsubscribe.tuples[i].topic;
        size_t rawlen   = e->data.unsubscribe.tuples[i].topic_len;

        /*
         * MQTT v3.1.1 §4.7.1: UNSUBACK carries no per-tuple return code, so a
         * malformed filter is simply skipped. Normalizing it anyway would map
         * an empty string to the bare "/" key and detach the whole root of
         * the tree, taking every other subscription of this client with it
         */
        if (!valid_topic_filter(raw, rawlen)) {
            log_warning("Ignoring malformed topic filter in UNSUBSCRIBE "
                        "from %s",
                        c->client_id);
            continue;
        }

        /*
         * The filter is normalized exactly like SUBSCRIBE does, both must
         * address the very same trie node and wildcard index entry
         */
        char filter[strlen(raw) + 2];
        bool multilevel = normalize_filter(filter, raw);

        if (multilevel || index(filter, '+')) {
            /*
             * A wildcard filter: drop it from the index so that no further
             * publish attaches the client to new topics, then stop the
             * deliveries it already drives on the topics attached so far
             */
            topic_store_remove_wildcard_filter(server.store, filter,
                                                c->client_id);
            detach_wild_attached(c->session, filter, multilevel);
        } else {
            struct topic *t = topic_store_get(server.store, filter);
            if (t)
                detach_exact(t, c->session);
        }
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif

    persistence_mark_dirty();
    if (outgoing_reserve(c, MQTT_ACK_LEN)) {
        mqtt_pack_mono(c->wbuf + c->towrite, UNSUBACK,
                       e->data.unsubscribe.pkt_id);
        c->towrite += MQTT_ACK_LEN;
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif

    log_debug("Sending UNSUBACK to %s", c->client_id);

    /*
     * The packet is deliberately NOT destroyed here: process_message()
     * owns io.data and releases it once the REPLY has been enqueued,
     * destroying it in the handler as well would free the tuples twice
     * and abort the broker on the second free
     */

    return REPLY;
}

/*
 * Callback invoked for every wildcard subscription matching the topic being
 * published, `arg` is the topic the subscriber has to be attached to: the
 * subscriber is lazily attached so that it takes part to this delivery (and
 * to the following ones)
 */
static void wildcard_match(struct subscription *s, void *arg)
{
    struct topic *t                  = arg;
    struct client_session *session   = s->subscriber->session;
    struct subscriber *ex            = NULL;

    /*
     * MQTT v3.1.1 §4.7.2: a wildcard filter must not match topics starting
     * with '$'. The wildcard index drives this callback for every filter
     * compatible with the published topic, so the rule is enforced here as
     * well or a subscriber of '#' would receive the $SOL statistics
     */
    if (t->name[0] == '$' && (s->topic[0] == '#' || s->topic[0] == '+'))
        return;

    HASH_FIND_STR(t->subscribers, session->session_id, ex);
    if (ex) {
        /*
         * Already attached (typically by an exact SUBSCRIBE of this very
         * topic): remember that a wildcard covers it too, so that an
         * UNSUBSCRIBE of the filter stops only what it names. The topic is
         * already tracked by the exact subscription that attached it
         */
        ex->origins |= SUBSCRIBER_WILD;
        return;
    }
    /*
     * We need to make a copy of the subscriber cause UTHASH needs
     * a proper handle to work correctly, otherwise we'll end up
     * freeing the same refernce on disconnect and break the table
     */
    struct subscriber *copy = subscriber_clone(s->subscriber);
    copy->origins           = SUBSCRIBER_WILD;
    INCREF(copy, struct subscriber);
    HASH_ADD_STR(t->subscribers, id, copy);
    session_track_topic(session, t);
}

/*
 * Deliver the Last Will message: it mirrors the store interaction a normal
 * PUBLISH performs (trailing '/' topic key, wildcard subscribers attached to
 * the concrete topic) instead of bypassing it, which is what used to leave
 * the will undelivered. The locks are released before the delivery, which
 * acquires the global mutex itself
 */
void publish_lwt(struct mqtt_packet *will)
{
    struct mqtt_publish *p = &will->publish;

    if (!p->topic || p->topiclen == 0)
        return;

    char topic[p->topiclen + 2];
    if (p->topic[p->topiclen - 1] != '/')
        snprintf(topic, p->topiclen + 2, "%s/", (const char *)p->topic);
    else
        snprintf(topic, p->topiclen + 1, "%s", (const char *)p->topic);

#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    struct topic *t = topic_store_get_or_put(server.store, topic);
    if (!topic_store_wildcards_empty(server.store))
        topic_store_match_wildcards(server.store, topic, wildcard_match, t);
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif

    if (t)
        publish_message(will, t);
}

static int publish_handler(struct io_event *e)
{

    struct client *c        = e->client;
    union mqtt_header *hdr  = &e->data.header;
    struct mqtt_publish *p  = &e->data.publish;
    unsigned short orig_mid = p->pkt_id;

    log_debug(
        "Received PUBLISH from %s (d%i, q%u, r%i, m%u, %s, ... (%llu bytes))",
        c->client_id, hdr->bits.dup, hdr->bits.qos, hdr->bits.retain, p->pkt_id,
        p->topic, p->payloadlen);

    info.messages_recv++;

    char topic[p->topiclen + 2];
    unsigned char qos = hdr->bits.qos;

    /*
     * QoS 2 is exactly-once (§4.3.3): an identifier still open in the
     * session's inbound set has already been delivered below and answered
     * with a PUBREC, only the PUBREL is outstanding. A retransmission of
     * that very PUBLISH (the client never saw the PUBREC and resends with
     * DUP) must not reach the subscribers a second time: it only earns the
     * PUBREC again, and pubrel_handler() closes the flow when the PUBREL
     * lands. The set lives in the session, so a persistent session keeps
     * deduplicating across a reconnect where the client retransmits what it
     * still had in flight. Duplicates are recognised by the identifier alone
     */
    if (qos == EXACTLY_ONCE && orig_mid != 0 && c->session) {
#if THREADSNR > 0
        pthread_mutex_lock(&c->mutex);
#endif
        bool dup = inbound_q2_find(c->session, orig_mid);
        if (dup && outgoing_reserve(c, MQTT_ACK_LEN)) {
            mqtt_pack_mono(c->wbuf + c->towrite, PUBREC, orig_mid);
            c->towrite += MQTT_ACK_LEN;
        }
#if THREADSNR > 0
        pthread_mutex_unlock(&c->mutex);
#endif
        if (dup) {
            log_debug("Duplicate QoS 2 PUBLISH from %s (m%u), PUBREC resent",
                      c->client_id, orig_mid);
            /*
             * The topic and payload unpacked from this duplicate are still
             * owned here: no packet was built to take them over and
             * process_message() never destroys a PUBLISH, so release them
             * now or every retransmission would leak them
             */
            mqtt_packet_destroy(&e->data);
            return REPLY;
        }
    }

    /*
     * For convenience we assure that all topics ends with a '/', indicating a
     * hierarchical level
     */
    if (p->topic[p->topiclen - 1] != '/')
        snprintf(topic, p->topiclen + 2, "%s/", (const char *)p->topic);
    else
        snprintf(topic, p->topiclen + 1, "%s", (const char *)p->topic);

    /*
     * MQTT has no way to refuse a PUBLISH: a topic the access control list
     * does not allow is dropped here, and the acknowledgement still flows so
     * the client is not left retransmitting it forever. The client sees a
     * publish that went through and reached nobody
     */
    if (!acl_check(c->client_id, c->username, (const char *)p->topic,
                   ACL_WRITE)) {
        log_warning("ACL: %s is not allowed to publish to %s, message dropped",
                    c->client_id, p->topic);
        goto ack_only;
    }

#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
    pthread_mutex_lock(&c->mutex);
#endif
    /*
     * Retrieve the topic from the global map, if it wasn't created before,
     * create a new one with the name selected
     */
    struct topic *t = topic_store_get_or_put(server.store, topic);

    /*
     * Attach to the topic every subscriber of a matching wildcard
     * subscription ('+' or '#'), the index only walks the levels of this
     * topic instead of scanning all the registered filters
     */
    if (!topic_store_wildcards_empty(server.store))
        topic_store_match_wildcards(server.store, topic, wildcard_match, t);
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif

    struct mqtt_packet *pkt = mqtt_packet_alloc(e->data.header.byte);
    /*
     * A deliberate shallow copy, not a missing deep copy: it transfers
     * ownership of the topic/payload heap buffers to the refcounted pkt,
     * which frees them in mqtt_packet_destroy(). process_message() never
     * destroys a PUBLISH io.data precisely so these buffers are released
     * exactly once, here, when pkt's refcount drops to zero. Deep-copying
     * would leave the original e->data.publish buffers unreachable and leak
     * them on every message
     */
    pkt->publish            = e->data.publish;

    /*
     * The retained message is store state: it is written with the global
     * mutex still held (released just after) and with the client mutex
     * already dropped, delivery below re-acquires the global one itself.
     * A new retained message replaces the previous one (a zero length
     * payload clears it, as MQTT requires): the old packet is released
     * first or it would be silently lost on every overwrite. The copy is
     * what makes it survive: `pkt` keeps travelling the delivery path below
     * and is mutated by it (QoS per subscriber, packet identifier on
     * retransmissions), while the store needs the message exactly as the
     * publisher left it
     */
    if (hdr->bits.retain == 1) {
        if (t->retained) {
            DECREF(t->retained, struct mqtt_packet);
            t->retained = NULL;
        }
        if (p->payloadlen > 0)
            t->retained = mqtt_publish_copy(pkt);
        persistence_mark_dirty();
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif

    if (publish_message(pkt, t) == 0)
        DECREF(pkt, struct mqtt_packet);

ack_only:
    // We have to answer to the publisher
    if (qos == AT_MOST_ONCE)
        goto exit;

    int ptype = qos == EXACTLY_ONCE ? PUBREC : PUBACK;

#if THREADSNR > 0
    pthread_mutex_lock(&c->mutex);
#endif
    /*
     * The reply is built by mqtt_pack_mono() straight from `ptype` and the
     * identifier of the incoming packet: the io_event union is not touched
     * here, its publish member is still the one the delivery above owns
     */
    if (outgoing_reserve(c, MQTT_ACK_LEN)) {
        mqtt_pack_mono(c->wbuf + c->towrite, ptype, orig_mid);
        c->towrite += MQTT_ACK_LEN;
    }
    /*
     * Open the exactly-once flow: while this identifier sits in the set, a
     * retransmitted PUBLISH naming it is a duplicate to be answered with the
     * PUBREC only, and the PUBREL closes the entry (identifier 0 can never
     * name a flow, §2.2.1, so it is never tracked and behaves as before)
     */
    if (qos == EXACTLY_ONCE && orig_mid != 0 && c->session)
        inbound_q2_track(c->session, orig_mid);
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    log_debug("Sending %s to %s (m%u)", ptype == PUBACK ? "PUBACK" : "PUBREC",
              c->client_id, orig_mid);
    return REPLY;

exit:

    /*
     * We're in the case of AT_MOST_ONCE QoS level, we don't need to send out
     * any byte, it's a fire-and-forget.
     */
    return NOREPLY;
}

static int puback_handler(struct io_event *e)
{
    struct client *c = e->client;
    unsigned pkt_id  = e->data.ack.pkt_id;
    log_debug("Received PUBACK from %s (m%u)", c->client_id, pkt_id);
#if THREADSNR > 0
    pthread_mutex_lock(&c->mutex);
#endif
    /*
     * A PUBACK may refer to a message we're not tracking anymore (retained
     * messages are flushed without an inflight entry, or the entry has been
     * cleared by a duplicate acknowledgement): in that case there's nothing
     * to release
     */
    if (!c->session) {
#if THREADSNR > 0
        pthread_mutex_unlock(&c->mutex);
#endif
        return NOREPLY;
    }
    if (c->session->i_msgs[pkt_id].packet) {
        inflight_msg_clear(&c->session->i_msgs[pkt_id]);
        --c->session->inflights;
    }
    c->session->i_msgs[pkt_id].packet = NULL;
    c->session->i_acks[pkt_id]        = -1;
    /* Drop the identifier from the list the retransmission scan walks */
    inflight_untrack_mid(c->session, pkt_id);
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    return NOREPLY;
}

static int pubrec_handler(struct io_event *e)
{
    struct client *c = e->client;
    unsigned pkt_id  = e->data.ack.pkt_id;
    log_debug("Received PUBREC from %s (m%u)", c->client_id, pkt_id);
#if THREADSNR > 0
    pthread_mutex_lock(&c->mutex);
#endif
    if (outgoing_reserve(c, MQTT_ACK_LEN)) {
        mqtt_pack_mono(c->wbuf + c->towrite, PUBREL, pkt_id);
        c->towrite += MQTT_ACK_LEN;
    }
    /*
     * Update the inflight acks table while still holding the client mutex:
     * the periodic retransmission check reads and rewrites the very same
     * entry under this lock, writing it after the unlock would race with it
     * and could be overwritten (or missed) by the scan
     */
    if (c->session) {
        /*
         * MQTT v3.1.1 §3.4.4-2: the PUBREC acknowledges the PUBLISH itself,
         * from now on only the PUBREL is (re)transmitted until its PUBCOMP
         * arrives. Keeping the packet stored made the retransmission scan
         * send the PUBLISH again with the DUP flag on top of the PUBREL —
         * a duplicate the spec reserves to a lost PUBREC. The inflight
         * counter is intentionally not dropped here: it counts the
         * identifiers still tracked (PUBACK or PUBCOMP pending), and the
         * scan only walks clients with has_inflight() set
         */
        if (c->session->i_msgs[pkt_id].packet)
            inflight_msg_clear(&c->session->i_msgs[pkt_id]);
        c->session->i_msgs[pkt_id].packet = NULL;
        c->session->i_acks[pkt_id]        = time(NULL);
        inflight_track_mid(c->session, pkt_id);
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    log_debug("Sending PUBREL to %s (m%u)", c->client_id, pkt_id);
    return REPLY;
}

static int pubrel_handler(struct io_event *e)
{
    struct client *c = e->client;
    unsigned pkt_id  = e->data.ack.pkt_id;
    log_debug("Received PUBREL from %s (m%u)", c->client_id, pkt_id);
#if THREADSNR > 0
    pthread_mutex_lock(&c->mutex);
#endif
    /*
     * The PUBREL completes the inbound exactly-once flow (§4.3.3): drop the
     * identifier so a later PUBLISH reusing it is a fresh message again. A
     * PUBREL for an identifier that is not open (duplicate PUBREL, stray
     * packet) is a no-op here and still earns the PUBCOMP below, which is
     * exactly what the spec wants for a retransmitted PUBREL
     */
    if (c->session)
        inbound_q2_untrack(c->session, (unsigned short) pkt_id);
    if (outgoing_reserve(c, MQTT_ACK_LEN)) {
        mqtt_pack_mono(c->wbuf + c->towrite, PUBCOMP, pkt_id);
        c->towrite += MQTT_ACK_LEN;
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    log_debug("Sending PUBCOMP to %s (m%u)", c->client_id, pkt_id);
    return REPLY;
}

static int pubcomp_handler(struct io_event *e)
{
    struct client *c = e->client;
    unsigned pkt_id  = e->data.ack.pkt_id;
    log_debug("Received PUBCOMP from %s (m%u)", c->client_id, pkt_id);
#if THREADSNR > 0
    pthread_mutex_lock(&c->mutex);
#endif
    /* A PUBCOMP can only arrive after a CONNECT, guard the session anyway */
    if (!c->session) {
#if THREADSNR > 0
        pthread_mutex_unlock(&c->mutex);
#endif
        return NOREPLY;
    }
    /*
     * The stored PUBLISH was already released by pubrec_handler (the PUBREC
     * acknowledges the message itself, from then on only the PUBREL is
     * retransmitted), so i_msgs[pkt_id].packet is NULL by the time the
     * PUBCOMP lands. The signal that this identifier is still counted as
     * inflight is the PUBREL-pending timestamp armed by pubrec_handler:
     * gating the decrement on the packet pointer leaked one inflight per
     * QoS 2 message and left has_inflight() true forever, making the
     * periodic scan keep walking a session with nothing left in flight
     */
    bool inflight = c->session->i_acks[pkt_id] > 0;
    if (c->session->i_msgs[pkt_id].packet) {
        inflight_msg_clear(&c->session->i_msgs[pkt_id]);
        inflight = true;
    }
    c->session->i_msgs[pkt_id].packet = NULL;
    c->session->i_acks[pkt_id]        = -1;
    /* Release the inflight slot taken by publish_message(), guarded against
     * a stray or duplicate PUBCOMP wrapping the unsigned counter */
    if (inflight && c->session->inflights > 0)
        --c->session->inflights;
    /* Drop the identifier from the list the retransmission scan walks */
    inflight_untrack_mid(c->session, pkt_id);
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    return NOREPLY;
}

static int pingreq_handler(struct io_event *e)
{
    log_debug("Received PINGREQ from %s", e->client->client_id);
    e->data.header.byte = PINGRESP_B;
#if THREADSNR > 0
    pthread_mutex_lock(&e->client->mutex);
#endif
    if (outgoing_reserve(e->client, MQTT_HEADER_LEN)) {
        mqtt_pack(&e->data, e->client->wbuf + e->client->towrite);
        e->client->towrite += MQTT_HEADER_LEN;
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&e->client->mutex);
#endif
    log_debug("Sending PINGRESP to %s", e->client->client_id);
    return REPLY;
}

/*
 * A client sent a packet that only the broker is allowed to send (or a
 * reserved type): there's no handler for it, log the violation and ask the
 * caller to drop the connection instead of dereferencing a NULL slot.
 */
static int protocol_violation_handler(struct io_event *e)
{
    log_error("Protocol violation: packet type %u received from %s is only "
              "valid server to client",
              e->data.header.bits.type, e->client->client_id);
    return -ERRPACKETERR;
}

/*
 * This is the only public API we expose from this module beside
 * publish_message. It just give access to handlers mapped by message type.
 */
int handle_command(unsigned type, struct io_event *event)
{
    /* The type is 4 bits wide, still never index the table out of bounds */
    if (type >= sizeof(handlers) / sizeof(*handlers))
        return protocol_violation_handler(event);

    /*
     * MQTT v3.1.1 §3.1: the first packet a client sends MUST be a CONNECT
     * (and a client whose CONNECT was refused never becomes connected).
     * Without this guard a SUBSCRIBE sent before CONNECT would reach a
     * handler that dereferences the still missing session: a remote client
     * could crash the broker without authenticating, and a PUBLISH sent
     * before CONNECT would sidestep the allow_anonymous check entirely
     */
    if (type != CONNECT && event->client->connected == false) {
        log_error("Protocol violation: packet type %u received from %s "
                  "before CONNECT",
                  type, event->client->client_id[0] ? event->client->client_id
                                                    : "unknown");
        return -ERRPACKETERR;
    }

    return handlers[type](event);
}
