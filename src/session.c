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

/* Session ownership, offline queues, packet identifiers, and inflight state. */
#include "handlers.h"
#include "handlers_internal.h"
#include "acl.h"
#include "config.h"
#include "logging.h"
#include "memory.h"
#include "mqtt.h"
#include "persistence.h"
#include "server.h"
#include "sol_internal.h"
#include "topic_match.h"
#include <stdio.h>
#include <time.h>

static void list_drop_prefix(List *, unsigned long);

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
     * Release the packets still awaiting an acknowledgement. The table only
     * holds the identifiers actually in flight, so this walks the messages
     * themselves rather than the whole 1-65535 space
     */
    struct inflight_msg *imsg, *itmp;
    HASH_ITER(hh, session->i_msgs, imsg, itmp)
    {
        if (imsg->packet) {
            DECREF(imsg->packet, struct mqtt_packet);
            imsg->packet = NULL;
        }
        HASH_DEL(session->i_msgs, imsg);
        free_memory(imsg);
    }
    free_memory(session->pending_mids);
    free_memory(session->q2_recv_mids);
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
    session->i_msgs       = NULL;
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

struct client_session *client_session_alloc(struct server *server,
                                                   const char *session_id)
{
    struct client_session *session = try_alloc(sizeof(*session));
    session_init(session, session_id);
    session->server = server;
    return session;
}

/*
 * Create a persistent session and register it in the server map: the entry
 * point the persistence loader uses to rebuild a session it read back from
 * disk. It runs before the event loops exist, so it takes no lock, and the
 * session starts exactly like the one a clean_session=false CONNECT leaves
 * behind when its client disconnects
 */
struct client_session *session_restore(struct server *server,
                                       const char *session_id)
{
    struct client_session *session = client_session_alloc(server, session_id);
    session->clean_session         = false;
    INCREF(session, struct client_session);
    HASH_ADD_STR(server->sessions, session_id, session);
    return session;
}

/*
 * Tear a session down: drop its wildcard index entries, detach every
 * subscriber record it still holds on the topics it is tracked on, remove it
 * from the map and release it. This is the body of both the clean session
 * teardown and the persistence `session-del` replay, so the two paths can
 * never drift apart. The caller must hold the global mutex (or be the
 * startup loader, which runs before the event loops exist)
 */
void session_delete(struct server *server, const char *session_id)
{
    if (!session_id || !session_id[0])
        return;

    struct client_session *s = NULL;
    HASH_FIND_STR(server->sessions, session_id, s);
    if (!s)
        return;

    topic_store_remove_wildcard(server->store, session_id);
    list_foreach(item, s->subscriptions)
    {
        struct topic *t        = item->data;
        struct subscriber *sub = NULL;
        HASH_FIND_STR(t->subscribers, session_id, sub);
        if (sub) {
            HASH_DEL(t->subscribers, sub);
            DECREF(sub, struct subscriber);
        }
    }
    HASH_DEL(server->sessions, s);
    DECREF(s, struct client_session);
    /* A session that goes away is a record the log and the file must not
       keep. The loader suppresses this while replaying a session-del */
    persistence_wal_session_del(server, session_id);
    persistence_mark_dirty();
}

/*
 * Keep the pending list in sync with the i_msgs hash: the periodic
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
bool inbound_q2_find(const struct client_session *session,
                            unsigned short mid)
{
    for (unsigned i = 0; i < session->q2_recv_len; ++i)
        if (session->q2_recv_mids[i] == mid)
            return true;
    return false;
}

void inbound_q2_track(struct client_session *session, unsigned short mid)
{
    struct server *server = session->server;
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
    persistence_wal_q2(server, session->session_id, mid);
}

void inbound_q2_untrack(struct client_session *session,
                               unsigned short mid)
{
    struct server *server = session->server;
    for (unsigned i = 0; i < session->q2_recv_len; ++i) {
        if (session->q2_recv_mids[i] == mid) {
            /* Swap with the last element: the order is not significant */
            session->q2_recv_mids[i] =
                session->q2_recv_mids[--session->q2_recv_len];
            persistence_wal_q2_del(server, session->session_id, mid);
            return;
        }
    }
}

/*
 * Look up the inflight entry of a packet identifier, NULL when the identifier
 * is free. The table is a hash keyed by mid: the old code indexed two arrays
 * of 65536 slots, which cost ~2MB per session before a single message was in
 * flight
 */
struct inflight_msg *session_inflight(struct client_session *session,
                                      unsigned short mid)
{
    struct inflight_msg *imsg = NULL;
    HASH_FIND(hh, session->i_msgs, &mid, sizeof(mid), imsg);
    return imsg;
}

/*
 * Return a packet identifier not currently awaiting an acknowledgement.
 * Reusing one that is still in flight would overwrite its inflight entry,
 * losing the reference to the message and its retransmission state. The
 * cursor makes the common case (a sparse table) a single lookup; the loop
 * only walks the identifiers that are taken, which is bounded by the number
 * of messages in flight
 */
unsigned short next_free_mid(struct client_session *session)
{
    for (unsigned tries = 0; tries < MAX_INFLIGHT_MSGS - 1; ++tries) {
        unsigned mid = session->next_free_mid;
        session->next_free_mid =
            (mid + 1 >= MAX_INFLIGHT_MSGS) ? 1 : (unsigned)(mid + 1);
        if (!session_inflight(session, (unsigned short)mid))
            return (unsigned short)mid;
    }
    /* Identifier space exhausted, the client is not acknowledging */
    return 0;
}

/*
 * Register a new inflight entry for `mid`, taking ownership of one reference
 * to `p`. The identifier must be free (next_free_mid() just returned it)
 */
void inflight_msg_init(struct client_session *s, unsigned short mid,
                              struct mqtt_packet *p)
{
    struct inflight_msg *imsg = try_alloc(sizeof(*imsg));
    imsg->seen                = time(NULL);
    imsg->ack_sent            = 0;
    imsg->packet              = p;
    imsg->qos                 = p->header.bits.qos;
    imsg->mid                 = mid;
    HASH_ADD(hh, s->i_msgs, mid, sizeof(imsg->mid), imsg);
    inflight_track_mid(s, mid);
    ++s->inflights;
}

/*
 * Arm the PUBREL retransmission of a QoS 2 delivery: the PUBREC acknowledged
 * the PUBLISH, so its packet is released and only the identifier (with the
 * time of the last PUBREL) stays. Creates the entry when the PUBREC names an
 * identifier the session does not know, which keeps the old array behaviour
 * (a stray but well formed PUBREC still earns its PUBREL and PUBCOMP)
 */
void inflight_ack_init(struct client_session *s, unsigned short mid)
{
    struct server *server     = s->server;
    time_t now                = time(NULL);
    struct inflight_msg *imsg = session_inflight(s, mid);
    if (imsg) {
        if (imsg->packet) {
            inflight_msg_clear(imsg);
            imsg->packet = NULL;
        }
        imsg->ack_sent = now;
    } else {
        imsg           = try_alloc(sizeof(*imsg));
        imsg->seen     = 0;
        imsg->ack_sent = now;
        imsg->packet   = NULL;
        imsg->qos      = 0;
        imsg->mid      = mid;
        HASH_ADD(hh, s->i_msgs, mid, sizeof(imsg->mid), imsg);
        inflight_track_mid(s, mid);
        ++s->inflights;
    }
    /* The PUBLISH half is gone, only the PUBREL timestamp is left to keep */
    persistence_wal_ack(server, s->session_id, mid, now);
}

/*
 * Drop an inflight entry, releasing the packet it owns (when it still has
 * one). No-op when the identifier is not tracked, which is what duplicate or
 * stray acknowledgements look like
 */
void inflight_msg_remove(struct client_session *s, unsigned short mid)
{
    struct server *server     = s->server;
    struct inflight_msg *imsg = session_inflight(s, mid);
    if (!imsg)
        return;
    if (imsg->packet) {
        inflight_msg_clear(imsg);
        imsg->packet = NULL;
    }
    HASH_DEL(s->i_msgs, imsg);
    free_memory(imsg);
    inflight_untrack_mid(s, mid);
    if (s->inflights > 0)
        --s->inflights;
    persistence_wal_inflight_del(server, s->session_id, mid);
}

/*
 * Queue a message for a session that is not connected: the queue entry keeps
 * the packet pointer and the identifier it was given, the inflight entry owns
 * the reference that keeps the packet alive and the pending list is kept in
 * sync. The PUBREL timestamp stays zero: a queued message is still a PUBLISH
 * waiting for its first acknowledgement, not a QoS 2 flow half completed
 */
void queue_offline_msg(struct client_session *s, struct mqtt_packet *pkt,
                              u16 mid)
{
    struct server *server = s->server;
    struct queued_msg *qm = try_alloc(sizeof(*qm));
    /*
     * The identifier travels with the entry and has to be in the packet too:
     * it is what the reconnect packs into the replayed PUBLISH
     */
    pkt->publish.pkt_id = mid;
    qm->packet          = pkt;
    qm->mid             = mid;
    /* The packet is shared with the other subscribers of the same publish */
    qm->qos = pkt->header.bits.qos;
    /* Appended at the back, drained in order on reconnect */
    list_push_back(s->outgoing_msgs, qm);
    inflight_msg_init(s, mid, pkt);
    persistence_wal_queued(server, s->session_id,
                           (const char *)pkt->publish.topic, qm->qos, mid,
                           pkt->publish.payload, pkt->publish.payloadlen);
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
 * Same as session_queue_message(), with the identifier read back from the
 * file. Restoring the identifier rather than taking a fresh one keeps the
 * queue and the inflight table pointing at the same entry, which is what a
 * later `dropped` or `flush` record names
 */
void session_restore_queued(struct client_session *s, const char *topic,
                            unsigned char qos, unsigned short mid,
                            const unsigned char *payload, size_t len)
{
    if (!s || !topic || mid == 0)
        return;

    /*
     * A record may replace an entry left by an earlier line. The queue slot
     * and the inflight entry are separate: a `flush` in the log may have
     * left only the inflight one, and adding the identifier twice to the
     * hash would make every later lookup ambiguous
     */
    session_restore_dropped(s, mid);
    inflight_msg_remove(s, mid);

    struct mqtt_packet *pkt = mqtt_packet_alloc(PUBLISH_B);
    pkt->header.bits.qos    = qos;
    pkt->publish            = (struct mqtt_publish){
        .topiclen   = strlen(topic),
        .topic      = dup_bytes((const unsigned char *)topic, strlen(topic)),
        .payloadlen = len,
        .payload    = dup_bytes(payload, len)};
    /* Owned by the inflight entry the queue call below fills in */
    INCREF(pkt, struct mqtt_packet);
    queue_offline_msg(s, pkt, mid);
}

/*
 * Rebuild one inflight entry: the packet is copied from the file and the
 * entry is marked as due immediately (`seen` zero), so the first
 * retransmission scan after the client reconnects sends it again with the
 * DUP flag set
 */
void session_restore_inflight(struct client_session *s, const char *topic,
                              unsigned char qos, unsigned short mid,
                              const unsigned char *payload, size_t len)
{
    if (!s || !topic || mid == 0)
        return;

    /* A record may replace an entry left by an earlier line */
    inflight_msg_remove(s, mid);

    struct mqtt_packet *pkt = mqtt_packet_alloc(PUBLISH_B);
    pkt->header.bits.qos    = qos;
    pkt->publish            = (struct mqtt_publish){
        .pkt_id     = mid,
        .topiclen   = strlen(topic),
        .topic      = dup_bytes((const unsigned char *)topic, strlen(topic)),
        .payloadlen = len,
        .payload    = dup_bytes(payload, len)};
    /* Owned by the inflight entry below */
    INCREF(pkt, struct mqtt_packet);
    inflight_msg_init(s, mid, pkt);

    struct inflight_msg *imsg = session_inflight(s, mid);
    if (imsg)
        imsg->seen = 0;
}

/*
 * Rebuild the PUBREL half of a QoS 2 flow (the PUBLISH was acknowledged, the
 * PUBCOMP is still outstanding) or replace the stored packet of an entry the
 * file named with an `ack` record
 */
void session_restore_ack(struct client_session *s, unsigned short mid,
                         time_t ack_sent)
{
    if (!s || mid == 0)
        return;

    /*
     * The stored timestamp is ignored on purpose: a restored PUBREL is due
     * immediately. The epoch is older than any retransmission interval, so
     * the first scan after the client reconnects sends it again, instead of
     * waiting up to a whole interval counted from before the restart
     */
    (void)ack_sent;
    struct inflight_msg *imsg = session_inflight(s, mid);
    if (imsg) {
        if (imsg->packet) {
            inflight_msg_clear(imsg);
            imsg->packet = NULL;
        }
        imsg->ack_sent = 1;
        return;
    }

    imsg           = try_alloc(sizeof(*imsg));
    imsg->seen     = 0;
    imsg->ack_sent = 1;
    imsg->packet   = NULL;
    imsg->qos      = 0;
    imsg->mid      = mid;
    HASH_ADD(hh, s->i_msgs, mid, sizeof(imsg->mid), imsg);
    inflight_track_mid(s, mid);
    ++s->inflights;
}

void session_restore_q2(struct client_session *s, unsigned short mid)
{
    if (s)
        inbound_q2_track(s, mid);
}

void session_restore_q2_del(struct client_session *s, unsigned short mid)
{
    if (s)
        inbound_q2_untrack(s, mid);
}

/* Comparator for list_remove_node(): 0 marks the node to unlink */
static int queued_mid_cmp(const void *node_ptr, const void *mid_ptr)
{
    const struct list_node *node = node_ptr;
    const struct queued_msg *qm  = node->data;
    return qm && qm->mid == *(const unsigned short *)mid_ptr ? 0 : 1;
}

/*
 * Remove the queued message named by `mid`, if any: the queue node and its
 * wrapper go away and the inflight entry that owns the packet is released
 */
static bool session_drop_mid(struct client_session *s, unsigned short mid)
{
    if (!s || !s->outgoing_msgs)
        return false;

    struct list_node *node =
        list_remove_node(s->outgoing_msgs, &mid, queued_mid_cmp);
    if (!node)
        return false;
    free_memory(node->data);
    free_memory(node);
    inflight_msg_remove(s, mid);
    return true;
}

void session_restore_inflight_del(struct client_session *s, unsigned short mid)
{
    if (s)
        inflight_msg_remove(s, mid);
}

void session_restore_dropped(struct client_session *s, unsigned short mid)
{
    session_drop_mid(s, mid);
}

/*
 * Forget the first `count` messages of the offline queue, keeping their
 * inflight entries: this is what a reconnect does once the messages have
 * been packed into the write buffer, they are still awaiting their
 * acknowledgements
 */
void session_flush_queue(struct client_session *s, unsigned long count)
{
    struct server *server = s ? s->server : NULL;
    if (!s || !s->outgoing_msgs)
        return;
    list_drop_prefix(s->outgoing_msgs, count);
    persistence_wal_flush(server, s->session_id, count);
    persistence_mark_dirty();
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
    struct server *server = s ? s->server : NULL;
    if (!s || !s->outgoing_msgs || !s->outgoing_msgs->head)
        return false;

    struct queued_msg *qm = s->outgoing_msgs->head->data;
    unsigned short mid    = qm ? qm->mid : 0;
    if (!session_drop_mid(s, mid))
        return false;
    /* The inflight entry is gone too: the log names the queue slot and the
       identifier in one record */
    persistence_wal_dropped(server, s->session_id, mid);
    persistence_mark_dirty();
    info.messages_dropped++;
    return true;
}

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
