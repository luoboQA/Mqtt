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

/*
 * One of the two exposed functions of the module, it's also needed on server
 * module to publish periodic messages (e.g. $SOL stats). It's responsible
 * of the normal publish but also taking care of disconnected clients, enqueuing
 * packets and setting up inflight messages for QoS > 0.
 * Returns the number of publish done or an error code in case of conditions
 * that requires de-allocation of the pkt argument occurs.
 */
int publish_message(struct server *server, struct mqtt_packet *pkt,
                    const struct topic *t)
{
    struct config *conf   = server->conf;
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
        HASH_FIND_STR(server->clients_map, s->session_id, sc);
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
            if (inflight_window_full(s)) {
                /*
                 * The per-session window is full: drop the message rather
                 * than grow the queue without bound. The log names the
                 * session so an operator can correlate it with a slow or
                 * stuck subscriber
                 */
                log_warning("Inflight window full (%u messages) for %s, "
                            "dropping PUBLISH to %s",
                            conf->max_inflight, s->session_id,
                            pkt->publish.topic);
                continue;
            }
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
            /*
             * The retransmission of the PUBLISH itself is driven by the
             * inflight entry above: its PUBREL timestamp stays zero until a
             * QoS 2 subscriber acknowledges with a PUBREC (see
             * pubrec_handler), so a QoS 1 flow never grows a PUBREL
             */
            inflight_msg_init(sc->session, mid, pkt);
            /* The packet is now tracked: a restart retransmits it on
               reconnect instead of losing it with the connection */
            persistence_wal_inflight(sc->session,
                                     session_inflight(sc->session, mid));
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
        if (!outgoing_reserve(sc, len)) {
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
 * PUBLISH performs (exact topic key, wildcard subscribers attached to the
 * concrete topic) instead of bypassing it, which is what used to leave the
 * will undelivered. The locks are released before the delivery, which
 * acquires the global mutex itself
 */
void publish_lwt(struct server *server, struct mqtt_packet *will)
{
    struct mqtt_publish *p = &will->publish;

    if (!p->topic || p->topiclen == 0)
        return;

    const char *topic = (const char *)p->topic;

#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    struct topic *t = topic_store_get_or_put(server->store, topic);
    if (!topic_store_wildcards_empty(server->store))
        topic_store_match_wildcards(server->store, topic, wildcard_match, t);
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif

    if (t)
        publish_message(server, will, t);
}

/*
 * Refill the client token bucket with the time elapsed since the previous
 * call and take one token. Returns false when the client is over its
 * max_publish_rate; the caller drops the packet but still acknowledges it.
 * The bucket starts full and holds at most one second worth of tokens, so a
 * client that stays quiet cannot accumulate an unbounded burst credit
 */
static bool publish_rate_allow(struct client *c)
{
    struct config *conf = c->server->conf;
    if (conf->max_publish_rate == 0)
        return true;

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    double now = (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
    if (c->rate_last > 0.0) {
        double elapsed = now - c->rate_last;
        if (elapsed > 0.0)
            c->rate_tokens += elapsed * (double)conf->max_publish_rate;
    }
    c->rate_last = now;
    if (c->rate_tokens > (double)conf->max_publish_rate)
        c->rate_tokens = (double)conf->max_publish_rate;
    if (c->rate_tokens < 1.0)
        return false;
    c->rate_tokens -= 1.0;
    return true;
}

/*
 * The session inflight window: true when the session already holds
 * max_inflight QoS 1/2 messages awaiting an acknowledgement. The counter
 * covers both the deliveries to a connected client and the messages queued
 * for an away one, which is what keeps a single slow subscriber from growing
 * its backlog without bound. A zero setting disables the cap
 */
bool inflight_window_full(const struct client_session *s)
{
    const struct config *conf = s->server->conf;
    return conf->max_inflight > 0 && s->inflights >= conf->max_inflight;
}

int publish_handler(struct io_event *e)
{
    struct server *server   = e->server;
    struct config *conf     = server->conf;
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
     * Server side rate limit: a client sending more than max_publish_rate
     * packets per second has the excess dropped. The QoS 1/2 acknowledgement
     * still flows through ack_only, because MQTT has no way to tell a client
     * to slow down and leaving the packet unacknowledged would only make it
     * retransmit the same message forever
     */
    if (!publish_rate_allow(c)) {
        log_warning("Publish rate limit (%u msg/s) exceeded for %s, dropping "
                    "PUBLISH to %s",
                    conf->max_publish_rate, c->client_id, p->topic);
        mqtt_packet_destroy(&e->data);
        goto ack_only;
    }

    /*
     * '$' is the broker's own namespace: the $SOL statistics are published
     * there and §4.7.2 already keeps wildcard filters away from it. A
     * client PUBLISH is dropped, but the QoS handshake still completes
     * (ack_only), otherwise a QoS 1/2 publisher would retransmit the same
     * refused message forever
     */
    if (p->topiclen > 0 && p->topic[0] == '$') {
        log_warning("Refusing PUBLISH to the reserved topic %s from %s",
                    p->topic, c->client_id);
        /* No packet takes over the strings unpacked from the wire, and
           process_message() never destroys a PUBLISH: release them here or
           every refused message would leak */
        mqtt_packet_destroy(&e->data);
        goto ack_only;
    }

    /*
     * The store keys the topic exactly as published: "a" and "a/" are two
     * different names (MQTT v3.1.1 §4.7), the old code appended a '/' and
     * made them collapse into the same node
     */
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
        /* The drop path owns the strings unpacked from the wire */
        mqtt_packet_destroy(&e->data);
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
    struct topic *t = topic_store_get_or_put(server->store, topic);

    /*
     * Attach to the topic every subscriber of a matching wildcard
     * subscription ('+' or '#'), the index only walks the levels of this
     * topic instead of scanning all the registered filters
     */
    if (!topic_store_wildcards_empty(server->store))
        topic_store_match_wildcards(server->store, topic, wildcard_match, t);
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
        if (p->payloadlen > 0) {
            t->retained = mqtt_publish_copy(pkt);
            persistence_wal_retained(server, topic, p->payload,
                                     p->payloadlen, qos);
        } else {
            persistence_wal_retained_clear(server, topic);
        }
        persistence_mark_dirty();
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif

    if (publish_message(server, pkt, t) == 0)
        DECREF(pkt, struct mqtt_packet);

ack_only:
    // We have to answer to the publisher
    if (qos == AT_MOST_ONCE)
        goto exit;

    int ptype = qos == EXACTLY_ONCE ? PUBREC : PUBACK;

    /* Global first (canonical order): tracking the inbound QoS 2 flow
       appends a record to the write-ahead log */
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
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
    pthread_mutex_unlock(&mutex);
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

int puback_handler(struct io_event *e)
{
    struct client *c = e->client;
    unsigned pkt_id  = e->data.ack.pkt_id;
    log_debug("Received PUBACK from %s (m%u)", c->client_id, pkt_id);
    /* The global mutex is taken first (canonical order) because dropping
       the inflight entry appends a record to the write-ahead log */
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
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
        pthread_mutex_unlock(&mutex);
#endif
        return NOREPLY;
    }
    /* Releases the stored packet and drops the identifier from the list the
       retransmission scan walks. An unknown identifier (a duplicate
       acknowledgement) is a no-op */
    inflight_msg_remove(c->session, (unsigned short)pkt_id);
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
    pthread_mutex_unlock(&mutex);
#endif
    return NOREPLY;
}

int pubrec_handler(struct io_event *e)
{
    struct client *c = e->client;
    unsigned pkt_id  = e->data.ack.pkt_id;
    log_debug("Received PUBREC from %s (m%u)", c->client_id, pkt_id);
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
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
        inflight_ack_init(c->session, (unsigned short)pkt_id);
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
    pthread_mutex_unlock(&mutex);
#endif
    log_debug("Sending PUBREL to %s (m%u)", c->client_id, pkt_id);
    return REPLY;
}

int pubrel_handler(struct io_event *e)
{
    struct client *c = e->client;
    unsigned pkt_id  = e->data.ack.pkt_id;
    log_debug("Received PUBREL from %s (m%u)", c->client_id, pkt_id);
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
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
    pthread_mutex_unlock(&mutex);
#endif
    log_debug("Sending PUBCOMP to %s (m%u)", c->client_id, pkt_id);
    return REPLY;
}

int pubcomp_handler(struct io_event *e)
{
    struct client *c = e->client;
    unsigned pkt_id  = e->data.ack.pkt_id;
    log_debug("Received PUBCOMP from %s (m%u)", c->client_id, pkt_id);
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
    pthread_mutex_lock(&c->mutex);
#endif
    /* A PUBCOMP can only arrive after a CONNECT, guard the session anyway */
    if (!c->session) {
#if THREADSNR > 0
        pthread_mutex_unlock(&c->mutex);
        pthread_mutex_unlock(&mutex);
#endif
        return NOREPLY;
    }
    /*
     * The stored PUBLISH was already released by pubrec_handler (the PUBREC
     * acknowledges the message itself, from then on only the PUBREL is
     * retransmitted), so the entry only carries the PUBREL timestamp by the
     * time the PUBCOMP lands. Dropping the entry releases whichever of the
     * two is left and decrements the inflight counter once, so a duplicate
     * PUBCOMP cannot wrap it
     */
    inflight_msg_remove(c->session, (unsigned short)pkt_id);
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
    pthread_mutex_unlock(&mutex);
#endif
    return NOREPLY;
}
