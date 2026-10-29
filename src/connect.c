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

static void set_connack(struct client *c, unsigned char rc, unsigned sp)
{
    struct config *conf         = c->server->conf;
    unsigned char connect_flags = 0 | (sp & 0x1) << 0;

    struct mqtt_packet response = {
        .header  = {.byte = CONNACK_B},
        .connack = (struct mqtt_connack){.byte = connect_flags, .rc = rc}};

    /*
     * The client is already published in the clients map by the time the
     * CONNACK is built, so another thread's publish_message() may be packing
     * into this very write buffer under the client mutex: taking it here is
     * the same protocol every other writer of wbuf/towrite follows. The
     * global mutex comes first because the reconnect flush below appends to
     * the write-ahead log, and the canonical order is always global then
     * client
     */
#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
    pthread_mutex_lock(&c->mutex);
#endif

    /*
     * The write buffer is exactly max_request_size bytes big: the CONNACK
     * must never be packed past its end (a fresh connection has an empty
     * buffer, so this only trips on an absurdly small configuration). Drop
     * the connection instead of leaving the client waiting forever for a
     * CONNACK that was never written
     */
    if (!outgoing_reserve(c, MQTT_ACK_LEN)) {
        log_error("Cannot send CONNACK to %s: write buffer too small "
                  "(max_request_size = %zu)",
                  c->client_id, conf->max_request_size);
        c->close_after_write = true;
#if THREADSNR > 0
        pthread_mutex_unlock(&c->mutex);
        pthread_mutex_unlock(&mutex);
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
            if (!outgoing_reserve(c, len))
                break;
            mqtt_pack(qm->packet, c->wbuf + c->towrite);
            c->towrite += len;
            ++packed;
        }
        // Forget just the messages flushed above, keeping their inflight
        // entries: they are still awaiting an acknowledgement
        session_flush_queue(c->session, packed);
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
    pthread_mutex_unlock(&mutex);
#endif
}

int connect_handler(struct io_event *e)
{
    struct server *server    = e->server;
    struct config *conf      = server->conf;
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
            HASH_FIND_STR(server->auths, (char *)c->payload.username, auth);
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
     * '$' names the broker's own namespace ($SOL statistics and whatever
     * control topic is added later). A client that could register a will on
     * it would be able to publish there without a PUBLISH packet ever
     * reaching publish_handler(), so the same policy is enforced here: the
     * CONNECT is refused instead of silently dropping the will, which would
     * leave the client believing it had one
     */
    if (c->bits.will && c->payload.will_topic[0] == '$') {
        log_warning("Refusing the will of %s: %s is a reserved topic",
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
    HASH_FIND_STR(server->sessions, cc->client_id, cc->session);
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
        session_delete(server, cc->client_id);
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
    bool new_session = false;
    if (c->bits.clean_session == true || !cc->session) {
        cc->session = client_session_alloc(server, cc->client_id);
        INCREF(cc->session, struct client_session);
        HASH_ADD_STR(server->sessions, session_id, cc->session);
        new_session = true;
    }

    cc->session->clean_session = c->bits.clean_session;
    /* A persistent session is the unit the snapshot and the log keep: a
       clean one dies with its connection and is never recorded */
    if (new_session && !cc->session->clean_session)
        persistence_wal_session(server, cc->client_id);

    // Let's track client on the global map to be used on publish
    HASH_ADD_STR(server->clients_map, client_id, cc);
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
        publish_lwt(server, &kicked_will);
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
         * The will topic is indexed exactly as it was sent, the same key a
         * PUBLISH to that name would use. The insert runs with the global
         * mutex held, a concurrent SUBSCRIBE or PUBLISH mutates the very
         * same trie from its own critical section
         */
#if THREADSNR > 0
        pthread_mutex_lock(&mutex);
#endif
        struct topic *t =
            topic_store_get_or_put(server->store, (const char *)will_topic);
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
                if (msg_len > 0) {
                    t->retained = mqtt_publish_copy(&cc->session->lwt_msg);
                    persistence_wal_retained(server, (const char *)will_topic,
                                             will_message, msg_len,
                                             c->bits.will_qos);
                } else {
                    persistence_wal_retained_clear(server,
                                                   (const char *)will_topic);
                }
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

int disconnect_handler(struct io_event *e)
{
    log_debug("Received DISCONNECT from %s", e->client->client_id);
    return -ERRCLIENTDC;
}
