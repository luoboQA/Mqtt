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
#include "config.h"
#include "logging.h"
#include "memory.h"
#include "mqtt.h"
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

static struct client_session *client_session_alloc(const char *);

static unsigned next_free_mid(struct client_session *);

static void inflight_msg_init(struct inflight_msg *, struct mqtt_packet *);

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
    list_destroy(session->outgoing_msgs, 0);
    if (has_inflight(session)) {
        for (int i = 0; i < MAX_INFLIGHT_MSGS; ++i) {
            if (session->i_msgs[i].packet)
                DECREF(session->i_msgs[i].packet, struct mqtt_packet);
        }
    }
    free_memory(session->i_acks);
    free_memory(session->i_msgs);
    /*
     * Release the strings of the will (delivered or not): the packet is
     * embedded in the session so only its members are heap owned
     */
    mqtt_packet_destroy(&session->lwt_msg);
    free_memory(session);
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

static inline unsigned next_free_mid(struct client_session *session)
{
    if (session->next_free_mid == MAX_INFLIGHT_MSGS)
        session->next_free_mid = 1;
    return session->next_free_mid++;
}

static inline void inflight_msg_init(struct inflight_msg *imsg,
                                     struct mqtt_packet *p)
{
    imsg->seen   = time(NULL);
    imsg->packet = p;
    imsg->qos    = p->header.bits.qos;
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
            mid                 = next_free_mid(s);
            pkt->publish.pkt_id = mid;
            INCREF(pkt, struct mqtt_packet);
            /*
             * If offline, we must enqueue messages in the inflight queue
             * of the client, they will be sent out only in case of a
             * clean_session == false connection
             */
            if (!sc || sc->online == false) {
                if (s->clean_session == false) {
                    list_push(s->outgoing_msgs, pkt);
                    all_at_most_once = false;
                    INCREF(pkt, struct mqtt_packet);
                    inflight_msg_init(&s->i_msgs[mid], pkt);
                    s->i_acks[mid] = time(NULL);
                    ++s->inflights;
                }
                continue;
            }
#if THREADSNR > 0
            pthread_mutex_lock(&sc->mutex);
#endif
            /*
             * The subscriber client is marked as online, so we proceed to
             * set the inflight messages according to the QoS level required
             * and write back the payload
             */
            inflight_msg_init(&sc->session->i_msgs[mid], pkt);
            sc->session->i_acks[mid] = time(NULL);
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

static void set_connack(struct client *c, unsigned char rc, unsigned sp)
{
    unsigned char connect_flags = 0 | (sp & 0x1) << 0;

    struct mqtt_packet response = {
        .header  = {.byte = CONNACK_B},
        .connack = (struct mqtt_connack){.byte = connect_flags, .rc = rc}};
    mqtt_pack(&response, c->wbuf + c->towrite);
    c->towrite += MQTT_ACK_LEN;

    /*
     * If a session was present and the connected client have disabled the
     * clean session flag, we have to take care of the outgoing messages
     * pending, strictly after the CONNACK encoding
     */
    if (c->clean_session == false && sp == 1) {
        log_info("Resuming session for %s", c->client_id);
        /*
         * If there's already some subscriptions and pending messages,
         * empty the queue
         */
        // TODO check for write buffer size exceed
        if (list_size(c->session->outgoing_msgs) > 0) {
            size_t len = 0;
            list_foreach(item, c->session->outgoing_msgs)
            {
                len = mqtt_size(item->data, NULL);
                mqtt_pack(item->data, c->wbuf + c->towrite);
                c->towrite += len;
            }
            // We want to clean up the queue after the payload set
            list_clear(c->session->outgoing_msgs, 0);
        }
    }
}

static int connect_handler(struct io_event *e)
{

    unsigned session_present = 0;
    struct mqtt_connect *c   = &e->data.connect;
    struct client *cc        = e->client;

    if (cc->connected == true) {
        /*
         * Already connected client, 2 CONNECT packet should be interpreted as
         * a violation of the protocol, causing disconnection of the client
         */
        log_info("Received double CONNECT from %s, disconnecting client",
                 c->payload.client_id);
        goto clientdc;
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
     * No client ID and clean_session == false? you're not authorized, we don't
     * know who you are
     */
    if (!c->payload.client_id[0] && c->bits.clean_session == false)
        goto not_authorized;

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

#if THREADSNR > 0
    pthread_mutex_lock(&mutex);
#endif
    // First we check if a session is present
    HASH_FIND_STR(server.sessions, cc->client_id, cc->session);
    if (cc->session && c->bits.clean_session == true)
        // Clean session true, we have to clean old session, if any
        HASH_DEL(server.sessions, cc->session);
    else if (cc->session)
        session_present = 1;

    cc->connected = true;

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

    // Add LWT topic and message if present
    if (c->bits.will) {
        cc->has_lwt              = true;
        const char *will_topic   = (const char *)c->payload.will_topic;
        const char *will_message = (const char *)c->payload.will_message;
        // TODO check for will_topic != NULL
        // I'm sure that the string will be NUL terminated by unpack function
        size_t msg_len = strlen(will_message);
        size_t tpc_len = strlen(will_topic);

        /*
         * The will topic is indexed under its normalized name (trailing '/',
         * like every other topic in the store) so that the delivery finds
         * the very same node subscriptions are attached to. The insert runs
         * with the global mutex held, a concurrent SUBSCRIBE or PUBLISH
         * mutates the very same trie from its own critical section
         */
        char will_store_name[tpc_len + 2];
        if (tpc_len > 0 && will_topic[tpc_len - 1] != '/')
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
         * fresh session is all zeros, destroy is a no-op on it). While an
         * inflight message still points at the embedded packet its strings
         * must stay alive, that rare case is left to session_free()
         */
        if (!has_inflight(cc->session))
            mqtt_packet_destroy(&cc->session->lwt_msg);

        cc->session->lwt_msg = (struct mqtt_packet){
            .header  = (union mqtt_header){.byte = PUBLISH_B},
            .publish = (struct mqtt_publish){
                .pkt_id     = 0, // placeholder
                .topiclen   = tpc_len,
                .topic      = (unsigned char *)try_strdup(will_topic),
                .payloadlen = msg_len,
                .payload    = (unsigned char *)try_strdup(will_message)}};

        cc->session->lwt_msg.header.bits.qos = c->bits.will_qos;
        /*
         * The packet is embedded in the session: park its reference count at
         * 1 so that the bookkeeping INCREFs publish_message() makes for
         * QoS > 0 inflight tracking can never bring it back to 0, which
         * would call a free function on the middle of the session struct
         */
        cc->session->lwt_msg.refcount.count = 1;
        // We must store the retained message in the topic
        if (c->bits.will_retain == 1) {
            size_t publen          = mqtt_size(&cc->session->lwt_msg, NULL);
            unsigned char *payload = try_alloc(publen);
            mqtt_pack(&cc->session->lwt_msg, payload);
            // We got a ready-to-be-sent bytestring in the retained message
            // field
#if THREADSNR > 0
            pthread_mutex_lock(&mutex);
#endif
            t->retained_msg = payload;
#if THREADSNR > 0
            pthread_mutex_unlock(&mutex);
#endif
        }
        log_info("Will message specified (%lu bytes)",
                 cc->session->lwt_msg.publish.payloadlen);
        log_info("\t%s", cc->session->lwt_msg.publish.payload);
    }

    // TODO check for session already present

    cc->clean_session = c->bits.clean_session;

    set_connack(cc, MQTT_CONNECTION_ACCEPTED, session_present);

    log_debug("Sending CONNACK to %s (%u, %u)", cc->client_id, session_present,
              MQTT_CONNECTION_ACCEPTED);

    return REPLY;

clientdc:

    return -ERRCLIENTDC;

bad_auth:
    log_debug("Sending CONNACK to %s (%u, %u)", cc->client_id, session_present,
              MQTT_BAD_USERNAME_OR_PASSWORD);
    set_connack(cc, MQTT_BAD_USERNAME_OR_PASSWORD, session_present);

    return MQTT_BAD_USERNAME_OR_PASSWORD;

not_authorized:
    log_debug("Sending CONNACK to %s (%u, %u)", cc->client_id, session_present,
              MQTT_NOT_AUTHORIZED);
    set_connack(cc, MQTT_NOT_AUTHORIZED, session_present);

    return MQTT_NOT_AUTHORIZED;
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
        list_push(s->session->subscriptions, t);
    } else {
        /*
         * Already attached to this very topic (typically by an exact
         * SUBSCRIBE): remember that a wildcard subscription covers it too so
         * that an UNSUBSCRIBE of the filter keeps the exact one alive, the
         * unused copy is dropped instead of being leaked
         */
        tmp->origins |= SUBSCRIBER_WILD;
        list_push(tmp->session->subscriptions, t);
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
    if (!t->retained_msg)
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
         * Check if the topic exists already or in case create it and store in
         * the global map
         */
        char topic[s->tuples[i].topic_len + 2];
        snprintf(topic, s->tuples[i].topic_len + 1, "%s", s->tuples[i].topic);

        log_debug("\t%s (QoS %i)", topic, s->tuples[i].qos);

        /*
         * Recursive subscribe to all children topics if the topic ends with
         * "/#" (or is a bare "#"): the wildcard part is stripped from the
         * stored filter and flagged as multilevel, see normalize_filter()
         */
        bool wildcard =
            normalize_filter(topic, (const char *)s->tuples[i].topic);

        /*
         * Let's explore two possible scenarios:
         * 1. Normal topic (no single level wildcard '+') which can end with
         *    multilevel wildcard '#'
         * 2. A topic contaning one or more single level wildcard '+'
         *
         * The store is always mutated with the global mutex held first and
         * the client mutex second, the canonical order shared with
         * publish_message() delivery, otherwise an ABBA deadlock between a
         * SUBSCRIBE and a concurrent PUBLISH would be possible
         */
#if THREADSNR > 0
        pthread_mutex_lock(&mutex);
        pthread_mutex_lock(&c->mutex);
#endif
        struct topic *t = topic_store_get_or_put(server.store, topic);
        if (!index(topic, '+')) {
            struct subscriber *tmp;
            HASH_FIND_STR(t->subscribers, c->client_id, tmp);
            if (c->clean_session == true || !tmp) {
                if (!tmp) {
                    tmp = topic_add_subscriber(t, e->client->session,
                                               s->tuples[i].qos);
                    // we increment reference for the subscriptions session
                    INCREF(tmp, struct subscriber);
                    /*
                     * A fresh entry living on a wildcard filter's own trie
                     * node is the record of that filter, not an exact
                     * subscription of the node's topic
                     */
                    if (wildcard)
                        tmp->origins = SUBSCRIBER_WILD;
                }
                list_push(e->client->session->subscriptions, t);
                if (wildcard == true) {
                    /*
                     * The entry covers the filter (and its own topic, as
                     * "a/#" matches "a"): remember it so that UNSUBSCRIBE
                     * of the wildcard drops the record too
                     */
                    tmp->origins |= SUBSCRIBER_WILD;
                    add_wildcard(topic, tmp, wildcard);
                    topic_store_map(server.store, topic, recursive_sub, tmp);
                } else {
                    /*
                     * The entry now doubles as an explicit subscription of
                     * this very topic: remembering the origin lets a
                     * concurrent wildcard covering it survive its
                     * UNSUBSCRIBE (and vice versa)
                     */
                    tmp->origins |= SUBSCRIBER_EXACT;
                }
            }
        } else {
            /*
             * Here we encountered at least 1 single level wildcard '+', we add
             * the subscription to the wildcard index as we can't know at this
             * point which topic it will match
             */
            struct subscriber *sub =
                subscriber_new(e->client->session, s->tuples[i].qos);
            add_wildcard(topic, sub, wildcard);
        }
        /*
         * Retained messages matching the subscriptions must be flushed after
         * the SUBACK, keep a copy of every filter to scan the store for them
         * once the SUBACK has been packed
         */
        rd[rd_len].filter     = try_strdup(topic);
        rd[rd_len].multilevel = wildcard;
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
    mqtt_pack(&pkt, c->wbuf + c->towrite);
    c->towrite += len;

    /*
     * Retained messages matching the subscriptions are published right after
     * the SUBACK: the store is walked a single time collecting the topics
     * holding a retained message and every filter is then matched against
     * them. The output is identical to scanning the whole store once per
     * filter, but the cost no longer grows with filters x topics
     */
    struct retained_collection rc = {NULL, 0, 0};
    topic_store_map(server.store, NULL, retained_collect, &rc);
    for (unsigned i = 0; i < rd_len; i++) {
        for (size_t j = 0; j < rc.len; j++) {
            struct topic *t = rc.topics[j];
            if (match_subscription(t->name, rd[i].filter,
                                   rd[i].multilevel) != SOL_OK)
                continue;
            size_t rlen = alloc_size(t->retained_msg);
            if (c->towrite + rlen > conf->max_request_size)
                continue;
            memcpy(c->wbuf + c->towrite, t->retained_msg, rlen);
            c->towrite += rlen;
        }
        free_memory((char *)rd[i].filter);
    }
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
    list_foreach(item, session->subscriptions)
    {
        struct topic *t = item->data;
        if (match_subscription(t->name, filter, multilevel) != SOL_OK)
            continue;
        struct subscriber *sub = NULL;
        HASH_FIND_STR(t->subscribers, session->session_id, sub);
        if (!sub || !(sub->origins & SUBSCRIBER_WILD))
            continue;
        sub->origins &= ~SUBSCRIBER_WILD;
        if (sub->origins == 0) {
            HASH_DEL(t->subscribers, sub);
            DECREF(sub, struct subscriber);
        }
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

    mqtt_pack_mono(c->wbuf + c->towrite, UNSUBACK, e->data.unsubscribe.pkt_id);
    c->towrite += MQTT_ACK_LEN;
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

    HASH_FIND_STR(t->subscribers, session->session_id, ex);
    if (ex) {
        /*
         * Already attached (typically by an exact SUBSCRIBE of this very
         * topic): remember that a wildcard covers it too, so that an
         * UNSUBSCRIBE of the filter stops only what it names
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
    list_push(session->subscriptions, t);
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
     * For convenience we assure that all topics ends with a '/', indicating a
     * hierarchical level
     */
    if (p->topic[p->topiclen - 1] != '/')
        snprintf(topic, p->topiclen + 2, "%s/", (const char *)p->topic);
    else
        snprintf(topic, p->topiclen + 1, "%s", (const char *)p->topic);

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
    // TODO must perform a deep copy here
    pkt->publish            = e->data.publish;

    /*
     * The retained message is store state: it is written with the global
     * mutex still held (released just after) and with the client mutex
     * already dropped, delivery below re-acquires the global one itself.
     * A new retained message replaces the previous one (a zero length
     * payload clears it, as MQTT requires): the old bytes are released
     * first or they would be silently lost on every overwrite
     */
    if (hdr->bits.retain == 1) {
        free_memory(t->retained_msg);
        t->retained_msg = NULL;
        if (p->payloadlen > 0) {
            t->retained_msg = try_alloc(mqtt_size(&e->data, NULL));
            mqtt_pack(&e->data, t->retained_msg);
        }
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif

    if (publish_message(pkt, t) == 0)
        DECREF(pkt, struct mqtt_packet);

    // We have to answer to the publisher
    if (qos == AT_MOST_ONCE)
        goto exit;

    int ptype = qos == EXACTLY_ONCE ? PUBREC : PUBACK;

#if THREADSNR > 0
    pthread_mutex_lock(&c->mutex);
#endif
    mqtt_ack(&e->data, ptype == PUBACK ? PUBACK_B : PUBREC_B);
    mqtt_pack_mono(c->wbuf + c->towrite, ptype, orig_mid);
    c->towrite += MQTT_ACK_LEN;
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
    mqtt_pack_mono(c->wbuf + c->towrite, PUBREL, pkt_id);
    c->towrite += MQTT_ACK_LEN;
#if THREADSNR > 0
    pthread_mutex_unlock(&c->mutex);
#endif
    // Update inflight acks table
    if (c->session) {
        c->session->i_acks[pkt_id] = time(NULL);
    }
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
    mqtt_pack_mono(c->wbuf + c->towrite, PUBCOMP, pkt_id);
    c->towrite += MQTT_ACK_LEN;
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
    c->session->i_acks[pkt_id] = -1;
    if (c->session->i_msgs[pkt_id].packet) {
        inflight_msg_clear(&c->session->i_msgs[pkt_id]);
        --c->session->inflights;
    }
    c->session->i_msgs[pkt_id].packet = NULL;
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
    mqtt_pack(&e->data, e->client->wbuf + e->client->towrite);
    e->client->towrite += MQTT_HEADER_LEN;
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
    return handlers[type](event);
}
