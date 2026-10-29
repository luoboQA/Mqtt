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

/* SUBSCRIBE/UNSUBSCRIBE state changes and retained-message delivery. */
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
    struct server *server      = c->server;
    struct config *conf        = server->conf;
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
        if (inflight_window_full(s)) {
            log_warning("Inflight window full (%u messages) for %s, dropping "
                        "retained message on %s",
                        conf->max_inflight, s->session_id, t->name);
            DECREF(out, struct mqtt_packet);
            return;
        }
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
        inflight_msg_init(s, mid, out);
        /* A retained delivery to an online client is an inflight message
           like any other: a restart must retransmit it */
        persistence_wal_inflight(s, session_inflight(s, mid));
    }

    size_t len = mqtt_size(out, NULL);
    if (!outgoing_reserve(c, len)) {
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

static inline void add_wildcard(struct server *server, const char *topic,
                                struct subscriber *s, bool wildcard)
{
    struct subscription *subscription = try_alloc(sizeof(*subscription));
    subscription->subscriber          = s;
    subscription->topic               = try_strdup(topic);
    subscription->multilevel          = wildcard;
    INCREF(s, struct subscriber);
    topic_store_add_wildcard(server->store, subscription);
}

/*
 * Argument of the store walk that attaches a '#' subscription to the topics
 * already present: the subscriber to clone, and whether the '$' namespace has
 * to be skipped. A filter starting with a wildcard never matches '$' topics
 * (§4.7.2), but "$SOL/#" spells the prefix out and must attach normally
 */
struct wild_attach_ctx {
    struct subscriber *sub;
    bool skip_dollar;
};

static void recursive_sub(struct trie_node *node, void *arg)
{
    if (!node || !node->data)
        return;
    struct topic *t              = node->data;
    struct wild_attach_ctx *ctx  = arg;
    if (ctx->skip_dollar && t->name[0] == '$')
        return;
    /*
     * We need to make a copy of the subscriber cause UTHASH needs a proper
     * handle to work correctly, otherwise we'll end up freeing the same
     * refernce on disconnect and break the table
     */
    struct subscriber *s = subscriber_clone(ctx->sub), *tmp;
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
 * Copy a subscription filter verbatim, reporting whether it carries a
 * multilevel wildcard. The store keys topics exactly as they are: "a" and
 * "a/" are two different topics (MQTT v3.1.1 §4.7), so no trailing separator
 * is ever appended here. The '#' is kept in the filter, the wildcard index
 * knows to stop on it and the persistence writer stores the filter as the
 * client sent it. Shared by SUBSCRIBE and UNSUBSCRIBE so both look up the
 * very same trie node and wildcard index entry. `dst` must hold strlen(src)+1
 * bytes, returns true when the filter is multilevel ('#')
 */
static bool normalize_filter(char *dst, const char *src)
{
    size_t len = strlen(src);
    snprintf(dst, len + 1, "%s", src);
    return len > 0 && src[len - 1] == '#';
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
    if (len == 0 || !utf8_valid((const unsigned char *)f, len))
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
 * Attach a subscriber record to one concrete topic, reusing the one the
 * session already holds there (a repeated SUBSCRIBE only re-grants the QoS)
 */
static struct subscriber *attach_subscriber(struct client_session *session,
                                            struct topic *t,
                                            unsigned char qos)
{
    struct subscriber *sub = NULL;
    HASH_FIND_STR(t->subscribers, session->session_id, sub);
    if (sub) {
        sub->granted_qos = qos;
        return sub;
    }
    sub = topic_add_subscriber(t, session, qos);
    /* Keep the entry alive for the subscriber map */
    INCREF(sub, struct subscriber);
    session_track_topic(session, t);
    return sub;
}

/*
 * Register one topic filter for a session: validation, normalization and the
 * whole store dance (the '+' and '#' filters land in the wildcard index, an
 * exact filter gets its subscriber entry on the node). It is the very body of
 * a SUBSCRIBE tuple, shared with the persistence loader so a session rebuilt
 * from disk ends up registered exactly like one that subscribed over the
 * wire. The caller must hold the global mutex; false means the filter is
 * malformed.
 *
 * Topics are keyed exactly as written: "a" and "a/" are distinct. A "a/#"
 * filter covers the parent "a" as well as everything below "a/", so it
 * attaches to the "a" node and maps the descendants once, at subscribe time,
 * while live publishes are attached lazily by the wildcard index
 */
bool session_subscribe(struct client_session *session, const char *filter,
                       unsigned char qos)
{
    struct server *server = session ? session->server : NULL;
    if (!session || !valid_topic_filter(filter, strlen(filter)))
        return false;

    char f[strlen(filter) + 1];
    bool wildcard = normalize_filter(f, filter);

    if (!index(f, '+')) {
        if (wildcard) {
            size_t len = strlen(f);

            /*
             * A bare '#' has no parent: it is indexed on the root and
             * attaches to every topic already in the store (except the
             * broker's own '$' namespace, §4.7.2)
             */
            if (len == 1 && f[0] == '#') {
                struct subscriber *sub = subscriber_new(session, qos);
                add_wildcard(server, f, sub, true);
                struct wild_attach_ctx ctx = {sub, true};
                topic_store_map(server->store, NULL, recursive_sub, &ctx);
                persistence_wal_sub(server, session->session_id, filter, qos,
                                    true);
                persistence_mark_dirty();
                return true;
            }

            /*
             * "a/#" -> parent "a"; "/#" -> an empty parent, no node. A bare
             * '#' (len 1) was handled above, so len is at least 2 here; the
             * guard keeps the subtraction explicit for the compiler
             */
            size_t plen = len > 2 ? len - 2 : 0;
            char parent[plen + 1];
            memcpy(parent, f, plen);
            parent[plen] = '\0';

            if (plen > 0) {
                struct topic *t = topic_store_get_or_put(server->store, parent);
                struct subscriber *tmp = attach_subscriber(session, t, qos);
                /*
                 * The parent node is the record of this filter, not an exact
                 * subscription: only the WILD origin, so unsubscribing the
                 * filter drops it (and a concurrent exact "a" survives)
                 */
                if (!(tmp->origins & SUBSCRIBER_WILD)) {
                    tmp->origins |= SUBSCRIBER_WILD;
                    add_wildcard(server, f, tmp, true);
                    char prefix[plen + 2];
                    snprintf(prefix, plen + 2, "%s/", parent);
                    struct wild_attach_ctx ctx = {tmp, false};
                    topic_store_map(server->store, prefix, recursive_sub, &ctx);
                }
            } else {
                struct subscriber *sub = subscriber_new(session, qos);
                add_wildcard(server, f, sub, true);
                /* Everything below the empty first level: "/", "/a", ... */
                struct wild_attach_ctx ctx = {sub, false};
                topic_store_map(server->store, "/", recursive_sub, &ctx);
            }
            persistence_wal_sub(server, session->session_id, filter, qos,
                                true);
            persistence_mark_dirty();
            return true;
        }

        /*
         * No wildcard at all: the filter names one topic. "a" and "a/" are
         * two different keys, exactly as the spec says
         */
        struct topic *t = topic_store_get_or_put(server->store, f);
        struct subscriber *tmp = attach_subscriber(session, t, qos);
        tmp->origins |= SUBSCRIBER_EXACT;
        persistence_wal_sub(server, session->session_id, filter, qos, false);
        persistence_mark_dirty();
        return true;
    }

    /*
     * At least one single level wildcard '+': the filter cannot be attached
     * to a concrete topic now, it is indexed and matched on every publish.
     *
     * MQTT v3.1.1 §3.8.4: a SUBSCRIBE for a filter already subscribed
     * replaces the previous subscription, only the granted QoS may differ.
     * Indexing a fresh record on every repeat grew the wildcard index (and
     * with it the client teardown loop) without bound while delivery kept
     * deduplicating anyway, so a repeat now re-grants the QoS on the
     * existing entry
     */
    struct subscription *ex = topic_store_find_wildcard_filter(
        server->store, f, session->session_id);
    if (ex) {
        ex->subscriber->granted_qos = qos;
    } else {
        struct subscriber *sub = subscriber_new(session, qos);
        add_wildcard(server, f, sub, wildcard);
    }
    persistence_wal_sub(server, session->session_id, filter, qos, wildcard);
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

    /* Both sides are used exactly as written: "a" and "a/" differ */
    char *pattern = try_alloc(strlen(filter) + 1);
    bool multilevel = normalize_filter(pattern, filter);

    bool match = topic_match(topic, pattern, multilevel);
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

int subscribe_handler(struct io_event *e)
{
    struct server *server    = e->server;
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
         * The filter is kept exactly as the client sent it, which is how
         * session_subscribe() indexed it, to match the retained messages
         * against it after the SUBACK: "a" and "a/" are distinct, and a
         * multilevel filter keeps its '#'
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
    topic_store_map(server->store, NULL, retained_collect, &rc);
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
            if (!topic_match(t->name, rd[i].filter, rd[i].multilevel))
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
void session_track_topic(struct client_session *s, struct topic *t)
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
        if (topic_match(t->name, filter, multilevel)) {
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

/*
 * Remove one filter from a session: the body of an UNSUBSCRIBE tuple, shared
 * with the persistence loader. A malformed filter is rejected without
 * touching the store, so an empty string can never map to the bare "/" key
 * and detach the whole root of the tree. The caller must hold the global
 * mutex
 */
bool session_unsubscribe(struct client_session *session, const char *filter)
{
    struct server *server = session ? session->server : NULL;
    if (!session || !filter || !valid_topic_filter(filter, strlen(filter)))
        return false;

    /*
     * The filter is normalized exactly like SUBSCRIBE does, both must
     * address the very same trie node and wildcard index entry
     */
    char f[strlen(filter) + 1];
    bool multilevel = normalize_filter(f, filter);

    if (multilevel || index(f, '+')) {
        /*
         * A wildcard filter: drop it from the index so that no further
         * publish attaches the client to new topics, then stop the
         * deliveries it already drives on the topics attached so far
         */
        topic_store_remove_wildcard_filter(server->store, f,
                                           session->session_id);
        detach_wild_attached(session, f, multilevel);
    } else {
        struct topic *t = topic_store_get(server->store, f);
        if (t)
            detach_exact(t, session);
    }
    persistence_wal_unsub(server, session->session_id, filter, multilevel);
    persistence_mark_dirty();
    return true;
}

int unsubscribe_handler(struct io_event *e)
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
         * MQTT v3.1.1 §4.7.1: UNSUBACK carries no per-tuple return code, so a
         * malformed filter is simply skipped
         */
        if (!session_unsubscribe(c->session, raw))
            log_warning("Ignoring malformed topic filter in UNSUBSCRIBE "
                        "from %s",
                        c->client_id);
    }
#if THREADSNR > 0
    pthread_mutex_unlock(&mutex);
#endif

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
