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

#include "persistence.h"
#include "config.h"
#include "handlers.h"
#include "logging.h"
#include "memory.h"
#include "mqtt.h"
#include "server.h"
#include "sol_internal.h"
#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * =================
 *  On disk format
 * =================
 *
 * One record per line, plain text so that `cat` shows what the broker kept,
 * with every field that is binary on the wire (topic names, filters, client
 * ids and payloads may all hold any byte) encoded as hexadecimal:
 *
 *   # sol persistence v1
 *   retained <topic> <qos> <payload>
 *   session  <client-id>
 *   sub      <client-id> <filter> <qos> <multilevel 0|1>
 *   queued   <client-id> <topic> <qos> <payload>
 *
 * A '#' starts a comment, an empty line is skipped. Anything that does not
 * fit the grammar is reported and skipped too: a corrupt line must never take
 * the broker down at startup, and the same goes for a file that does not
 * exist (first run) or holds nothing the broker recognises.
 *
 * The multilevel flag of a subscription is what lets the loader rebuild the
 * filter the client sent: the wildcard index keys a '#' filter by the prefix
 * it matched on ("a/#" is stored as "a/"), so the '#' is put back here rather
 * than guessed from the name.
 */

static atomic_bool dirty = ATOMIC_VAR_INIT(false);

void persistence_mark_dirty(void) { dirty = true; }

bool persistence_is_dirty(void) { return dirty; }

/* ------------------------------------------------------------------ */
/*  Hexadecimal helpers                                               */
/* ------------------------------------------------------------------ */

static void fput_hex(FILE *fh, const unsigned char *data, size_t len)
{
    static const char digits[] = "0123456789abcdef";
    char buffer[512];
    size_t used = 0;

    for (size_t i = 0; i < len; ++i) {
        buffer[used++] = digits[(data[i] >> 4) & 0x0F];
        buffer[used++] = digits[data[i] & 0x0F];
        if (used == sizeof(buffer) - 2) {
            /* Flushed in chunks: a payload can be hundreds of kilobytes */
            buffer[used] = '\0';
            fputs(buffer, fh);
            used = 0;
        }
    }
    buffer[used] = '\0';
    fputs(buffer, fh);
}

static int hex_value(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

/*
 * Decode one hexadecimal field into a fresh NUL terminated buffer, owned by
 * the caller. An odd number of digits or a stray character invalidates the
 * whole field, the caller reports it and skips the record
 */
static unsigned char *hex_decode(const char *field, size_t *out_len)
{
    size_t len = strlen(field);
    if (len % 2 != 0)
        return NULL;

    unsigned char *out = try_alloc(len / 2 + 1);
    for (size_t i = 0; i < len / 2; ++i) {
        int hi = hex_value(field[2 * i]);
        int lo = hex_value(field[2 * i + 1]);
        if (hi < 0 || lo < 0) {
            free_memory(out);
            return NULL;
        }
        out[i] = (unsigned char)((hi << 4) | lo);
    }
    out[len / 2] = '\0';
    *out_len      = len / 2;
    return out;
}

/* ------------------------------------------------------------------ */
/*  Writing                                                           */
/* ------------------------------------------------------------------ */

static void save_retained(struct trie_node *node, void *arg)
{
    if (!node || !node->data)
        return;
    struct topic *t = node->data;
    if (!t->retained)
        return;

    FILE *fh = arg;
    fputs("retained ", fh);
    fput_hex(fh, t->retained->publish.topic, t->retained->publish.topiclen);
    fprintf(fh, " %u ", t->retained->header.bits.qos);
    fput_hex(fh, t->retained->publish.payload, t->retained->publish.payloadlen);
    fputc('\n', fh);
}

static void save_sub_record(FILE *fh, const char *id, const char *filter,
                            unsigned char qos, bool multilevel)
{
    fputs("sub ", fh);
    fput_hex(fh, (const unsigned char *)id, strlen(id));
    fputc(' ', fh);
    fput_hex(fh, (const unsigned char *)filter, strlen(filter));
    fprintf(fh, " %u %d\n", qos, multilevel ? 1 : 0);
}

struct wildcard_save_ctx {
    FILE *fh;
    const char *id;
};

static void save_wildcard_sub(const char *filter, bool multilevel,
                              unsigned char qos, void *arg)
{
    struct wildcard_save_ctx *ctx = arg;
    save_sub_record(ctx->fh, ctx->id, filter, qos, multilevel);
}

static void save_session(FILE *fh, struct client_session *s)
{
    fputs("session ", fh);
    fput_hex(fh, (const unsigned char *)s->session_id, strlen(s->session_id));
    fputc('\n', fh);

    /*
     * The exact subscriptions are the topics this session holds a plain
     * subscriber entry on: the filters carrying '+' or '#' are not attached
     * to a topic at subscription time, their filter string only exists in the
     * wildcard index and is walked just below
     */
    list_foreach(item, s->subscriptions)
    {
        struct topic *t        = item->data;
        struct subscriber *sub = NULL;
        HASH_FIND_STR(t->subscribers, s->session_id, sub);
        if (!sub || !(sub->origins & SUBSCRIBER_EXACT))
            continue;
        save_sub_record(fh, s->session_id, t->name, sub->granted_qos, false);
    }

    struct wildcard_save_ctx ctx = {.fh = fh, .id = s->session_id};
    topic_store_wildcards_foreach(server.store, s->session_id,
                                  save_wildcard_sub, &ctx);

    /* Messages queued while the client was away, oldest first */
    list_foreach(item, s->outgoing_msgs)
    {
        struct queued_msg *qm = item->data;
        if (!qm || !qm->packet)
            continue;
        fputs("queued ", fh);
        fput_hex(fh, (const unsigned char *)s->session_id,
                 strlen(s->session_id));
        fputc(' ', fh);
        fput_hex(fh, qm->packet->publish.topic, qm->packet->publish.topiclen);
        /* The packet is shared by every subscriber of the original publish,
           the per session level lives in the inflight entry */
        fprintf(fh, " %u ", s->i_msgs[qm->mid].qos);
        fput_hex(fh, qm->packet->publish.payload,
                 qm->packet->publish.payloadlen);
        fputc('\n', fh);
    }
}

static void persistence_write(FILE *fh)
{
    fputs("# sol persistence v1\n", fh);

    /* Retained messages, one record per topic holding one */
    topic_store_map(server.store, NULL, save_retained, fh);

    /* Persistent sessions with their subscriptions and queued messages */
    struct client_session *s, *tmp;
    HASH_ITER(hh, server.sessions, s, tmp)
    {
        /* A clean session dies with the connection of its client: there is
           nothing to keep for the next run */
        if (s->clean_session)
            continue;
        save_session(fh, s);
    }
}

void persistence_save(const char *path)
{
    if (!path || !path[0])
        return;

    /*
     * Written aside and renamed over the destination: a crash in the middle
     * of the write leaves the previous snapshot (or nothing at all on the
     * first run) rather than a truncated file that the next start would try
     * to parse
     */
    size_t plen = strlen(path);
    char *tmp   = try_alloc(plen + 5);
    snprintf(tmp, plen + 5, "%s.tmp", path);

    FILE *fh = fopen(tmp, "w");
    if (!fh) {
        log_warning("Cannot open the persistence file %s: %s", tmp,
                    strerror(errno));
        free_memory(tmp);
        return;
    }

    persistence_write(fh);

    if (fclose(fh) != 0 || rename(tmp, path) != 0) {
        log_warning("Cannot update the persistence file %s: %s", path,
                    strerror(errno));
        remove(tmp);
    } else {
        /* The caller held the global mutex for the whole snapshot, so no
           state change could slip in behind the write */
        dirty = false;
        log_info("Persisted the broker state to %s", path);
    }
    free_memory(tmp);
}

/* ------------------------------------------------------------------ */
/*  Reading                                                           */
/* ------------------------------------------------------------------ */

/* Next space delimited field of a line, NULL once the line is consumed */
static char *next_field(char **cursor)
{
    char *p = *cursor;

    while (*p == ' ')
        p++;
    if (*p == '\0') {
        *cursor = p;
        return NULL;
    }

    char *field = p;
    while (*p && *p != ' ')
        p++;
    if (*p != '\0') {
        *p = '\0';
        p++;
    }
    *cursor = p;
    return field;
}

static struct client_session *load_session_id(const char *id)
{
    struct client_session *s = NULL;
    HASH_FIND_STR(server.sessions, id, s);
    if (s)
        return s;
    return session_restore(id);
}

static void load_retained_record(char *fields)
{
    char *cursor      = fields;
    char *topic_field = next_field(&cursor);
    char *qos_field   = next_field(&cursor);
    char *data_field  = next_field(&cursor);

    if (!topic_field || !qos_field || !data_field) {
        log_warning("Malformed retained record in the persistence file, "
                    "skipped");
        return;
    }

    size_t topic_len = 0, payload_len = 0;
    unsigned char *topic   = hex_decode(topic_field, &topic_len);
    unsigned char *payload = hex_decode(data_field, &payload_len);
    if (!topic || !payload) {
        log_warning("Malformed hexadecimal field in a retained record, "
                    "skipped");
        free_memory(topic);
        free_memory(payload);
        return;
    }

    int qos = atoi(qos_field);
    if (qos < AT_MOST_ONCE || qos > EXACTLY_ONCE) {
        log_warning("Invalid QoS in a retained record, keeping QoS 0");
        qos = AT_MOST_ONCE;
    }

    /*
     * The store keys a topic by its normalized name (trailing '/', the same
     * rule every PUBLISH follows), while the packet keeps the name as it was
     * published: that is what the subscribers receive
     */
    size_t nlen = strlen((char *)topic);
    char *name  = try_alloc(nlen + 2);
    if (nlen > 0 && topic[nlen - 1] == '/')
        snprintf(name, nlen + 1, "%s", topic);
    else
        snprintf(name, nlen + 2, "%s/", topic);

    struct topic *t = topic_store_get_or_put(server.store, name);
    free_memory(name);

    if (t->retained) {
        /* Two records for the same topic: the last one wins */
        DECREF(t->retained, struct mqtt_packet);
        t->retained = NULL;
    }

    struct mqtt_packet *pkt = mqtt_packet_alloc(PUBLISH_B);
    pkt->header.bits.qos    = (unsigned char)qos;
    /* The buffers decoded above are handed over to the packet */
    pkt->publish = (struct mqtt_publish){.pkt_id     = 0,
                                         .topiclen   = topic_len,
                                         .topic      = topic,
                                         .payloadlen = payload_len,
                                         .payload    = payload};
    INCREF(pkt, struct mqtt_packet);
    t->retained = pkt;
}

static void load_sub_record(char *fields)
{
    char *cursor       = fields;
    char *id_field     = next_field(&cursor);
    char *filter_field = next_field(&cursor);
    char *qos_field    = next_field(&cursor);
    char *multi_field  = next_field(&cursor);

    if (!id_field || !filter_field || !qos_field || !multi_field) {
        log_warning("Malformed subscription record, skipped");
        return;
    }

    size_t id_len = 0, filter_len = 0;
    unsigned char *id     = hex_decode(id_field, &id_len);
    unsigned char *filter = hex_decode(filter_field, &filter_len);
    if (!id || !filter) {
        log_warning("Malformed hexadecimal field in a subscription record, "
                    "skipped");
        free_memory(id);
        free_memory(filter);
        return;
    }

    struct client_session *s = load_session_id((const char *)id);
    free_memory(id);
    if (!s) {
        free_memory(filter);
        return;
    }

    bool multilevel = atoi(multi_field) != 0;

    /*
     * Rebuild the filter as the client sent it: the wildcard index keys a
     * multilevel filter by its prefix, so the '#' has to go back on. A bare
     * '#' is already complete, it has no prefix to append to
     */
    char *full = NULL;
    if (multilevel && strcmp((const char *)filter, "#") != 0) {
        full = try_alloc(filter_len + 2);
        snprintf(full, filter_len + 2, "%s#", (const char *)filter);
    }

    session_subscribe(s, full ? full : (const char *)filter,
                      (unsigned char)atoi(qos_field));
    free_memory(full);
    free_memory(filter);
}

static void load_queued_record(char *fields)
{
    char *cursor      = fields;
    char *id_field    = next_field(&cursor);
    char *topic_field = next_field(&cursor);
    char *qos_field   = next_field(&cursor);
    char *data_field  = next_field(&cursor);

    if (!id_field || !topic_field || !qos_field || !data_field) {
        log_warning("Malformed queued message record, skipped");
        return;
    }

    size_t id_len = 0, topic_len = 0, payload_len = 0;
    unsigned char *id      = hex_decode(id_field, &id_len);
    unsigned char *topic   = hex_decode(topic_field, &topic_len);
    unsigned char *payload = hex_decode(data_field, &payload_len);
    if (!id || !topic || !payload) {
        log_warning("Malformed hexadecimal field in a queued message "
                    "record, skipped");
        free_memory(id);
        free_memory(topic);
        free_memory(payload);
        return;
    }

    struct client_session *s = load_session_id((const char *)id);
    if (s) {
        int qos = atoi(qos_field);
        if (qos < AT_LEAST_ONCE || qos > EXACTLY_ONCE)
            qos = AT_LEAST_ONCE; /* a queued message is never QoS 0 */
        session_queue_message(s, (const char *)topic, (unsigned char)qos,
                              payload, payload_len);
    }

    free_memory(id);
    free_memory(topic);
    free_memory(payload);
}

static bool load_line(char *line)
{
    if (line[0] == '#' || line[0] == '\0')
        return false;

    char *cursor = line;
    char *tag    = next_field(&cursor);
    if (!tag)
        return false;

    if (STREQ(tag, "retained", 8)) {
        load_retained_record(cursor);
    } else if (STREQ(tag, "session", 7)) {
        size_t id_len      = 0;
        char *id_field     = next_field(&cursor);
        unsigned char *id  = id_field ? hex_decode(id_field, &id_len) : NULL;
        if (!id) {
            log_warning("Malformed session record, skipped");
            return false;
        }
        load_session_id((const char *)id);
        free_memory(id);
    } else if (STREQ(tag, "sub", 3)) {
        load_sub_record(cursor);
    } else if (STREQ(tag, "queued", 6)) {
        load_queued_record(cursor);
    } else {
        log_warning("Unknown persistence record '%s', skipped", tag);
        return false;
    }
    return true;
}

void persistence_load(const char *path)
{
    if (!path || !path[0])
        return;

    FILE *fh = fopen(path, "r");
    if (!fh) {
        log_info("No persistence file at %s, starting with an empty store",
                 path);
        return;
    }

    /*
     * The whole file is read in one go instead of line by line: a record
     * carrying a payload is as long as twice that payload, which a fixed
     * sized buffer could not hold
     */
    if (fseek(fh, 0, SEEK_END) != 0) {
        log_warning("Cannot read the persistence file %s", path);
        fclose(fh);
        return;
    }
    long length = ftell(fh);
    rewind(fh);
    if (length < 0) {
        log_warning("Cannot read the persistence file %s", path);
        fclose(fh);
        return;
    }

    char *content = try_alloc((size_t)length + 1);
    size_t got    = fread(content, 1, (size_t)length, fh);
    content[got]  = '\0';
    fclose(fh);

    size_t records = 0;
    char *line     = content;
    while (line && *line) {
        char *eol = strchr(line, '\n');
        if (eol)
            *eol = '\0';
        if (load_line(line))
            records++;
        line = eol ? eol + 1 : NULL;
    }

    log_info("Restored %lu records from %s", (unsigned long)records, path);
    free_memory(content);
}
