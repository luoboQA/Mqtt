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

/* Snapshot and replay broker state; append each mutation to the WAL first. */
#include "persistence.h"
#include "config.h"
#include "handlers.h"
#include "logging.h"
#include "memory.h"
#include "mqtt.h"
#include "server.h"
#include "sol_internal.h"
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/*
 * =================
 *  On disk format
 * =================
 *
 * One record per line, plain text so that `cat` shows what the broker kept,
 * with every field that is binary on the wire (topic names, filters, client
 * ids and payloads may all hold any byte) encoded as hexadecimal:
 *
 *   # sol persistence v2 <wal-generation> <wal-records>
 *   retained <topic> <qos> <payload>
 *   session  <client-id>
 *   sub      <client-id> <filter> <qos> <multilevel 0|1>
 *   queued   <client-id> <topic> <qos> <mid> <payload>
 *   inflight <client-id> <topic> <qos> <mid> <payload>
 *   ack      <client-id> <mid> <last-pubrel-timestamp>
 *   q2       <client-id> <mid>
 *
 * A '#' starts a comment, an empty line is skipped. Anything that does not
 * fit the grammar is reported and skipped too: a corrupt line must never take
 * the broker down at startup, and the same goes for a file that does not
 * exist (first run) or holds nothing the broker recognises.
 *
 * The multilevel flag of a subscription is what lets the loader rebuild a v1
 * filter: v1 stored the wildcard index key ("a/#" was written as "a/"), so
 * the '#' is put back here rather than guessed from the name. A v2 filter is
 * stored exactly as the client sent it and the flag is informational.
 *
 * The write-ahead log (`<persistence_file>.wal`) uses the same grammar plus
 * the records that undo a previous one:
 *
 *   # sol wal v2 <wal-generation>
 *   retained-clear <topic>
 *   session-del    <client-id>
 *   unsub          <client-id> <filter> <multilevel 0|1>
 *   dropped        <client-id> <mid>
 *   flush          <client-id> <count>
 *   inflight-del   <client-id> <mid>
 *   q2-del         <client-id> <mid>
 *
 * The snapshot is the compaction point: once it has been renamed over the
 * destination the log is truncated, and its generation is bumped so that a
 * log left behind by a crash between the rename and the truncation is
 * recognised as already contained in the snapshot instead of being replayed
 * on top of it.
 */

static atomic_bool dirty = ATOMIC_VAR_INIT(false);

/* Defined with the other hexadecimal helpers, used by the log appends */
static void fput_hex(FILE *fh, const unsigned char *data, size_t len);

/*
 * Version of the file being read. v1 keyed every topic and filter with an
 * artificial trailing '/', which made "a" and "a/" the same name; the loader
 * strips it while reading a v1 file so the in-memory store gets the exact
 * names the v2 format writes
 */
static int load_version = 2;

/*
 * Write-ahead log state. The log is opened lazily, so a broker with
 * persistence disabled (or one that never mutates anything) never touches
 * it. `wal_generation` changes every time the log is truncated after a
 * snapshot: the snapshot names the generation and the number of records it
 * already contains, which is what keeps a replay from applying a record
 * twice. All the appends happen under the global server mutex (or during the
 * single-threaded startup load, where `wal_loading` suppresses them)
 */
static FILE *wal_fh                      = NULL;
static bool wal_loading                  = false;
static unsigned long long wal_generation = 0;
static unsigned long wal_records         = 0;
static off_t wal_mark                    = 0;

/* persistence_file is a char[0xFFF], the suffix and the NUL fit after it */
static char wal_path_buf[0x1004];

static bool wal_path(struct server *server)
{
    if (!server || !server->conf || !server->conf->persistence_file[0])
        return false;
    int n = snprintf(wal_path_buf, sizeof(wal_path_buf), "%s.wal",
                     server->conf->persistence_file);
    return n > 0 && (size_t)n < sizeof(wal_path_buf);
}

/*
 * Open the log on first use and remember the current end of file, so a
 * failed append can be truncated away instead of leaving a half line for the
 * next start to parse
 */
static bool wal_ready(struct server *server)
{
    if (wal_loading || !wal_path(server))
        return false;

    if (!wal_fh) {
        struct stat st;
        bool empty = stat(wal_path_buf, &st) != 0 || st.st_size == 0;
        wal_fh = fopen(wal_path_buf, "a");
        if (!wal_fh) {
            log_warning("Cannot open the write-ahead log %s: %s",
                        wal_path_buf, strerror(errno));
            return false;
        }
        if (empty)
            fprintf(wal_fh, "# sol wal v2 %llu\n", wal_generation);
    }

    struct stat st;
    wal_mark = fstat(fileno(wal_fh), &st) == 0 ? st.st_size : 0;
    return true;
}

/*
 * Make the record durable. The log is fsynced on every append: the whole
 * point of the log is to survive a crash between two snapshots, and a record
 * still sitting in the kernel page cache would not
 */
static void wal_finish(void)
{
    if (!wal_fh)
        return;
    if (fflush(wal_fh) != 0 || fsync(fileno(wal_fh)) != 0) {
        log_warning("Cannot flush the write-ahead log %s: %s", wal_path_buf,
                    strerror(errno));
        fclose(wal_fh);
        wal_fh = NULL;
        if (truncate(wal_path_buf, wal_mark) != 0)
            log_warning("Cannot truncate the write-ahead log %s: %s",
                        wal_path_buf, strerror(errno));
        return;
    }
    ++wal_records;
    dirty = true;
}

/*
 * A session is recorded only when it survives its connection: the snapshot
 * and the log deliberately ignore clean sessions
 */
static bool wal_persistent(struct server *server, const char *id)
{
    if (!id || !id[0])
        return false;
    struct client_session *s = NULL;
    HASH_FIND_STR(server->sessions, id, s);
    return s && !s->clean_session;
}

void persistence_wal_retained(struct server *server, const char *topic,
                              const unsigned char *payload, size_t len,
                              unsigned char qos)
{
    if (!topic || !wal_ready(server))
        return;
    fputs("retained ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)topic, strlen(topic));
    fprintf(wal_fh, " %u ", qos);
    fput_hex(wal_fh, payload, len);
    fputc('\n', wal_fh);
    wal_finish();
}

void persistence_wal_retained_clear(struct server *server, const char *topic)
{
    if (!topic || !wal_ready(server))
        return;
    fputs("retained-clear ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)topic, strlen(topic));
    fputc('\n', wal_fh);
    wal_finish();
}

void persistence_wal_session(struct server *server, const char *session_id)
{
    if (!wal_persistent(server, session_id) || !wal_ready(server))
        return;
    fputs("session ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fputc('\n', wal_fh);
    wal_finish();
}

void persistence_wal_session_del(struct server *server, const char *session_id)
{
    if (!session_id || !session_id[0] || !wal_ready(server))
        return;
    fputs("session-del ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fputc('\n', wal_fh);
    wal_finish();
}

void persistence_wal_sub(struct server *server, const char *session_id,
                         const char *filter, unsigned char qos,
                         bool multilevel)
{
    if (!wal_persistent(server, session_id) || !filter || !wal_ready(server))
        return;
    fputs("sub ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fputc(' ', wal_fh);
    fput_hex(wal_fh, (const unsigned char *)filter, strlen(filter));
    fprintf(wal_fh, " %u %d\n", qos, multilevel ? 1 : 0);
    wal_finish();
}

void persistence_wal_unsub(struct server *server, const char *session_id,
                           const char *filter, bool multilevel)
{
    if (!wal_persistent(server, session_id) || !filter || !wal_ready(server))
        return;
    fputs("unsub ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fputc(' ', wal_fh);
    fput_hex(wal_fh, (const unsigned char *)filter, strlen(filter));
    fprintf(wal_fh, " %d\n", multilevel ? 1 : 0);
    wal_finish();
}

void persistence_wal_queued(struct server *server, const char *session_id,
                            const char *topic, unsigned char qos,
                            unsigned short mid,
                            const unsigned char *payload, size_t len)
{
    if (!wal_persistent(server, session_id) || !topic || !wal_ready(server))
        return;
    fputs("queued ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fputc(' ', wal_fh);
    fput_hex(wal_fh, (const unsigned char *)topic, strlen(topic));
    fprintf(wal_fh, " %u %u ", qos, mid);
    fput_hex(wal_fh, payload, len);
    fputc('\n', wal_fh);
    wal_finish();
}

void persistence_wal_dropped(struct server *server, const char *session_id,
                             unsigned short mid)
{
    if (!wal_persistent(server, session_id) || !wal_ready(server))
        return;
    fputs("dropped ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fprintf(wal_fh, " %u\n", mid);
    wal_finish();
}

void persistence_wal_flush(struct server *server, const char *session_id,
                           unsigned long count)
{
    if (!wal_persistent(server, session_id) || !wal_ready(server))
        return;
    fputs("flush ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fprintf(wal_fh, " %lu\n", count);
    wal_finish();
}

void persistence_wal_inflight(struct client_session *session,
                              struct inflight_msg *imsg)
{
    struct server *server = session ? session->server : NULL;
    if (!session || !imsg || !imsg->packet || !server ||
        !wal_persistent(server, session->session_id) || !wal_ready(server))
        return;
    fputs("inflight ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session->session_id,
             strlen(session->session_id));
    fputc(' ', wal_fh);
    fput_hex(wal_fh, imsg->packet->publish.topic,
             imsg->packet->publish.topiclen);
    fprintf(wal_fh, " %u %u ", imsg->qos, imsg->mid);
    fput_hex(wal_fh, imsg->packet->publish.payload,
             imsg->packet->publish.payloadlen);
    fputc('\n', wal_fh);
    wal_finish();
}

void persistence_wal_inflight_del(struct server *server,
                                  const char *session_id, unsigned short mid)
{
    if (!wal_persistent(server, session_id) || !wal_ready(server))
        return;
    fputs("inflight-del ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fprintf(wal_fh, " %u\n", mid);
    wal_finish();
}

void persistence_wal_ack(struct server *server, const char *session_id,
                         unsigned short mid, time_t ack_sent)
{
    if (!wal_persistent(server, session_id) || !wal_ready(server))
        return;
    fputs("ack ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fprintf(wal_fh, " %u %ld\n", mid, (long)ack_sent);
    wal_finish();
}

void persistence_wal_q2(struct server *server, const char *session_id,
                        unsigned short mid)
{
    if (!wal_persistent(server, session_id) || !wal_ready(server))
        return;
    fputs("q2 ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fprintf(wal_fh, " %u\n", mid);
    wal_finish();
}

void persistence_wal_q2_del(struct server *server, const char *session_id,
                            unsigned short mid)
{
    if (!wal_persistent(server, session_id) || !wal_ready(server))
        return;
    fputs("q2-del ", wal_fh);
    fput_hex(wal_fh, (const unsigned char *)session_id, strlen(session_id));
    fprintf(wal_fh, " %u\n", mid);
    wal_finish();
}

void persistence_wal_compact(struct server *server)
{
    if (wal_loading || !server || !server->conf ||
        !server->conf->persistence_file[0])
        return;
    if (wal_fh) {
        fclose(wal_fh);
        wal_fh = NULL;
    }
    if (!wal_path(server))
        return;
    if (remove(wal_path_buf) != 0 && errno != ENOENT) {
        log_warning("Cannot remove the write-ahead log %s: %s", wal_path_buf,
                    strerror(errno));
        /* An empty file still gets a fresh header on the next append */
        if (truncate(wal_path_buf, 0) != 0)
            log_warning("Cannot truncate the write-ahead log %s: %s",
                        wal_path_buf, strerror(errno));
    }
    ++wal_generation;
    wal_records = 0;
}

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

static void save_session(struct server *server, FILE *fh,
                         struct client_session *s)
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
    topic_store_wildcards_foreach(server->store, s->session_id,
                                  save_wildcard_sub, &ctx);

    /*
     * Inflight state, in the order the retransmission scan walks it. A
     * message still sitting in the offline queue is written as `queued`; a
     * stored packet waiting for its PUBACK/PUBREC as `inflight`; the PUBREL
     * half of a QoS 2 flow (the PUBLISH was acknowledged, the PUBCOMP is
     * still outstanding) as `ack`. The queue and the inflight table share
     * the identifier, which is what makes `dropped` and `flush` replay
     * against the same entries
     */
    for (unsigned i = 0; i < s->pending_len; ++i) {
        unsigned short mid       = s->pending_mids[i];
        struct inflight_msg *imsg = session_inflight(s, mid);
        if (!imsg)
            continue;

        struct queued_msg *qm = NULL;
        list_foreach(qitem, s->outgoing_msgs)
        {
            struct queued_msg *candidate = qitem->data;
            if (candidate && candidate->mid == mid) {
                qm = candidate;
                break;
            }
        }

        if (qm && qm->packet) {
            fputs("queued ", fh);
            fput_hex(fh, (const unsigned char *)s->session_id,
                     strlen(s->session_id));
            fputc(' ', fh);
            fput_hex(fh, qm->packet->publish.topic,
                     qm->packet->publish.topiclen);
            /* The packet is shared by every subscriber of the original
               publish, the per session level lives in the queue entry */
            fprintf(fh, " %u %u ", qm->qos, qm->mid);
            fput_hex(fh, qm->packet->publish.payload,
                     qm->packet->publish.payloadlen);
            fputc('\n', fh);
        } else if (imsg->packet) {
            fputs("inflight ", fh);
            fput_hex(fh, (const unsigned char *)s->session_id,
                     strlen(s->session_id));
            fputc(' ', fh);
            fput_hex(fh, imsg->packet->publish.topic,
                     imsg->packet->publish.topiclen);
            fprintf(fh, " %u %u ", imsg->qos, imsg->mid);
            fput_hex(fh, imsg->packet->publish.payload,
                     imsg->packet->publish.payloadlen);
            fputc('\n', fh);
        } else if (imsg->ack_sent > 0) {
            fputs("ack ", fh);
            fput_hex(fh, (const unsigned char *)s->session_id,
                     strlen(s->session_id));
            fprintf(fh, " %u %ld\n", imsg->mid, (long)imsg->ack_sent);
        }
    }

    /* Inbound exactly-once flows still waiting for their PUBREL */
    for (unsigned i = 0; i < s->q2_recv_len; ++i) {
        fputs("q2 ", fh);
        fput_hex(fh, (const unsigned char *)s->session_id,
                 strlen(s->session_id));
        fprintf(fh, " %u\n", s->q2_recv_mids[i]);
    }
}

static void persistence_write(struct server *server, FILE *fh)
{
    fprintf(fh, "# sol persistence v2 %llu %lu\n", wal_generation,
            wal_records);

    /* Retained messages, one record per topic holding one */
    topic_store_map(server->store, NULL, save_retained, fh);

    /* Persistent sessions with their subscriptions and queued messages */
    struct client_session *s, *tmp;
    HASH_ITER(hh, server->sessions, s, tmp)
    {
        /* A clean session dies with the connection of its client: there is
           nothing to keep for the next run */
        if (s->clean_session)
            continue;
        save_session(server, fh, s);
    }
}

/*
 * Flush the directory entry changed by the rename: on most filesystems the
 * rename itself only becomes durable once the directory holding it has been
 * fsynced. Best effort on purpose — a filesystem that refuses to open or
 * fsync a directory must not turn a snapshot that did reach the disk into a
 * reported failure
 */
static void fsync_parent_dir(const char *path)
{
    char *copy    = try_strdup(path);
    char *slash   = strrchr(copy, '/');
    const char *dir = ".";
    if (slash) {
        *slash = '\0';
        dir    = copy;
    }
    int fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd >= 0) {
        (void)fsync(fd);
        close(fd);
    }
    free_memory(copy);
}

void persistence_save(struct server *server, const char *path)
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

    persistence_write(server, fh);

    /*
     * fclose() only hands the bytes to the kernel: without fsync() a power
     * loss can leave the renamed file empty or half written even though the
     * rename itself is atomic. Both are checked, and a failed flush keeps
     * the previous snapshot in place instead of replacing it with a file
     * nobody can vouch for
     */
    int flush_rc = fflush(fh);
    int sync_rc  = flush_rc == 0 ? fsync(fileno(fh)) : -1;
    int close_rc = fclose(fh);

    if (flush_rc != 0 || sync_rc != 0 || close_rc != 0) {
        log_warning("Cannot flush the persistence file %s: %s", tmp,
                    strerror(errno));
        remove(tmp);
    } else if (rename(tmp, path) != 0) {
        log_warning("Cannot update the persistence file %s: %s", path,
                    strerror(errno));
        remove(tmp);
    } else {
        fsync_parent_dir(path);
        /* The caller held the global mutex for the whole snapshot, so no
           state change could slip in behind the write. The log is now
           redundant: truncating it here is the compaction point */
        dirty = false;
        persistence_wal_compact(server);
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

static struct client_session *load_session_id(struct server *server,
                                              const char *id)
{
    struct client_session *s = NULL;
    HASH_FIND_STR(server->sessions, id, s);
    if (s)
        return s;
    return session_restore(server, id);
}

/* Look up a session without creating it: used by the undo records */
static struct client_session *find_session_id(struct server *server,
                                              const char *id)
{
    struct client_session *s = NULL;
    HASH_FIND_STR(server->sessions, id, s);
    return s;
}

static void load_retained_record(struct server *server, char *fields)
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
     * The store keys the topic exactly as the packet carries it. A v1 file
     * wrote the raw name too (the trailing '/' it added lived only in the
     * key), so the same rule migrates both formats without inventing a level
     */
    struct topic *t = topic_store_get_or_put(server->store, (char *)topic);

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

static void load_sub_record(struct server *server, char *fields)
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

    struct client_session *s = load_session_id(server, (const char *)id);
    free_memory(id);
    if (!s) {
        free_memory(filter);
        return;
    }

    bool multilevel = atoi(multi_field) != 0;

    /*
     * A v2 record already holds the filter exactly as the client sent it
     * ("a/#", "a/+" or "a/"). A v1 record holds the old normalized key: a
     * trailing '/' was appended to every topic and filter, and the '#' of a
     * multilevel filter was stripped. Undo both here, keeping "/" (a valid
     * two-empty-level topic) as it is
     */
    char *full = NULL;
    if (load_version == 1) {
        if (multilevel && strcmp((const char *)filter, "#") != 0) {
            full = try_alloc(filter_len + 2);
            snprintf(full, filter_len + 2, "%s#", (const char *)filter);
        } else if (!multilevel && filter_len > 1 &&
                   filter[filter_len - 1] == '/') {
            full = try_alloc(filter_len);
            snprintf(full, filter_len, "%s", (const char *)filter);
            full[filter_len - 1] = '\0';
        }
    }

    session_subscribe(s, full ? full : (const char *)filter,
                      (unsigned char)atoi(qos_field));
    free_memory(full);
    free_memory(filter);
}

/* A packet identifier is 1-65535 (§2.2.1), 0 never names a flow */
static bool parse_mid(const char *field, unsigned short *mid)
{
    if (!field)
        return false;
    char *end          = NULL;
    unsigned long value = strtoul(field, &end, 10);
    if (end == field || *end != '\0' || value == 0 ||
        value >= MAX_INFLIGHT_MSGS)
        return false;
    *mid = (unsigned short)value;
    return true;
}

/*
 * Decode the payload field, which is the last one and is therefore absent
 * when the message carried no bytes at all: an empty payload is a valid MQTT
 * message, not a malformed record
 */
static unsigned char *decode_payload(const char *field, size_t *len)
{
    if (!field) {
        unsigned char *empty = try_alloc(1);
        empty[0]             = '\0';
        *len                 = 0;
        return empty;
    }
    return hex_decode(field, len);
}

static void load_queued_record(struct server *server, char *fields)
{
    char *cursor      = fields;
    char *id_field    = next_field(&cursor);
    char *topic_field = next_field(&cursor);
    char *qos_field   = next_field(&cursor);

    if (!id_field || !topic_field || !qos_field) {
        log_warning("Malformed queued message record, skipped");
        return;
    }

    /*
     * v1 wrote four fields (id topic qos payload); v2 adds the packet
     * identifier before the payload, so that a later dropped/flush record
     * can name the very same entry
     */
    char *mid_field  = NULL;
    char *data_field = NULL;
    if (load_version == 1)
        data_field = next_field(&cursor);
    else {
        mid_field  = next_field(&cursor);
        data_field = next_field(&cursor);
    }

    unsigned short mid = 0;
    if (load_version != 1 && !parse_mid(mid_field, &mid)) {
        log_warning("Malformed packet identifier in a queued message "
                    "record, skipped");
        return;
    }

    size_t id_len = 0, topic_len = 0, payload_len = 0;
    unsigned char *id      = hex_decode(id_field, &id_len);
    unsigned char *topic   = hex_decode(topic_field, &topic_len);
    unsigned char *payload = decode_payload(data_field, &payload_len);
    if (!id || !topic || !payload) {
        log_warning("Malformed hexadecimal field in a queued message "
                    "record, skipped");
        free_memory(id);
        free_memory(topic);
        free_memory(payload);
        return;
    }

    struct client_session *s = load_session_id(server, (const char *)id);
    if (s) {
        int qos = atoi(qos_field);
        if (qos < AT_LEAST_ONCE || qos > EXACTLY_ONCE)
            qos = AT_LEAST_ONCE; /* a queued message is never QoS 0 */
        if (load_version == 1)
            session_queue_message(s, (const char *)topic, (unsigned char)qos,
                                  payload, payload_len);
        else
            session_restore_queued(s, (const char *)topic,
                                   (unsigned char)qos, mid, payload,
                                   payload_len);
    }

    free_memory(id);
    free_memory(topic);
    free_memory(payload);
}

static void load_inflight_record(struct server *server, char *fields)
{
    char *cursor      = fields;
    char *id_field    = next_field(&cursor);
    char *topic_field = next_field(&cursor);
    char *qos_field   = next_field(&cursor);
    char *mid_field   = next_field(&cursor);
    char *data_field  = next_field(&cursor);

    if (!id_field || !topic_field || !qos_field) {
        log_warning("Malformed inflight record, skipped");
        return;
    }

    unsigned short mid = 0;
    if (!parse_mid(mid_field, &mid)) {
        log_warning("Malformed packet identifier in an inflight record, "
                    "skipped");
        return;
    }

    size_t id_len = 0, topic_len = 0, payload_len = 0;
    unsigned char *id      = hex_decode(id_field, &id_len);
    unsigned char *topic   = hex_decode(topic_field, &topic_len);
    unsigned char *payload = decode_payload(data_field, &payload_len);
    if (!id || !topic || !payload) {
        log_warning("Malformed hexadecimal field in an inflight record, "
                    "skipped");
        free_memory(id);
        free_memory(topic);
        free_memory(payload);
        return;
    }

    struct client_session *s = load_session_id(server, (const char *)id);
    if (s) {
        int qos = atoi(qos_field);
        if (qos < AT_LEAST_ONCE || qos > EXACTLY_ONCE)
            qos = AT_LEAST_ONCE;
        session_restore_inflight(s, (const char *)topic, (unsigned char)qos,
                                 mid, payload, payload_len);
    }

    free_memory(id);
    free_memory(topic);
    free_memory(payload);
}

static void load_inflight_del_record(struct server *server, char *fields)
{
    char *cursor     = fields;
    char *id_field   = next_field(&cursor);
    char *mid_field  = next_field(&cursor);

    unsigned short mid = 0;
    if (!id_field || !parse_mid(mid_field, &mid)) {
        log_warning("Malformed inflight-del record, skipped");
        return;
    }

    size_t id_len     = 0;
    unsigned char *id = hex_decode(id_field, &id_len);
    if (!id) {
        log_warning("Malformed client identifier in an inflight-del "
                    "record, skipped");
        return;
    }

    struct client_session *s = find_session_id(server, (const char *)id);
    if (s)
        session_restore_inflight_del(s, mid);
    free_memory(id);
}

static void load_ack_record(struct server *server, char *fields)
{
    char *cursor      = fields;
    char *id_field    = next_field(&cursor);
    char *mid_field   = next_field(&cursor);
    char *sent_field  = next_field(&cursor);

    unsigned short mid = 0;
    if (!id_field || !parse_mid(mid_field, &mid) || !sent_field) {
        log_warning("Malformed ack record, skipped");
        return;
    }

    size_t id_len     = 0;
    unsigned char *id = hex_decode(id_field, &id_len);
    if (!id) {
        log_warning("Malformed client identifier in an ack record, skipped");
        return;
    }

    struct client_session *s = load_session_id(server, (const char *)id);
    if (s)
        session_restore_ack(s, mid, (time_t)strtol(sent_field, NULL, 10));
    free_memory(id);
}

static void load_q2_record(struct server *server, char *fields)
{
    char *cursor     = fields;
    char *id_field   = next_field(&cursor);
    char *mid_field  = next_field(&cursor);

    unsigned short mid = 0;
    if (!id_field || !parse_mid(mid_field, &mid)) {
        log_warning("Malformed q2 record, skipped");
        return;
    }

    size_t id_len     = 0;
    unsigned char *id = hex_decode(id_field, &id_len);
    if (!id) {
        log_warning("Malformed client identifier in a q2 record, skipped");
        return;
    }

    struct client_session *s = load_session_id(server, (const char *)id);
    if (s)
        session_restore_q2(s, mid);
    free_memory(id);
}

static void load_q2_del_record(struct server *server, char *fields)
{
    char *cursor     = fields;
    char *id_field   = next_field(&cursor);
    char *mid_field  = next_field(&cursor);

    unsigned short mid = 0;
    if (!id_field || !parse_mid(mid_field, &mid)) {
        log_warning("Malformed q2-del record, skipped");
        return;
    }

    size_t id_len     = 0;
    unsigned char *id = hex_decode(id_field, &id_len);
    if (!id) {
        log_warning("Malformed client identifier in a q2-del record, "
                    "skipped");
        return;
    }

    struct client_session *s = find_session_id(server, (const char *)id);
    if (s)
        session_restore_q2_del(s, mid);
    free_memory(id);
}

static void load_retained_clear_record(struct server *server, char *fields)
{
    char *cursor      = fields;
    char *topic_field = next_field(&cursor);

    if (!topic_field) {
        log_warning("Malformed retained-clear record, skipped");
        return;
    }

    size_t topic_len = 0;
    unsigned char *topic = hex_decode(topic_field, &topic_len);
    if (!topic) {
        log_warning("Malformed topic in a retained-clear record, skipped");
        return;
    }

    struct topic *t = topic_store_get(server->store, (const char *)topic);
    if (t && t->retained) {
        DECREF(t->retained, struct mqtt_packet);
        t->retained = NULL;
    }
    free_memory(topic);
}

static void load_unsub_record(struct server *server, char *fields)
{
    char *cursor       = fields;
    char *id_field     = next_field(&cursor);
    char *filter_field = next_field(&cursor);

    if (!id_field || !filter_field) {
        log_warning("Malformed unsub record, skipped");
        return;
    }

    size_t id_len = 0, filter_len = 0;
    unsigned char *id     = hex_decode(id_field, &id_len);
    unsigned char *filter = hex_decode(filter_field, &filter_len);
    if (!id || !filter) {
        log_warning("Malformed hexadecimal field in an unsub record, "
                    "skipped");
        free_memory(id);
        free_memory(filter);
        return;
    }

    struct client_session *s = find_session_id(server, (const char *)id);
    if (s)
        session_unsubscribe(s, (const char *)filter);
    free_memory(id);
    free_memory(filter);
}

static void load_dropped_record(struct server *server, char *fields)
{
    char *cursor     = fields;
    char *id_field   = next_field(&cursor);
    char *mid_field  = next_field(&cursor);

    unsigned short mid = 0;
    if (!id_field || !parse_mid(mid_field, &mid)) {
        log_warning("Malformed dropped record, skipped");
        return;
    }

    size_t id_len     = 0;
    unsigned char *id = hex_decode(id_field, &id_len);
    if (!id) {
        log_warning("Malformed client identifier in a dropped record, "
                    "skipped");
        return;
    }

    struct client_session *s = find_session_id(server, (const char *)id);
    if (s)
        session_restore_dropped(s, mid);
    free_memory(id);
}

static void load_flush_record(struct server *server, char *fields)
{
    char *cursor     = fields;
    char *id_field   = next_field(&cursor);
    char *count_field = next_field(&cursor);

    if (!id_field || !count_field) {
        log_warning("Malformed flush record, skipped");
        return;
    }

    size_t id_len     = 0;
    unsigned char *id = hex_decode(id_field, &id_len);
    if (!id) {
        log_warning("Malformed client identifier in a flush record, "
                    "skipped");
        return;
    }

    struct client_session *s = find_session_id(server, (const char *)id);
    if (s)
        session_flush_queue(s, strtoul(count_field, NULL, 10));
    free_memory(id);
}

static void load_session_del_record(struct server *server, char *fields)
{
    char *cursor     = fields;
    char *id_field   = next_field(&cursor);

    if (!id_field) {
        log_warning("Malformed session-del record, skipped");
        return;
    }

    size_t id_len     = 0;
    unsigned char *id = hex_decode(id_field, &id_len);
    if (!id) {
        log_warning("Malformed client identifier in a session-del record, "
                    "skipped");
        return;
    }
    session_delete(server, (const char *)id);
    free_memory(id);
}

static bool load_line(struct server *server, char *line)
{
    if (line[0] == '#' || line[0] == '\0')
        return false;

    char *cursor = line;
    char *tag    = next_field(&cursor);
    if (!tag)
        return false;

    if (STREQ(tag, "retained-clear", 14)) {
        load_retained_clear_record(server, cursor);
    } else if (STREQ(tag, "retained", 8)) {
        load_retained_record(server, cursor);
    } else if (STREQ(tag, "session-del", 11)) {
        load_session_del_record(server, cursor);
    } else if (STREQ(tag, "session", 7)) {
        size_t id_len      = 0;
        char *id_field     = next_field(&cursor);
        unsigned char *id  = id_field ? hex_decode(id_field, &id_len) : NULL;
        if (!id) {
            log_warning("Malformed session record, skipped");
            return false;
        }
        load_session_id(server, (const char *)id);
        free_memory(id);
    } else if (STREQ(tag, "sub", 3)) {
        load_sub_record(server, cursor);
    } else if (STREQ(tag, "unsub", 5)) {
        load_unsub_record(server, cursor);
    } else if (STREQ(tag, "queued", 6)) {
        load_queued_record(server, cursor);
    } else if (STREQ(tag, "inflight-del", 12)) {
        load_inflight_del_record(server, cursor);
    } else if (STREQ(tag, "inflight", 8)) {
        load_inflight_record(server, cursor);
    } else if (STREQ(tag, "dropped", 7)) {
        load_dropped_record(server, cursor);
    } else if (STREQ(tag, "flush", 5)) {
        load_flush_record(server, cursor);
    } else if (STREQ(tag, "ack", 3)) {
        load_ack_record(server, cursor);
    } else if (STREQ(tag, "q2-del", 6)) {
        load_q2_del_record(server, cursor);
    } else if (STREQ(tag, "q2", 2)) {
        load_q2_record(server, cursor);
    } else {
        log_warning("Unknown persistence record '%s', skipped", tag);
        return false;
    }
    return true;
}

/*
 * Read a whole file in one go instead of line by line: a record carrying a
 * payload is as long as twice that payload, which a fixed sized buffer could
 * not hold. Returns NULL when the file cannot be opened or read
 */
static char *read_file(const char *path, size_t *length)
{
    FILE *fh = fopen(path, "r");
    if (!fh)
        return NULL;

    if (fseek(fh, 0, SEEK_END) != 0) {
        fclose(fh);
        return NULL;
    }
    long size = ftell(fh);
    rewind(fh);
    if (size < 0) {
        fclose(fh);
        return NULL;
    }

    char *content = try_alloc((size_t)size + 1);
    size_t got    = fread(content, 1, (size_t)size, fh);
    content[got]  = '\0';
    fclose(fh);
    *length = got;
    return content;
}

/*
 * Read the generation and the record count out of a snapshot header, leaving
 * `load_version` set for the loader. A v1 file has neither (it predates the
 * log) and a file without a header is treated as the current format: the only
 * difference the loader cares about is the trailing '/' of v1
 */
static unsigned long long snapshot_header(char *content,
                                          unsigned long *records)
{
    load_version = 2;
    *records     = 0;

    if (strncmp(content, "# sol persistence v1", 20) == 0) {
        load_version = 1;
        return 0;
    }

    if (strncmp(content, "# sol persistence v2", 20) == 0) {
        char *end              = NULL;
        unsigned long long gen = strtoull(content + 20, &end, 10);
        if (end)
            *records = strtoul(end, NULL, 10);
        return gen;
    }

    return 0;
}

/*
 * Walk the records of `content`, calling load_line() on the ones past the
 * first `skip`. Comments and empty lines are not records and are not counted.
 * The returned count is what the snapshot header uses to say how many log
 * records it already contains; a trailing fragment without its newline is an
 * append a crash cut short and is ignored rather than parsed
 */
static unsigned long load_records(struct server *server, char *content,
                                  unsigned long skip, size_t *applied)
{
    unsigned long index = 0;
    char *line          = content;

    while (line && *line) {
        char *eol = strchr(line, '\n');
        if (!eol)
            break;
        *eol = '\0';
        if (line[0] != '#' && line[0] != '\0') {
            if (index >= skip && load_line(server, line))
                (*applied)++;
            index++;
        }
        line = eol + 1;
    }
    return index;
}

void persistence_load(struct server *server, const char *path)
{
    if (!path || !path[0])
        return;

    /*
     * The loader runs before any event loop exists, so no other thread can
     * mutate the state, and every append is suppressed: replaying a record
     * must never write it back to the very log it came from
     */
    wal_loading = true;

    unsigned long long snap_gen = 0;
    unsigned long snap_records  = 0;
    size_t applied              = 0;
    size_t length               = 0;

    char *content = read_file(path, &length);
    if (!content) {
        log_info("No persistence file at %s, starting with an empty store",
                 path);
    } else {
        snap_gen = snapshot_header(content, &snap_records);
        load_records(server, content, 0, &applied);
        log_info("Restored %lu records from %s", (unsigned long)applied, path);
        free_memory(content);
    }

    /*
     * Then the write-ahead log: its records are the mutations that happened
     * since the snapshot was taken. A log naming the same generation as the
     * snapshot starts exactly where the snapshot stopped, so its first
     * `snap_records` records are already contained in the snapshot and are
     * skipped; a newer generation (the normal case once a compaction has
     * happened) is replayed from the start; an older one is stale and can be
     * dropped
     */
    char walpath[0x1004];
    int n = snprintf(walpath, sizeof(walpath), "%s.wal", path);
    bool wal_seen = false;
    if (n > 0 && (size_t)n < sizeof(walpath)) {
        content = read_file(walpath, &length);
        if (content) {
            unsigned long long gen = 0;
            if (strncmp(content, "# sol wal v2", 12) == 0)
                gen = strtoull(content + 12, NULL, 10);

            /*
             * Drop an append a crash cut in the middle: a line without its
             * newline is not a record and, left there, would swallow the next
             * one appended after it
             */
            if (length > 0 && content[length - 1] != '\n') {
                char *last = strrchr(content, '\n');
                off_t keep = last ? (off_t)(last - content + 1) : 0;
                log_warning("Discarding the partial trailing record of %s",
                            walpath);
                if (truncate(walpath, keep) != 0)
                    log_warning("Cannot truncate %s: %s", walpath,
                                strerror(errno));
                content[keep] = '\0';
                length        = (size_t)keep;
            }

            if (gen < snap_gen) {
                log_info("Ignoring the stale write-ahead log %s (generation "
                         "%llu, snapshot %llu)",
                         walpath, gen, snap_gen);
                if (remove(walpath) != 0 && errno != ENOENT) {
                    log_warning("Cannot remove the stale write-ahead log %s: "
                                "%s",
                                walpath, strerror(errno));
                    /* An empty file gets a fresh header on the next append */
                    if (truncate(walpath, 0) != 0)
                        log_warning("Cannot truncate %s: %s", walpath,
                                    strerror(errno));
                }
            } else {
                unsigned long skip = gen == snap_gen ? snap_records : 0;
                size_t replayed    = 0;
                load_version       = 2;
                wal_records = load_records(server, content, skip, &replayed);
                wal_generation     = gen;
                wal_seen           = true;
                log_info("Replayed %lu records from %s (generation %llu)",
                         (unsigned long)replayed, walpath, gen);
            }
            free_memory(content);
        }
    }

    /*
     * No usable log: the next append starts the generation right after the
     * one the snapshot names, so it is unambiguously newer. The replay left
     * the in-memory state exactly where the snapshot plus the log put it, so
     * there is nothing to snapshot because of the load alone
     */
    if (!wal_seen) {
        wal_generation = snap_gen + 1;
        wal_records    = 0;
    }

    dirty       = false;
    wal_loading = false;
}
