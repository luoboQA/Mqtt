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

#ifndef PERSISTENCE_H
#define PERSISTENCE_H

#include <stdbool.h>
#include <time.h>

/*
 * On disk persistence of the state a broker restart must not lose: the
 * retained message of every topic and the persistent sessions, with their
 * subscriptions, the messages queued while their client was away and the
 * in flight state of their connections (a stored PUBLISH awaiting its
 * acknowledgement, the PUBREL half of a QoS 2 flow and the inbound QoS 2
 * deduplication set).
 *
 * A snapshot is written periodically (and once more on a clean shutdown) to
 * the file named by the `persistence_file` configuration key: the whole state
 * is rewritten into a temporary file that is renamed over the destination, so
 * a crash in the middle of a write can never leave a truncated file behind
 * for the next start to read.
 *
 * Between two snapshots every mutation is appended to a write-ahead log
 * (`<persistence_file>.wal`), fsynced before the broker moves on: a crash
 * loses at most the last append, not the whole interval since the previous
 * snapshot. The log is replayed on top of the snapshot at startup and
 * truncated once a fresh snapshot has replaced the old one.
 *
 * What is *not* persisted, deliberately (see the README): the wills, which
 * belong to the network connection of a client rather than to its session.
 */

/*
 * Record that something covered by the snapshot changed (a retained message,
 * a subscription, a queued message, a session). The periodic task only
 * rewrites the file when this has been called
 */
void persistence_mark_dirty(void);

/* True when the state changed since the last save */
bool persistence_is_dirty(void);

/*
 * Write-ahead log. Every mutation covered by the snapshot appends one record
 * to `<persistence_file>.wal` before the in-memory state is used again; the
 * snapshot becomes the compaction point, after which the log is truncated.
 * A crash between two snapshots therefore loses at most the records the
 * kernel did not accept, not the whole interval. All the calls below are
 * no-ops when persistence is disabled, during the initial load, and for the
 * state of a clean session (which the snapshot never keeps).
 */
struct client_session;
struct inflight_msg;

void persistence_wal_retained(const char *topic, const unsigned char *payload,
                              size_t len, unsigned char qos);
void persistence_wal_retained_clear(const char *topic);
void persistence_wal_session(const char *session_id);
void persistence_wal_session_del(const char *session_id);
void persistence_wal_sub(const char *session_id, const char *filter,
                         unsigned char qos, bool multilevel);
void persistence_wal_unsub(const char *session_id, const char *filter,
                           bool multilevel);
void persistence_wal_queued(const char *session_id, const char *topic,
                            unsigned char qos, unsigned short mid,
                            const unsigned char *payload, size_t len);
void persistence_wal_dropped(const char *session_id, unsigned short mid);
void persistence_wal_flush(const char *session_id, unsigned long count);
void persistence_wal_inflight(struct client_session *session,
                              struct inflight_msg *imsg);
void persistence_wal_inflight_del(const char *session_id, unsigned short mid);
void persistence_wal_ack(const char *session_id, unsigned short mid,
                         time_t ack_sent);
void persistence_wal_q2(const char *session_id, unsigned short mid);
void persistence_wal_q2_del(const char *session_id, unsigned short mid);

/* Truncate the log after a successful snapshot (compaction) */
void persistence_wal_compact(void);

/*
 * Write the state to `path`. A NULL or empty path disables persistence
 * entirely, as does a file that cannot be opened (logged, not fatal)
 */
void persistence_save(const char *path);

/*
 * Rebuild the state saved by persistence_save(). Meant to run at startup,
 * before the event loops exist: it takes no lock and never fails, a missing
 * or unreadable file simply means "start empty"
 */
void persistence_load(const char *path);

#endif
