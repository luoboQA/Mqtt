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

#ifndef HANDLERS_H
#define HANDLERS_H

#include <stdbool.h>
#include <stddef.h>
#include <time.h>

struct server;
struct topic;
struct mqtt_packet;
struct io_event;
struct client;
struct client_session;

/*
 * Make room for `len` bytes in the client write buffer, growing it on demand
 * up to max_request_size. False when the reply would exceed the configured
 * limit: the caller drops the reply, the sender retransmits
 */
bool outgoing_reserve(struct client *, size_t);

/*
 * The inflight entry of `mid`, NULL when the identifier is free. The table
 * only holds the messages actually awaiting an acknowledgement
 */
struct inflight_msg *session_inflight(struct client_session *,
                                      unsigned short);

int publish_message(struct server *, struct mqtt_packet *,
                    const struct topic *);

/*
 * Create a persistent session and register it in the server map, the entry
 * point the persistence loader uses to rebuild what it read back from disk.
 * Meant to run before the event loops start: it takes no lock
 */
struct client_session *session_restore(struct server *server,
                                       const char *session_id);

/*
 * Register `filter` for `session`, exactly like one tuple of a SUBSCRIBE
 * packet would (the filter is validated and normalized here). The caller must
 * hold the global mutex; returns false when the filter is malformed
 */
bool session_subscribe(struct client_session *, const char *filter,
                       unsigned char qos);

/*
 * Queue a message for a session that is not connected, at QoS > 0: the packet
 * is built here, takes an identifier of its own and is registered in the
 * session inflight tables, so a later reconnect replays it. The caller must
 * hold the global mutex
 */
void session_queue_message(struct client_session *, const char *topic,
                           unsigned char qos, const unsigned char *payload,
                           size_t len);

/*
 * Same as session_queue_message(), but with the packet identifier read back
 * from the file: the persistence loader needs it to keep the offline queue
 * and the inflight table of a restored session consistent, so that a later
 * `dropped` or `flush` record names the very same entry. The caller must
 * hold the global mutex (or be the startup loader)
 */
void session_restore_queued(struct client_session *, const char *topic,
                            unsigned char qos, unsigned short mid,
                            const unsigned char *payload, size_t len);

/*
 * Rebuild one inflight entry (a stored PUBLISH awaiting its PUBACK/PUBREC)
 * or the PUBREL half of a QoS 2 flow (`ack_sent`), and the inbound QoS 2
 * deduplication set. Restored entries are due immediately: their `seen`
 * timestamp is zero, so the first retransmission scan after a reconnect
 * sends them again
 */
void session_restore_inflight(struct client_session *, const char *topic,
                              unsigned char qos, unsigned short mid,
                              const unsigned char *payload, size_t len);
void session_restore_ack(struct client_session *, unsigned short mid,
                         time_t ack_sent);
void session_restore_q2(struct client_session *, unsigned short mid);
void session_restore_q2_del(struct client_session *, unsigned short mid);

/* Drop only the inflight entry of `mid`, leaving the offline queue alone */
void session_restore_inflight_del(struct client_session *, unsigned short mid);

/* Remove the queued message named by `mid` (and its inflight entry) */
void session_restore_dropped(struct client_session *, unsigned short mid);

/* Forget the first `count` queued messages, keeping their inflight entries */
void session_flush_queue(struct client_session *, unsigned long count);

/*
 * Drop the oldest message queued for a disconnected session, releasing the
 * packet, its inflight entry and the queue slot: what the max_memory policy
 * does to give memory back. Returns false when the queue is already empty
 */
bool session_drop_oldest_queued(struct client_session *);

/*
 * Remove a session from the map and detach every subscription it still
 * holds, exactly like the teardown of a clean session does. Used by the
 * persistence loader to replay a `session-del` record
 */
void session_delete(struct server *server, const char *session_id);
void session_destroy_all(struct server *server);

/*
 * Remove one filter from a session, the body of an UNSUBSCRIBE tuple shared
 * with the persistence loader. Returns false when the filter is malformed
 */
bool session_unsubscribe(struct client_session *, const char *filter);

/*
 * Deliver an LWT message through the same path a normal PUBLISH takes
 * (exact topic key, wildcard subscriber attach): acquires the locks it needs
 * itself
 */
void publish_lwt(struct server *, struct mqtt_packet *);

int handle_command(unsigned, struct io_event *);

/*
 * Does a topic name match a filter, with the same wildcard rules a
 * subscription follows? Used by the access control list
 */
bool topic_access_match(const char *topic, const char *filter);

#endif
