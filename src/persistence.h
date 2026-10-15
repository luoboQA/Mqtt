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

/*
 * On disk persistence of the state a broker restart must not lose: the
 * retained message of every topic and the persistent sessions, with their
 * subscriptions and the messages queued while their client was away.
 *
 * A snapshot is written periodically (and once more on a clean shutdown) to
 * the file named by the `persistence_file` configuration key: the whole state
 * is rewritten into a temporary file that is renamed over the destination, so
 * a crash in the middle of a write can never leave a truncated file behind
 * for the next start to read.
 *
 * What is *not* persisted, deliberately (see the README): the in flight state
 * of the connections that are online at the time of the snapshot, the inbound
 * QoS 2 deduplication set and the wills, all of which only make sense for a
 * connection that a restart has already torn down.
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
