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

#ifndef ACL_H
#define ACL_H

#include <stdbool.h>

/*
 * Topic level authorization: authentication says *who* a client is, an access
 * control list says what it may do once it is in. Without one any
 * authenticated client can read and write every topic, including the control
 * topics of somebody else's devices, which is what keeps a broker out of any
 * deployment that is not entirely trusted.
 *
 * The file is read at startup (see acl_load()) and holds one rule per line:
 *
 *   # comment
 *   <allow|deny> <read|write|readwrite> <principal> <topic filter>
 *
 * where the principal is 'client:<id>', 'user:<name>' (the username the
 * client authenticated with) or '*' for anybody. Rules are evaluated in
 * order, the first one whose principal and filter match wins; a topic that no
 * rule covers is denied, so an empty file denies everything. Example:
 *
 *   allow readwrite client:sensor-1  sensors/#
 *   allow read      user:alice       sensors/#
 *   allow readwrite *                public/#
 */

enum acl_access {
    ACL_READ  = 0x1, /* SUBSCRIBE, and the retained flush that follows it */
    ACL_WRITE = 0x2  /* PUBLISH, and the will a CONNECT registers */
};

/* Access or deny `topic` for the given client, `access` is ACL_* */
bool acl_check(const char *client_id, const char *username, const char *topic,
               int access);

/* True when an ACL file is in use (and therefore the default is to deny) */
bool acl_enabled(void);

/*
 * Load the rules from `path`. A missing path (or an empty one) disables the
 * ACL entirely, everything is allowed: that is the historical behaviour and
 * what a broker without a `acl_file` key configured keeps doing. A configured
 * file that cannot be read is fatal: starting with no rules at all would
 * silently deny everything to everybody, and a broker that cannot do its job
 * is better off not starting
 */
void acl_load(const char *path);

/* Release the rules */
void acl_destroy(void);

#endif
