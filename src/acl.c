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

#include "acl.h"
#include "handlers.h"
#include "list.h"
#include "logging.h"
#include "memory.h"
#include "sol_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* One line of the access control file */
struct acl_rule {
    bool allow;
    int access;      /* ACL_READ, ACL_WRITE or both */
    bool any;        /* principal is '*': the rule applies to anybody */
    bool by_user;    /* match against the username instead of the client id */
    char *principal; /* client id, or username, NUL terminated */
    char *filter;    /* topic filter, as written in the file */
};

/*
 * The rules, in file order: the first one matching wins, which is what makes
 * a "deny ... #" line at the top of a file useful
 */
static List *rules = NULL;

bool acl_enabled(void) { return rules != NULL; }

static int rule_destructor(struct list_node *node)
{
    if (!node)
        return -SOL_ERR;
    struct acl_rule *rule = node->data;
    free_memory(rule->principal);
    free_memory(rule->filter);
    free_memory(rule);
    free_memory(node);
    return SOL_OK;
}

void acl_destroy(void)
{
    if (rules) {
        list_destroy(rules, 0);
        rules = NULL;
    }
}

/*
 * Read the next whitespace delimited token, NULL when the line is consumed.
 * The line is modified in place, the rules own their copies
 */
static char *acl_next_token(char **cursor)
{
    char *p = *cursor;

    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')
        p++;
    if (*p == '\0') {
        *cursor = p;
        return NULL;
    }

    char *token = p;
    while (*p && *p != ' ' && *p != '\t' && *p != '\r' && *p != '\n')
        p++;
    if (*p != '\0') {
        *p = '\0';
        p++;
    }
    *cursor = p;
    return token;
}

static void acl_add_rule(const char *allow, const char *access,
                         const char *principal, const char *filter)
{
    struct acl_rule *rule = try_alloc(sizeof(*rule));

    rule->allow = STREQ(allow, "allow", 5);

    if (STREQ(access, "readwrite", 9))
        rule->access = ACL_READ | ACL_WRITE;
    else if (STREQ(access, "write", 5))
        rule->access = ACL_WRITE;
    else if (STREQ(access, "read", 4))
        rule->access = ACL_READ;
    else {
        log_warning("ACL: unknown access '%s', the rule only allows reads",
                    access);
        rule->access = ACL_READ;
    }

    rule->any      = strcmp(principal, "*") == 0;
    rule->by_user  = strncmp(principal, "user:", 5) == 0;
    bool by_client = strncmp(principal, "client:", 7) == 0;
    if (!rule->any && !rule->by_user && !by_client)
        log_warning("ACL: principal '%s' has no 'client:' or 'user:' prefix, "
                    "treating it as a client id",
                    principal);

    const char *name = rule->any ? "" : rule->by_user ? principal + 5
                                        : by_client ? principal + 7
                                                    : principal;
    rule->principal = try_strdup(name);
    rule->filter    = try_strdup(filter);

    rules = list_push(rules, rule);
}

/*
 * One rule per line: <allow|deny> <read|write|readwrite> <principal> <filter>
 */
static void acl_parse_line(char *line, int linenr, const char *path)
{
    char *cursor = line;
    char *allow  = acl_next_token(&cursor);
    if (!allow || allow[0] == '#')
        return;

    char *access    = acl_next_token(&cursor);
    char *principal = acl_next_token(&cursor);
    char *filter    = acl_next_token(&cursor);

    if (!access || !principal || !filter ||
        !(STREQ(allow, "allow", 5) || STREQ(allow, "deny", 4))) {
        log_warning("ACL: malformed rule at %s:%d, skipped", path, linenr);
        return;
    }
    acl_add_rule(allow, access, principal, filter);
}

void acl_load(const char *path)
{
    if (!path || !path[0])
        return;

    FILE *fh = fopen(path, "r");
    if (!fh) {
        log_fatal("Cannot read the access control file %s", path);
        exit(EXIT_FAILURE);
    }

    rules = list_new(rule_destructor);

    char line[0x400];
    int linenr = 0;
    while (fgets(line, sizeof(line), fh) != NULL) {
        linenr++;
        acl_parse_line(line, linenr, path);
    }
    fclose(fh);

    log_info("Loaded the access control rules from %s", path);
}

/*
 * Does the rule apply to this client? A rule with a principal of '*' applies
 * to anybody, the others match the client id or the username it authenticated
 * with, depending on the prefix the rule used
 */
static bool acl_rule_matches(const struct acl_rule *rule, const char *client_id,
                             const char *username)
{
    if (rule->any)
        return true;
    if (rule->by_user)
        return username && strcmp(rule->principal, username) == 0;
    return strcmp(rule->principal, client_id) == 0;
}

bool acl_check(const char *client_id, const char *username, const char *topic,
               int access)
{
    if (!rules)
        return true; /* no ACL configured: everything is allowed */

    list_foreach(item, rules)
    {
        struct acl_rule *rule = item->data;
        if (!(rule->access & access))
            continue;
        if (!acl_rule_matches(rule, client_id, username))
            continue;
        if (!topic_access_match(topic, rule->filter))
            continue;
        return rule->allow;
    }

    /*
     * No rule covers this topic: the default of a broker holding an access
     * control list is to deny, an empty file locks everybody out
     */
    return false;
}
