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

#include "topic_match.h"
#include <stddef.h>
#include <string.h>

/*
 * Next '/' separated level of a topic or filter. Unlike the old helpers this
 * reports an empty level as a level of its own: "a/" is ["a", ""], which is
 * what makes it different from "a". Returns false once the string is
 * exhausted; `*p` is set to NULL at that point
 */
static bool next_level(const char **p, const char **level, size_t *len)
{
    const char *start = *p;
    if (!start)
        return false;

    const char *slash = strchr(start, '/');
    *level            = start;
    if (slash) {
        *len = (size_t)(slash - start);
        *p   = slash + 1;
    } else {
        *len = strlen(start);
        *p   = NULL;
    }
    return true;
}

bool topic_match(const char *topic, const char *filter, bool multilevel)
{
    if (!topic || !filter)
        return false;

    const char *wp = filter;
    const char *tp = topic;
    const char *w, *t;
    size_t wlen, tlen;

    /*
     * MQTT v3.1.1 §4.7.2: a filter starting with a wildcard must never match
     * a topic name beginning with '$' (the broker's own $SOL statistics live
     * there), only a filter spelling the '$' explicitly is allowed to
     */
    if (topic[0] == '$' && (wp[0] == '#' || wp[0] == '+'))
        return false;

    while (next_level(&wp, &w, &wlen)) {
        /* '#' consumes the rest of the topic, including no level at all */
        if (wlen == 1 && w[0] == '#')
            return true;

        if (!next_level(&tp, &t, &tlen))
            return false;

        /*
         * '+' consumes exactly one level, whatever it contains (the empty
         * level included), every other level is a literal that must match
         * the topic byte for byte
         */
        if (!(wlen == 1 && w[0] == '+') &&
            (wlen != tlen || memcmp(w, t, wlen) != 0))
            return false;
    }

    /*
     * The filter is exhausted: the topic must be exhausted as well, unless
     * the caller flagged the filter as multilevel (a v1 record whose '#' was
     * stored as a prefix)
     */
    return tp == NULL || multilevel;
}
