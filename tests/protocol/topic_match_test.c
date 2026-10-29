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

#include "proto_test.h"
#include "../unit/unit.h"
#include "../../src/topic_match.h"
#include <stdio.h>

struct match_case {
    const char *topic;
    const char *filter;
    bool multilevel;
    bool expected;
};

/*
 * Table driven: every row is one §4.7 rule. The empty level cases are the
 * ones the old trailing-'/' normalization got wrong, so they are spelled out
 * explicitly.
 */
static const struct match_case cases[] = {
    {"a", "a", false, true},
    {"a", "a/", false, false},
    {"a/", "a", false, false},
    {"a/", "a/", false, true},
    {"a/b", "a/+", false, true},
    {"a/", "a/+", false, true},
    {"a", "a/+", false, false},
    {"a/b/c", "a/#", false, true},
    {"a", "a/#", false, true},
    {"a/", "a/#", false, true},
    {"a/b", "a/b/#", false, true},
    {"b", "a/#", false, false},
    {"a//b", "a/+/b", false, true},
    {"a/b", "a/+/b", false, false},
    {"a/b/c", "a/+/+", false, true},
    {"a/b", "a/+/+", false, false},
    {"$SYS/x", "#", false, false},
    {"$SYS/x", "+/x", false, false},
    {"$SYS/x", "$SYS/#", false, true},
    {"/", "/", false, true},
    {"a/b/c", "a/b", true, true}, /* v1 record: '#' was stored as prefix */
    {"a/b", "a/b", false, true},
};

char *topic_match_test(void)
{
    for (size_t i = 0; i < sizeof(cases) / sizeof(*cases); ++i) {
        const struct match_case *c = &cases[i];
        bool got = topic_match(c->topic, c->filter, c->multilevel);
        if (got != c->expected) {
            printf("topic::match[%zu] (%s vs %s)...FAIL\n", i, c->topic,
                   c->filter);
            return "topic::match...FAIL";
        }
    }
    tests_run++;
    printf("topic::match_table...OK (%zu cases)\n",
           sizeof(cases) / sizeof(*cases));
    return 0;
}
