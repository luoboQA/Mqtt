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

#ifndef UTIL_H
#define UTIL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

bool is_integer(const char *);
int parse_int(const char *);
int number_len(size_t);
void generate_random_id(char *);
char *remove_occur(char *, char);
char *append_string(const char *, char *, size_t);
bool check_passwd(const char *, const char *);

/*
 * MQTT v3.1.1 §1.5.3: a UTF-8 encoded string must be well formed, must not
 * contain U+0000 and must not encode a surrogate. Returns false on an
 * overlong form, a truncated sequence or a code point above U+10FFFF.
 */
bool utf8_valid(const unsigned char *, size_t);

/* Compare two NUL terminated strings without an early exit on mismatch */
bool constant_time_eq(const char *, const char *);

long get_fh_soft_limit(void);

#define STREQ(s1, s2, len) (strncasecmp((s1), (s2), (len)) == 0)

#define container_of(ptr, type, field)                                         \
    ((type *)((char *)(ptr) - offsetof(type, field)))

#endif
