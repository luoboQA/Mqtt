/* BSD 2-Clause License
 *
 * Copyright (c) 2023, Andrea Giacomo Baldan All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
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
#include "unit.h"
#include "../src/util.h"
#include <stdio.h>
#include <string.h>

/*
 * §1.5.3: well formed UTF-8, no U+0000, no surrogate, no overlong form. The
 * broker refuses the malformed strings at unpack time, so this table is what
 * keeps the validator honest
 */
static char *test_utf8_valid(void)
{
    struct {
        const char *bytes;
        size_t len;
        bool valid;
    } cases[] = {
        {"hello", 5, true},
        {"caf\xc3\xa9", 5, true},          /* é, two bytes */
        {"\xe2\x82\xac", 3, true},         /* €, three bytes */
        {"\xf0\x9f\x98\x80", 4, true},     /* emoji, four bytes */
        {"\x00", 1, false},                /* U+0000 is forbidden */
        {"a\x00b", 3, false},
        {"\xc0\xaf", 2, false},            /* overlong '/' */
        {"\xe0\x80\xaf", 3, false},        /* overlong '/' */
        {"\xed\xa0\x80", 3, false},        /* UTF-16 surrogate */
        {"\xf4\x90\x80\x80", 4, false},    /* above U+10FFFF */
        {"\x80", 1, false},                /* stray continuation byte */
        {"\xc3", 1, false},                /* truncated sequence */
        {"\xc3\x28", 2, false},            /* bad continuation byte */
        {"\xf8\x88\x80\x80\x80", 5, false},/* five byte form */
        {"", 0, true},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(*cases); ++i) {
        bool got = utf8_valid((const unsigned char *)cases[i].bytes,
                              cases[i].len);
        if (got != cases[i].valid) {
            printf("util::utf8[%zu]...FAIL\n", i);
            return "util::utf8_valid...FAIL";
        }
    }
    tests_run++;
    printf("util::utf8_valid...OK (%zu cases)\n",
           sizeof(cases) / sizeof(*cases));
    return 0;
}

static char *test_constant_time_eq(void)
{
    ASSERT("util::ct_eq equal...FAIL", constant_time_eq("abc", "abc"));
    ASSERT("util::ct_eq differ...FAIL", !constant_time_eq("abc", "abd"));
    ASSERT("util::ct_eq prefix...FAIL", !constant_time_eq("abc", "abcd"));
    ASSERT("util::ct_eq empty...FAIL", constant_time_eq("", ""));
    ASSERT("util::ct_eq null...FAIL", !constant_time_eq(NULL, "a"));
    printf("util::constant_time_eq...OK\n");
    return 0;
}

/*
 * The scrypt hash of "secret" with the fixed salt 001122...ff, the same
 * format passwd.py writes. Only the scrypt path is exercised here; crypt(3)
 * depends on the system's scheme and is covered by the ACL integration tests
 */
static char *test_check_passwd_scrypt(void)
{
    const char *hash =
        "$scrypt$16384$8$1$00112233445566778899aabbccddeeff$"
        "3e83302f4925189a3822090d5b99c3609fec7126bef8f441f058461279235e79"
        "c115535b352ca14e87bed14668660cc488d61321fb7fb6f68c2dd8541589429d";

    ASSERT("util::passwd scrypt ok...FAIL", check_passwd("secret", hash));
    ASSERT("util::passwd scrypt wrong...FAIL",
           !check_passwd("Secret", hash));
    ASSERT("util::passwd scrypt empty...FAIL", !check_passwd("", hash));
    ASSERT("util::passwd scrypt null...FAIL", !check_passwd(NULL, hash));
    /* A truncated or hand edited record is refused, not crashed on */
    ASSERT("util::passwd scrypt bad format...FAIL",
           !check_passwd("secret", "$scrypt$16384$8$1$00"));
    ASSERT("util::passwd scrypt bad N...FAIL",
           !check_passwd("secret",
                         "$scrypt$3$8$1$00112233445566778899aabbccddeeff$"
                         "3e83302f4925189a3822090d5b99c3609fec7126bef8f441f058"
                         "461279235e79c115535b352ca14e87bed14668660cc488d61321f"
                         "b7fb6f68c2dd8541589429d"));
    printf("util::check_passwd_scrypt...OK\n");
    return 0;
}

char *util_test(void)
{
    RUN_TEST(test_utf8_valid);
    RUN_TEST(test_constant_time_eq);
    RUN_TEST(test_check_passwd_scrypt);
    return 0;
}
