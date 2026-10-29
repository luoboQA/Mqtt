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

#include "util.h"
#include "memory.h"
#include "mqtt.h"
#include <assert.h>
#include <string.h>
#include <crypt.h>
#include <ctype.h>
#include <errno.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define SOL_PREFIX "sol"

/* Auxiliary function to check wether a string is an integer */
bool is_integer(const char *string)
{
    /* An empty string holds no digit at all */
    if (!string || !*string)
        return false;
    for (; *string; ++string)
        if (!isdigit(*string))
            return false;
    return true;
}

/* Parse the integer part of a string, by effectively iterate through it and
   converting the numbers found */
int parse_int(const char *string)
{
    int n = 0;

    while (*string && isdigit(*string)) {
        n = (n * 10) + (*string - '0');
        string++;
    }
    return n;
}

char *remove_occur(char *str, char c)
{
    char *p  = str;
    char *pp = str;

    while (*p) {
        *pp = *p++;
        pp += (*pp != c);
    }

    *pp = '\0';

    return str;
}

char *update_integer_string(char *str, int num)
{

    int n = parse_int(str);
    n += num;
    /*
     * Check for realloc if the new value is "larger" then
     * previous
     */
    /*
     * number_len() returns the exact digit count, so tmp holds digits+1
     * bytes: room for the digits and the NUL terminator written by sprintf.
     * A negative n widens to a huge size_t in number_len(), only growing the
     * buffer. The formatting is bounded, not unsafe
     */
    char tmp[number_len(n) + 1];
    sprintf(tmp, "%d", n);
    size_t len = strlen(tmp);
    str        = try_realloc(str, len + 1);
    memcpy(str, tmp, len + 1);

    return str;
}

/*
 * Append a string to another for the destination part that will be appended
 * the function require the length, the resulting string will be heap alloced
 * and nul-terminated.
 */
char *append_string(const char *src, char *dst, size_t chunklen)
{
    size_t srclen = strlen(src);
    char *ret     = try_alloc(srclen + chunklen + 1);
    memcpy(ret, src, srclen);
    memcpy(ret + srclen, dst, chunklen);
    ret[srclen + chunklen] = '\0';
    return ret;
}

/*
 * Return the number of decimal digits of a positive number, i.e. the number
 * of chars it would take in a string, counting the single digit of 0
 */
int number_len(size_t number)
{
    int len = 1;
    while (number /= 10)
        len++;
    return len;
}

/*
 * Return the current time expressed in microseconds since the Epoch; despite
 * the historic name mentioning "ns", the resolution of gettimeofday() is
 * microseconds and the value is only used to generate unique client ids
 */
unsigned long unix_time_ns(void)
{
    struct timeval time;
    gettimeofday(&time, NULL);
    return time.tv_sec * (int)1e6 + time.tv_usec;
}

void generate_random_id(char *dest)
{
    unsigned long utime_ns = unix_time_ns();
    snprintf(dest, MQTT_CLIENT_ID_LEN - 1, "%s-%lu", SOL_PREFIX, utime_ns);
}

bool utf8_valid(const unsigned char *s, size_t len)
{
    size_t i = 0;
    while (i < len) {
        unsigned char c = s[i];
        if (c == 0x00)
            return false;
        if (c < 0x80) {
            i++;
            continue;
        }

        size_t extra;
        unsigned int cp;
        if ((c & 0xE0) == 0xC0) {
            extra = 1;
            cp    = c & 0x1F;
        } else if ((c & 0xF0) == 0xE0) {
            extra = 2;
            cp    = c & 0x0F;
        } else if ((c & 0xF8) == 0xF0) {
            extra = 3;
            cp    = c & 0x07;
        } else {
            /* A continuation byte in lead position, or 0xF8..0xFF */
            return false;
        }
        if (i + extra >= len)
            return false;
        for (size_t j = 1; j <= extra; ++j) {
            unsigned char cc = s[i + j];
            if ((cc & 0xC0) != 0x80)
                return false;
            cp = (cp << 6) | (cc & 0x3F);
        }
        /* Overlong encodings are not a valid UTF-8 form (§1.5.3) */
        if ((extra == 1 && cp < 0x80) || (extra == 2 && cp < 0x800) ||
            (extra == 3 && cp < 0x10000))
            return false;
        if (cp > 0x10FFFF)
            return false;
        if (cp >= 0xD800 && cp <= 0xDFFF)
            return false;
        i += extra + 1;
    }
    return true;
}

bool constant_time_eq(const char *a, const char *b)
{
    if (!a || !b)
        return false;
    size_t la = strlen(a);
    size_t lb = strlen(b);
    /*
     * The length itself is not a secret (the hash format fixes it), but the
     * loop below must not stop at the first differing byte: always walk the
     * longer string and fold the length difference into the result
     */
    size_t n         = la > lb ? la : lb;
    unsigned char rc = (unsigned char)(la ^ lb);
    for (size_t i = 0; i < n; ++i) {
        unsigned char ca = i < la ? (unsigned char)a[i] : 0;
        unsigned char cb = i < lb ? (unsigned char)b[i] : 0;
        rc |= (unsigned char)(ca ^ cb);
    }
    return rc == 0;
}

/*
 * Decode a lowercase/uppercase hexadecimal string into `out`. Returns false
 * on an odd length, an invalid digit or a result longer than `outsz`.
 */
static bool hex_decode(const char *hex, unsigned char *out, size_t outsz,
                       size_t *outlen)
{
    size_t len = strlen(hex);
    if (len == 0 || len % 2 != 0 || len / 2 > outsz)
        return false;
    for (size_t i = 0; i < len / 2; ++i) {
        int hi = isdigit((unsigned char)hex[2 * i])
                     ? hex[2 * i] - '0'
                     : tolower((unsigned char)hex[2 * i]) - 'a' + 10;
        int lo = isdigit((unsigned char)hex[2 * i + 1])
                     ? hex[2 * i + 1] - '0'
                     : tolower((unsigned char)hex[2 * i + 1]) - 'a' + 10;
        if (hi < 0 || hi > 15 || lo < 0 || lo > 15)
            return false;
        out[i] = (unsigned char)((hi << 4) | lo);
    }
    *outlen = len / 2;
    return true;
}

/*
 * Verify a scrypt hash written by passwd.py as
 *
 *   $scrypt$N$r$p$<salt hex>$<hash hex>
 *
 * scrypt is memory hard, which is what makes an offline dictionary attack
 * against a leaked password file expensive; crypt(3) alone (the SHA-512
 * scheme most systems pick) is not. The parameters are bounded so a
 * hand-edited file cannot make the broker allocate gigabytes per login.
 */
static bool check_scrypt(const char *passwd, const char *encoded)
{
    char buf[512];
    if (strlen(encoded) >= sizeof(buf))
        return false;
    snprintf(buf, sizeof(buf), "%s", encoded);

    char *saveptr = NULL;
    char *scheme  = strtok_r(buf, "$", &saveptr);
    char *sn      = strtok_r(NULL, "$", &saveptr);
    char *sr      = strtok_r(NULL, "$", &saveptr);
    char *sp      = strtok_r(NULL, "$", &saveptr);
    char *shex    = strtok_r(NULL, "$", &saveptr);
    char *hhex    = strtok_r(NULL, "$", &saveptr);
    if (!scheme || strcmp(scheme, "scrypt") != 0 || !sn || !sr || !sp ||
        !shex || !hhex || strtok_r(NULL, "$", &saveptr))
        return false;

    errno                  = 0;
    char *end              = NULL;
    unsigned long long N   = strtoull(sn, &end, 10);
    if (errno == ERANGE || end == sn || *end != '\0' || N < 2 ||
        N > (1ULL << 20) || (N & (N - 1)) != 0)
        return false;
    unsigned long long r = strtoul(sr, &end, 10);
    if (end == sr || *end != '\0' || r == 0 || r > 32)
        return false;
    unsigned long long p = strtoul(sp, &end, 10);
    if (end == sp || *end != '\0' || p == 0 || p > 16)
        return false;

    unsigned char salt[64], expected[64];
    size_t saltlen = 0, hashlen = 0;
    if (!hex_decode(shex, salt, sizeof(salt), &saltlen) || saltlen < 8)
        return false;
    if (!hex_decode(hhex, expected, sizeof(expected), &hashlen) || hashlen < 16)
        return false;

    unsigned char derived[64];
    if (hashlen > sizeof(derived))
        return false;
    if (EVP_PBE_scrypt(passwd, strlen(passwd), salt, saltlen, N, r, p,
                       64 * 1024 * 1024, derived, hashlen) != 1)
        return false;
    return CRYPTO_memcmp(derived, expected, hashlen) == 0;
}

bool check_passwd(const char *passwd, const char *salt)
{
    if (!passwd || !salt)
        return false;
    if (strncmp(salt, "$scrypt$", 8) == 0)
        return check_scrypt(passwd, salt);
#if defined(__APPLE__)
    /*
     * macOS provides no crypt_r(): crypt() writes its result in a static
     * buffer, so the comparison below must happen before any other call
     * overwrites it (it does, on this very thread only)
     */
    char *hashed = crypt(passwd, salt);
#else
    /*
     * crypt() keeps its result in a static buffer shared by every thread
     * of the process: two clients authenticating at the same time could
     * end up comparing against each other's hash. crypt_r() takes the
     * working state from the caller instead
     */
    struct crypt_data data = {0};
    char *hashed           = crypt_r(passwd, salt, &data);
#endif
    if (!hashed)
        return false;
    return constant_time_eq(hashed, salt);
}

long get_fh_soft_limit(void)
{
    struct rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit)) {
        perror("Failed to get limit");
        return -1;
    }
    return limit.rlim_cur;
}
