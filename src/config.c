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

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <strings.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/eventfd.h>
#endif
#include "config.h"
#include "logging.h"
#include "memory.h"
#include "network.h"
#include "util.h"

/*
 * Number of problems found by the last config_load(): an unknown key, an
 * unparsable value, an out of range number. The parser keeps going so the
 * operator sees every mistake in one run, but the caller refuses to start
 * (or --check-config exits non zero) when this is not zero
 */
static unsigned config_errors;

/* Smallest request size that can hold a CONNECT with an empty client id */
#define MQTT_MIN_REQUEST_SIZE 16

struct llevel {
    const char *lname;
    int loglevel;
};

static const struct llevel lmap[5] = {{"DEBUG", DEBUG},
                                      {"WARNING", WARNING},
                                      {"ERROR", ERROR},
                                      {"INFO", INFORMATION},
                                      {"INFORMATION", INFORMATION}};

static inline void strip_spaces(char **str)
{
    if (!*str)
        return;
    while (isspace(**str) && **str)
        ++(*str);
}

static size_t read_memory_with_mul(const char *memory_string)
{

    /* Extract digit part */
    size_t num = parse_int(memory_string);
    int mul    = 1;

    /* Move the pointer forward till the first non-digit char */
    while (isdigit(*memory_string))
        memory_string++;

    /* Set multiplier */
    if (STREQ(memory_string, "kb", 2))
        mul = 1024;
    else if (STREQ(memory_string, "mb", 2))
        mul = 1024 * 1024;
    else if (STREQ(memory_string, "gb", 2))
        mul = 1024 * 1024 * 1024;

    return num * mul;
}

static size_t read_time_with_mul(const char *time_string)
{

    /* Extract digit part */
    size_t num = parse_int(time_string);
    int mul    = 1;

    /* Move the pointer forward till the first non-digit char */
    while (isdigit(*time_string))
        time_string++;

    /* Set multiplier */
    switch (*time_string) {
    case 'm':
        /*
         * A bare 'm' means minutes; a trailing 's' after it ("500ms") would
         * be silently read as minutes otherwise. Every interval of the
         * configuration is expressed in seconds, so a sub-second period is
         * rounded up to one second: rounding it down to 0 would silently
         * turn a periodic task off
         */
        if (time_string[1] == 's') {
            num = (num + 999) / 1000;
            if (num == 0)
                num = 1;
            mul = 1;
        } else {
            mul = 60;
        }
        break;
    case 'h':
        mul = 60 * 60;
        break;
    case 'd':
        mul = 60 * 60 * 24;
        break;
    default:
        mul = 1;
        break;
    }

    return num * mul;
}

/*
 * Strict counterparts of the two helpers above, for values read from the
 * configuration file: the lenient versions return 0 on garbage ("abc",
 * "10xb", "-1"), which used to turn a typo into a disabled feature or a
 * zero sized buffer without a single warning. These reject anything that is
 * not a number followed by one of the accepted suffixes and report it.
 */
static bool parse_memory(const char *s, size_t *out)
{
    if (!s || !isdigit((unsigned char)*s))
        return false;

    errno                  = 0;
    char *end              = NULL;
    unsigned long long num = strtoull(s, &end, 10);
    if (errno == ERANGE || end == s)
        return false;

    size_t mul = 1;
    if (*end != '\0') {
        if (strcasecmp(end, "kb") == 0)
            mul = 1024;
        else if (strcasecmp(end, "mb") == 0)
            mul = 1024 * 1024;
        else if (strcasecmp(end, "gb") == 0)
            mul = 1024 * 1024 * 1024;
        else
            return false;
    }
    if (num > SIZE_MAX / mul)
        return false;
    *out = (size_t)num * mul;
    return true;
}

/*
 * Seconds, with the same suffixes read_time_with_mul() accepts (ms, s, m, h,
 * d). A bare number is seconds. Zero is accepted here: keepalive 0 has a
 * meaning (no cap), the caller decides whether it makes sense for its key.
 */
static bool parse_interval(const char *s, size_t *out)
{
    if (!s || !isdigit((unsigned char)*s))
        return false;

    errno                  = 0;
    char *end              = NULL;
    unsigned long long num = strtoull(s, &end, 10);
    if (errno == ERANGE || end == s)
        return false;

    size_t mul = 1;
    if (*end != '\0') {
        if (strcasecmp(end, "ms") == 0) {
            /* Sub-second periods are rounded up to one second, as the
             * lenient parser does: rounding down to 0 would silently turn a
             * periodic task off */
            if (num > 0 && num < 1000)
                num = 1;
            else
                num = (num + 999) / 1000;
        } else if (strcasecmp(end, "s") == 0) {
            mul = 1;
        } else if (strcasecmp(end, "m") == 0) {
            mul = 60;
        } else if (strcasecmp(end, "h") == 0) {
            mul = 60 * 60;
        } else if (strcasecmp(end, "d") == 0) {
            mul = 60 * 60 * 24;
        } else {
            return false;
        }
    }
    if (num > SIZE_MAX / mul)
        return false;
    *out = (size_t)num * mul;
    return true;
}

/* A TCP port: decimal, 1..65535, nothing else */
static bool parse_port(const char *s, unsigned *out)
{
    if (!s || !isdigit((unsigned char)*s))
        return false;
    errno             = 0;
    char *end         = NULL;
    unsigned long num = strtoul(s, &end, 10);
    if (errno == ERANGE || end == s || *end != '\0' || num == 0 ||
        num > 65535)
        return false;
    *out = (unsigned)num;
    return true;
}

/*
 * A non negative decimal that fits an unsigned int, for the quota settings
 * where 0 has the meaning "disabled". The old parse_int() stopped at the
 * first non digit, so "20x" silently became 20
 */
static bool parse_uint(const char *s, unsigned *out)
{
    if (!s || !isdigit((unsigned char)*s))
        return false;
    errno             = 0;
    char *end         = NULL;
    unsigned long num = strtoul(s, &end, 10);
    if (errno == ERANGE || end == s || *end != '\0' || num > UINT_MAX)
        return false;
    *out = (unsigned)num;
    return true;
}

/* Format a memory in bytes to a more human-readable form, e.g. 64b or 18Kb
 * instead of huge numbers like 130230234 bytes */
char *memory_to_string(size_t memory)
{

    int numlen            = 0;
    int translated_memory = 0;

    char *mstring         = NULL;

    if (memory < 1024) {
        translated_memory = memory;
        numlen            = number_len(translated_memory);
        // +1 for 'b' +1 for nul terminating
        mstring           = try_alloc(numlen + 2);
        snprintf(mstring, numlen + 2, "%db", translated_memory);
    } else if (memory < 1048576) {
        translated_memory = memory / 1024;
        numlen            = number_len(translated_memory);
        // +2 for 'Kb' +1 for nul terminating
        mstring           = try_alloc(numlen + 3);
        snprintf(mstring, numlen + 3, "%dKb", translated_memory);
    } else if (memory < 1073741824) {
        translated_memory = memory / (1024 * 1024);
        numlen            = number_len(translated_memory);
        // +2 for 'Mb' +1 for nul terminating
        mstring           = try_alloc(numlen + 3);
        snprintf(mstring, numlen + 3, "%dMb", translated_memory);
    } else {
        translated_memory = memory / (1024 * 1024 * 1024);
        numlen            = number_len(translated_memory);
        // +2 for 'Gb' +1 for nul terminating
        mstring           = try_alloc(numlen + 3);
        snprintf(mstring, numlen + 3, "%dGb", translated_memory);
    }

    return mstring;
}

/* Purely utility function, format a time in seconds to a more human-readable
 * form, e.g. 2m or 4h instead of huge numbers */
char *time_to_string(size_t time)
{

    int numlen          = 0;
    int translated_time = 0;

    char *tstring       = NULL;

    if (time < 60) {
        translated_time = time;
        numlen          = number_len(translated_time);
        // +1 for 's' +1 for nul terminating
        tstring         = try_alloc(numlen + 2);
        snprintf(tstring, numlen + 2, "%ds", translated_time);
    } else if (time < 60 * 60) {
        translated_time = time / 60;
        numlen          = number_len(translated_time);
        // +1 for 'm' +1 for nul terminating
        tstring         = try_alloc(numlen + 2);
        snprintf(tstring, numlen + 2, "%dm", translated_time);
    } else if (time < 60 * 60 * 24) {
        translated_time = time / (60 * 60);
        numlen          = number_len(translated_time);
        // +1 for 'h' +1 for nul terminating
        tstring         = try_alloc(numlen + 2);
        snprintf(tstring, numlen + 2, "%dh", translated_time);
    } else {
        translated_time = time / (60 * 60 * 24);
        numlen          = number_len(translated_time);
        // +1 for 'd' +1 for nul terminating
        tstring         = try_alloc(numlen + 2);
        snprintf(tstring, numlen + 2, "%dd", translated_time);
    }

    return tstring;
}

static int parse_config_tls_protocols(char *token)
{
    int protocols = 0;
    if (STREQ(token, "tlsv1_1", 7) == true)
        protocols |= SOL_TLSv1_1;
    else if (STREQ(token, "tlsv1_2", 7) == true)
        protocols |= SOL_TLSv1_2;
    else if (STREQ(token, "tlsv1_3", 7) == true)
        protocols |= SOL_TLSv1_3;
    else if (STREQ(token, "tlsv1", 5) == true)
        protocols |= SOL_TLSv1;
    return protocols;
}

/* Set configuration values based on what is read from the persistent
   configuration on disk */

/*
 * Record one configuration problem: every caller keeps parsing so a single
 * run lists all of them, and config_load() returns false at the end. The
 * message names the key and the offending value, which is what the operator
 * needs to fix the file.
 */
static void config_error(const char *key, const char *value, const char *why)
{
    log_error("Configuration error: %s = '%s': %s", key, value, why);
    config_errors++;
}

/*
 * Copy a configuration value into a fixed size field, rejecting (with a
 * warning naming the key) a value that would not fit: a silent truncation
 * would leave a hostname or a port that was never asked for, and a blind
 * strcpy could overflow the field with a long conf line
 */
static bool set_config_str(const char *key, char *dest, size_t destsz,
                           const char *value)
{
    size_t len = strlen(value);
    if (len >= destsz) {
        log_error("Ignoring configuration %s: value of %zu bytes does not "
                  "fit in %zu",
                  key, len, destsz);
        config_errors++;
        return false;
    }
    memcpy(dest, value, len + 1);
    return true;
}

static bool add_config_value(struct config *c, const char *key,
                            const char *value)
{

    size_t klen = strlen(key);
    size_t vlen = strlen(value);
    size_t num  = 0;

    if (STREQ("log_level", key, klen) == true) {
        /* lmap holds 5 levels: stopping at 3 silently dropped INFO and
         * INFORMATION, which the parser used to ignore without warning */
        for (size_t i = 0; i < sizeof(lmap) / sizeof(*lmap); i++) {
            if (STREQ(lmap[i].lname, value, vlen) == true) {
                c->loglevel = lmap[i].loglevel;
                return true;
            }
        }
        config_error(key, value, "unknown log level");
        return false;
    } else if (STREQ("log_path", key, klen) == true) {
        return set_config_str(key, c->logpath, sizeof(c->logpath),
                              value);
    } else if (STREQ("unix_socket", key, klen) == true) {
        c->socket_family = UNIX;
        return set_config_str(key, c->hostname, sizeof(c->hostname),
                              value);
    } else if (STREQ("ip_address", key, klen) == true) {
        c->socket_family = INET;
        return set_config_str(key, c->hostname, sizeof(c->hostname),
                              value);
    } else if (STREQ("ip_port", key, klen) == true) {
        unsigned port = 0;
        if (!parse_port(value, &port)) {
            config_error(key, value, "not a TCP port between 1 and 65535");
            return false;
        }
        return set_config_str(key, c->port, sizeof(c->port), value);
    } else if (STREQ("max_memory", key, klen) == true) {
        if (!parse_memory(value, &num) || num == 0) {
            config_error(key, value, "expected a positive size, e.g. 512MB");
            return false;
        }
        c->max_memory = num;
    } else if (STREQ("max_request_size", key, klen) == true) {
        if (!parse_memory(value, &num) || num < MQTT_MIN_REQUEST_SIZE) {
            config_error(key, value,
                         "expected a size of at least 16 bytes, e.g. 512KB");
            return false;
        }
        c->max_request_size = num;
    } else if (STREQ("tcp_backlog", key, klen) == true) {
        unsigned backlog = 0;
        if (!parse_port(value, &backlog)) {
            config_error(key, value, "expected a positive number");
            return false;
        }
        c->tcp_backlog =
            (int)(backlog <= SOMAXCONN ? backlog : SOMAXCONN);
    } else if (STREQ("stats_publish_interval", key, klen) == true) {
        if (!parse_interval(value, &num) || num == 0) {
            config_error(key, value, "expected a positive interval, e.g. 10s");
            return false;
        }
        c->stats_pub_interval = num;
    } else if (STREQ("keepalive", key, klen) == true) {
        /* 0 is a valid value here: it disables the configured cap */
        if (!parse_interval(value, &num)) {
            config_error(key, value, "expected an interval, e.g. 60s or 0");
            return false;
        }
        c->keepalive = num;
    } else if (STREQ("retransmit_interval", key, klen) == true) {
        if (!parse_interval(value, &num) || num == 0) {
            config_error(key, value, "expected a positive interval, e.g. 20s");
            return false;
        }
        c->retransmit_interval = num;
    } else if (STREQ("cafile", key, klen) == true) {
        c->tls = true;
        return set_config_str(key, c->cafile, sizeof(c->cafile),
                              value);
    } else if (STREQ("certfile", key, klen) == true) {
        return set_config_str(key, c->certfile, sizeof(c->certfile),
                              value);
    } else if (STREQ("keyfile", key, klen) == true) {
        return set_config_str(key, c->keyfile, sizeof(c->keyfile),
                              value);
    } else if (STREQ("allow_anonymous", key, klen) == true) {
        if (strcasecmp(value, "true") == 0)
            c->allow_anonymous = true;
        else if (strcasecmp(value, "false") == 0)
            c->allow_anonymous = false;
        else {
            config_error(key, value, "expected true or false");
            return false;
        }
    } else if (STREQ("acl_file", key, klen) == true) {
        return set_config_str(key, c->acl_file, sizeof(c->acl_file),
                              value);
    } else if (STREQ("persistence_file", key, klen) == true) {
        return set_config_str(key, c->persistence_file,
                              sizeof(c->persistence_file), value);
    } else if (STREQ("persistence_interval", key, klen) == true) {
        if (!parse_interval(value, &num) || num == 0) {
            config_error(key, value, "expected a positive interval, e.g. 30s");
            return false;
        }
        c->persistence_interval = num;
    } else if (STREQ("reclaim_interval", key, klen) == true) {
        if (!parse_interval(value, &num) || num == 0) {
            config_error(key, value, "expected a positive interval, e.g. 60s");
            return false;
        }
        c->reclaim_interval = num;
    } else if (STREQ("password_file", key, klen) == true) {
        return set_config_str(key, c->password_file,
                              sizeof(c->password_file), value);
    } else if (STREQ("pid_file", key, klen) == true) {
        return set_config_str(key, c->pid_file, sizeof(c->pid_file),
                              value);
    } else if (STREQ("max_inflight", key, klen) == true) {
        unsigned n = 0;
        if (!parse_uint(value, &n)) {
            config_error(key, value,
                         "expected a non negative number of messages");
            return false;
        }
        c->max_inflight = n;
    } else if (STREQ("max_publish_rate", key, klen) == true) {
        unsigned n = 0;
        if (!parse_uint(value, &n)) {
            config_error(key, value,
                         "expected a non negative number of messages per "
                         "second");
            return false;
        }
        c->max_publish_rate = n;
    } else if (STREQ("max_connections_per_ip", key, klen) == true) {
        unsigned n = 0;
        if (!parse_uint(value, &n)) {
            config_error(key, value,
                         "expected a non negative number of connections");
            return false;
        }
        c->max_connections_per_ip = n;
    } else if (STREQ("require_client_cert", key, klen) == true) {
        if (strcasecmp(value, "true") == 0)
            c->require_client_cert = true;
        else if (strcasecmp(value, "false") == 0)
            c->require_client_cert = false;
        else {
            config_error(key, value, "expected true or false");
            return false;
        }
    } else if (STREQ("tls_protocols", key, klen) == true) {
        if (vlen == 0) {
            config_error(key, value, "expected at least one protocol");
            return false;
        }
        int protocols        = 0;
        char buf[0xFFF]      = {0};
        snprintf(buf, sizeof(buf), "%s", value);
        char *saveptr        = NULL;
        char *token          = strtok_r(buf, ",", &saveptr);
        while (token) {
            int parsed = parse_config_tls_protocols(token);
            if (parsed == 0) {
                config_error(key, token, "unknown TLS protocol");
                return false;
            }
            protocols |= parsed;
            token = strtok_r(NULL, ",", &saveptr);
        }
        c->tls_protocols = protocols;
    } else {
        config_error(key, value, "unknown configuration key");
        return false;
    }
    return true;
}

/*
 * Copy the next whitespace delimited token into dest, never writing more
 * than destsz bytes (including the terminator): the destination used to be
 * written until the token ended, so a single overlong line of the conf file
 * overflowed the small key buffer on the stack. Whatever is left of an
 * overlong token is skipped so the parser stays in sync with the line
 */
static inline void unpack_bytes(char **str, char *dest, size_t destsz)
{

    if (!str || !dest || destsz == 0)
        return;

    size_t i = 0;
    while (!isspace(**str) && **str) {
        if (i + 1 < destsz)
            dest[i++] = **str;
        (*str)++;
    }
    dest[i] = '\0';
}

int config_load(struct config *c, const char *configpath)
{

    assert(configpath);

    config_errors = 0;

    FILE *fh = fopen(configpath, "r");

    if (!fh) {
        log_warning("WARNING: Unable to open conf file %s", configpath);
        log_warning("To specify a config file run sol -c /path/to/conf");
        return false;
    }

    char line[0xFFF], key[0xFF], value[0xFFF];
    int linenr = 0;
    char *pline, *pkey, *pval;

    while (fgets(line, 0xFFF, fh) != NULL) {

        memset(key, 0x00, 0xFF);
        memset(value, 0x00, 0xFFF);

        linenr++;

        // Skip comments or empty lines
        if (line[0] == '#')
            continue;

        // Remove whitespaces if any before the key
        pline = line;
        strip_spaces(&pline);

        if (*pline == '\0')
            continue;

        // Read key
        pkey = key;
        unpack_bytes(&pline, pkey, sizeof(key));

        // Remove whitespaces if any after the key and before the value
        strip_spaces(&pline);

        // Ignore eventually incomplete configuration, but notify it
        if (*pline == '\0') {
            log_warning("WARNING: Incomplete configuration '%s' at line %d. "
                        "Fallback to default.",
                        key, linenr);
            continue;
        }

        // Read value
        pval = value;
        unpack_bytes(&pline, pval, sizeof(value));

        // Apply the parsed key/value pair to the caller-owned configuration.
        add_config_value(c, key, value);
    }

    fclose(fh);
    if (config_errors > 0) {
        log_error("%u configuration error%s in %s", config_errors,
                  config_errors == 1 ? "" : "s", configpath);
        return false;
    }
    return true;
}

/*
 * Cross field checks, run after the command line overrides so that a bad -p
 * is caught as well. Returns false and logs every problem found.
 */
bool config_check(const struct config *c)
{
    unsigned before = config_errors;

    if (c->socket_family == INET) {
        unsigned port = 0;
        if (!parse_port(c->port, &port))
            config_error("ip_port", c->port,
                         "not a TCP port between 1 and 65535");
    }

    /*
     * TLS needs all three files: load_certificates() calls
     * SSL_CTX_load_verify_locations(), use_certificate_chain_file() and
     * use_PrivateKey_file() unconditionally. Failing here gives the operator
     * one clear message instead of three OpenSSL errors followed by exit()
     * from inside start_server()
     */
    if (c->tls) {
        const char *files[] = {c->cafile, c->certfile,
                               c->keyfile};
        const char *keys[]  = {"cafile", "certfile", "keyfile"};
        for (size_t i = 0; i < sizeof(files) / sizeof(*files); i++) {
            if (files[i][0] == '\0') {
                config_error(keys[i], "", "required when TLS is enabled");
                continue;
            }
            if (access(files[i], R_OK) != 0)
                config_error(keys[i], files[i], "file is not readable");
        }
    }

    if (c->max_inflight > 65535)
        config_error("max_inflight", "> 65535",
                     "the MQTT packet identifier space holds at most 65535 "
                     "messages in flight");

    if (c->require_client_cert && !c->tls)
        config_error("require_client_cert", "true",
                     "requires TLS (set cafile, certfile and keyfile)");

    if (c->allow_anonymous == false) {
        if (c->password_file[0] == '\0')
            config_error("password_file", "",
                         "required when allow_anonymous is false");
        else if (access(c->password_file, R_OK) != 0)
            config_error("password_file", c->password_file,
                         "file is not readable");
    }

    if (c->acl_file[0] != '\0' && access(c->acl_file, R_OK) != 0)
        config_error("acl_file", c->acl_file, "file is not readable");

    return config_errors == before;
}

void config_set_default(struct config *c)
{
    // Set default values
    c->version       = VERSION;
    c->socket_family = DEFAULT_SOCKET_FAMILY;
    c->loglevel      = DEFAULT_LOG_LEVEL;
    memset(c->logpath, 0x00, 0xFFF);
    strcpy(c->hostname, DEFAULT_HOSTNAME);
    strcpy(c->port, DEFAULT_PORT);
#ifdef __linux__
    c->run = eventfd(0, EFD_NONBLOCK);
#else
    pipe(c->run);
#endif
    c->max_memory         = read_memory_with_mul(DEFAULT_MAX_MEMORY);
    c->max_request_size   = read_memory_with_mul(DEFAULT_MAX_REQUEST_SIZE);
    c->tcp_backlog        = SOMAXCONN;
    c->stats_pub_interval = read_time_with_mul(DEFAULT_STATS_INTERVAL);
    memset(c->acl_file, 0x00, sizeof(c->acl_file));
    memset(c->persistence_file, 0x00, sizeof(c->persistence_file));
    memset(c->pid_file, 0x00, sizeof(c->pid_file));
    c->persistence_interval =
        read_time_with_mul(DEFAULT_PERSISTENCE_INTERVAL);
    c->reclaim_interval     = read_time_with_mul(DEFAULT_RECLAIM_INTERVAL);
    c->keepalive            = read_time_with_mul(DEFAULT_KEEPALIVE);
    c->retransmit_interval  = read_time_with_mul(DEFAULT_RETRANSMIT);
    c->max_inflight         = DEFAULT_MAX_INFLIGHT;
    c->max_publish_rate     = 0;
    c->max_connections_per_ip = 0;
    c->require_client_cert  = false;
    c->tls                  = false;
    c->tls_protocols        = DEFAULT_TLS_PROTOCOLS;
    c->allow_anonymous      = true;
}

static void config_print_tls_versions(const struct config *c)
{
    char protocols[64] = {0};
    int pos            = 0;
    if (c->tls_protocols & SOL_TLSv1) {
        strncpy(protocols, "TLSv1, ", 64);
        pos += 7;
    }
    if (c->tls_protocols & SOL_TLSv1_1) {
        strncpy(protocols + pos, "TLSv1_1, ", 64 - pos);
        pos += 9;
    }
    if (c->tls_protocols & SOL_TLSv1_2) {
        strncpy(protocols + pos, "TLSv1_2, ", 64 - pos);
        pos += 9;
    }
    if (c->tls_protocols & SOL_TLSv1_3) {
        strncpy(protocols + pos, "TLSv1_3, ", 64 - pos);
        pos += 9;
    }
    /*
     * The trailing ", " is trimmed by overwriting the last two characters:
     * with no protocol bit set pos is still 0 and that write would land
     * two bytes before the array
     */
    if (pos >= 2)
        protocols[pos - 2] = '\0';
    log_info("\tTLS: %s", protocols);
}

void config_print(const struct config *c)
{
    if (c->loglevel < WARNING) {
        const char *sfamily = c->socket_family == UNIX ? "UNIX" : "TCP";
        const char *llevel  = NULL;
        for (int i = 0; i < 4; i++) {
            if (lmap[i].loglevel == c->loglevel)
                llevel = lmap[i].lname;
        }
        log_info("Sol v%s is starting", VERSION);
        log_info("Network settings:");
        log_info("\tSocket family: %s", sfamily);
        if (c->socket_family == UNIX) {
            log_info("\tUnix socket: %s", c->hostname);
        } else {
            log_info("\tAddress: %s", c->hostname);
            log_info("\tPort: %s", c->port);
            log_info("\tTcp backlog: %d", c->tcp_backlog);
            log_info("\tKeepalive: %zu", c->keepalive);
            log_info("\tRetransmit interval: %zu", c->retransmit_interval);
            if (c->acl_file[0])
                log_info("\tAccess control file: %s", c->acl_file);
            if (c->persistence_file[0])
                log_info("\tPersistence file: %s (every %zu seconds)",
                         c->persistence_file, c->persistence_interval);
            if (c->tls == true)
                config_print_tls_versions(c);
            if (c->max_inflight)
                log_info("\tMax inflight messages per session: %u",
                         c->max_inflight);
            if (c->max_publish_rate)
                log_info("\tMax publish rate per client: %u msg/s",
                         c->max_publish_rate);
            if (c->max_connections_per_ip)
                log_info("\tMax connections per IP: %u",
                         c->max_connections_per_ip);
            log_info("\tFile handles soft limit: %li", get_fh_soft_limit());
        }
        const char *human_rsize = memory_to_string(c->max_request_size);
        log_info("\tMax request size: %s", human_rsize);
        log_info("Logging:");
        log_info("\tlevel: %s", llevel);
        if (c->logpath[0])
            log_info("\tlogpath: %s", c->logpath);
        const char *human_memory = memory_to_string(c->max_memory);
        log_info("Max memory: %s", human_memory);
        log_info("Event loop backend: %s", EVENTLOOP_BACKEND);
        free_memory((char *)human_memory);
        free_memory((char *)human_rsize);
    }
}

bool config_read_passwd_file(const char *path, struct authentication **auth_map)
{

    assert(path);

    FILE *fh = fopen(path, "r");

    if (!fh) {
        log_warning("WARNING: Unable to open passwd file %s", path);
        return false;
    }

    char line[0xFFF], username[0xFF], password[0xFFF];
    char *pline, *puname;

    while (fgets(line, 0xFFF, fh) != NULL) {
        memset(username, 0x00, 0xFF);
        memset(password, 0x00, 0xFFF);

        pline = line;
        if (*pline == '\0')
            continue;

        /*
         * Bounded on both ends: the loops below used to stop only at ':'
         * and '\n', so a line with no colon, one longer than the fixed
         * username/password buffers, or (most commonly) a final line
         * without a trailing newline walked and wrote past the buffers and
         * past the end of `line`, corrupting the stack. The '\0' check
         * also stops a truncated line, and a line with no ':' is skipped
         * instead of feeding an empty pair to the map
         */
        puname = line;
        int i  = 0;
        while (*puname != ':' && *puname != '\0' && i < 0xFF - 1)
            username[i++] = *puname++;
        if (*puname != ':')
            continue;
        puname++;
        i = 0;
        while (*puname != '\n' && *puname != '\r' && *puname != '\0' &&
               i < 0xFFF - 1)
            password[i++] = *puname++;

        /*
         * An empty username (a line such as ":secret" or a stray ":") has no
         * meaning and would shadow nothing but make HASH_FIND return an
         * entry no client can ever match
         */
        if (username[0] == '\0')
            continue;

        /*
         * uthash keys must stay unique: HASH_ADD_STR happily chains a second
         * entry under the same key and HASH_FIND then always returns the
         * first one, so a repeated line would silently pin the original
         * password while the duplicate sat in the map forever. The first
         * occurrence wins, which is what a reader expects from a passwd file
         */
        struct authentication *dup = NULL;
        HASH_FIND_STR(*auth_map, username, dup);
        if (dup) {
            log_warning("WARNING: Duplicate user '%s' in passwd file %s, "
                        "keeping the first entry",
                        username, path);
            continue;
        }

        struct authentication *auth = try_alloc(sizeof(*auth));
        auth->username              = try_strdup(username);
        auth->salt                  = try_strdup(password);
        HASH_ADD_STR(*auth_map, username, auth);
    }

    fclose(fh);
    return true;
}
