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
#ifdef __linux__
#include <sys/eventfd.h>
#else
#include <unistd.h>
#endif
#include "config.h"
#include "logging.h"
#include "memory.h"
#include "network.h"
#include "util.h"

/* The main configuration structure */
static struct config config;
struct config *conf;

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
 * Copy a configuration value into a fixed size field, rejecting (with a
 * warning naming the key) a value that would not fit: a silent truncation
 * would leave a hostname or a port that was never asked for, and a blind
 * strcpy could overflow the field with a long conf line
 */
static void set_config_str(const char *key, char *dest, size_t destsz,
                           const char *value)
{
    size_t len = strlen(value);
    if (len >= destsz) {
        log_warning("Ignoring configuration %s: value of %zu bytes does not "
                    "fit in %zu",
                    key, len, destsz);
        return;
    }
    memcpy(dest, value, len + 1);
}

static void add_config_value(const char *key, const char *value)
{

    size_t klen = strlen(key);
    size_t vlen = strlen(value);

    if (STREQ("log_level", key, klen) == true) {
        /* lmap holds 5 levels: stopping at 3 silently dropped INFO and
         * INFORMATION, which the parser used to ignore without warning */
        for (size_t i = 0; i < sizeof(lmap) / sizeof(*lmap); i++) {
            if (STREQ(lmap[i].lname, value, vlen) == true)
                config.loglevel = lmap[i].loglevel;
        }
    } else if (STREQ("log_path", key, klen) == true) {
        set_config_str(key, config.logpath, sizeof(config.logpath), value);
    } else if (STREQ("unix_socket", key, klen) == true) {
        config.socket_family = UNIX;
        set_config_str(key, config.hostname, sizeof(config.hostname), value);
    } else if (STREQ("ip_address", key, klen) == true) {
        config.socket_family = INET;
        set_config_str(key, config.hostname, sizeof(config.hostname), value);
    } else if (STREQ("ip_port", key, klen) == true) {
        set_config_str(key, config.port, sizeof(config.port), value);
    } else if (STREQ("max_memory", key, klen) == true) {
        config.max_memory = read_memory_with_mul(value);
    } else if (STREQ("max_request_size", key, klen) == true) {
        config.max_request_size = read_memory_with_mul(value);
    } else if (STREQ("tcp_backlog", key, klen) == true) {
        int tcp_backlog    = parse_int(value);
        config.tcp_backlog = tcp_backlog <= SOMAXCONN ? tcp_backlog : SOMAXCONN;
    } else if (STREQ("stats_publish_interval", key, klen) == true) {
        config.stats_pub_interval = read_time_with_mul(value);
    } else if (STREQ("keepalive", key, klen) == true) {
        config.keepalive = read_time_with_mul(value);
    } else if (STREQ("retransmit_interval", key, klen) == true) {
        config.retransmit_interval = read_time_with_mul(value);
    } else if (STREQ("cafile", key, klen) == true) {
        config.tls = true;
        set_config_str(key, config.cafile, sizeof(config.cafile), value);
    } else if (STREQ("certfile", key, klen) == true) {
        set_config_str(key, config.certfile, sizeof(config.certfile), value);
    } else if (STREQ("keyfile", key, klen) == true) {
        set_config_str(key, config.keyfile, sizeof(config.keyfile), value);
    } else if (STREQ("allow_anonymous", key, klen) == true) {
        // TODO add strict checks
        if (STREQ(value, "false", 5) == true)
            config.allow_anonymous = false;
        else
            config.allow_anonymous = true;
    } else if (STREQ("persistence_file", key, klen) == true) {
        set_config_str(key, config.persistence_file,
                       sizeof(config.persistence_file), value);
    } else if (STREQ("persistence_interval", key, klen) == true) {
        config.persistence_interval = read_time_with_mul(value);
    } else if (STREQ("reclaim_interval", key, klen) == true) {
        config.reclaim_interval = read_time_with_mul(value);
    } else if (STREQ("password_file", key, klen) == true) {
        set_config_str(key, config.password_file, sizeof(config.password_file),
                       value);
    } else if (STREQ("tls_protocols", key, klen) == true) {
        if (vlen == 0)
            return;
        config.tls_protocols = 0;
        char *token          = strtok((char *)value, ",");
        if (!token) {
            config.tls_protocols = parse_config_tls_protocols((char *)value);
        } else {
            while (token) {
                config.tls_protocols |=
                    parse_config_tls_protocols((char *)token);
                token = strtok(NULL, ",");
            }
        }
    }
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

int config_load(const char *configpath)
{

    assert(configpath);

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

        // At this point we have key -> value ready to be ingested on the
        // global configuration object
        add_config_value(key, value);
    }

    fclose(fh);
    return true;
}

void config_set_default(void)
{

    // Set the global pointer
    conf                 = &config;

    // Set default values
    config.version       = VERSION;
    config.socket_family = DEFAULT_SOCKET_FAMILY;
    config.loglevel      = DEFAULT_LOG_LEVEL;
    memset(config.logpath, 0x00, 0xFFF);
    strcpy(config.hostname, DEFAULT_HOSTNAME);
    strcpy(config.port, DEFAULT_PORT);
#ifdef __linux__
    config.run = eventfd(0, EFD_NONBLOCK);
#else
    pipe(config.run);
#endif
    config.max_memory         = read_memory_with_mul(DEFAULT_MAX_MEMORY);
    config.max_request_size   = read_memory_with_mul(DEFAULT_MAX_REQUEST_SIZE);
    config.tcp_backlog        = SOMAXCONN;
    config.stats_pub_interval = read_time_with_mul(DEFAULT_STATS_INTERVAL);
    memset(config.persistence_file, 0x00, sizeof(config.persistence_file));
    config.persistence_interval =
        read_time_with_mul(DEFAULT_PERSISTENCE_INTERVAL);
    config.reclaim_interval = read_time_with_mul(DEFAULT_RECLAIM_INTERVAL);
    config.keepalive          = read_time_with_mul(DEFAULT_KEEPALIVE);
    config.retransmit_interval = read_time_with_mul(DEFAULT_RETRANSMIT);
    config.tls                = false;
    config.tls_protocols      = DEFAULT_TLS_PROTOCOLS;
    config.allow_anonymous    = true;
}

void config_print_tls_versions(void)
{
    char protocols[64] = {0};
    int pos            = 0;
    if (config.tls_protocols & SOL_TLSv1) {
        strncpy(protocols, "TLSv1, ", 64);
        pos += 7;
    }
    if (config.tls_protocols & SOL_TLSv1_1) {
        strncpy(protocols + pos, "TLSv1_1, ", 64 - pos);
        pos += 9;
    }
    if (config.tls_protocols & SOL_TLSv1_2) {
        strncpy(protocols + pos, "TLSv1_2, ", 64 - pos);
        pos += 9;
    }
    if (config.tls_protocols & SOL_TLSv1_3) {
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

void config_print(void)
{
    if (config.loglevel < WARNING) {
        const char *sfamily = config.socket_family == UNIX ? "UNIX" : "TCP";
        const char *llevel  = NULL;
        for (int i = 0; i < 4; i++) {
            if (lmap[i].loglevel == config.loglevel)
                llevel = lmap[i].lname;
        }
        log_info("Sol v%s is starting", VERSION);
        log_info("Network settings:");
        log_info("\tSocket family: %s", sfamily);
        if (config.socket_family == UNIX) {
            log_info("\tUnix socket: %s", config.hostname);
        } else {
            log_info("\tAddress: %s", config.hostname);
            log_info("\tPort: %s", config.port);
            log_info("\tTcp backlog: %d", config.tcp_backlog);
            log_info("\tKeepalive: %zu", config.keepalive);
            log_info("\tRetransmit interval: %zu", config.retransmit_interval);
            if (config.persistence_file[0])
                log_info("\tPersistence file: %s (every %zu seconds)",
                         config.persistence_file, config.persistence_interval);
            if (config.tls == true)
                config_print_tls_versions();
            log_info("\tFile handles soft limit: %li", get_fh_soft_limit());
        }
        const char *human_rsize = memory_to_string(config.max_request_size);
        log_info("\tMax request size: %s", human_rsize);
        log_info("Logging:");
        log_info("\tlevel: %s", llevel);
        if (config.logpath[0])
            log_info("\tlogpath: %s", config.logpath);
        const char *human_memory = memory_to_string(config.max_memory);
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
