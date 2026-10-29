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
#include "../unit/unit.h"
#include "../../src/config.h"
#include "../../src/logging.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Write `content` to a fresh temporary file and return its path */
static int write_conf(char *path, const char *content)
{
    int fd = mkstemp(path);
    if (fd < 0)
        return -1;
    ssize_t len = (ssize_t)strlen(content);
    int rc      = write(fd, content, len) == len ? 0 : -1;
    close(fd);
    return rc;
}

/*
 * config_set_default() opens the eventfd/pipe the event loop waits on; a
 * test that never starts the loop has to close it or every test leaks one
 */
static void close_run_fd(struct config *cfg)
{
#ifdef __linux__
    close(cfg->run);
#else
    close(cfg->run[0]);
    close(cfg->run[1]);
#endif
}

static char *test_config_load(void)
{
    struct config cfg;

    char path[] = "/tmp/sol_cfg_test_XXXXXX";

    const char *content = "# a comment\n"
                          "\n"
                          "log_level WARNING\n"
                          "ip_port 19999\n"
                          "max_request_size 32KB\n"
                          "keepalive 15s\n";
    ASSERT("config::mkstemp...FAIL", write_conf(path, content) == 0);

    config_set_default(&cfg);
    ASSERT("config::load...FAIL", config_load(&cfg, path) == true);
    ASSERT("config::log_level...FAIL", cfg.loglevel == WARNING);
    ASSERT("config::ip_port...FAIL", strcmp(cfg.port, "19999") == 0);
    ASSERT("config::max_request_size...FAIL",
           cfg.max_request_size == 32 * 1024);
    ASSERT("config::keepalive...FAIL", cfg.keepalive == 15);

    unlink(path);
    close_run_fd(&cfg);
    printf("config::load...OK\n");
    return 0;
}

static char *test_config_rejects_unknown_key(void)
{
    struct config cfg;

    char path[] = "/tmp/sol_cfg_test_XXXXXX";
    ASSERT("config::unknown::mkstemp...FAIL",
           write_conf(path, "ip_port 1883\nbogus_key x\n") == 0);

    config_set_default(&cfg);
    ASSERT("config::unknown_key...FAIL", config_load(&cfg, path) == false);
    unlink(path);
    close_run_fd(&cfg);
    printf("config::unknown_key...OK\n");
    return 0;
}

static char *test_config_rejects_bad_values(void)
{
    struct config cfg;

    static const char *bad[] = {
        "ip_port 70000\n",
        "ip_port http\n",
        "max_memory 12XB\n",
        "max_request_size 0\n",
        "keepalive 5parsecs\n",
        "allow_anonymous maybe\n",
        "log_level LOUD\n",
        "tls_protocols tlsv9\n",
        "stats_publish_interval 0\n",
        "max_inflight -1\n",
        "max_publish_rate 1.5\n",
        "max_connections_per_ip x\n",
        "require_client_cert maybe\n",
    };

    for (size_t i = 0; i < sizeof(bad) / sizeof(*bad); ++i) {
        char path[] = "/tmp/sol_cfg_test_XXXXXX";
        ASSERT("config::bad_value::mkstemp...FAIL",
               write_conf(path, bad[i]) == 0);
        config_set_default(&cfg);
        if (config_load(&cfg, path) != false) {
            printf("config::bad_value[%zu] (%s)...FAIL\n", i, bad[i]);
            unlink(path);
            close_run_fd(&cfg);
            return "config::bad_value...FAIL";
        }
        unlink(path);
        close_run_fd(&cfg);
    }
    tests_run++;
    printf("config::bad_value...OK (%zu cases)\n",
           sizeof(bad) / sizeof(*bad));
    return 0;
}

static char *test_config_check_rejects_bad_port(void)
{
    struct config cfg;

    char path[] = "/tmp/sol_cfg_test_XXXXXX";
    /* config_load() accepts the string, config_check() is the one that
     * validates the port range after the command line overrides */
    ASSERT("config::check::mkstemp...FAIL",
           write_conf(path, "log_level WARNING\n") == 0);

    config_set_default(&cfg);
    ASSERT("config::check::load...FAIL", config_load(&cfg, path) == true);
    strcpy(cfg.port, "70000");
    ASSERT("config::check::port...FAIL", config_check(&cfg) == false);

    unlink(path);
    close_run_fd(&cfg);
    printf("config::check_bad_port...OK\n");
    return 0;
}

static char *test_config_limits(void)
{
    struct config cfg;

    char path[] = "/tmp/sol_cfg_test_XXXXXX";
    const char *content = "max_inflight 100\n"
                          "max_publish_rate 25\n"
                          "max_connections_per_ip 4\n";
    ASSERT("config::limits::mkstemp...FAIL", write_conf(path, content) == 0);

    config_set_default(&cfg);
    ASSERT("config::limits::load...FAIL", config_load(&cfg, path) == true);
    ASSERT("config::limits::inflight...FAIL", cfg.max_inflight == 100);
    ASSERT("config::limits::rate...FAIL", cfg.max_publish_rate == 25);
    ASSERT("config::limits::per_ip...FAIL",
           cfg.max_connections_per_ip == 4);
    unlink(path);
    close_run_fd(&cfg);
    printf("config::limits...OK\n");
    return 0;
}

static char *test_config_check_cross_field(void)
{
    struct config cfg;

    char path[] = "/tmp/sol_cfg_test_XXXXXX";

    /* max_inflight cannot exceed the 16 bit packet identifier space */
    ASSERT("config::cross::mkstemp...FAIL",
           write_conf(path, "max_inflight 70000\n") == 0);
    config_set_default(&cfg);
    ASSERT("config::cross::load...FAIL", config_load(&cfg, path) == true);
    ASSERT("config::cross::inflight...FAIL", config_check(&cfg) == false);
    unlink(path);
    close_run_fd(&cfg);

    /* Mutual TLS needs a TLS listener: there is no certificate to ask for
     * otherwise */
    char path2[] = "/tmp/sol_cfg_test_XXXXXX";
    ASSERT("config::cross2::mkstemp...FAIL",
           write_conf(path2, "require_client_cert true\n") == 0);
    config_set_default(&cfg);
    ASSERT("config::cross2::load...FAIL", config_load(&cfg, path2) == true);
    ASSERT("config::cross2::tls...FAIL", config_check(&cfg) == false);
    unlink(path2);
    close_run_fd(&cfg);

    printf("config::check_cross_field...OK\n");
    return 0;
}

char *config_test(void)
{
    RUN_TEST(test_config_load);
    RUN_TEST(test_config_rejects_unknown_key);
    RUN_TEST(test_config_rejects_bad_values);
    RUN_TEST(test_config_check_rejects_bad_port);
    RUN_TEST(test_config_limits);
    RUN_TEST(test_config_check_cross_field);
    return 0;
}
