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
#include "../src/config.h"
#include "../src/logging.h"
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
static void close_run_fd(void)
{
#ifdef __linux__
    close(conf->run);
#else
    close(conf->run[0]);
    close(conf->run[1]);
#endif
}

static char *test_config_load(void)
{
    char path[] = "/tmp/sol_cfg_test_XXXXXX";

    const char *content = "# a comment\n"
                          "\n"
                          "log_level WARNING\n"
                          "ip_port 19999\n"
                          "max_request_size 32KB\n"
                          "keepalive 15s\n";
    ASSERT("config::mkstemp...FAIL", write_conf(path, content) == 0);

    config_set_default();
    ASSERT("config::load...FAIL", config_load(path) == true);
    ASSERT("config::log_level...FAIL", conf->loglevel == WARNING);
    ASSERT("config::ip_port...FAIL", strcmp(conf->port, "19999") == 0);
    ASSERT("config::max_request_size...FAIL",
           conf->max_request_size == 32 * 1024);
    ASSERT("config::keepalive...FAIL", conf->keepalive == 15);

    unlink(path);
    close_run_fd();
    printf("config::load...OK\n");
    return 0;
}

static char *test_config_rejects_unknown_key(void)
{
    char path[] = "/tmp/sol_cfg_test_XXXXXX";
    ASSERT("config::unknown::mkstemp...FAIL",
           write_conf(path, "ip_port 1883\nbogus_key x\n") == 0);

    config_set_default();
    ASSERT("config::unknown_key...FAIL", config_load(path) == false);
    unlink(path);
    close_run_fd();
    printf("config::unknown_key...OK\n");
    return 0;
}

static char *test_config_rejects_bad_values(void)
{
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
    };

    for (size_t i = 0; i < sizeof(bad) / sizeof(*bad); ++i) {
        char path[] = "/tmp/sol_cfg_test_XXXXXX";
        ASSERT("config::bad_value::mkstemp...FAIL",
               write_conf(path, bad[i]) == 0);
        config_set_default();
        if (config_load(path) != false) {
            printf("config::bad_value[%zu] (%s)...FAIL\n", i, bad[i]);
            unlink(path);
            close_run_fd();
            return "config::bad_value...FAIL";
        }
        unlink(path);
        close_run_fd();
    }
    tests_run++;
    printf("config::bad_value...OK (%zu cases)\n",
           sizeof(bad) / sizeof(*bad));
    return 0;
}

static char *test_config_check_rejects_bad_port(void)
{
    char path[] = "/tmp/sol_cfg_test_XXXXXX";
    /* config_load() accepts the string, config_check() is the one that
     * validates the port range after the command line overrides */
    ASSERT("config::check::mkstemp...FAIL",
           write_conf(path, "log_level WARNING\n") == 0);

    config_set_default();
    ASSERT("config::check::load...FAIL", config_load(path) == true);
    strcpy(conf->port, "70000");
    ASSERT("config::check::port...FAIL", config_check() == false);

    unlink(path);
    close_run_fd();
    printf("config::check_bad_port...OK\n");
    return 0;
}

char *config_test(void)
{
    RUN_TEST(test_config_load);
    RUN_TEST(test_config_rejects_unknown_key);
    RUN_TEST(test_config_rejects_bad_values);
    RUN_TEST(test_config_check_rejects_bad_port);
    return 0;
}
