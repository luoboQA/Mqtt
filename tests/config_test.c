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
#include "unit.h"
#include "../src/config.h"
#include "../src/logging.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static char *test_config_load(void)
{
    char path[] = "/tmp/sol_cfg_test_XXXXXX";
    int fd      = mkstemp(path);
    ASSERT("config::mkstemp...FAIL", fd >= 0);

    const char *content = "# a comment\n"
                          "\n"
                          "log_level WARNING\n"
                          "ip_port 19999\n"
                          "max_request_size 32KB\n"
                          "keepalive 15s\n";
    ASSERT("config::write...FAIL",
           write(fd, content, strlen(content)) == (ssize_t)strlen(content));
    close(fd);

    config_set_default();
    ASSERT("config::load...FAIL", config_load(path) == true);
    ASSERT("config::log_level...FAIL", conf->loglevel == WARNING);
    ASSERT("config::ip_port...FAIL", strcmp(conf->port, "19999") == 0);
    ASSERT("config::max_request_size...FAIL",
           conf->max_request_size == 32 * 1024);
    ASSERT("config::keepalive...FAIL", conf->keepalive == 15);

    unlink(path);
    /* config_set_default() opens the eventfd/pipe the event loop waits on */
#ifdef __linux__
    close(conf->run);
#else
    close(conf->run[0]);
    close(conf->run[1]);
#endif
    printf("config::load...OK\n");
    return 0;
}

char *config_test(void)
{
    RUN_TEST(test_config_load);
    return 0;
}
