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

#include <signal.h>
#ifdef __linux__
#include <sys/eventfd.h>
#endif
#include "config.h"
#include "logging.h"
#include "server.h"
#include "util.h"
#include <unistd.h>

/*
 * Stops the epoll_wait loops of every worker by making the shared run
 * descriptor readable. Only async-signal-safe calls belong here: usleep() is
 * not one (and it re-entered the handler on a second signal), it is gone.
 * The eventfd is level triggered and deliberately never read back, so a
 * single write already wakes every loop parked on it and keeps them awake
 * until the shutdown completes
 */
static void sigint_handler(int signum)
{
    (void)signum;
#ifdef __linux__
    (void)eventfd_write(conf->run, 1);
#else
    /*
     * Write to the pipe's write end (index 1), the read end (index 0) is
     * what the loops register: writing to the read end failed with EBADF
     * and left the loops hanging, so SIGTERM could never stop the broker
     * without a kill -9
     */
    (void)write(conf->run[1], &(unsigned long){1}, sizeof(unsigned long));
#endif
}

static const char *flag_description[] = {
    "Print this help",
    "Set a configuration file to load and use",
    "Set the listening host address",
    "Set the listening port",
    "Enable all logs, setting log level to DEBUG",
    "Run in daemon mode"};

void print_help(char *me)
{
    printf("\nSol v%s MQTT broker 3.1.1\n\n", VERSION);
    printf("Usage: %s [-a addr] [-p port] [-c conf] [-v|-d|-h]\n\n", me);
    const char flags[6] = "hcapvd";
    for (int i = 0; i < 6; ++i)
        printf(" -%c: %s\n", flags[i], flag_description[i]);
    printf("\n");
}

int main(int argc, char **argv)
{

    /* NULL means "not given on the command line" */
    char *addr     = NULL;
    char *port     = NULL;
    char *confpath = DEFAULT_CONF_PATH;
    int debug = 0, daemon = 0;
    int opt;

    // Set default configuration
    config_set_default();

    /*
     * The handlers read conf->run, so they must be installed only after
     * config_set_default() has created it: a signal arriving in between used
     * to dereference a NULL conf from inside the handler itself
     */
    struct sigaction sa = {0};
    sa.sa_handler       = sigint_handler;
    sa.sa_flags         = SA_RESTART;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /*
     * A write on a socket whose peer already closed raises SIGPIPE, which by
     * default kills the whole broker: every other client on that loop is
     * dropped with it. send() already passes MSG_NOSIGNAL on the plain path,
     * but SSL_write() has no such flag, so the only safe option is to ignore
     * the signal process wide
     */
    signal(SIGPIPE, SIG_IGN);

    while ((opt = getopt(argc, argv, "a:c:p:vhd")) != -1) {
        switch (opt) {
        case 'a':
            addr = optarg;
            break;
        case 'c':
            confpath = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case 'v':
            debug = 1;
            break;
        case 'd':
            daemon = 1;
            break;
        case 'h':
            print_help(argv[0]);
            exit(EXIT_SUCCESS);
        default:
            fprintf(stderr,
                    "Usage: %s [-a addr] [-p port] [-c conf] [-v|-d|-h]\n",
                    argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    // Try to load a configuration, if found
    config_load(confpath);

    /*
     * The command line has the last word: the options are applied after the
     * configuration file has been read. They used to be applied before it,
     * so an `ip_address`/`ip_port` entry of the conf file silently overrode
     * an explicit -a/-p and the broker listened somewhere else than asked
     * (this very bug made the integration tests connect to a closed port)
     */
    if (addr) {
        if (strlen(addr) >= sizeof(conf->hostname)) {
            fprintf(stderr, "Address too long\n");
            exit(EXIT_FAILURE);
        }
        strcpy(conf->hostname, addr);
    }
    if (port) {
        if (strlen(port) >= sizeof(conf->port)) {
            fprintf(stderr, "Port too long\n");
            exit(EXIT_FAILURE);
        }
        strcpy(conf->port, port);
    }

    /* -v forces DEBUG, without it the level set by the conf file (or the
     * default one) stands */
    if (debug == 1)
        conf->loglevel = DEBUG;

    sol_log_init(conf->logpath, conf->loglevel);

    if (daemon == 1)
        daemonize();

    // Print configuration
    config_print();

    start_server(conf->hostname, conf->port);

    sol_log_close();

    return 0;
}
