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

#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/eventfd.h>
#endif
#include "config.h"
#include "logging.h"
#include "server.h"
#include "util.h"

/* PID file written after daemonize(), removed on a clean shutdown */
static const char *pid_path;

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

/*
 * SIGHUP asks for the log file to be reopened, which is what logrotate
 * needs after it has renamed the old one. The handler only sets a flag: the
 * event loop calls sol_log_maybe_reopen() on its one second tick, because
 * fopen() is not async-signal-safe
 */
static void sighup_handler(int signum)
{
    (void)signum;
    sol_log_request_reopen();
}

static int write_pid_file(const char *path)
{
    if (!path || !path[0])
        return 0;
    FILE *fh = fopen(path, "w");
    if (!fh) {
        log_error("Unable to write PID file %s: %s", path, strerror(errno));
        return -1;
    }
    fprintf(fh, "%ld\n", (long)getpid());
    fclose(fh);
    pid_path = path;
    return 0;
}

static void remove_pid_file(void)
{
    if (pid_path && pid_path[0])
        unlink(pid_path);
}

static const char *flag_description[] = {
    "Print this help",
    "Set a configuration file to load and use",
    "Set the listening host address",
    "Set the listening port",
    "Enable all logs, setting log level to DEBUG",
    "Run in daemon mode",
    "Print the version and exit",
    "Check the configuration and exit"};

void print_help(char *me)
{
    printf("\nSol v%s MQTT broker 3.1.1\n\n", VERSION);
    printf("Usage: %s [-a addr] [-p port] [-c conf] [-v|-d|-h|-V|-t]\n\n",
           me);
    const char flags[8] = "hcapvdVt";
    for (int i = 0; i < 8; ++i)
        printf(" -%c: %s\n", flags[i], flag_description[i]);
    printf("\n");
}

int main(int argc, char **argv)
{

    /* NULL means "not given on the command line" */
    char *addr       = NULL;
    char *port       = NULL;
    char *confpath   = DEFAULT_CONF_PATH;
    int debug = 0, daemon = 0, check_config = 0, conf_given = 0;
    int opt;

    static const struct option long_options[] = {
        {"help", no_argument, NULL, 'h'},
        {"config", required_argument, NULL, 'c'},
        {"address", required_argument, NULL, 'a'},
        {"port", required_argument, NULL, 'p'},
        {"verbose", no_argument, NULL, 'v'},
        {"daemon", no_argument, NULL, 'd'},
        {"version", no_argument, NULL, 'V'},
        {"check-config", no_argument, NULL, 't'},
        {NULL, 0, NULL, 0}};

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

    struct sigaction hup = {0};
    hup.sa_handler       = sighup_handler;
    hup.sa_flags         = SA_RESTART;
    sigemptyset(&hup.sa_mask);
    sigaction(SIGHUP, &hup, NULL);

    /*
     * A write on a socket whose peer already closed raises SIGPIPE, which by
     * default kills the whole broker: every other client on that loop is
     * dropped with it. send() already passes MSG_NOSIGNAL on the plain path,
     * but SSL_write() has no such flag, so the only safe option is to ignore
     * the signal process wide
     */
    signal(SIGPIPE, SIG_IGN);

    while ((opt = getopt_long(argc, argv, "a:c:p:vhdtV", long_options, NULL)) !=
           -1) {
        switch (opt) {
        case 'a':
            addr = optarg;
            break;
        case 'c':
            confpath   = optarg;
            conf_given = 1;
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
        case 't':
            check_config = 1;
            break;
        case 'V':
            printf("Sol v%s MQTT broker 3.1.1\n", VERSION);
            exit(EXIT_SUCCESS);
        case 'h':
            print_help(argv[0]);
            exit(EXIT_SUCCESS);
        default:
            fprintf(stderr,
                    "Usage: %s [-a addr] [-p port] [-c conf] "
                    "[-v|-d|-h|-V|-t]\n",
                    argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    // Try to load a configuration, if found
    int loaded = config_load(confpath);

    /*
     * A missing or malformed file is fatal when it was named explicitly (or
     * when only checking it): starting with defaults instead would hide a
     * typo in the unit file. The built in path may legitimately be absent,
     * and that keeps working as before
     */
    if (!loaded && (conf_given || check_config)) {
        fprintf(stderr, "Invalid configuration file: %s\n", confpath);
        exit(EXIT_FAILURE);
    }

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

    if (!config_check()) {
        fprintf(stderr, "Configuration check failed: %s\n", confpath);
        exit(EXIT_FAILURE);
    }

    if (check_config) {
        printf("Configuration OK: %s\n", confpath);
        exit(EXIT_SUCCESS);
    }

    sol_log_init(conf->logpath, conf->loglevel);

    if (daemon == 1)
        daemonize();

    if (write_pid_file(conf->pid_file) < 0)
        exit(EXIT_FAILURE);

    // Print configuration
    config_print();

    start_server(conf->hostname, conf->port);

    remove_pid_file();
    sol_log_close();

    return 0;
}
