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

#include "logging.h"
#include <assert.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define MAX_LOG_SIZE 120

static FILE *fh          = NULL;
static int logging_level = DEBUG;
/* Path sol_log_init() opened, kept so SIGHUP can reopen the same file */
static char logpath[0xFFF];
/* Set from the signal handler, consumed by sol_log_maybe_reopen() */
static volatile sig_atomic_t reopen_requested;

void sol_log_init(const char *file, int level)
{
    logging_level = level;
    logpath[0]    = '\0';
    if (!file)
        return;
    snprintf(logpath, sizeof(logpath), "%s", file);
    if (!logpath[0])
        return;
    fh = fopen(logpath, "a+");
    if (!fh)
        printf("%lu * WARNING: Unable to open file %s\n",
               (unsigned long)time(NULL), file);
}

void sol_log_request_reopen(void)
{
    reopen_requested = 1;
}

void sol_log_maybe_reopen(void)
{
    if (!reopen_requested)
        return;
    reopen_requested = 0;
    if (!logpath[0])
        return;
    if (fh) {
        fflush(fh);
        fclose(fh);
    }
    fh = fopen(logpath, "a+");
}

void sol_log_close(void)
{
    if (fh) {
        fflush(fh);
        fclose(fh);
        fh = NULL;
    }
}

void sol_log(int level, const char *fmt, ...)
{

    if (level < logging_level)
        return;

    assert(fmt);

    va_list ap;
    char msg[MAX_LOG_SIZE + 4];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    /* Truncate messages longer than MAX_LOG_SIZE, appending three dots */
    if (strlen(msg) > MAX_LOG_SIZE) {
        memcpy(msg + MAX_LOG_SIZE, "...", 3);
        msg[MAX_LOG_SIZE + 3] = '\0';
    }

    // Open two handler, one for standard output and a second for the
    // persistent log file
    FILE *fp              = stdout;

    if (!fp)
        return;

    fprintf(fp, "%lu %s\n", (unsigned long)time(NULL), msg);
    if (fh) {
        fprintf(fh, "%lu %s\n", (unsigned long)time(NULL), msg);
        /*
         * Flushed right away: a broker is usually watched through its log
         * file (`tail -f`), and a message sitting in a stdio buffer for
         * minutes is a message nobody sees. The volume of a broker log is
         * low enough for the extra write to be irrelevant
         */
        fflush(fh);
    }
}
