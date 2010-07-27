/*
 * moxi logging API
 * Based on log.[ch] from lighttpd source
 * mtaneja@zynga.com
 */
/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */


#include <sys/types.h>

#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>

#include <stdarg.h>
#include <stdio.h>
#include <syslog.h>
#include <assert.h>

#include "log.h"
#ifdef HAVE_VALGRIND_VALGRIND_H
#include <valgrind/valgrind.h>
#endif

#ifndef O_LARGEFILE
# define O_LARGEFILE 0
#endif

#define  MAX_LOGBUF_LEN 4096

/**
 * open the errorlog
 *
 * we have 3 possibilities:
 * - stderr (default)
 * - syslog
 * - logfile
 *
 * if the open failed, report to the user and die
 *
 */


int log_error_open(moxi_log *ml) {
    assert(ml);

    if (!ml->logbuf) {
        ml->logbuf = (char *) malloc(MAX_LOGBUF_LEN);
        bzero(ml->logbuf, MAX_LOGBUF_LEN);
        ml->logbuf_used = 0;
    }

    ml->log_mode = ERRORLOG_STDERR;

    if (ml->log_file) {
        const char *logfile = ml->log_file;

        if (-1 == (ml->fd = open(logfile, O_APPEND | O_WRONLY | O_CREAT | O_LARGEFILE, 0644))) {
            fprintf(stderr, "opening errorlog '%s' failed. error : %s, Switching to syslog",
                    logfile, strerror(errno));

            ml->use_syslog = 1;

        } else
            ml->log_mode = ERRORLOG_FILE;
    }

    if (ml->use_syslog) {
        openlog(ml->log_ident, LOG_CONS | LOG_PID, LOG_DAEMON);
        ml->log_mode = ERRORLOG_SYSLOG;
    }


    return 0;
}

/**
 * open the errorlog
 *
 * if the open failed, report to the user and die
 * if no filename is given, use syslog instead
 *
 */

int log_error_cycle(moxi_log *ml) {
    /* only cycle if we are not in syslog-mode */

    if (ml->log_mode == ERRORLOG_FILE) {
        const char *logfile = ml->log_file;
        /* already check of opening time */

        int new_fd;

        log_error_write(ml, __FILE__, __LINE__, "About to cycle log \n");

        if (-1 == (new_fd = open(logfile, O_APPEND | O_WRONLY | O_CREAT | O_LARGEFILE, 0644))) {
            /* write to old log */
            log_error_write(ml, __FILE__, __LINE__,
                    "cycling errorlog '%s' failed : %s. failing back to syslog()", logfile, strerror(errno));

            close(ml->fd);
            ml->fd = -1;
            ml->log_mode = ERRORLOG_SYSLOG;

        } else {
            /* ok, new log is open, close the old one */
            close(ml->fd);
            ml->fd = new_fd;
            log_error_write(ml, __FILE__, __LINE__, "Log Cycled \n");
        }
    }

    return 0;
}

int log_error_close(moxi_log *ml) {
    switch(ml->log_mode) {
        case ERRORLOG_FILE:
            close(ml->fd);
            break;
        case ERRORLOG_SYSLOG:
#ifdef HAVE_SYSLOG_H
            closelog();
#endif
            break;
        case ERRORLOG_STDERR:
            break;
    }

    return 0;
}

#define mappend_log(ml,str)                                     \
    if (ml->logbuf_used < MAX_LOGBUF_LEN) {                     \
        memcpy(ml->logbuf + ml->logbuf_used, str, strlen(str)); \
        ml->logbuf_used += strlen(str);                         \
    }                                                           \

#define mappend_log_int(ml, num)                                \
    if (ml->logbuf_used < MAX_LOGBUF_LEN) {                     \
        char buf[32] = {0};                                     \
        snprintf(buf, 32, "%d", num);                           \
        memcpy(ml->logbuf + ml->logbuf_used, buf, strlen(buf)); \
        ml->logbuf_used += strlen(buf);                         \
    }


int log_error_write(moxi_log *ml, const char *filename, unsigned int line, const char *fmt, ...) {
    va_list ap;
    static char ts_debug_str[255];
    int written = 0;

    ml->logbuf_used = 0;

    switch(ml->log_mode) {
        case ERRORLOG_FILE:
        case ERRORLOG_STDERR:
            /* cache the generated timestamp */
            if (!ml->cur_ts)
                ml->cur_ts = time(NULL);

            if (ml->cur_ts != ml->last_generated_debug_ts) {
                bzero(ts_debug_str, 255);
                strftime(ts_debug_str, 254, "%Y-%m-%d %H:%M:%S", localtime(&(ml->cur_ts)));
                ml->last_generated_debug_ts = ml->cur_ts;
            }

            mappend_log(ml, ts_debug_str);

            /*mappend_log(zl, ts_debug_str)*/
            mappend_log(ml, ": (");
            break;
        case ERRORLOG_SYSLOG:
            bzero(ml->logbuf, MAX_LOGBUF_LEN);
            /* syslog is generating its own timestamps */
            mappend_log(ml, "(");
            break;
    }

    mappend_log(ml, filename);
    mappend_log(ml, ".");
    mappend_log_int(ml, line);
    mappend_log(ml, ") ");


    va_start(ap, fmt);
    ml->logbuf_used +=
        vsnprintf((ml->logbuf + ml->logbuf_used), (MAX_LOGBUF_LEN - ml->logbuf_used - 1), fmt, ap);
    va_end(ap);

    switch(ml->log_mode) {
        case ERRORLOG_FILE:
            written = write(ml->fd, ml->logbuf, ml->logbuf_used);
            break;
        case ERRORLOG_STDERR:
            written = write(STDERR_FILENO, ml->logbuf, ml->logbuf_used);
            break;
        case ERRORLOG_SYSLOG:
            syslog(LOG_ERR, "%s", ml->logbuf);
            break;
    }

    return 0;
}



