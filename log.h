/*
 * moxi logging API
 * mtaneja@zynga.com
 */

#ifndef _LOG_H_
#define _LOG_H_

/*
 * define the log levels
 */

#define MOXI_LOG_CRIT    1
#define MOXI_LOG_ERR     5
#define MOXI_LOG_INFO    10
#define MOXI_LOG_DEBUG   15


#define ERRORLOG_STDERR        0x1
#define ERRORLOG_FILE          0x2
#define ERRORLOG_SYSLOG        0x4


struct moxi_log {

    int fd;             /* log fd */
    int log_level;      /* logging level. default 5 */
    int log_mode;       /* syslog, log file, stderr */
    char *log_ident;    /* syslog identifier */
    char *log_file;     /* if log file is specified */
    int use_syslog;     /* set if syslog is being used */
    char *logbuf;       /* scratch buffer */
    int logbuf_used;    /* length of scratch buffer */
    time_t cur_ts;      /* current timestamp */
    time_t last_generated_debug_ts;
};

typedef struct moxi_log moxi_log;

int log_error_open(moxi_log *);
int log_error_close(moxi_log *);
int log_error_write(moxi_log *, const char *filename, unsigned int line, const char *fmt, ...);
int log_error_cycle(moxi_log *);

#ifndef MAIN_CHECK
#define moxi_log_write(...) log_error_write (ml, __FILE__, __LINE__, __VA_ARGS__)
#else
#define moxi_log_write(...) fprintf(stderr, __VA_ARGS__)
#endif

#endif
