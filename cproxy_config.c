/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <math.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"
#include "log.h"

// Local declarations.
//
static char *readfile(char *path);

volatile uint32_t  msec_current_time = 0;
int                msec_cycle = 200;
struct event       msec_clockevent;
struct event_base *msec_clockevent_base = NULL;

char cproxy_hostname[300] = {0}; // Immutable after init.

void msec_clock_handler(const int fd, const short which, void *arg);
void msec_set_current_time(void);

int cproxy_init_string(char *cfg_str,
                       proxy_behavior behavior,
                       int nthreads);

int cproxy_init_agent(char *cfg_str,
                      proxy_behavior behavior,
                      int nthreads);

proxy_behavior behavior_default_g = {
    .cycle = 0,
    .downstream_max = 4,
    .downstream_weight = 0,
    .downstream_retry = 1,
    .downstream_protocol = proxy_downstream_ascii_prot,
    .downstream_timeout = {
        .tv_sec  = 0,
        .tv_usec = 0
    },
    .wait_queue_timeout = {
        .tv_sec  = 0,
        .tv_usec = 0
    },
    .front_cache_max = 200,
    .front_cache_lifespan = 0,
    .front_cache_spec = {0},
    .front_cache_unspec = {0},
    .key_stats_max = 4000,
    .key_stats_lifespan = 0,
    .key_stats_spec = {0},
    .key_stats_unspec = {0},
    .optimize_set = {0},
    .host = {0},
    .port = 0,
    .bucket = {0},
    .usr = {0},
    .pwd = {0},
    .port_listen = MOXI_DEFAULT_LISTEN_PORT,
    .default_bucket_name = "default"
};

/** Length of key that may be zero or space terminated.
 */
size_t skey_len(const char *key) {
    assert(key);

    char *x = (char *) key;
    while (*x != ' ' && *x != '\0')
        x++;

    return x - key;
}

/** Hash of key that may be zero or space terminated.
 */
int skey_hash(const void *v) {
    assert(v);

    const char *key = v;
    size_t      len = skey_len(key);

    return murmur_hash(key, len);
}

/** Returns true if two keys are equal, where the
 *  keys may be zero or space terminated.
 */
int skey_equal(const void *v1, const void *v2) {
    assert(v1);
    assert(v2);

    const char *k1 = v1;
    const char *k2 = v2;

    size_t n1 = skey_len(k1);
    size_t n2 = skey_len(k2);

    return (n1 == n2 && strncmp(k1, k2, n1) == 0);
}

static void *noop_dup(const void *v)
{
    return (void*)v;
}

void noop_free(void *v) {
    /* Nothing */
}

static int
str_eq(const void* p1, const void*p2)
{
    char *str1=(char *)p1;
    char *str2=(char *)p2;
    assert(str1 != NULL);
    assert(str2 != NULL);
    return strcmp(str1, str2) == 0;
}

struct hash_ops strhash_ops = {
   .hashfunc = genhash_string_hash,
   .hasheq = str_eq,
   .dupKey = noop_dup,
   .dupValue = noop_dup,
   .freeKey = noop_free,
   .freeValue = noop_free
};

struct hash_ops skeyhash_ops = {
   .hashfunc = skey_hash,
   .hasheq = skey_equal,
   .dupKey = noop_dup,
   .dupValue = noop_dup,
   .freeKey = noop_free,
   .freeValue = noop_free
};

/** Returns pointer to first non-space char in string.
 */
char *skipspace(char *s) {
    if (s == NULL) {
        return NULL;
    }

    while (isspace(*s) && *s != '\0') {
        s++;
    }

    return s;
}

/** Modifies string by zero'ing out any trailing spaces.
 */
char *trailspace(char *s) {
    if (s == NULL) {
        return NULL;
    }

    for (char *e = s + strlen(s) - 1; e >= s && isspace(*e); e--) {
        *e = '\0';
    }

    return s;
}

/** Modifies string by removing prefix and suffix whitespace chars.
 *  Returns the pointer into the string that you should use.
 */
char *trimstr(char *s) {
    return trailspace(skipspace(s));
}

/** Like strdup(), but trims spaces from start and end.
 *  Unlike with trimstr(strdup(s)), you can call free(trimstrdup(s)),
 *  and the correct memory is free()'ed.
 */
char *trimstrdup(char *s) {
    return trailspace(strdup(skipspace(s)));
}

/** Returns true if first word in a string equals a given word.
 */
bool wordeq(char *s, char *word) {
    assert(s);
    assert(word);

    char *end = s;
    while (!isspace(*end) && *end != '\0')
        end++;

    return strncmp(s, word, end - s) == 0;
}

// ---------------------------------------

static bool cproxy_core_initted = false;

/** The cfg_str may be a path to a file like...
 *
 *    /full/path
 *      or...
 *    ./relative/path
 *      or...
 *    ../another/relative/path
 *
 *  But not...
 *
 *    incorrect/relative/path
 *
 *  Paths are detected by the first character of  '/' or '.'.
 *
 *  The contents of the file should be in cfg_str format.
 */
int cproxy_init(char *cfg_str,
                char *behavior_str,
                int nthreads,
                struct event_base *main_base) {
    assert(nthreads > 1); // Main + at least one worker.
    assert(nthreads == settings.num_threads);

    if (cproxy_core_initted == false) {
        cproxy_core_initted = true;

        gethostname(cproxy_hostname, sizeof(cproxy_hostname));

        cproxy_init_a2a();
        cproxy_init_a2b();
        cproxy_init_b2b();
    }

    if (cfg_str == NULL ||
        strlen(cfg_str) <= 0) {
        return 0;
    }

    if (behavior_str != NULL &&
        (behavior_str[0] == '.' ||
         behavior_str[0] == '/')) {
        char *buf = readfile(behavior_str);
        if (buf != NULL) {
            int rv = cproxy_init(cfg_str, buf, nthreads, main_base);
            free(buf);
            return rv;
        } else {
            moxi_log_write("could not read behavior file: %s\n", behavior_str);
            exit(EXIT_FAILURE);
        }
    }

    if (cfg_str[0] == '.' ||
        cfg_str[0] == '/') {
        char *buf = readfile(cfg_str);
        if (buf != NULL) {
            int rv = cproxy_init(buf, behavior_str, nthreads, main_base);
            free(buf);
            return rv;
        } else {
            moxi_log_write("could not read cfg file: %s\n", cfg_str);
            exit(EXIT_FAILURE);
        }
    }

    if (behavior_str == NULL) {
        behavior_str = "";
    }

#ifdef MOXI_USE_VBUCKET
    // The vbucket feature only works in binary protocol.
    //
    behavior_default_g.downstream_protocol = proxy_downstream_binary_prot;
#endif

    char *env_usr = getenv("MOXI_SASL_PLAIN_USR");
    if (env_usr != NULL) {
        strncpy(behavior_default_g.usr, env_usr, sizeof(behavior_default_g.usr) - 1);
        behavior_default_g.usr[sizeof(behavior_default_g.usr) - 1] = '\0';

        moxi_log_write("env: MOXI_SASL_PLAIN_USR (%d)\n",
                       strlen(behavior_default_g.usr));
    }

    char *env_pwd = getenv("MOXI_SASL_PLAIN_PWD");
    if (env_pwd != NULL) {
        strncpy(behavior_default_g.pwd, env_pwd, sizeof(behavior_default_g.pwd) - 1);
        behavior_default_g.pwd[sizeof(behavior_default_g.pwd) - 1] = '\0';

        moxi_log_write("env: MOXI_SASL_PLAIN_PWD (%d)\n",
                       strlen(behavior_default_g.pwd));
    }

    proxy_behavior behavior =
        cproxy_parse_behavior(behavior_str,
                              behavior_default_g);

    if (behavior.cycle > 0) {
        msec_cycle = behavior.cycle;
    }

    msec_clockevent_base = main_base;
    msec_clock_handler(0, 0, NULL);

    // Not jid format and not a URL, so it must be a simple cmd-line
    // or file-based config.
    //
    if (strchr(cfg_str, '@') == NULL &&
        strstr(cfg_str, "http://") == NULL) {
        return cproxy_init_string(cfg_str,
                                  behavior,
                                  nthreads);
    }

#ifdef HAVE_CONFLATE_H
    return cproxy_init_agent(cfg_str,
                             behavior,
                             nthreads);
#else
    moxi_log_write("missing conflate\n");
    exit(EXIT_FAILURE);
    return 1;
#endif
}

int cproxy_init_string(char *cfg_str,
                       proxy_behavior behavior,
                       int nthreads) {
    /* cfg looks like "local_port=host:port,host:port;local_port=host:port"
     * like "11222=memcached1.foo.net:11211"  This means local port 11222
     * will be a proxy to downstream memcached server running at
     * host memcached1.foo.net on port 11211.
     */
    if (cfg_str== NULL ||
        strlen(cfg_str) <= 0) {
        return 0;
    }

    char *buff;
    char *next;
    char *proxy_name = "default";
    char *proxy_sect;
    char *proxy_port_str;
    int   proxy_port;

    if (settings.verbose > 1) {
        cproxy_dump_behavior(&behavior, "init_string", 2);
    }

    buff = trimstrdup(cfg_str);
    next = buff;
    while (next != NULL) {
        proxy_sect = strsep(&next, ";");

        proxy_port_str = trimstr(strsep(&proxy_sect, "="));
        if (proxy_sect == NULL) {
            moxi_log_write("bad moxi config, missing =\n");
            exit(EXIT_FAILURE);
        }
        proxy_port = atoi(proxy_port_str);
        if (proxy_port <= 0) {
            moxi_log_write("missing proxy port\n");
            exit(EXIT_FAILURE);
        }
        proxy_sect = trimstr(proxy_sect);

        int behaviors_num = 1; // Number of servers.
        for (char *x = proxy_sect; *x != '\0'; x++) {
            if (*x == ',') {
                behaviors_num++;
            }
        }

        proxy_behavior_pool behavior_pool;
        memset(&behavior_pool, 0, sizeof(proxy_behavior_pool));

        behavior_pool.base = behavior;
        behavior_pool.num  = behaviors_num;
        behavior_pool.arr  = calloc(behaviors_num,
                                    sizeof(proxy_behavior));

        if (behavior_pool.arr != NULL) {
            for (int i = 0; i < behaviors_num; i++) {
                behavior_pool.arr[i] = behavior;
            }

            proxy_main *m = cproxy_gen_proxy_main(behavior, nthreads,
                                                  PROXY_CONF_TYPE_STATIC);
            if (m == NULL) {
                moxi_log_write("could not alloc proxy_main\n");
                exit(EXIT_FAILURE);
            }

            proxy *p = cproxy_create(m,
                                     proxy_name,
                                     proxy_port,
                                     proxy_sect,
                                     0, // config_ver.
                                     &behavior_pool,
                                     nthreads);
            if (p != NULL) {
                pthread_mutex_lock(&m->proxy_main_lock);
                p->next = m->proxy_head;
                m->proxy_head = p;
                pthread_mutex_unlock(&m->proxy_main_lock);

                int n = cproxy_listen(p);
                if (n > 0) {
                    if (settings.verbose > 1) {
                        moxi_log_write("moxi listening on %d with %d conns\n",
                                proxy_port, n);
                    }
                } else {
                    moxi_log_write("moxi error -- port %d unavailable?\n",
                            proxy_port);
                    exit(EXIT_FAILURE);
                }
            } else {
                moxi_log_write("could not alloc proxy\n");
                exit(EXIT_FAILURE);
            }

            free(behavior_pool.arr);
        } else {
            moxi_log_write("could not alloc behaviors\n");
            exit(EXIT_FAILURE);
        }
    }

    free(buff);

    return 0;
}

proxy_main *cproxy_gen_proxy_main(proxy_behavior behavior, int nthreads,
                                  enum_proxy_conf_type conf_type) {
    proxy_main *m = calloc(1, sizeof(proxy_main));
    if (m != NULL) {
        m->proxy_head = NULL;
        m->behavior   = behavior;
        m->nthreads   = nthreads;
        m->conf_type  = conf_type;

        pthread_mutex_init(&m->proxy_main_lock, NULL);

        m->stat_configs      = 0;
        m->stat_config_fails = 0;
        m->stat_proxy_starts      = 0;
        m->stat_proxy_start_fails = 0;
        m->stat_proxy_existings   = 0;
        m->stat_proxy_shutdowns   = 0;
    }

    return m;
}

proxy_behavior cproxy_parse_behavior(char          *behavior_str,
                                     proxy_behavior behavior_default) {
    // These are the default proxy behaviors.
    //
    struct proxy_behavior behavior = behavior_default;

    if (behavior_str == NULL ||
        strlen(behavior_str) <= 0) {
        return behavior;
    }

    // Parse the key-value behavior_str, to override the defaults.
    //
    char *buff = trimstrdup(behavior_str);
    char *next = buff;

    while (next != NULL) {
        char *key_val = trimstr(strsep(&next, ","));
        if (key_val != NULL) {
            cproxy_parse_behavior_key_val_str(key_val, &behavior);
        }
    }

    free(buff);

    return behavior;
}

/** Note: the key_val param buffer is modified.
 */
void cproxy_parse_behavior_key_val_str(char *key_val,
                                       proxy_behavior *behavior) {
    assert(behavior != NULL);

    if (key_val != NULL) {
        char *key = strsep(&key_val, "=");
        char *val = key_val;
        cproxy_parse_behavior_key_val(key, val, behavior);
    }
}

/** Note: the key and val param buffers are modified.
 */
void cproxy_parse_behavior_key_val(char *key,
                                   char *val,
                                   proxy_behavior *behavior) {
    assert(behavior != NULL);

    if (key != NULL &&
        val != NULL) {
        key = trimstr(key);
        val = trimstr(val);

        if (wordeq(key, "cycle")) {
            behavior->cycle = strtol(val, NULL, 10);
            assert(behavior->cycle >= 0);
        } else if (wordeq(key, "downstream_max")) {
            behavior->downstream_max = strtol(val, NULL, 10);
            assert(behavior->downstream_max >= 0);
        } else if (wordeq(key, "weight") ||
                   wordeq(key, "downstream_weight")) {
            behavior->downstream_weight = strtol(val, NULL, 10);
            assert(behavior->downstream_max >= 0);
        } else if (wordeq(key, "retry") ||
                   wordeq(key, "downstream_retry")) {
            behavior->downstream_retry = strtol(val, NULL, 10);
            assert(behavior->downstream_retry >= 0);
        } else if (wordeq(key, "protocol") ||
                   wordeq(key, "downstream_protocol")) {
            if (wordeq(val, "ascii") ||
                wordeq(val, "memcached-ascii") ||
                wordeq(val, "membase-ascii")) {
                behavior->downstream_protocol =
                    proxy_downstream_ascii_prot;
            } else if (wordeq(val, "binary") ||
                       wordeq(val, "memcached-binary") ||
                       wordeq(val, "membase-binary")) {
                behavior->downstream_protocol =
                    proxy_downstream_binary_prot;
            } else {
                if (settings.verbose > 1) {
                    moxi_log_write("unknown behavior prot: %s\n", val);
                }
            }
        } else if (wordeq(key, "timeout") ||
                   wordeq(key, "downstream_timeout")) {
            int ms = strtol(val, NULL, 10);
            behavior->downstream_timeout.tv_sec  = floor(ms / 1000.0);
            behavior->downstream_timeout.tv_usec = (ms % 1000) * 1000;
        } else if (wordeq(key, "wait_queue_timeout")) {
            int ms = strtol(val, NULL, 10);
            behavior->wait_queue_timeout.tv_sec  = floor(ms / 1000.0);
            behavior->wait_queue_timeout.tv_usec = (ms % 1000) * 1000;
        } else if (wordeq(key, "front_cache_max")) {
            behavior->front_cache_max = strtol(val, NULL, 10);
        } else if (wordeq(key, "front_cache_lifespan")) {
            behavior->front_cache_lifespan = strtol(val, NULL, 10);
        } else if (wordeq(key, "front_cache_spec")) {
            if (strlen(val) < sizeof(behavior->front_cache_spec)) {
                strcpy(behavior->front_cache_spec, val);
            }
        } else if (wordeq(key, "front_cache_unspec")) {
            if (strlen(val) < sizeof(behavior->front_cache_unspec)) {
                strcpy(behavior->front_cache_unspec, val);
            }
        } else if (wordeq(key, "key_stats_max")) {
            behavior->key_stats_max = strtol(val, NULL, 10);
        } else if (wordeq(key, "key_stats_lifespan")) {
            behavior->key_stats_lifespan = strtol(val, NULL, 10);
        } else if (wordeq(key, "key_stats_spec")) {
            if (strlen(val) < sizeof(behavior->key_stats_spec)) {
                strcpy(behavior->key_stats_spec, val);
            }
        } else if (wordeq(key, "key_stats_unspec")) {
            if (strlen(val) < sizeof(behavior->key_stats_unspec)) {
                strcpy(behavior->key_stats_unspec, val);
            }
        } else if (wordeq(key, "optimize_set")) {
            if (strlen(val) < sizeof(behavior->optimize_set)) {
                strcpy(behavior->optimize_set, val);
            }
        } else if (wordeq(key, "usr")) {
            if (strlen(val) < sizeof(behavior->usr)) {
                strcpy(behavior->usr, val);
            }
        } else if (wordeq(key, "pwd")) {
            if (strlen(val) < sizeof(behavior->pwd)) {
                strcpy(behavior->pwd, val);
            }
        } else if (wordeq(key, "host")) {
            if (strlen(val) < sizeof(behavior->host)) {
                strcpy(behavior->host, val);
            }
        } else if (wordeq(key, "port")) {
            behavior->port = strtol(val, NULL, 10);
        } else if (wordeq(key, "bucket")) {
            if (strlen(val) < sizeof(behavior->bucket)) {
                strcpy(behavior->bucket, val);
            }
        } else if (wordeq(key, "port_listen")) {
            behavior->port_listen = strtol(val, NULL, 10);
        } else if (wordeq(key, "default_bucket_name")) {
            if (strlen(val) < sizeof(behavior->default_bucket_name)) {
                strcpy(behavior->default_bucket_name, val);
            }
        } else {
            if (settings.verbose > 1) {
                moxi_log_write("unknown behavior key: %s\n", key);
            }
        }
    }
}

/**
 * Size of array should be arr_size.
 */
proxy_behavior *cproxy_copy_behaviors(int arr_size, proxy_behavior *arr) {
    int arr_size_alloc = arr_size > 0 ? arr_size : 1;
    proxy_behavior *rv = calloc(arr_size_alloc, sizeof(proxy_behavior));
    if (rv != NULL) {
        memcpy(rv, arr, arr_size * sizeof(proxy_behavior));
    }
    return rv;
}

/**
 * Size of x/y array should be x/y_size.
 */
bool cproxy_equal_behaviors(int x_size, proxy_behavior *x,
                            int y_size, proxy_behavior *y) {
    if (x_size != y_size) {
        return false;
    }

    if (x == NULL && y == NULL) {
        return true;
    }

    if (x == NULL || y == NULL) {
        return false;
    }

    for (int i = 0; i < x_size; i++) {
        if (cproxy_equal_behavior(&x[i], &y[i]) == false) {
            if (settings.verbose > 1) {
                moxi_log_write("behaviors not equal (%d)\n", i);
                cproxy_dump_behavior(&x[i], "x", 0);
                cproxy_dump_behavior(&y[i], "y", 0);
            }

            return false;
        }
    }

    return true;
}

bool cproxy_equal_behavior(proxy_behavior *x,
                           proxy_behavior *y) {
    if (x == NULL && y == NULL) {
        return true;
    }

    if (x == NULL || y == NULL) {
        return false;
    }

    return memcmp(x, y, sizeof(proxy_behavior)) == 0;
}

void cproxy_dump_behavior(proxy_behavior *b, char *prefix, int level) {
    cproxy_dump_behavior_ex(b, prefix, level,
                            cproxy_dump_behavior_stderr, NULL);
}

void cproxy_dump_behavior_ex(proxy_behavior *b, char *prefix, int level,
                             void (*dump)(const void *dump_opaque,
                                          const char *prefix,
                                          const char *key,
                                          const char *buf),
                             const void *dump_opaque) {
    assert(b);
    assert(dump);

    char vbuf[8000];

#define vdump(key, vfmt, val) {              \
    snprintf(vbuf, sizeof(vbuf), vfmt, val); \
    dump(dump_opaque, prefix, key, vbuf);    \
}

    if (level >= 2) {
        vdump("cycle", "%u", b->cycle);
    }
    if (level >= 1) {
        vdump("downstream_max", "%u", b->downstream_max);
    }

    vdump("downstream_weight",   "%u", b->downstream_weight);
    vdump("downstream_retry",    "%u", b->downstream_retry);
    vdump("downstream_protocol", "%d", b->downstream_protocol);
    vdump("downstream_timeout", "%ld", // In millisecs.
          (b->downstream_timeout.tv_sec * 1000 +
           b->downstream_timeout.tv_usec / 1000));

    if (level >= 1) {
        vdump("wait_queue_timeout", "%ld", // In millisecs.
              (b->wait_queue_timeout.tv_sec * 1000 +
               b->wait_queue_timeout.tv_usec / 1000));
    }
    if (level >= 1) {
        vdump("front_cache_max", "%u", b->front_cache_max);
    }
    if (level >= 1) {
        vdump("front_cache_lifespan", "%u", b->front_cache_lifespan);
    }
    if (level >= 1) {
        vdump("front_cache_spec", "%s", b->front_cache_spec);
    }
    if (level >= 1) {
        vdump("front_cache_unspec", "%s", b->front_cache_unspec);
    }
    if (level >= 1) {
        vdump("key_stats_max", "%u", b->key_stats_max);
    }
    if (level >= 1) {
        vdump("key_stats_lifespan", "%u", b->key_stats_lifespan);
    }
    if (level >= 1) {
        vdump("key_stats_spec", "%s", b->key_stats_spec);
    }
    if (level >= 1) {
        vdump("key_stats_unspec", "%s", b->key_stats_unspec);
    }
    if (level >= 1) {
        vdump("optimize_set", "%s", b->optimize_set);
    }

    vdump("usr",    "%s", b->usr);
    vdump("host",   "%s", b->host);
    vdump("port",   "%d", b->port);
    vdump("bucket", "%s", b->bucket);

    if (level >= 1) {
        vdump("port_listen", "%d", b->port_listen);
    }

    if (level >= 1) {
        vdump("default_bucket_name", "%s", b->default_bucket_name);
    }
}

void cproxy_dump_behavior_stderr(const void *dump_opaque,
                                 const char *prefix,
                                 const char *key,
                                 const char *val) {
    assert(key);
    assert(val);

    if (prefix == NULL) {
        prefix = "";
    }

    moxi_log_write("%s %s: %s\n",
            prefix, key, val);
}

// ---------------------------------------

/* Time-sensitive callers can call it by hand with this,
 * outside the normal subsecond timer
 */
void msec_set_current_time(void) {
    struct timeval timer;
    gettimeofday(&timer, NULL);
    msec_current_time =
        (timer.tv_sec - process_started) * 1000 + (timer.tv_usec / 1000);
}

void msec_clock_handler(const int fd, const short which, void *arg) {
    // Subsecond resolution timer.
    //
    struct timeval t = { .tv_sec = 0,
                         .tv_usec = msec_cycle * 1000 };

    static bool initialized = false;

    if (initialized) {
        /* only delete the event if it's actually there. */
        evtimer_del(&msec_clockevent);
    } else {
        initialized = true;
    }

    evtimer_set(&msec_clockevent, msec_clock_handler, 0);
    event_base_set(msec_clockevent_base, &msec_clockevent);
    evtimer_add(&msec_clockevent, &t);

    msec_set_current_time();
}

// ---------------------------------------

static char *readfile(char *path) {
    FILE *fp = fopen(path, "r");
    if (fp != NULL) {
        if (fseek(fp, 0, SEEK_END) == 0) {
            long len = ftell(fp);
            if (len > 0 &&
                fseek(fp, 0, SEEK_SET) == 0) {
                char *buf = (char *) malloc(len + 1);
                if (buf != NULL) {
                    if (fread(buf, len, 1, fp) == 1) {
                        fclose(fp);
                        buf[len] = '\0';
                        return buf;
                    }
                    free(buf);
                }
            }
        }
        fclose(fp);
    }
    return NULL;
}
