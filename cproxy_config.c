/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <math.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"

// From libmemcached.
//
uint32_t murmur_hash(const char *key, size_t length);

// Local declarations.
//
volatile uint32_t  msec_current_time = 0;
int                msec_cycle = 200;
struct event       msec_clockevent;
struct event_base *msec_clockevent_base = NULL;

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
    .downstream_max = 1,
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
    .host = {0},
    .port = 0,
    .bucket = {0},
    .usr = {0},
    .pwd = {0}
};

// Key may be zero or space terminated.
//
size_t skey_len(const char *key) {
    assert(key);

    char *x = (char *) key;
    while (*x != ' ' && *x != '\0')
        x++;

    return x - key;
}

guint skey_hash(gconstpointer v) {
    assert(v);

    const char *key = v;
    size_t      len = skey_len(key);

    return murmur_hash(key, len);
}

gboolean skey_equal(gconstpointer v1, gconstpointer v2) {
    assert(v1);
    assert(v2);

    const char *k1 = v1;
    const char *k2 = v2;

    size_t n1 = skey_len(k1);
    size_t n2 = skey_len(k2);

    return (n1 == n2 && strncmp(k1, k2, n1) == 0);
}

void helper_g_free(gpointer data) {
    free(data);
}

// ---------------------------------------

int cproxy_init(char *cfg_str,
                char *behavior_str,
                int nthreads,
                struct event_base *main_base) {
    assert(nthreads > 1); // Main + at least one worker.
    assert(nthreads == settings.num_threads);

    if (cfg_str == NULL ||
        strlen(cfg_str) <= 0)
        return 0;

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_init (%s)\n", cfg_str);

    cproxy_init_a2a();
    cproxy_init_a2b();

    if (behavior_str == NULL)
        behavior_str = "";

    proxy_behavior behavior =
        cproxy_parse_behavior(behavior_str,
                              behavior_default_g);

    if (behavior.cycle > 0)
        msec_cycle = behavior.cycle;

    msec_clockevent_base = main_base;
    msec_clock_handler(0, 0, NULL);

    if (strchr(cfg_str, '@') == NULL) // Not jid format.
        return cproxy_init_string(cfg_str,
                                  behavior,
                                  nthreads);

#ifdef HAVE_CONFLATE_H
    return cproxy_init_agent(cfg_str,
                             behavior,
                             nthreads);
#else
    fprintf(stderr, "missing conflate\n");
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
        strlen(cfg_str) <= 0)
        return 0;

    char *buff;
    char *next;
    char *proxy_name = "default";
    char *proxy_sect;
    char *proxy_port_str;
    int   proxy_port;

    if (settings.verbose > 1) {
        cproxy_dump_behavior(&behavior, "init_string");
    }

    buff = strdup(cfg_str);
    next = buff;
    while (next != NULL) {
        proxy_sect = strsep(&next, ";");

        proxy_port_str = strsep(&proxy_sect, "=");
        if (proxy_sect == NULL) {
            fprintf(stderr, "bad moxi config, missing =\n");
            exit(EXIT_FAILURE);
        }
        proxy_port = atoi(proxy_port_str);
        if (proxy_port <= 0) {
            fprintf(stderr, "missing proxy port\n");
            exit(EXIT_FAILURE);
        }

        int behaviors_num = 1; // Number of servers.
        for (char *x = proxy_sect; *x != '\0'; x++)
            if (*x == ',')
                behaviors_num++;

        proxy_behavior *behaviors =
            calloc(behaviors_num, sizeof(proxy_behavior));

        if (behaviors != NULL) {
            for (int i = 0; i < behaviors_num; i++) {
                behaviors[i] = behavior;
            }

            proxy *p = cproxy_create(proxy_name,
                                     proxy_port,
                                     proxy_sect,
                                     0, // config_ver.
                                     behavior,
                                     behaviors_num,
                                     behaviors,
                                     nthreads);
            if (p != NULL) {
                int n = cproxy_listen(p);
                if (n > 0) {
                    if (settings.verbose > 1)
                        fprintf(stderr,
                                "moxi listening on %d with %d conns\n",
                                proxy_port, n);
                } else {
                    fprintf(stderr,
                            "moxi error -- port %d unavailable?\n",
                            proxy_port);
                    exit(EXIT_FAILURE);
                }
            } else {
                fprintf(stderr, "could not alloc proxy\n");
                exit(EXIT_FAILURE);
            }

            free(behaviors);
        } else {
            fprintf(stderr, "could not alloc behaviors\n");
            exit(EXIT_FAILURE);
        }
    }

    free(buff);

    return 0;
}

proxy_behavior cproxy_parse_behavior(char          *behavior_str,
                                     proxy_behavior behavior_default) {
    // These are the default proxy behaviors.
    //
    struct proxy_behavior behavior = behavior_default;

    if (behavior_str == NULL ||
        strlen(behavior_str) <= 0)
        return behavior;

    // Parse the key-value behavior_str, to override the defaults.
    //
    char *buff = strdup(behavior_str);
    char *next = buff;

    while (next != NULL) {
        char *key_val = strsep(&next, ",");
        if (key_val != NULL) {
            cproxy_parse_behavior_key_val_str(key_val, &behavior);
        }
    }

    free(buff);

    assert(IS_PROXY(behavior.downstream_protocol));

    return behavior;
}

void cproxy_parse_behavior_key_val_str(char *key_val,
                                       proxy_behavior *behavior) {
    assert(behavior != NULL);

    if (key_val != NULL) {
        char *key = strsep(&key_val, "=");
        char *val = key_val;
        cproxy_parse_behavior_key_val(key, val, behavior);
    }
}

void cproxy_parse_behavior_key_val(char *key,
                                   char *val,
                                   proxy_behavior *behavior) {
    assert(behavior != NULL);

    if (key != NULL &&
        val != NULL) {
        if (strcmp(key, "cycle") == 0) {
            behavior->cycle = strtol(val, NULL, 10);
            assert(behavior->cycle > 0);
        } else if (strcmp(key, "downstream_max") == 0) {
            behavior->downstream_max = strtol(val, NULL, 10);
            assert(behavior->downstream_max >= 0);
        } else if (strcmp(key, "weight") == 0 ||
                   strcmp(key, "downstream_weight") == 0) {
            behavior->downstream_weight = strtol(val, NULL, 10);
            assert(behavior->downstream_max >= 0);
        } else if (strcmp(key, "retry") == 0 ||
                   strcmp(key, "downstream_retry") == 0) {
            behavior->downstream_retry = strtol(val, NULL, 10);
            assert(behavior->downstream_retry >= 0);
        } else if (strcmp(key, "protocol") == 0 ||
                   strcmp(key, "downstream_protocol") == 0) {
            if (strcmp(val, "ascii") == 0)
                behavior->downstream_protocol =
                    proxy_downstream_ascii_prot;
            else if (strcmp(val, "binary") == 0)
                behavior->downstream_protocol =
                    proxy_downstream_binary_prot;
            else {
                if (settings.verbose > 1)
                    fprintf(stderr, "unknown behavior prot: %s\n", val);
            }
        } else if (strcmp(key, "timeout") == 0 ||
                   strcmp(key, "downstream_timeout") == 0) {
            int ms = strtol(val, NULL, 10);
            behavior->downstream_timeout.tv_sec  = floor(ms / 1000.0);
            behavior->downstream_timeout.tv_usec = (ms % 1000) * 1000;
        } else if (strcmp(key, "wait_queue_timeout") == 0) {
            int ms = strtol(val, NULL, 10);
            behavior->wait_queue_timeout.tv_sec  = floor(ms / 1000.0);
            behavior->wait_queue_timeout.tv_usec = (ms % 1000) * 1000;
        } else if (strcmp(key, "usr") == 0) {
            if (strlen(val) < sizeof(behavior->usr) + 1) {
                strcpy(behavior->usr, val);
            }
        } else if (strcmp(key, "pwd") == 0) {
            if (strlen(val) < sizeof(behavior->pwd) + 1) {
                strcpy(behavior->pwd, val);
            }
        } else if (strcmp(key, "host") == 0) {
            if (strlen(val) < sizeof(behavior->host) + 1) {
                strcpy(behavior->host, val);
            }
        } else if (strcmp(key, "port") == 0) {
            behavior->port = strtol(val, NULL, 10);
        } else if (strcmp(key, "bucket") == 0) {
            if (strlen(val) < sizeof(behavior->bucket) + 1) {
                strcpy(behavior->bucket, val);
            }
        } else {
            if (settings.verbose > 1)
                fprintf(stderr, "unknown behavior key: %s\n", key);
        }
    }
}

proxy_behavior *cproxy_copy_behaviors(int arr_size, proxy_behavior *arr) {
    proxy_behavior *rv = calloc(arr_size, sizeof(proxy_behavior));
    if (rv != NULL)
        memcpy(rv, arr, arr_size * sizeof(proxy_behavior));
    return rv;
}

bool cproxy_equal_behaviors(int x_size, proxy_behavior *x,
                            int y_size, proxy_behavior *y) {
    if (x_size != y_size)
        return false;

    for (int i = 0; i < x_size; i++) {
        if (cproxy_equal_behavior(&x[i], &y[i]) == false) {
            if (settings.verbose > 1) {
                fprintf(stderr, "behaviors not equal (%d)\n", i);
                cproxy_dump_behavior(&x[i], "x");
                cproxy_dump_behavior(&y[i], "y");
            }

            return false;
        }
    }

    return true;
}

bool cproxy_equal_behavior(proxy_behavior *x,
                           proxy_behavior *y) {
    if (x == NULL && y == NULL)
        return true;

    if (x == NULL || y == NULL)
        return false;

    return memcmp(x, y, sizeof(proxy_behavior)) == 0;
}

void cproxy_dump_behavior(proxy_behavior *b, char *prefix) {
    if (prefix == NULL)
        prefix = "";
    fprintf(stderr, "%s downstream_max: %d\n",
            prefix, b->downstream_max);
    fprintf(stderr, "%s downstream_weight: %d\n",
            prefix, b->downstream_weight);
    fprintf(stderr, "%s downstream_retry: %d\n",
            prefix, b->downstream_retry);
    fprintf(stderr, "%s downstream_protocol: %d\n",
            prefix, b->downstream_protocol);
    fprintf(stderr, "%s downstream_timeout: %ld\n", // In millisecs.
            prefix,
            b->downstream_timeout.tv_sec * 1000 +
            b->downstream_timeout.tv_usec / 1000);
    fprintf(stderr, "%s usr: %s\n",
            prefix, b->usr);
    fprintf(stderr, "%s host: %s\n",
            prefix, b->host);
    fprintf(stderr, "%s port: %d\n",
            prefix, b->port);
    fprintf(stderr, "%s bucket: %s\n",
            prefix, b->bucket);
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

