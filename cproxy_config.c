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

int cproxy_init_string(char *cfg_str,
                       char *behavior_str,
                       proxy_behavior behavior);

int cproxy_init_agent(char *cfg_str,
                      char *behavior_str,
                      proxy_behavior behavior);

proxy_behavior behavior_default_g = {
    .nthreads = 0,
    .downstream_max = 1,
    .downstream_prot = proxy_downstream_ascii_prot,
    .downstream_timeout = {
        .tv_sec  = 0,
        .tv_usec = 0
    },
    .wait_queue_timeout = {
        .tv_sec  = 0,
        .tv_usec = 0
    },
    .sasl_mech = "PLAIN",
    .sasl_auth = ""
};

int cproxy_init(char *cfg_str,
                char *behavior_str,
                int nthreads) {
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

    behavior.nthreads = nthreads;

    if (strchr(cfg_str, '@') == NULL) // Not jid format.
        return cproxy_init_string(cfg_str,
                                  behavior_str,
                                  behavior);

#ifdef HAVE_CONFLATE_H
    return cproxy_init_agent(cfg_str,
                             behavior_str,
                             behavior);
#else
    fprintf(stderr, "missing conflate\n");
    exit(EXIT_FAILURE);
    return 1;
#endif
}

int cproxy_init_string(char *cfg_str,
                       char *behavior_str,
                       proxy_behavior behavior) {
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

        char *behaviors_str =
            calloc(behaviors_num, strlen(behavior_str) + 4);
        proxy_behavior *behaviors =
            calloc(behaviors_num, sizeof(proxy_behavior));

        if (behaviors_str != NULL &&
            behaviors != NULL) {
            for (int i = 0; i < behaviors_num; i++) {
                char *c = behaviors_str + strlen(behaviors_str);
                if (c != behaviors_str)
                    *c++ = ';';
                strcpy(c, behavior_str);

                behaviors[i] = behavior;
            }

            proxy *p = cproxy_create(proxy_name,
                                     proxy_port,
                                     proxy_sect,
                                     0, // config_ver.
                                     behaviors_str,
                                     behaviors_num,
                                     behaviors);
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

            free(behaviors_str);
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
            char *key = strsep(&key_val, "=");
            char *val = key_val;

            if (key != NULL &&
                val != NULL) {
                if (strcmp(key, "downstream_max") == 0) {
                    behavior.downstream_max = strtol(val, NULL, 10);
                    assert(behavior.downstream_max > 0);
                } else if (strcmp(key, "downstream_prot") == 0) {
                    if (strcmp(val, "ascii") == 0)
                        behavior.downstream_prot =
                            proxy_downstream_ascii_prot;
                    else if (strcmp(val, "binary") == 0)
                        behavior.downstream_prot =
                            proxy_downstream_binary_prot;
                    else {
                        // TODO: Error in behavior config string.
                    }
                } else if (strcmp(key, "downstream_timeout") == 0) {
                    int ms = strtol(val, NULL, 10);
                    behavior.downstream_timeout.tv_sec  = floor(ms / 1000.0);
                    behavior.downstream_timeout.tv_usec = (ms % 1000) * 1000;
                } else if (strcmp(key, "wait_queue_timeout") == 0) {
                    int ms = strtol(val, NULL, 10);
                    behavior.wait_queue_timeout.tv_sec  = floor(ms / 1000.0);
                    behavior.wait_queue_timeout.tv_usec = (ms % 1000) * 1000;
                } else {
                    // TODO: Error in behavior config string.
                }
            }
        }
    }

    free(buff);

    assert(IS_PROXY(behavior.downstream_prot));

    return behavior;
}

proxy_behavior *cproxy_copy_behaviors(int arr_size, proxy_behavior *arr) {
    proxy_behavior *rv = calloc(arr_size, sizeof(proxy_behavior));
    if (rv != NULL)
        memcpy(rv, arr, arr_size * sizeof(proxy_behavior));
    return rv;
}

