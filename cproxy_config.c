/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"

int cproxy_init_string(const char *cfg, int nthreads,
                       int downstream_max,
                       struct timeval downstream_timeout);

int cproxy_init_agent(const char *cfg, int nthreads,
                      int downstream_max,
                      struct timeval downstream_timeout);

int cproxy_init(const char *cfg, int nthreads,
                int downstream_max,
                struct timeval downstream_timeout) {
    assert(nthreads > 1); // Main + at least one worker.
    assert(nthreads == settings.num_threads);
    assert(downstream_max > 0);

    if (cfg == NULL ||
        strlen(cfg) <= 0)
        return 0;

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_init %s\n", cfg);

    if (strchr(cfg, '@') == NULL) // Not jid format.
        return cproxy_init_string(cfg, nthreads,
                                  downstream_max,
                                  downstream_timeout);

#ifdef BUILD_MEMAGENT
    return cproxy_init_agent(cfg, nthreads,
                             downstream_max,
                             downstream_timeout);
#else
    return 1;
#endif
}

int cproxy_init_string(const char *cfg,
                       int nthreads,
                       int downstream_max,
                       struct timeval downstream_timeout) {
    /* cfg should look like "local_port=host:port,host:port;local_port=host:port"
     * like "11222=memcached1.foo.net:11211"  This means local port 11222
     * will be a proxy to downstream memcached server running at
     * host memcached1.foo.net on port 11211.
     */
    assert(nthreads > 1); // Main + at least one worker.
    assert(nthreads == settings.num_threads);
    assert(downstream_max > 0);

    if (cfg == NULL ||
        strlen(cfg) <= 0)
        return 0;

    char *buff;
    char *next;
    char *proxy_name = "default";
    char *proxy_sect;
    char *proxy_port_str;
    int   proxy_port;

    buff = strdup(cfg);
    next = buff;
    while (next != NULL) {
        proxy_sect = strsep(&next, ";");

        proxy_port_str = strsep(&proxy_sect, "=");
        if (proxy_sect == NULL) {
            fprintf(stderr, "bad cproxy config, missing =\n");
            exit(EXIT_FAILURE);
        }
        proxy_port = atoi(proxy_port_str);
        if (proxy_port <= 0) {
            fprintf(stderr, "bad cproxy config, bad proxy port\n");
            exit(EXIT_FAILURE);
        }

        proxy *p = cproxy_create(proxy_name,
                                 proxy_port,
                                 proxy_sect,
                                 0, // config_ver.
                                 downstream_timeout,
                                 nthreads,
                                 downstream_max);
        if (p != NULL) {
            int n = cproxy_listen(p);
            if (n > 0) {
                if (settings.verbose > 1)
                    fprintf(stderr, "cproxy listening on %d conns\n", n);
            }
        } else {
            fprintf(stderr, "could not alloc proxy\n");
            exit(EXIT_FAILURE);
        }
    }
    free(buff);

    return 0;
}

