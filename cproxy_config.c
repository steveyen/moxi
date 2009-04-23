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
                       int downstream_max);

int cproxy_init_agent(char *jid, char *jpw,
                      char *config, char *host,
                      int nthreads, int downstream_max);

int cproxy_init(const char *cfg, int nthreads,
                int downstream_max) {
    assert(nthreads > 1); // Main + at least one worker.
    assert(nthreads == settings.num_threads);
    assert(downstream_max > 0);

    if (cfg == NULL ||
        strlen(cfg) <= 0)
        return 0;

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_init %s\n", cfg);

    if (strchr(cfg, '@') == NULL) // Not jid format.
        return cproxy_init_string(cfg, nthreads, downstream_max);

    char *buff = strdup(cfg);
    char *next = buff;

    // Each sec (or section) looks like...
    //
    //   apikey=jidname@jhostname%jpassword,config=config,host=host
    //
    // Only the apikey is needed.
    //
    int rv = 0;

    while (next != NULL) {
        char *jid    = NULL;
        char *jpw    = NULL;
        char *config = "/tmp/memscale.cfg"; // TODO: Revisit.
        char *host   = "localhost";         // TODO: Revisit.

        char *cur = strsep(&next, ";");
        while (cur != NULL) {
            char *key_val = strsep(&cur, ",");
            if (key_val != NULL) {
                char *key = strsep(&key_val, "=");
                char *val = key_val;

                if (settings.verbose > 1)
                    fprintf(stderr, "cproxy_init kv %s %s\n", key, val);

                if (val != NULL) {
                    if (strcmp(key, "apikey") == 0) {
                        jid = strsep(&val, "%");
                        jpw = val;

                        if (settings.verbose > 1)
                            fprintf(stderr, "cproxy_init apikey %s %s\n",
                                    jid, jpw);
                    }
                    if (strcmp(key, "config") == 0)
                        config = val;
                    if (strcmp(key, "host") == 0)
                        host = val;
                }
            }
        }

        // TODO: Better config/init error handling.
        //
        if (jid == NULL) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init missing jid\n");
        } else if (jpw == NULL) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init missing jpw\n");
        } else if (config == NULL) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init missing config\n");
        } else if (host == NULL) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init missing host\n");
        } else {
            if (cproxy_init_agent(jid, jpw, config, host,
                                  nthreads, downstream_max) == 0)
                rv++;
        }
    }

    free(buff);

    return rv;
}

int cproxy_init_string(const char *cfg, int nthreads,
                       int downstream_max) {
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

        proxy *p = cproxy_create(proxy_name, proxy_port, proxy_sect, 0,
                                 nthreads, downstream_max);
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

