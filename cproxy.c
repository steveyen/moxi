/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"

typedef struct downstream MC_DOWNSTREAM;
struct downstream {
    memcached_st    mst;
    MC_DOWNSTREAM  *next;
    conn          **conns;
};

typedef struct proxy MC_PROXY;
struct proxy {
    MC_DOWNSTREAM *downstream_busy;
    MC_DOWNSTREAM *downstream_free;
    conn *wait_head;
    conn *wait_tail;
};

MC_PROXY      *cproxy_create(int proxy_port, char *proxy_sect);
MC_DOWNSTREAM *cproxy_create_downstream(char *proxy_sect);

/**
 * cfg should look like "local_port=host:port,host:port;local_port=host:port"
 * like "11222=memcached1.foo.net:11211"  This means local port 11222
 * will be a proxy to downstream memcached server running at
 * host memcached1.foo.net on port 11211.
 */
int cproxy_init(const char *cfg) {
    if (cfg == NULL)
        return 0;

    char *buff;
    char *next;
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

        cproxy_create(proxy_port, proxy_sect);
    }
    free(buff);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy initted\n");

    return 0;
}

MC_PROXY *cproxy_create(int proxy_port, char *proxy_cfg) {
    MC_PROXY *p = (MC_PROXY *) calloc(1, sizeof(MC_PROXY));
    if (proxy_cfg == NULL) {
        fprintf(stderr, "could not alloc MC_PROXY\n");
        exit(EXIT_FAILURE);
    }
    p->wait_head = NULL;
    p->wait_tail = NULL;
    p->downstream_busy = NULL;
    p->downstream_free = NULL;

    p->downstream_free = cproxy_create_downstream(proxy_cfg);
    if (p->downstream_free != NULL) {
        if (server_socket(proxy_port, proxy_upstream_ascii_prot) == 0) {
            // TODO: Memory leak, need to clean up conn->extra.
            listen_conn->extra = p; // The listen_conn global is set by server_socket().
            return p;
        } else {
            fprintf(stderr, "failed to listen as proxy on TCP port %d\n", proxy_port);
            if (errno != 0)
                perror("tcp listen");
            exit(EX_OSERR);
        }
    }
    return NULL;
}

MC_DOWNSTREAM *cproxy_create_downstream(char *proxy_cfg) {
    MC_DOWNSTREAM *d = (MC_DOWNSTREAM *) calloc(1, sizeof(MC_DOWNSTREAM));
    if (d != NULL) {
        if (memcached_create(&d->mst) != NULL) {
            memcached_server_st *mservers;

            mservers = memcached_servers_parse(proxy_cfg);
            if (mservers != NULL) {
                memcached_server_push(&d->mst, mservers);
                memcached_server_list_free(mservers);
                mservers = NULL;

                d->conns = (conn **) calloc(memcached_server_count(&d->mst), sizeof(conn *));
                if (d->conns != NULL)
                    return d;
            }
            if (mservers != NULL)
                memcached_server_list_free(mservers);
        }
        free(d);
    }
    return NULL;
}


