/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <assert.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"

#define DOWNSTREAM_MAX 10

typedef struct proxy      MC_PROXY;
typedef struct downstream MC_DOWNSTREAM;

struct proxy {
    int   port;
    char *config;
    conn *wait_head;
    conn *wait_tail;
    conn *listen_conn;
    MC_DOWNSTREAM *downstream_busy;
    MC_DOWNSTREAM *downstream_free;
    int            downstream_num;
    int            downstream_max;
};

struct downstream {
    memcached_st    mst;
    MC_DOWNSTREAM  *next; // For busy and free lists.
    conn          **conns;
};

MC_PROXY      *cproxy_create(int proxy_port, char *proxy_sect);
conn          *cproxy_listen(MC_PROXY *p);
MC_DOWNSTREAM *cproxy_add_downstream(MC_PROXY *p);
MC_DOWNSTREAM *cproxy_create_downstream(char *proxy_sect);
int            cproxy_connect_downstream(MC_DOWNSTREAM *d);
void           cproxy_init_conn(conn *c);

conn_funcs cproxy_listen_funcs = {
    cproxy_init_conn,
    add_bytes_read,
    out_string,
    try_read_command,
    reset_cmd_handler,
    complete_nread
};

conn_funcs cproxy_upstream_funcs = {
    cproxy_init_conn,
    add_bytes_read,
    out_string,
    try_read_command,
    reset_cmd_handler,
    complete_nread
};

conn_funcs cproxy_downstream_funcs = {
    cproxy_init_conn,
    add_bytes_read,
    out_string,
    try_read_command,
    reset_cmd_handler,
    complete_nread
};


/** From libmemcached. */
memcached_return memcached_version(memcached_st *ptr);

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

        MC_PROXY *p = cproxy_create(proxy_port, proxy_sect);
        if (p != NULL) {
            if (cproxy_add_downstream(p) != NULL) {
                if (cproxy_connect_downstream(p->downstream_free) == 0) {
                    cproxy_listen(p);
                }
            }
        } else {
            fprintf(stderr, "could not alloc proxy\n");
            exit(EXIT_FAILURE);
        }
    }
    free(buff);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy initted\n");

    return 0;
}

MC_PROXY *cproxy_create(int port, char *config) {
    assert(port > 0);
    assert(config != NULL);

    MC_PROXY *p = (MC_PROXY *) calloc(1, sizeof(MC_PROXY));
    if (p != NULL) {
        p->port   = port;
        p->config = strdup(config);
        p->wait_head = NULL;
        p->wait_tail = NULL;
        p->listen_conn = NULL;
        p->downstream_busy = NULL;
        p->downstream_free = NULL;
        p->downstream_num  = 0;
        p->downstream_max  = DOWNSTREAM_MAX;
    }
    return p;
}

conn *cproxy_listen(MC_PROXY *p) {
    assert(p != NULL);

    if (p->listen_conn == NULL &&
        server_socket(p->port, proxy_upstream_ascii_prot) == 0) {

        if (settings.verbose > 1)
            fprintf(stderr, "cproxy listening on %d to %s\n", p->port, p->config);

        // TODO: Memory leak, need to clean up listen_conn->extra.
        p->listen_conn = listen_conn; // The listen_conn global is set by server_socket().
        p->listen_conn->extra = p;
        p->listen_conn->funcs = &cproxy_listen_funcs;
    }
    return p->listen_conn;
}

MC_DOWNSTREAM *cproxy_add_downstream(MC_PROXY *p) {
    assert(p != NULL);

    if (p->downstream_num < p->downstream_max) {
        MC_DOWNSTREAM *d = cproxy_create_downstream(p->config);
        if (d != NULL) {
            d->next = p->downstream_free;
            p->downstream_free = d;
            p->downstream_num++;
            return d;
        }
    }
    return NULL;
}

MC_DOWNSTREAM *cproxy_create_downstream(char *config) {
    MC_DOWNSTREAM *d = (MC_DOWNSTREAM *) calloc(1, sizeof(MC_DOWNSTREAM));
    if (d != NULL) {
        if (memcached_create(&d->mst) != NULL) {
            memcached_server_st *mservers;

            mservers = memcached_servers_parse(config);
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

            memcached_free(&d->mst);
        }
        free(d);
    }
    return NULL;
}

int cproxy_connect_downstream(MC_DOWNSTREAM *d) {
    assert(d != NULL);

    memcached_return rc = memcached_version(&d->mst); // Connects to downstream servers.
    if (rc == MEMCACHED_SUCCESS) {
        d->conns = (conn **) calloc(memcached_server_count(&d->mst), sizeof(conn *));
        if (d->conns != NULL)
            return 0;
    }

    return 1;
}

void cproxy_init_conn(conn *c) {
    fprintf(stderr, "cproxy_init_conn %d\n", c->sfd);
}
