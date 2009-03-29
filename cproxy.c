/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <assert.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"

/** From libmemcached. */
memcached_return memcached_version(memcached_st *ptr);

// TODO: Move into configurable settings one day.
//
#define DOWNSTREAM_MAX 10

typedef struct proxy      proxy;
typedef struct proxy_td   proxy_td;
typedef struct downstream downstream;

struct proxy {
    int   port;   // Immutable.
    char *config; // Immutable.

    // Number of listening conn's acting as a proxy,
    // where ((proxy *) conn->extra == this).
    //
    int listening;

    proxy_td *thread_data;     // Immutable.
    int       thread_data_num; // Immutable.
};

struct proxy_td {      // Per proxy, per worker-thread struct.
    proxy      *proxy; // Immutable parent pointer.
    conn       *wait_head;
    conn       *wait_tail;
    downstream *downstream_busy;
    downstream *downstream_free;
    int         downstream_num;
    int         downstream_max;
};

struct downstream {
    memcached_st   mst;   // Immutable.
    downstream    *next;  // To track busy and free lists.
    conn         **conns; // Immutable.
};

proxy      *cproxy_create(int proxy_port, char *proxy_sect, int nthreads);
int         cproxy_listen(proxy *p);
proxy_td   *cproxy_find_thread_data(proxy *p, pthread_t thread_id);
void        cproxy_init_upstream_conn(conn *c);
void        cproxy_init_downstream_conn(conn *c);
downstream *cproxy_add_downstream(proxy_td *ptd);
downstream *cproxy_create_downstream(char *proxy_sect);
int         cproxy_connect_downstream(downstream *d);

conn_funcs cproxy_upstream_funcs = {
    cproxy_init_upstream_conn,
    add_bytes_read,
    out_string,
    process_command,
    dispatch_bin_command,
    reset_cmd_handler,
    complete_nread
};

conn_funcs cproxy_downstream_funcs = {
    cproxy_init_downstream_conn,
    add_bytes_read,
    out_string,
    process_command,
    dispatch_bin_command,
    reset_cmd_handler,
    complete_nread
};

/**
 * cfg should look like "local_port=host:port,host:port;local_port=host:port"
 * like "11222=memcached1.foo.net:11211"  This means local port 11222
 * will be a proxy to downstream memcached server running at
 * host memcached1.foo.net on port 11211.
 */
int cproxy_init(const char *cfg, int nthreads) {
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

        proxy *p = cproxy_create(proxy_port, proxy_sect, nthreads);
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

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_init done\n");

    return 0;
}

proxy *cproxy_create(int port, char *config, int nthreads) {
    assert(port > 0);
    assert(config != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_create on port %d, downstream %s\n",
                port, config);

    proxy *p = (proxy *) calloc(1, sizeof(proxy));
    if (p != NULL) {
        p->port   = port;
        p->config = strdup(config);
        p->listening = 0;

        p->thread_data_num = nthreads;
        p->thread_data = (proxy_td *) calloc(p->thread_data_num,
                                             sizeof(proxy_td));
        if (p->thread_data != NULL) {
            // We start at 1, because thread[0] is the main listen/accept
            // thread, and not a true worker thread.
            //
            for (int i = 1; i < p->thread_data_num; i++) {
                proxy_td *ptd = &p->thread_data[i];
                ptd->proxy = p;
                ptd->wait_head = NULL;
                ptd->wait_tail = NULL;
                ptd->downstream_busy = NULL;
                ptd->downstream_free = NULL;
                ptd->downstream_num  = 0;
                ptd->downstream_max  = DOWNSTREAM_MAX;
            }
            return p;
        }
        free(p->config);
    }
    free(p);

    return NULL;
}

int cproxy_listen(proxy *p) {
    assert(p != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_listen on port %d, downstream %s\n",
                p->port, p->config);

    conn *listen_conn_orig = listen_conn;

    if (p->listening == 0 &&
        server_socket(p->port, proxy_upstream_ascii_prot) == 0) {
        assert(listen_conn != NULL);

        // The listen_conn global list is changed by server_socket(),
        // which adds a new listening conn on p->port for each bindable
        // host address.
        //
        // For example, there might be two new listening conn's --
        // one for localhost, another for 127.0.0.1.
        //
        conn *c = listen_conn;
        while (c != NULL &&
               c != listen_conn_orig) {
            if (settings.verbose > 1)
                fprintf(stderr, "<%d cproxy listening on port %d, downstream %s\n",
                        c->sfd, p->port, p->config);

            p->listening++;

            // TODO: Memory leak, need to clean up listen_conn->extra.
            //
            c->extra = p;
            c->funcs = &cproxy_upstream_funcs;
            c = c->next;
        }
    }

    return p->listening;
}

proxy_td *cproxy_find_thread_data(proxy *p, pthread_t thread_id) {
    int i = thread_index(thread_id);

    // 0 is the main listen thread, not a worker thread.
    assert(i > 0);
    assert(i < p->thread_data_num);

    if (i > 0 && i < p->thread_data_num)
        return &p->thread_data[i];

    return NULL;
}

void cproxy_init_upstream_conn(conn *c) {
    assert(c->extra != NULL);

    // We're called once per client/upstream conn early in its
    // lifecycle, so it's a good place to remember the proxy_td.
    //
    proxy *p = c->extra;
    if (p != NULL) {
        if (settings.verbose > 1)
            fprintf(stderr, "<%d cproxy_init_upstream_conn (%s) for %d, downstream %s\n",
                    c->sfd, state_text(c->state), p->port, p->config);

        proxy_td *ptd = cproxy_find_thread_data(p, pthread_self());
        if (ptd != NULL) {
            c->extra = ptd;
            return;
        }
    }

    // TODO: Error, so close the conn?
}

void cproxy_init_downstream_conn(conn *c) {
    assert(c->extra != NULL);

    proxy *p = c->extra;
    if (p != NULL) {
        if (settings.verbose > 1)
            fprintf(stderr, "<%d cproxy_init_downstream_conn (%s) for %d, downstream %s\n",
                    c->sfd, state_text(c->state), p->port, p->config);
    }
}

downstream *cproxy_add_downstream(proxy_td *ptd) {
    assert(ptd != NULL);

    if (ptd->downstream_num < ptd->downstream_max) {
        downstream *d = cproxy_create_downstream(ptd->proxy->config);
        if (d != NULL) {
            d->next = ptd->downstream_free;
            ptd->downstream_free = d;
            ptd->downstream_num++;
            return d;
        }
    }
    return NULL;
}

downstream *cproxy_create_downstream(char *config) {
    downstream *d = (downstream *) calloc(1, sizeof(downstream));
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

int cproxy_connect_downstream(downstream *d) {
    assert(d != NULL);

    memcached_return rc = memcached_version(&d->mst); // Connects to downstream servers.
    if (rc == MEMCACHED_SUCCESS) {
        d->conns = (conn **) calloc(memcached_server_count(&d->mst), sizeof(conn *));
        if (d->conns != NULL)
            return 0;
    }

    return 1;
}

