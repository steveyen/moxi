/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "memagent.h"
#include "cproxy.h"
#include "work.h"

/** From libmemcached. */
memcached_return memcached_version(memcached_st *ptr);
memcached_return memcached_connect(memcached_server_st *ptr);
uint32_t memcached_generate_hash(memcached_st *ptr, const char *key,
                                 size_t key_length);
void memcached_quit_server(memcached_server_st *ptr, uint8_t io_death);

#define NOT_CAS -1

typedef struct proxy      proxy;
typedef struct proxy_td   proxy_td;
typedef struct proxy_main proxy_main;
typedef struct downstream downstream;

struct proxy_main {
    agent_config_t config; // Immutable.

    work_queue work_queue;

    int nthreads;
    int default_downstream_max;

    proxy *proxy_head; // Start of proxy list.
};

struct proxy {
    int   port;       // Immutable.
    char *name;       // Mutable, covered by proxy_lock, mostly for debugging.
    char *config;     // Mutable, covered by proxy_lock, mem owned by proxy.
    int   config_ver; // Mutable, covered by proxy_lock, incremented
                      // whenever config changes.

    pthread_mutex_t proxy_lock;

    // Immutable number of listening conn's acting as a proxy,
    // where ((proxy *) conn->extra == this).
    //
    int listening;

    proxy_td *thread_data;     // Immutable.
    int       thread_data_num; // Immutable.

    proxy *next;
};

struct proxy_td { // Per proxy, per worker-thread data struct.
    proxy *proxy; // Immutable parent pointer.

    work_queue work_queue;

    // Upstream conns that are paused, waiting for
    // an available, released downstream.
    //
    conn *waiting_for_downstream_head;
    conn *waiting_for_downstream_tail;

    downstream *downstream_reserved; // Downstreams assigned to upstreams.
    downstream *downstream_released; // Downstreams not assigned to upstreams.
    int         downstream_num;      // Number downstreams created.
    int         downstream_max;      // Max downstream concurrency number.

    int num_upstream; // # of upstreams conns where conn->extra == this.
};

struct downstream {
    proxy_td     *ptd;        // Immutable parent pointer.
    char         *config;     // Immutable, mem owned by downstream.
    int           config_ver; // Immutable, snapshot of proxy->config_ver.
    memcached_st  mst;        // Immutable, from libmemcached.

    downstream *next;         // To track reserved/free lists.

    conn **downstream_conns;  // Wraps the fd's of mst with conns.
    int    downstream_used;   // Number of in-use downstream conns, might
                              // be >1 during scatter-gather commands.
    conn  *upstream_conn;     // Non-NULL when downstream is reserved.
    char  *upstream_suffix;   // Last bit to write when downstreams are done.
};

proxy    *cproxy_create(char *name, int port,
                        char *config, int nthreads, int downstream_max);
int       cproxy_listen(proxy *p);
proxy_td *cproxy_find_thread_data(proxy *p, pthread_t thread_id);
void      cproxy_init_upstream_conn(conn *c);
void      cproxy_init_downstream_conn(conn *c);
void      cproxy_on_close_upstream_conn(conn *c);
void      cproxy_on_close_downstream_conn(conn *c);
void      cproxy_on_pause_downstream_conn(conn *c);

void        cproxy_add_downstream(proxy_td *ptd);
void        cproxy_free_downstream(downstream *d);
downstream *cproxy_create_downstream(char *config, int config_ver);
downstream *cproxy_reserve_downstream(proxy_td *ptd);
bool        cproxy_release_downstream(downstream *d, bool force);
void        cproxy_release_downstream_conn(downstream *d, conn *c);
bool        cproxy_check_downstream_config(downstream *d);

int   cproxy_connect_downstream(downstream *d, LIBEVENT_THREAD *thread);
void  cproxy_wait_for_downstream(proxy_td *ptd, conn *c);
void  cproxy_assign_downstream(proxy_td *ptd);
bool  cproxy_forward_downstream(downstream *d);
bool  cproxy_forward_multiget_downstream(downstream *d, char *command, conn *uc);
bool  cproxy_forward_simple_downstream(downstream *d, char *command, conn *uc);
bool  cproxy_forward_item_downstream(downstream *d, short cmd, item *it, conn *uc);
bool  cproxy_broadcast_downstream(downstream *d, char *command, conn *uc,
                                  char *suffix);
void  cproxy_pause_upstream_for_downstream(proxy_td *ptd, conn *upstream);
conn *cproxy_find_downstream_conn(downstream *d, char *key, int key_length);
bool  cproxy_prep_conn_for_write(conn *c);
int   cproxy_server_index(downstream *d, char *key, size_t key_length);

bool cproxy_dettach_if_noreply(downstream *d, conn *uc);

void cproxy_reset_upstream(conn *uc);

void cproxy_process_upstream_ascii(conn *c, char *line);
void cproxy_process_upstream_ascii_nread(conn *c);

void cproxy_process_downstream_ascii(conn *c, char *line);
void cproxy_process_downstream_ascii_nread(conn *c);

rel_time_t cproxy_realtime(const time_t exptime);

void cproxy_close_conn(conn *c);

bool  add_conn_item(conn *c, item *it);
char *add_conn_suffix(conn *c);

downstream *downstream_list_remove(downstream *head, downstream *d);

size_t scan_tokens(char *command, token_t *tokens, const size_t max_tokens);

char *nread_text(short x);

void on_memagent_new_serverlist(void *userdata, memcached_server_list_t **lists);
void on_memagent_get_stats(void *userdata, void *opaque, agent_add_stat add_stat);

void cproxy_on_new_serverlist(void *data0, void *data1);

conn_funcs cproxy_listen_funcs = {
    cproxy_init_upstream_conn,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};

conn_funcs cproxy_upstream_funcs = {
    NULL,
    cproxy_on_close_upstream_conn,
    cproxy_process_upstream_ascii,
    NULL,
    cproxy_process_upstream_ascii_nread,
    NULL,
    cproxy_realtime
};

conn_funcs cproxy_downstream_funcs = {
    cproxy_init_downstream_conn,
    cproxy_on_close_downstream_conn,
    cproxy_process_downstream_ascii,
    NULL,
    cproxy_process_downstream_ascii_nread,
    cproxy_on_pause_downstream_conn,
    cproxy_realtime
};

void cproxy_on_new_serverlist(void *data0, void *data1) {
    proxy_main *m = data0;
    assert(m);
    memcached_server_list_t *list = data1;
    assert(list);
    assert(list->servers);

    // Create a config string that libmemcached likes,
    // first by counting up buffer size needed.
    //
    int j;
    int n = 0;
    for (j = 0; list->servers[j]; j++) {
        memcached_server_t *server = list->servers[j];
        n = n + strlen(server->host) + 50;
    }

    char *cfg = calloc(n, 1);

    for (int j = 0; list->servers[j]; j++) {
        memcached_server_t *server = list->servers[j];
        char *cur = cfg + strlen(cfg);
        if (j == 0)
            sprintf(cur, "%s:%u", server->host, server->port);
        else
            sprintf(cur, ",%s:%u", server->host, server->port);
    }

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy main has new cfg: %s (bound to %d)\n",
                cfg, list->binding);

    // See if we've already got a proxy running on the port,
    // and create one if needed.
    //
    // TODO: Need to shutdown old proxies.
    //
    proxy *p = m->proxy_head;
    while (p != NULL &&
           p->port != list->binding)
        p = p->next;

    if (p == NULL) {
        if (settings.verbose > 1)
            fprintf(stderr, "cproxy main creating new proxy for %s on %d\n",
                    cfg, list->binding);

        p = cproxy_create(list->name, list->binding, cfg,
                          m->nthreads, m->default_downstream_max);
        if (p != NULL) {
            p->next = m->proxy_head;
            m->proxy_head = p;

            int n = cproxy_listen(p);
            if (n > 0) {
                if (settings.verbose > 1)
                    fprintf(stderr, "cproxy listening on %d conns\n", n);
            } else {
                if (settings.verbose > 1)
                    fprintf(stderr, "cproxy_listen failed on %u\n", p->port);
            }
        }
    } else {
        if (settings.verbose > 1)
            fprintf(stderr, "cproxy main handling config change %u\n", p->port);

        pthread_mutex_lock(&p->proxy_lock);

        if (p->name != NULL && list->name != NULL &&
            strcmp(p->name, list->name) != 0) {
            if (p->name != NULL) {
                free(p->name);
                p->name = NULL;
            }
        }
        if (p->name == NULL &&
            list->name != NULL)
            p->name = strdup(list->name);

        if (strcmp(p->config, cfg) != 0) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy main config changed from %s to %s\n",
                        p->config, cfg);

            free(p->config);
            p->config = cfg;
            p->config_ver++;
            cfg = NULL;
        }

        pthread_mutex_unlock(&p->proxy_lock);
    }

    if (cfg != NULL)
        free(cfg);

    free_server_list(list);
}

void on_memagent_new_serverlist(void *userdata, memcached_server_list_t **lists) {
    assert(lists != NULL);

    proxy_main *m = userdata;
    assert(m != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "on_memagent_new_serverlist\n");

    bool err = false;

    for (int i = 0; lists[i] && !err; i++) {
        memcached_server_list_t *list = lists[i];
        memcached_server_list_t *list_copy;

        list_copy = copy_server_list(list);
        if (list_copy != NULL) {
            err = !work_send(&m->work_queue, cproxy_on_new_serverlist,
                             m, list_copy);
        } else
            err = true;

        if (err) {
            if (list_copy != NULL)
                free_server_list(list_copy);
        }
    }
}

void on_memagent_get_stats(void *userdata, void *opaque, agent_add_stat add_stat) {
    add_stat(opaque, "stat1", "val1");
    add_stat(opaque, "stat2", "val2");
    add_stat(opaque, NULL, NULL);
}

int cproxy_init(const char *cfg, int nthreads, int default_downstream_max) {
    assert(nthreads == settings.num_threads);
    assert(default_downstream_max > 0);

    proxy_main *m = calloc(1, sizeof(proxy_main));
    if (m != NULL) {
        LIBEVENT_THREAD *mthread = thread_by_index(0);
        assert(mthread != NULL);
        assert(mthread->base != NULL);

        m->proxy_head             = NULL;
        m->nthreads               = nthreads;
        m->default_downstream_max = default_downstream_max;

        if (work_queue_init(&m->work_queue, mthread->base)) {
            // Different jid's for production, staging, etc.
            m->config.jid = "customer@stevenmb.local";
            m->config.pass = "password";
            m->config.host = "localhost"; // TODO: XMPP server host, for dev.
            m->config.software = "memscale";
            m->config.version = "0.1";
            m->config.save_path = "/tmp/memscale.db";
            m->config.userdata = m;
            m->config.new_serverlist = on_memagent_new_serverlist;
            m->config.get_stats = on_memagent_get_stats;

            if (start_agent(m->config)) {
                if (settings.verbose > 1)
                    fprintf(stderr, "cproxy_init done\n");

                return 0;
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy could not start memagent\n");

    return 1;
}

proxy *cproxy_create(char *name, int port, char *config,
                     int nthreads, int downstream_max) {
    assert(name != NULL);
    assert(port > 0);
    assert(config != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_create on port %d, downstream %s\n",
                port, config);

    proxy *p = (proxy *) calloc(1, sizeof(proxy));
    if (p != NULL) {
        p->name       = strdup(name);
        p->port       = port;
        p->config     = strdup(config);
        p->config_ver = 0;
        p->listening  = 0;

        pthread_mutex_init(&p->proxy_lock, NULL);

        p->thread_data_num = nthreads;
        p->thread_data = (proxy_td *) calloc(p->thread_data_num,
                                             sizeof(proxy_td));
        if (p->thread_data != NULL) {
            // We start at 1, because thread[0] is the main listen/accept
            // thread, and not a true worker thread.  Too lazy to save
            // the wasted thread[0] slot memory.
            //
            int i;

            for (i = 1; i < p->thread_data_num; i++) {
                proxy_td *ptd = &p->thread_data[i];
                ptd->proxy = p;
                ptd->waiting_for_downstream_head = NULL;
                ptd->waiting_for_downstream_tail = NULL;
                ptd->downstream_reserved = NULL;
                ptd->downstream_released = NULL;
                ptd->downstream_num = 0;
                ptd->downstream_max = downstream_max;
                ptd->num_upstream = 0;

                LIBEVENT_THREAD *t = thread_by_index(i);
                if (t == NULL ||
                    t->base == NULL ||
                    !work_queue_init(&ptd->work_queue, t->base)) {
                    break;
                }
            }

            if (i >= p->thread_data_num)
                return p;

            free(p->thread_data);
        }

        free(p->name);
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

    // Idempotent, remembers if it already created listening socket(s).
    //
    if (p->listening == 0 &&
        server_socket(p->port, proxy_upstream_ascii_prot) == 0) {
        assert(listen_conn != NULL);

        // The listen_conn global list is changed by server_socket(),
        // which adds a new listening conn on p->port for each bindable
        // host address.
        //
        // For example, after the call to server_socket(), there
        // might be two new listening conn's -- one for localhost,
        // another for 127.0.0.1.
        //
        conn *c = listen_conn;
        while (c != NULL &&
               c != listen_conn_orig) {
            if (settings.verbose > 1)
                fprintf(stderr,
                        "<%d cproxy listening on port %d, downstream %s\n",
                        c->sfd, p->port, p->config);

            p->listening++;

            // TODO: Listening conn's never seem to close,
            //       but need to handle cleanup if they do,
            //       such as if we handle graceful shutdown one day.
            //
            c->extra = p;
            c->funcs = &cproxy_listen_funcs;
            c = c->next;
        }
    }

    return p->listening;
}

proxy_td *cproxy_find_thread_data(proxy *p, pthread_t thread_id) {
    if (p != NULL) {
        int i = thread_index(thread_id);

        // 0 is the main listen thread, not a worker thread.
        assert(i > 0);
        assert(i < p->thread_data_num);

        if (i > 0 && i < p->thread_data_num)
            return &p->thread_data[i];
    }

    return NULL;
}

void cproxy_init_upstream_conn(conn *c) {
    assert(c != NULL);

    // We're called once per client/upstream conn early in its
    // lifecycle, so it's a good place to remember the proxy_td.
    //
    proxy *p = c->extra;
    assert(p != NULL);

    if (settings.verbose > 1)
        fprintf(stderr,
                "<%d cproxy_init_upstream_conn (%s)"
                " for %d, downstream %s\n",
                c->sfd, state_text(c->state), p->port, p->config);

    proxy_td *ptd = cproxy_find_thread_data(p, pthread_self());
    assert(ptd != NULL);

    ptd->num_upstream++;
    c->extra = ptd;
    c->funcs = &cproxy_upstream_funcs;
}

void cproxy_init_downstream_conn(conn *c) {
    assert(c->extra != NULL);

    downstream *d = c->extra;
    if (d != NULL) {
        if (settings.verbose > 1)
            fprintf(stderr,
                    "<%d cproxy_init_downstream_conn (%s)"
                    " to downstream %s\n",
                    c->sfd, state_text(c->state), d->ptd->proxy->config);
    }
}

void cproxy_on_close_upstream_conn(conn *c) {
    assert(c != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d cproxy_on_close_upstream_conn\n", c->sfd);

    proxy_td *ptd = c->extra;
    assert(ptd != NULL);
    c->extra = NULL;

    ptd->num_upstream--;
    assert(ptd->num_upstream >= 0);

    // Delink from any reserved downstream.
    //
    for (downstream *d = ptd->downstream_reserved; d != NULL; d = d->next) {
        if (d->upstream_conn == c) {
            d->upstream_conn = NULL;
            d->upstream_suffix = NULL;

            // Don't need to do anything else, as we'll now just
            // read and drop any remaining inflight downstream replies.
            // Eventually, the downstream will be released.
        }
    }

    // Delink from any wait queue.
    //
    conn *prev = NULL;
    conn *curr = ptd->waiting_for_downstream_head;

    while (curr != NULL) {
        if (curr == c) {
            if (ptd->waiting_for_downstream_tail == curr)
                ptd->waiting_for_downstream_tail = prev;

            if (prev != NULL) {
                assert(curr != ptd->waiting_for_downstream_head);
                prev->next = curr->next;
                break;
            }

            assert(curr == ptd->waiting_for_downstream_head);
            ptd->waiting_for_downstream_head = curr->next;
            break;
        }

        prev = curr;
        curr = curr->next;
    }

    c->next = NULL;
}

void cproxy_on_close_downstream_conn(conn *c) {
    assert(c != NULL);
    assert(c->sfd >= 0);
    assert(c->state == conn_closing);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d cproxy_on_close_downstream_conn\n", c->sfd);

    downstream *d = c->extra;

    // Might have been set to NULL during cproxy_free_downstream().
    //
    if (d == NULL)
        return;

    c->extra = NULL;

    int n = memcached_server_count(&d->mst);

    for (int i = 0; i < n; i++) {
        if (d->downstream_conns[i] == c) {
            d->downstream_conns[i] = NULL;
            assert(d->mst.hosts[i].fd == c->sfd);
            memcached_quit_server(&d->mst.hosts[i], 1);
            assert(d->mst.hosts[i].fd == -1);
        }
    }

    if (d->upstream_conn != NULL &&
        d->upstream_suffix == NULL)
        d->upstream_suffix = "SERVER_ERROR proxy downstream closed\r\n";

    // Are we over-decrementing here, and in handling conn_pause?
    //
    // Case 1: we're in conn_pause, and socket is closed concurrently.
    // We unpause due to reserve, we move to conn_write/conn_mwrite,
    // fail and move to conn_closing.  So, no over-decrement.
    //
    // Case 2: we're waiting for a downstream response in conn_new_cmd,
    // and socket is closed concurrently.  State goes to conn_closing,
    // so, no over-decrement.
    //
    // Case 3: we've finished processing downstream response (in
    // conn_parse_cmd or conn_nread), and the downstream socket
    // is closed concurrently.  We then move to conn_pause,
    // and same as Case 1.
    //
    cproxy_release_downstream_conn(d, c);
}

void cproxy_add_downstream(proxy_td *ptd) {
    assert(ptd != NULL);

    if (ptd != NULL &&
        ptd->downstream_num < ptd->downstream_max) {
        // TODO: The lock area here is big, but we don't
        //       want the config getting free'ed out from under us.
        //
        pthread_mutex_lock(&ptd->proxy->proxy_lock);

        char *config     = config;
        int   config_ver = config_ver;

        downstream *d = cproxy_create_downstream(config, config_ver);
        if (d != NULL) {
            d->ptd = ptd;
            ptd->downstream_num++;
            cproxy_release_downstream(d, true);
        }

        pthread_mutex_unlock(&ptd->proxy->proxy_lock);
    }
}

downstream *cproxy_reserve_downstream(proxy_td *ptd) {
    assert(ptd != NULL);

    // Loop in case we need to clear out downstreams that have outdated configs.
    //
    while (true) {
        downstream *d;

        d = ptd->downstream_released;
        if (d == NULL)
            cproxy_add_downstream(ptd);

        d = ptd->downstream_released;
        if (d == NULL)
            return NULL;

        ptd->downstream_released = d->next;

        assert(d->upstream_conn == NULL);
        assert(d->upstream_suffix == NULL);
        assert(d->downstream_used == 0);

        d->upstream_conn = NULL;
        d->upstream_suffix = NULL;
        d->downstream_used = 0;

        if (cproxy_check_downstream_config(d)) {
            d->next = ptd->downstream_reserved;
            ptd->downstream_reserved = d;

            return d;
        }

        cproxy_free_downstream(d);
    }
}

bool cproxy_release_downstream(downstream *d, bool force) {
    if (settings.verbose > 1)
        fprintf(stderr, "release_downstream\n");

    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->next == NULL);

    if (d->upstream_conn != NULL &&
        d->upstream_suffix != NULL) {
        // Do a last write on the upstream_conn.  For example,
        // the upstream_suffix might be "END\r\n" or other
        // way to mark the end of a scatter-gather or
        // multiline response.
        //
        if (add_iov(d->upstream_conn,
                    d->upstream_suffix,
                    strlen(d->upstream_suffix)) == 0 &&
            update_event(d->upstream_conn, EV_WRITE | EV_PERSIST)) {
            conn_set_state(d->upstream_conn, conn_mwrite);
        } else {
            if (settings.verbose > 1)
                fprintf(stderr,
                        "Could not update upstream write event\n");

            cproxy_close_conn(d->upstream_conn);
        }
    }

    d->upstream_conn = NULL;
    d->upstream_suffix = NULL; // No free(), expecting a static string.
    d->downstream_used = 0;

    // TODO: Consider adding a downstream->prev backpointer
    //       or doubly-linked list to save on this scan.
    //
    // Remove from the reserved downstream list.
    //
    d->ptd->downstream_reserved =
        downstream_list_remove(d->ptd->downstream_reserved, d);

    // If this downstream still has the same configuration as our top-level
    // proxy config, go back onto the available, released downstream list.
    //
    if (cproxy_check_downstream_config(d) || force) {
        d->next = d->ptd->downstream_released;
        d->ptd->downstream_released = d;

        return true;
    }

    cproxy_free_downstream(d);

    return false;
}

/* This should be called when the downstream is neither on
 * the reserved list or the released list.
 */
void cproxy_free_downstream(downstream *d) {
    assert(d != NULL);
    assert(d->next == NULL);
    assert(d->upstream_conn == NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_free_downstream\n");

    int n = memcached_server_count(&d->mst);

    for (int i = 0; i < n; i++)
        d->downstream_conns[i]->extra = NULL;

    // This will close sockets, which will force associated conn's
    // to go to conn_closing state.  Since we've already cleared
    // the conn->extra pointers, there's no extra release/free.
    //
    memcached_free(&d->mst);

    if (d->downstream_conns != NULL)
        free(d->downstream_conns);

    if (d->config != NULL)
        free(d->config);

    free(d);
}

/* The config input is something libmemcached can parse.
 * See memcached_servers_parse().
 */
downstream *cproxy_create_downstream(char *config, int config_ver) {
    downstream *d = (downstream *) calloc(1, sizeof(downstream));
    if (d != NULL) {
        d->config     = strdup(config);
        d->config_ver = config_ver;

        if (memcached_create(&d->mst) != NULL) {
            memcached_behavior_set(&d->mst, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);

            memcached_server_st *mservers;

            mservers = memcached_servers_parse(d->config);
            if (mservers != NULL) {
                memcached_server_push(&d->mst, mservers);
                memcached_server_list_free(mservers);
                mservers = NULL;

                int nconns = memcached_server_count(&d->mst);

                d->downstream_conns = (conn **)
                    calloc(nconns, sizeof(conn *));
                if (d->downstream_conns != NULL) {
                    return d;
                }
            }

            if (mservers != NULL)
                memcached_server_list_free(mservers);

            memcached_free(&d->mst);
        }

        free(d->config);
        free(d);
    }

    return NULL;
}

/* See if the downstream config matches the top-level proxy config.
 */
bool cproxy_check_downstream_config(downstream *d) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);

    int rv = false;

    pthread_mutex_lock(&d->ptd->proxy->proxy_lock);

    assert(d->ptd->proxy->config != NULL);

    if (d->config_ver == d->ptd->proxy->config_ver) {
        rv = true;
    } else if (strcmp(d->config, d->ptd->proxy->config) == 0) {
        d->config_ver = d->ptd->proxy->config_ver;
        rv = true;
    }

    pthread_mutex_lock(&d->ptd->proxy->proxy_lock);

    return rv;
}

int cproxy_connect_downstream(downstream *d, LIBEVENT_THREAD *thread) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->downstream_released != d); // Should not be in free list.
    assert(d->next == NULL);
    assert(d->downstream_conns != NULL);
    assert(memcached_server_count(&d->mst) > 0);

    memcached_return rc;

    int s = 0; // Number connected.
    int n = memcached_server_count(&d->mst);

    for (int i = 0; i < n; i++) {
        if (d->downstream_conns[i] == NULL) {
            rc = memcached_connect(&d->mst.hosts[i]);
            if (rc == MEMCACHED_SUCCESS) {
                int fd = d->mst.hosts[i].fd;
                if (fd >= 0) {
                    d->downstream_conns[i] =
                        conn_new(fd, conn_pause, 0, DATA_BUFFER_SIZE,
                                 proxy_downstream_ascii_prot,
                                 thread->base,
                                 &cproxy_downstream_funcs, d);
                    if (d->downstream_conns[i] != NULL)
                        d->downstream_conns[i]->thread = thread;
                }
            }
        }
        if (d->downstream_conns[i] != NULL)
            s++;
    }

    return s;
}

#define COMMAND_TOKEN    0
#define SUBCOMMAND_TOKEN 1
#define KEY_TOKEN        1
#define MAX_TOKENS       8

void cproxy_process_upstream_ascii(conn *c, char *line) {
    assert(c != NULL);
    assert(c->next == NULL);
    assert(c->extra != NULL);
    assert(c->cmd == -1);
    assert(c->item == NULL);
    assert(line != NULL);
    assert(line == c->rcurr);
    assert(IS_ASCII(c->protocol));
    assert(IS_PROXY(c->protocol));

    if (settings.verbose > 1)
        fprintf(stderr, "<%d cproxy_process_upstream_ascii %s\n",
                c->sfd, line);

    /* For commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */
    if (!cproxy_prep_conn_for_write(c)) {
        out_string(c, "SERVER_ERROR out of memory preparing response");
        return;
    }

    proxy_td *ptd = c->extra;

    assert(ptd != NULL);

    token_t tokens[MAX_TOKENS];
    size_t  ntokens = scan_tokens(line, tokens, MAX_TOKENS);
    char   *cmd     = tokens[COMMAND_TOKEN].value;
    int     comm;

    if (ntokens >= 3 &&
        (strncmp(cmd, "get", 3) == 0)) {

        // Handles get and gets.
        //
        cproxy_pause_upstream_for_downstream(ptd, c);

    } else if ((ntokens == 6 || ntokens == 7) &&
               ((strncmp(cmd, "add", 3) == 0     && (comm = NREAD_ADD)) ||
                (strncmp(cmd, "set", 3) == 0     && (comm = NREAD_SET)) ||
                (strncmp(cmd, "replace", 7) == 0 && (comm = NREAD_REPLACE)) ||
                (strncmp(cmd, "prepend", 7) == 0 && (comm = NREAD_PREPEND)) ||
                (strncmp(cmd, "append", 6) == 0  && (comm = NREAD_APPEND)) )) {

        process_update_command(c, tokens, ntokens, comm, false);

    } else if ((ntokens == 7 || ntokens == 8) &&
               (strncmp(cmd, "cas", 3) == 0 && (comm = NREAD_CAS))) {

        process_update_command(c, tokens, ntokens, comm, true);

    } else if ((ntokens == 4 || ntokens == 5) &&
               (strncmp(cmd, "incr", 4) == 0 ||
                strncmp(cmd, "decr", 4) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

    } else if (ntokens >= 3 && ntokens <= 4 &&
               (strncmp(cmd, "delete", 6) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

    } else if (ntokens >= 2 && ntokens <= 4 &&
               (strncmp(cmd, "flush_all", 9) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

    } else if (ntokens >= 2 &&
               (strncmp(cmd, "stats", 5) == 0)) {

        out_string(c, "ERROR"); // TODO

    } else if (ntokens == 2 &&
               (strncmp(cmd, "version", 7) == 0)) {

        out_string(c, "VERSION " VERSION);

    } else if ((ntokens == 3 || ntokens == 4) &&
               (strncmp(cmd, "verbosity", 9) == 0)) {

        process_verbosity_command(c, tokens, ntokens);

    } else if (ntokens == 2 &&
               (strncmp(cmd, "quit", 4) == 0)) {

        conn_set_state(c, conn_closing);

    } else {
        out_string(c, "ERROR");
    }
}

/* We get here after reading the value in set/add/replace
 * commands. The command has been stored in c->cmd, and
 * the item is ready in c->item.
 */
void cproxy_process_upstream_ascii_nread(conn *c) {
    assert(c != NULL);

    item *it = c->item;

    assert(it != NULL);

    // pthread_mutex_lock(&c->thread->stats.mutex);
    // c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    // pthread_mutex_unlock(&c->thread->stats.mutex);

    if (strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) == 0) {
        proxy_td *ptd = c->extra;

        assert(ptd != NULL);

        cproxy_pause_upstream_for_downstream(ptd, c);
    } else
        out_string(c, "CLIENT_ERROR bad data chunk");
}

void cproxy_process_downstream_ascii(conn *c, char *line) {
    assert(c != NULL);
    assert(c->next == NULL);
    assert(c->extra != NULL);
    assert(c->cmd == -1);
    assert(c->item == NULL);
    assert(line != NULL);
    assert(line == c->rcurr);
    assert(IS_ASCII(c->protocol));
    assert(IS_PROXY(c->protocol));

    if (settings.verbose > 1)
        fprintf(stderr, "<%d cproxy_process_downstream_ascii %s\n",
                c->sfd, line);

    downstream *d = c->extra;

    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->next == NULL);

    if (strncmp(line, "VALUE ", 6) == 0) {
        token_t      tokens[MAX_TOKENS];
        size_t       ntokens;
        unsigned int flags;
        int          vlen;
        uint64_t     cas = NOT_CAS;

        ntokens = scan_tokens(line, tokens, MAX_TOKENS);
        if (ntokens >= 5 && // Accounts for extra termimation token.
            ntokens <= 6 &&
            tokens[KEY_TOKEN].length <= KEY_MAX_LENGTH &&
            safe_strtoul(tokens[2].value, (uint32_t *) &flags) &&
            safe_strtoul(tokens[3].value, (uint32_t *) &vlen)) {
            char  *key  = tokens[KEY_TOKEN].value;
            size_t nkey = tokens[KEY_TOKEN].length;

            item *it = item_alloc(key, nkey, flags, 0, vlen + 2);
            if (it != NULL) {
                if (ntokens == 5 ||
                    safe_strtoull(tokens[4].value, &cas)) {
                    ITEM_set_cas(it, cas);

                    c->item = it;
                    c->ritem = ITEM_data(it);
                    c->rlbytes = it->nbytes;
                    c->cmd = -1;

                    conn_set_state(c, conn_nread);

                    return; // Success.
                } else {
                    if (settings.verbose > 1)
                        fprintf(stderr, "cproxy could not parse cas\n");
                }
            } else {
                if (settings.verbose > 1)
                    fprintf(stderr, "cproxy could not item_alloc size %u\n",
                            vlen + 2);
            }

            if (it != NULL)
                item_remove(it);
            it = NULL;

            c->sbytes = vlen + 2; // Number of bytes to swallow.

            conn_set_state(c, conn_swallow);

            // Note, eventually, we'll see an END later.
        } else {
            // We don't know how much to swallow, so close the downstream.
            // The conn_closing should release the downstream,
            // which should write a suffix/error to the upstream.
            //
            conn_set_state(c, conn_closing);
        }
    } else if (strncmp(line, "END", 3) == 0 ||
               strncmp(line, "OK", 2) == 0) {
        conn_set_state(c, conn_pause);
    } else if (strncmp(line, "STAT ", 5) == 0 ||
               strncmp(line, "ITEM ", 5) == 0 ||
               strncmp(line, "PREFIX ", 7) == 0) { // TODO
        conn_set_state(c, conn_new_cmd);
    } else {
        conn_set_state(c, conn_pause);

        // The upstream conn might be NULL when closed already
        // or while handling a noreply.
        //
        conn *uc = d->upstream_conn;
        if (uc != NULL) {
            out_string(uc, line);

            if (!update_event(uc, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "Can't update upstream write event\n");

                cproxy_close_conn(uc);
            }
        }
    }
}

/* We get here after reading the value in a VALUE reply.
 * The item is ready in c->item.
 */
void cproxy_process_downstream_ascii_nread(conn *c) {
    assert(c != NULL);

    if (settings.verbose > 1)
        fprintf(stderr,
                "<%d cproxy_process_downstream_ascii_nread %d %d\n",
                c->sfd, c->ileft, c->isize);

    downstream *d = c->extra;
    assert(d != NULL);

    item *it = c->item;
    assert(it != NULL);

    // Clear c->item because we either move it to the upstream or
    // item_remove() it on error.
    //
    c->item = NULL;

    conn_set_state(c, conn_new_cmd);

    // pthread_mutex_lock(&c->thread->stats.mutex);
    // c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    // pthread_mutex_unlock(&c->thread->stats.mutex);

    conn *uc = d->upstream_conn;
    if (uc != NULL) {
        assert(uc->funcs != NULL);
        assert(IS_ASCII(uc->protocol));
        assert(IS_PROXY(uc->protocol));

        if (strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) == 0) {
            uint64_t cas = ITEM_get_cas(it);
            if (cas == NOT_CAS) {
                if (add_iov(uc, "VALUE ", 6) == 0 &&
                    add_iov(uc, ITEM_key(it), it->nkey) == 0 &&
                    add_iov(uc, ITEM_suffix(it), it->nsuffix + it->nbytes) == 0 &&
                    add_conn_item(uc, it)) {
                    if (settings.verbose > 1)
                        fprintf(stderr,
                                "<%d cproxy_process_downstream_ascii success\n",
                                c->sfd);

                    return; // Success.
                }
            } else {
                char *suffix = add_conn_suffix(uc);
                if (suffix != NULL) {
                    sprintf(suffix, " %llu\r\n", (unsigned long long) cas);

                    if (add_iov(uc, "VALUE ", 6) == 0 &&
                        add_iov(uc, ITEM_key(it), it->nkey) == 0 &&
                        add_iov(uc, ITEM_suffix(it), it->nsuffix - 2) == 0 &&
                        add_iov(uc, suffix, strlen(suffix)) == 0 &&
                        add_iov(uc, ITEM_data(it), it->nbytes) == 0 &&
                        add_conn_item(uc, it)) {
                        if (settings.verbose > 1)
                            fprintf(stderr,
                                    "<%d cproxy_process_downstream_ascii ok\n",
                                    c->sfd);

                        return; // Success.
                    }
                }
            }

            if (settings.verbose > 1)
                fprintf(stderr, "proxy out of response memory");
        } else {
            if (settings.verbose > 1)
                fprintf(stderr, "unexpected downstream data block");
        }
    } else {
        if (settings.verbose > 1)
            fprintf(stderr, "proxy upstream seems closed already");
    }

    item_remove(it);
}

conn *cproxy_find_downstream_conn(downstream *d, char *key, int key_length) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(key != NULL);
    assert(key_length > 0);

    int s = cproxy_server_index(d, key, key_length);
    if (s >= 0 &&
        s < memcached_server_count(&d->mst)) {
        return d->downstream_conns[s];
    }
    return NULL;
}

bool cproxy_prep_conn_for_write(conn *c) {
    if (c != NULL) {
        assert(c->item == NULL);
        assert(IS_ASCII(c->protocol));
        assert(IS_PROXY(c->protocol));
        assert(c->ilist != NULL);
        assert(c->isize > 0);
        assert(c->suffixlist != NULL);
        assert(c->suffixsize > 0);

        c->icurr      = c->ilist;
        c->ileft      = 0;
        c->suffixcurr = c->suffixlist;
        c->suffixleft = 0;

        c->msgcurr = 0; // TODO: Mem leak just by blowing these to 0?
        c->msgused = 0;
        c->iovused = 0;

        if (add_msghdr(c) == 0)
            return true;

        if (settings.verbose > 1)
            fprintf(stderr, "%d: cproxy_prep_conn_for_write failed\n", c->sfd);
    }

    return false;
}

int cproxy_server_index(downstream *d, char *key, size_t key_length) {
    assert(d != NULL);
    assert(key != NULL);
    assert(key_length > 0);

    // memcached_return rc;
    //
    // rc = memcached_validate_key_length(key_length,
    //          d->mst.flags & MEM_BINARY_PROTOCOL);
    // unlikely (rc != MEMCACHED_SUCCESS)
    //    return -1;

    if (memcached_server_count(&d->mst) <= 0)
        return -1;

    // if (memcached_key_test((char **) &key, &key_length, 1) ==
    //     MEMCACHED_BAD_KEY_PROVIDED)
    //     return -1;

    return (int) memcached_generate_hash(&d->mst, key, key_length);
}

void cproxy_assign_downstream(proxy_td *ptd) {
    assert(ptd != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "assign_downstream\n");

    // Key loop that tries to reserve any available, released
    // downstream resources to waiting upstream conns.
    //
    // Remember the wait list tail when we start, in case more
    // upstream conns are tacked onto the wait list while we're
    // processing.  This helps avoid infinite loop where upstream
    // conns just keep on moving to the tail.
    //
    conn *tail = ptd->waiting_for_downstream_tail;
    bool  stop = false;

    while (ptd->waiting_for_downstream_head != NULL && !stop) {
        if (ptd->waiting_for_downstream_head == tail)
            stop = true;

        downstream *d = cproxy_reserve_downstream(ptd);
        if (d == NULL)
            break; // If no downstreams are available, stop loop.

        assert(d->next == NULL);
        assert(d->upstream_conn == NULL);
        assert(d->downstream_used == 0);

        // We have a downstream reserved, so assign the first
        // waiting upstream conn to it.
        //
        d->upstream_conn = ptd->waiting_for_downstream_head;
        ptd->waiting_for_downstream_head =
            ptd->waiting_for_downstream_head->next;
        if (ptd->waiting_for_downstream_head == NULL)
            ptd->waiting_for_downstream_tail = NULL;
        d->upstream_conn->next = NULL;

        if (settings.verbose > 1)
            fprintf(stderr, "assign_downstream, matched to upstream %d\n",
                    d->upstream_conn->sfd);

        if (!cproxy_forward_downstream(d)) {
            // We reach here on error, so put upstream conn back
            // on the wait list to retry, and release the downstream.
            //
            conn *uc = d->upstream_conn;
            if (uc != NULL) {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "%d could not forward upstream to downstream\n",
                            uc->sfd);
            }

            cproxy_release_downstream(d, false);

            if (uc != NULL) {
                // TOOD: Count retrying instead of error, but need counter.
                //
                // cproxy_wait_for_downstream(ptd, uc);
                //
                out_string(uc, "SERVER_ERROR proxy could not write to downstream");

                update_event(uc, EV_WRITE | EV_PERSIST);
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "assign_downstream, done\n");
}

/* Do the actual work of forwarding the command from an
 * upstream conn to its assigned downstream.
 */
bool cproxy_forward_downstream(downstream *d) {
    assert(d != NULL);

    conn *uc = d->upstream_conn;

    assert(uc != NULL);
    assert(uc->state == conn_pause);
    assert(uc->rcurr != NULL);
    assert(uc->thread != NULL);
    assert(uc->thread->base != NULL);
    assert(IS_ASCII(uc->protocol));
    assert(IS_PROXY(uc->protocol));

    if (cproxy_connect_downstream(d, uc->thread) > 0) {
        assert(d->downstream_conns != NULL);

        if (uc->cmd == -1) {
            return cproxy_forward_simple_downstream(d, uc->rcurr, uc);
        } else {
            return cproxy_forward_item_downstream(d, uc->cmd, uc->item, uc);
        }
    }

    return false;
}

/* Forward a simple one-liner command downstream.
 * For example, get, incr/decr, delete, etc.
 * The response, though, might be a simple line or
 * multiple VALUE+END lines.
 */
bool cproxy_forward_simple_downstream(downstream *d, char *command, conn *uc) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->item == NULL);

    if (strncmp(command, "get", 3) == 0)
        return cproxy_forward_multiget_downstream(d, command, uc);

    if (strncmp(command, "flush_all", 9) == 0)
        return cproxy_broadcast_downstream(d, command, uc, "OK\r\n");

    token_t  tokens[MAX_TOKENS];
    size_t   ntokens = scan_tokens(command, tokens, MAX_TOKENS);
    char    *key     = tokens[KEY_TOKEN].value;
    int      key_len = tokens[KEY_TOKEN].length;

    if (ntokens <= 1) { // This was checked long ago, while parsing
        assert(false);  // the upstream conn.
        return false;
    }

    // Assuming we're already connected to downstream.
    //
    conn *c = cproxy_find_downstream_conn(d, key, key_len);
    if (c != NULL &&
        cproxy_prep_conn_for_write(c)) {
        assert(c->state == conn_pause);

        out_string(c, command);

        if (settings.verbose > 1)
            fprintf(stderr, "forwarding to %d, noreply %d\n",
                    c->sfd, uc->noreply);

        if (update_event(c, EV_WRITE | EV_PERSIST)) {
            d->downstream_used = 1; // TODO: Need timeout?

            if (cproxy_dettach_if_noreply(d, uc))
                c->write_and_go = conn_pause;

            return true;
        }

        if (settings.verbose > 1)
            fprintf(stderr, "Couldn't update cproxy write event\n");

        cproxy_close_conn(c);
    }

    return false;
}

bool cproxy_forward_multiget_downstream(downstream *d, char *command, conn *uc) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->item == NULL);

    int nwrite = 0;
    int nconns = memcached_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        cproxy_prep_conn_for_write(d->downstream_conns[i]);
        assert(d->downstream_conns[i]->state == conn_pause);
    }

    char *space = strchr(command, ' ');
    assert(space > command);

    int cmd_len = space - command;
    assert(cmd_len == 3 || cmd_len == 4); // Either get or gets.

    if (settings.verbose > 1)
        fprintf(stderr, "forward multiget %s (%d)\n", command, cmd_len);

    while (space != NULL) {
        char *key = space + 1;
        char *next_space = strchr(key, ' ');
        int   key_len;

        if (next_space != NULL)
            key_len = next_space - key;
        else
            key_len = strlen(key);

        // This key_len check helps skips consecutive spaces.
        //
        if (key_len > 0) {
            conn *c = cproxy_find_downstream_conn(d, key, key_len);
            if (c != NULL) {
                assert(c->item == NULL);
                assert(c->state == conn_pause);
                assert(IS_ASCII(c->protocol));
                assert(IS_PROXY(c->protocol));
                assert(c->ilist != NULL);
                assert(c->isize > 0);

                c->icurr = c->ilist;
                c->ileft = 0;

                if (c->msgused <= 1 &&
                    c->msgbytes <= 0) {
                    add_iov(c, command, cmd_len);
                }

                // Write the key, including the preceding space.
                //
                add_iov(c, key - 1, key_len + 1);
            } else {
                // TODO: Handle when downstream conn is down.
            }
        }

        space = next_space;
    }

    for (int i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL &&
            (c->msgused > 1 ||
             c->msgbytes > 0)) {
            add_iov(c, "\r\n", 2);

            conn_set_state(c, conn_mwrite);
            c->write_and_go = conn_new_cmd;

            if (update_event(c, EV_WRITE | EV_PERSIST)) {
                nwrite++;

                if (uc->noreply) {
                    c->write_and_go = conn_pause;
                }
            } else {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "Couldn't update cproxy write event\n");

                cproxy_close_conn(c);
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "forward multiget nwrite %d out of %d\n",
                nwrite, nconns);

    d->downstream_used = nwrite; // TODO: Need timeout?

    if (cproxy_dettach_if_noreply(d, uc) == false)
        d->upstream_suffix = "END\r\n";

    return nwrite > 0;
}

/* Used for broadcast commands, like flush_all or stats.
 */
bool cproxy_broadcast_downstream(downstream *d, char *command, conn *uc,
                                 char *suffix) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->item == NULL);

    int nwrite = 0;
    int nconns = memcached_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL &&
            cproxy_prep_conn_for_write(c)) {
            assert(c->state == conn_pause);

            out_string(c, command);

            if (update_event(c, EV_WRITE | EV_PERSIST)) {
                nwrite++;

                if (uc->noreply) {
                    c->write_and_go = conn_pause;
                }
            } else {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "Update cproxy write event failed\n");

                cproxy_close_conn(c);
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "forward multiget nwrite %d out of %d\n",
                nwrite, nconns);

    d->downstream_used = nwrite; // TODO: Need timeout?

    if (cproxy_dettach_if_noreply(d, uc) == false)
        d->upstream_suffix = suffix;

    return nwrite > 0;
}

/* Forward an upstream command that came with item data,
 * like set/add/replace/etc.
 */
bool cproxy_forward_item_downstream(downstream *d, short cmd, item *it, conn *uc) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(it != NULL);
    assert(uc != NULL);

    // Assuming we're already connected to downstream.
    //
    conn *c = cproxy_find_downstream_conn(d, ITEM_key(it), it->nkey);
    if (c != NULL &&
        cproxy_prep_conn_for_write(c)) {
        assert(c->state == conn_pause);

        char *verb = nread_text(cmd);

        assert(verb != NULL);

        char *str_flags   = ITEM_suffix(it);
        char *str_length  = strchr(str_flags + 1, ' ');
        int   len_flags   = str_length - str_flags;
        int   len_length  = it->nsuffix - len_flags - 2;
        char *str_exptime = add_conn_suffix(c);
        char *str_cas     = (cmd == NREAD_CAS ? add_conn_suffix(c) : NULL);

        if (str_flags != NULL &&
            str_length != NULL &&
            len_flags > 1 &&
            len_length > 1 &&
            str_exptime != NULL &&
            (cmd != NREAD_CAS ||
             str_cas != NULL)) {
            sprintf(str_exptime, " %u", it->exptime);

            if (str_cas != NULL)
                sprintf(str_cas, " %llu", (unsigned long long) ITEM_get_cas(it));

            if (add_iov(c, verb, strlen(verb)) == 0 &&
                add_iov(c, ITEM_key(it), it->nkey) == 0 &&
                add_iov(c, str_flags, len_flags) == 0 &&
                add_iov(c, str_exptime, strlen(str_exptime)) == 0 &&
                add_iov(c, str_length, len_length) == 0 &&
                (str_cas == NULL ||
                 add_iov(c, str_cas, strlen(str_cas)) == 0) &&
                (uc->noreply == false ||
                 add_iov(c, " noreply", 8) == 0) &&
                add_iov(c, ITEM_data(it) - 2, it->nbytes + 2) == 0) {
                conn_set_state(c, conn_mwrite);
                c->write_and_go = conn_new_cmd;

                if (update_event(c, EV_WRITE | EV_PERSIST)) {
                    d->downstream_used = 1; // TODO: Need timeout?

                    if (cproxy_dettach_if_noreply(d, uc))
                        c->write_and_go = conn_pause;

                    return true;
                }
            }
        }

        if (settings.verbose > 1)
            fprintf(stderr, "Proxy item write out of memory");

        // TODO: Need better out-of-memory behavior.
    }

    return false;
}

void cproxy_reset_upstream(conn *uc) {
    assert(uc != NULL);

    conn_set_state(uc, conn_new_cmd);

    if (uc->rbytes <= 0) {
        if (update_event(uc, EV_READ | EV_PERSIST)) {
            return;
        } else {
            if (settings.verbose > 1)
                fprintf(stderr, "Couldn't update uc READ event\n");

            cproxy_close_conn(uc);

            return;
        }
    }

    // TODO: Subtle potential bug, where we may have already
    // read incoming bytes into the uc's buffer, so that
    // libevent never sees any EV_READ events, leaving the
    // uc seemingly stuck, never hitting drive_machine() loop.
    //
    // This depends on what libevent does here.
    //
    // May need to use the pipe to get drive_machine onto the uc?
    //
    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_reset_upstream with bytes available\n");
}

bool cproxy_dettach_if_noreply(downstream *d, conn *uc) {
    if (uc->noreply) {
        uc->noreply        = false;
        d->upstream_conn   = NULL;
        d->upstream_suffix = NULL;

        cproxy_reset_upstream(uc);

        return true;
    }

    return false;
}

void cproxy_wait_for_downstream(proxy_td *ptd, conn *c) {
    assert(c != NULL);
    assert(ptd != NULL);
    assert(!ptd->waiting_for_downstream_tail ||
           !ptd->waiting_for_downstream_tail->next);

    // Add the conn to the wait list.
    //
    c->next = NULL;
    if (ptd->waiting_for_downstream_tail != NULL)
        ptd->waiting_for_downstream_tail->next = c;
    ptd->waiting_for_downstream_tail = c;
    if (ptd->waiting_for_downstream_head == NULL)
        ptd->waiting_for_downstream_head = c;
}

void cproxy_release_downstream_conn(downstream *d, conn *c) {
    assert(c != NULL);
    assert(d != NULL);

    proxy_td *ptd = d->ptd;
    assert(ptd != NULL);

    if (settings.verbose > 1)
        fprintf(stderr,
                "%d release_downstream_conn, downstream_used %d\n",
                c->sfd, d->downstream_used);

    d->downstream_used--;
    if (d->downstream_used <= 0) {
        cproxy_release_downstream(d, false);
        cproxy_assign_downstream(ptd);
    }
}

void cproxy_on_pause_downstream_conn(conn *c) {
    assert(c != NULL);
    assert(c->extra != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d cproxy_on_pause_downstream_conn\n",
                c->sfd);

    downstream *d = c->extra;

    // Must update_event() before releasing the downstream conn,
    // because the release might call udpate_event(), too,
    // and we don't want to override its work.
    //
    if (update_event(c, 0))
        cproxy_release_downstream_conn(d, c);
    else
        conn_set_state(c, conn_closing);
}

void cproxy_pause_upstream_for_downstream(proxy_td *ptd, conn *upstream) {
    assert(ptd != NULL);
    assert(upstream != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "%d pause_upstream_for_downstream\n",
                upstream->sfd);

    conn_set_state(upstream, conn_pause);
    cproxy_wait_for_downstream(ptd, upstream);
    cproxy_assign_downstream(ptd);
}

rel_time_t cproxy_realtime(const time_t exptime) {
    // Input is a long...
    //
    // 0       | (0...REALIME_MAXDELTA] | (REALTIME_MAXDELTA...
    // forever | delta,                 | unix_time
    //
    // Storage is an unsigned int.
    //
    // TODO: Handle data loss.
    //
    // The cproxy version of realtime doesn't do any
    // time math munging, just pass through.
    //
    return (rel_time_t) exptime;
}

void cproxy_close_conn(conn *c) {
    assert(c != NULL);

    conn_set_state(c, conn_closing);

    // We need this to handle case when the conn is not the
    // current conn in the thread's drive_machine() loop.
    // This should wakeup libevent on the next run to
    // cleanup the conn.
    //
    // The shutdown() should help prevent blocking at close().
    //
    shutdown(c->sfd, SHUT_RDWR);
    close(c->sfd);
}

bool add_conn_item(conn *c, item *it) {
    assert(it != NULL);
    assert(c != NULL);
    assert(c->ilist != NULL);
    assert(c->icurr != NULL);
    assert(c->isize > 0);

    if (c->ileft >= c->isize) {
        item **new_list =
            realloc(c->ilist, sizeof(item *) * c->isize * 2);
        if (new_list) {
            c->isize *= 2;
            c->ilist = new_list;
            c->icurr = new_list;
        }
    }

    if (c->ileft < c->isize) {
        c->ilist[c->ileft] = it;
        c->ileft++;

        return true;
    }

    return false;
}

char *add_conn_suffix(conn *c) {
    assert(c != NULL);
    assert(c->suffixlist != NULL);
    assert(c->suffixcurr != NULL);
    assert(c->suffixsize > 0);

    if (c->suffixleft >= c->suffixsize) {
        char **new_suffix_list =
            realloc(c->suffixlist,
                    sizeof(char *) * c->suffixsize * 2);
        if (new_suffix_list) {
            c->suffixsize *= 2;
            c->suffixlist = new_suffix_list;
            c->suffixcurr = new_suffix_list;
        }
    }

    if (c->suffixleft < c->suffixsize) {
        char *suffix = suffix_from_freelist();
        if (suffix != NULL) {
            c->suffixlist[c->suffixleft] = suffix;
            c->suffixleft++;

            return suffix;
        }
    }

    return NULL;
}

char *nread_text(short x) {
    char *rv = NULL;
    switch(x) {
    case NREAD_SET:
        rv = "set ";
        break;
    case NREAD_ADD:
        rv = "add ";
        break;
    case NREAD_REPLACE:
        rv = "replace ";
        break;
    case NREAD_APPEND:
        rv = "append ";
        break;
    case NREAD_PREPEND:
        rv = "prepend ";
        break;
    case NREAD_CAS:
        rv = "cas ";
        break;
    }
    return rv;
}

/* Tokenize the command string by updating the token array
 * with pointers to start of each token and length.
 * Does not modify the input command string.
 *
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while (scan_tokens(command, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      command = tokens[ix].value;
 *  }
 */
size_t scan_tokens(char *command, token_t *tokens, const size_t max_tokens) {
    char *s, *e;
    size_t ntokens = 0;

    assert(command != NULL && tokens != NULL && max_tokens > 1);

    for (s = e = command; ntokens < max_tokens - 1; ++e) {
        if (*e == '\0' || *e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }
            if (*e == '\0')
                break; /* string end */
            s = e + 1;
        }
    }

    /* If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    tokens[ntokens].value = (*e == '\0' ? NULL : e);
    tokens[ntokens].length = 0;
    ntokens++;

    return ntokens;
}

/* Returns the new head of the list.
 */
downstream *downstream_list_remove(downstream *head, downstream *d) {
    downstream *prev = NULL;
    downstream *curr = head;

    while (curr != NULL) {
        if (curr == d) {
            if (prev != NULL) {
                assert(curr != head);
                prev->next = curr->next;
                return head;
            }

            assert(curr == head);
            return curr->next;
        }

        prev = curr;
        curr = curr ->next;
    }

    return head;
}

