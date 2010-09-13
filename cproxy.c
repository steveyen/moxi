/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"
#include "log.h"

// Internal forward declarations.
//
downstream *downstream_list_remove(downstream *head, downstream *d);
downstream *downstream_list_waiting_remove(downstream *head,
                                           downstream **tail,
                                           downstream *d);

void downstream_timeout(const int fd,
                        const short which,
                        void *arg);
void wait_queue_timeout(const int fd,
                        const short which,
                        void *arg);

conn *conn_list_remove(conn *head, conn **tail,
                       conn *c, bool *found);

bool is_compatible_request(conn *existing, conn *candidate);

void propagate_error(downstream *d);

void downstream_reserved_time_sample(proxy_stats_td *ptds, uint64_t duration);
void downstream_connect_time_sample(proxy_stats_td *ptds, uint64_t duration);

bool downstream_connect_init(downstream *d, mcs_server_st *msst,
                             proxy_behavior *behavior, conn *c);

int init_mcs_st(mcs_st *mst, char *config);

bool cproxy_on_connect_downstream_conn(conn *c);

conn *zstored_acquire_downstream_conn(downstream *d,
                                      LIBEVENT_THREAD *thread,
                                      mcs_server_st *msst,
                                      proxy_behavior *behavior,
                                      bool *downstream_conn_max_reached);

void zstored_release_downstream_conn(conn *dc, bool closing);

void zstored_error_count(LIBEVENT_THREAD *thread,
                         const char *host_ident,
                         bool has_error);

bool zstored_downstream_waiting_add(downstream *d, LIBEVENT_THREAD *thread,
                                    mcs_server_st *msst,
                                    proxy_behavior *behavior);

void zstored_downstream_waiting_remove(downstream *d);

typedef struct {
    conn      *dc;          // Linked-list of available downstream conns.
    uint32_t   dc_acquired; // Count of acquired (in-use) downstream conns.
    char      *host_ident;
    uint32_t   error_count;
    rel_time_t error_time;

    // Head & tail of singly linked-list/queue, using
    // downstream->next_waiting pointers, where we've reached
    // downstream_conn_max, so there are waiting downstreams.
    //
    downstream *downstream_waiting_head;
    downstream *downstream_waiting_tail;
} zstored_downstream_conns;

zstored_downstream_conns *zstored_get_downstream_conns(LIBEVENT_THREAD *thread,
                                                       const char *host_ident);

void format_host_ident(char *buf, int buf_len,
                       mcs_server_st *msst,
                       enum protocol host_protocol);

bool cproxy_forward_or_error(downstream *d);

int delink_from_downstream_conns(conn *c);

// Function tables.
//
conn_funcs cproxy_listen_funcs = {
    .conn_init                   = cproxy_init_upstream_conn,
    .conn_close                  = NULL,
    .conn_connect                = NULL,
    .conn_process_ascii_command  = NULL,
    .conn_process_binary_command = NULL,
    .conn_complete_nread_ascii   = NULL,
    .conn_complete_nread_binary  = NULL,
    .conn_pause                  = NULL,
    .conn_realtime               = NULL,
    .conn_state_change           = NULL,
    .conn_binary_command_magic   = 0
};

conn_funcs cproxy_upstream_funcs = {
    .conn_init                   = NULL,
    .conn_close                  = cproxy_on_close_upstream_conn,
    .conn_connect                = NULL,
    .conn_process_ascii_command  = cproxy_process_upstream_ascii,
    .conn_process_binary_command = cproxy_process_upstream_binary,
    .conn_complete_nread_ascii   = cproxy_process_upstream_ascii_nread,
    .conn_complete_nread_binary  = cproxy_process_upstream_binary_nread,
    .conn_pause                  = NULL,
    .conn_realtime               = cproxy_realtime,
    .conn_state_change           = cproxy_upstream_state_change,
    .conn_binary_command_magic   = PROTOCOL_BINARY_REQ
};

conn_funcs cproxy_downstream_funcs = {
    .conn_init                   = cproxy_init_downstream_conn,
    .conn_close                  = cproxy_on_close_downstream_conn,
    .conn_connect                = cproxy_on_connect_downstream_conn,
    .conn_process_ascii_command  = cproxy_process_downstream_ascii,
    .conn_process_binary_command = cproxy_process_downstream_binary,
    .conn_complete_nread_ascii   = cproxy_process_downstream_ascii_nread,
    .conn_complete_nread_binary  = cproxy_process_downstream_binary_nread,
    .conn_pause                  = cproxy_on_pause_downstream_conn,
    .conn_realtime               = cproxy_realtime,
    .conn_state_change           = NULL,
    .conn_binary_command_magic   = PROTOCOL_BINARY_RES
};

/* Main function to create a proxy struct.
 */
proxy *cproxy_create(proxy_main *main,
                     char    *name,
                     int      port,
                     char    *config,
                     uint32_t config_ver,
                     proxy_behavior_pool *behavior_pool,
                     int nthreads) {
    assert(name != NULL);
    assert(port > 0 || settings.socketpath != NULL);
    assert(config != NULL);
    assert(behavior_pool);
    assert(nthreads > 1); // Main thread + at least one worker.
    assert(nthreads == settings.num_threads);

    if (settings.verbose > 1) {
        moxi_log_write("cproxy_create on port %d, name %s, config %s\n",
                       port, name, config);
    }

    proxy *p = (proxy *) calloc(1, sizeof(proxy));
    if (p != NULL) {
        p->main       = main;
        p->name       = trimstrdup(name);
        p->port       = port;
        p->config     = trimstrdup(config);
        p->config_ver = config_ver;

        p->behavior_pool.base = behavior_pool->base;
        p->behavior_pool.num  = behavior_pool->num;
        p->behavior_pool.arr  = cproxy_copy_behaviors(behavior_pool->num,
                                                      behavior_pool->arr);

        p->listening        = 0;
        p->listening_failed = 0;

        p->next = NULL;

        pthread_mutex_init(&p->proxy_lock, NULL);

        mcache_init(&p->front_cache, true, &mcache_item_funcs, true);
        matcher_init(&p->front_cache_matcher, true);
        matcher_init(&p->front_cache_unmatcher, true);

        matcher_init(&p->optimize_set_matcher, true);

        if (behavior_pool->base.front_cache_max > 0 &&
            behavior_pool->base.front_cache_lifespan > 0) {
            mcache_start(&p->front_cache,
                         behavior_pool->base.front_cache_max);

            if (strlen(behavior_pool->base.front_cache_spec) > 0) {
                matcher_start(&p->front_cache_matcher,
                              behavior_pool->base.front_cache_spec);
            }

            if (strlen(behavior_pool->base.front_cache_unspec) > 0) {
                matcher_start(&p->front_cache_unmatcher,
                              behavior_pool->base.front_cache_unspec);
            }
        }

        if (strlen(behavior_pool->base.optimize_set) > 0) {
            matcher_start(&p->optimize_set_matcher,
                          behavior_pool->base.optimize_set);
        }

        p->thread_data_num = nthreads;
        p->thread_data = (proxy_td *) calloc(p->thread_data_num,
                                             sizeof(proxy_td));
        if (p->thread_data != NULL &&
            p->name != NULL &&
            p->config != NULL &&
            p->behavior_pool.arr != NULL) {
            // We start at 1, because thread[0] is the main listen/accept
            // thread, and not a true worker thread.  Too lazy to save
            // the wasted thread[0] slot memory.
            //
            for (int i = 1; i < p->thread_data_num; i++) {
                proxy_td *ptd = &p->thread_data[i];
                ptd->proxy = p;

                ptd->config     = strdup(p->config);
                ptd->config_ver = p->config_ver;

                ptd->behavior_pool.base = behavior_pool->base;
                ptd->behavior_pool.num  = behavior_pool->num;
                ptd->behavior_pool.arr =
                    cproxy_copy_behaviors(behavior_pool->num,
                                          behavior_pool->arr);

                ptd->waiting_any_downstream_head = NULL;
                ptd->waiting_any_downstream_tail = NULL;
                ptd->downstream_reserved = NULL;
                ptd->downstream_released = NULL;
                ptd->downstream_tot = 0;
                ptd->downstream_num = 0;
                ptd->downstream_max = behavior_pool->base.downstream_max;
                ptd->downstream_assigns = 0;
                ptd->timeout_tv.tv_sec = 0;
                ptd->timeout_tv.tv_usec = 0;
                ptd->stats.stats.num_upstream = 0;
                ptd->stats.stats.num_downstream_conn = 0;

                cproxy_reset_stats_td(&ptd->stats);

                mcache_init(&ptd->key_stats, true,
                            &mcache_key_stats_funcs, false);
                matcher_init(&ptd->key_stats_matcher, false);
                matcher_init(&ptd->key_stats_unmatcher, false);

                if (behavior_pool->base.key_stats_max > 0 &&
                    behavior_pool->base.key_stats_lifespan > 0) {
                    mcache_start(&ptd->key_stats,
                                 behavior_pool->base.key_stats_max);

                    if (strlen(behavior_pool->base.key_stats_spec) > 0) {
                        matcher_start(&ptd->key_stats_matcher,
                                      behavior_pool->base.key_stats_spec);
                    }

                    if (strlen(behavior_pool->base.key_stats_unspec) > 0) {
                        matcher_start(&ptd->key_stats_unmatcher,
                                      behavior_pool->base.key_stats_unspec);
                    }
                }
            }

            return p;
        }

        free(p->name);
        free(p->config);
        free(p->behavior_pool.arr);
        free(p->thread_data);
        free(p);
    }

    return NULL;
}

/* Must be called on the main listener thread.
 */
int cproxy_listen(proxy *p) {
    assert(p != NULL);
    assert(is_listen_thread());

    if (settings.verbose > 1) {
        moxi_log_write("cproxy_listen on port %d, downstream %s\n",
                p->port, p->config);
    }

    // Idempotent, remembers if it already created listening socket(s).
    //
    if (p->listening == 0) {
        enum protocol listen_protocol = negotiating_proxy_prot;

        if (IS_ASCII(settings.binding_protocol)) {
            listen_protocol = proxy_upstream_ascii_prot;
        }
        if (IS_BINARY(settings.binding_protocol)) {
            listen_protocol = proxy_upstream_binary_prot;
        }

        int listening = cproxy_listen_port(p->port, listen_protocol,
                                           tcp_transport,
                                           p,
                                           &cproxy_listen_funcs);
        if (listening > 0) {
            p->listening += listening;
        } else {
            p->listening_failed++;

            moxi_log_write("ERROR: could not listen on port %d. "
                           "Please use -Z port_listen=PORT_NUM "
                           "to specify a different port number.\n", p->port);
            if (ml->log_mode != ERRORLOG_STDERR) {
                fprintf(stderr, "ERROR: could not listen on port %d. "
                        "Please use -Z port_listen=PORT_NUM "
                        "to specify a different port number.\n", p->port);
            }
            exit(EXIT_FAILURE);
        }
    }

    return p->listening;
}

int cproxy_listen_port(int port,
                       enum protocol protocol,
                       enum network_transport transport,
                       void       *conn_extra,
                       conn_funcs *funcs) {
    assert(port > 0 || settings.socketpath != NULL);
    assert(conn_extra);
    assert(funcs);
    assert(is_listen_thread());

    int   listening = 0;
    conn *listen_conn_orig = listen_conn;

    conn *x = listen_conn_orig;
    while (x != NULL) {
        if (x->extra != NULL &&
            x->funcs == funcs) {
            struct in_addr in = {0};
            struct sockaddr_in s_in = {.sin_family = 0};
            socklen_t sin_len = sizeof(s_in);

            if (getsockname(x->sfd, (struct sockaddr *) &s_in, &sin_len) == 0) {
                in.s_addr = s_in.sin_addr.s_addr;

                int x_port = ntohs(s_in.sin_port);
                if (x_port == port) {
                    if (settings.verbose > 1) {
                        moxi_log_write(
                                "<%d cproxy listening reusing listener on port %d\n",
                                x->sfd, port);
                    }

                    listening++;
                }
            }
        }

        x = x->next;
    }

    if (listening > 0) {
        // If we're already listening on the required port, then
        // we don't need to start a new server_socket().  This happens
        // in the multi-bucket case with binary protocol buckets.
        // There will be multiple proxy struct's (one per bucket), but
        // only one proxy struct will actually be pointed at by a
        // listening conn->extra (usually 11211).
        //
        // TODO: Add a refcount to handle shutdown properly?
        //
        return listening;
    }
#ifdef HAVE_SYS_UN_H
    if (settings.socketpath ?
        (server_socket_unix(settings.socketpath, settings.access) == 0) :
        (server_socket(port, transport, NULL) == 0)) {
#else
    if (server_socket(port, transport, NULL) == 0) {
#endif
        assert(listen_conn != NULL);

        // The listen_conn global list is changed by server_socket(),
        // which adds a new listening conn on port for each bindable
        // host address.
        //
        // For example, after the call to server_socket(), there
        // might be two new listening conn's -- one for localhost,
        // another for 127.0.0.1.
        //
        conn *c = listen_conn;
        while (c != NULL &&
               c != listen_conn_orig) {
            if (settings.verbose > 1) {
                moxi_log_write(
                        "<%d cproxy listening on port %d\n",
                        c->sfd, port);
            }

            listening++;

            // TODO: Listening conn's never seem to close,
            //       but need to handle cleanup if they do,
            //       such as if we handle graceful shutdown one day.
            //
            c->extra = conn_extra;
            c->funcs = funcs;
            c->protocol = protocol;
            c = c->next;
        }
    }

    return listening;
}

/* Finds the proxy_td associated with a worker thread.
 */
proxy_td *cproxy_find_thread_data(proxy *p, pthread_t thread_id) {
    if (p != NULL) {
        int i = thread_index(thread_id);

        // 0 is the main listen thread, not a worker thread.
        assert(i > 0);
        assert(i < p->thread_data_num);

        if (i > 0 && i < p->thread_data_num) {
            return &p->thread_data[i];
        }
    }

    return NULL;
}

void cproxy_init_upstream_conn(conn *c) {
    assert(c != NULL);

    // We're called once per client/upstream conn early in its
    // lifecycle, on the worker thread, so it's a good place
    // to record the proxy_td into the conn->extra.
    //
    assert(!is_listen_thread());

    proxy *p = c->extra;
    assert(p != NULL);

    proxy_td *ptd = cproxy_find_thread_data(p, pthread_self());
    assert(ptd != NULL);

    ptd->stats.stats.num_upstream++;
    ptd->stats.stats.tot_upstream++;

    c->extra = ptd;
    c->funcs = &cproxy_upstream_funcs;
}

void cproxy_init_downstream_conn(conn *c) {
    downstream *d = c->extra;
    assert(d != NULL);

    d->ptd->stats.stats.num_downstream_conn++;
    d->ptd->stats.stats.tot_downstream_conn++;
}

void cproxy_on_close_upstream_conn(conn *c) {
    assert(c != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_on_close_upstream_conn\n", c->sfd);
    }

    proxy_td *ptd = c->extra;
    assert(ptd != NULL);
    c->extra = NULL;

    if (ptd->stats.stats.num_upstream > 0) {
        ptd->stats.stats.num_upstream--;
    }

    // Delink from any reserved downstream.
    //
    for (downstream *d = ptd->downstream_reserved; d != NULL; d = d->next) {
        bool found = false;

        d->upstream_conn = conn_list_remove(d->upstream_conn, NULL,
                                            c, &found);
        if (d->upstream_conn == NULL) {
            d->upstream_suffix = NULL;
            d->upstream_suffix_len = 0;
            d->upstream_retry = 0;

            // Don't need to do anything else, as we'll now just
            // read and drop any remaining inflight downstream replies.
            // Eventually, the downstream will be released.
        }

        // If the downstream was reserved for this upstream conn,
        // also clear the upstream from any multiget de-duplication
        // tracking structures.
        //
        if (found) {
            if (d->multiget != NULL) {
                genhash_iter(d->multiget, multiget_remove_upstream, c);
            }

            // The downstream conn's might have iov's that
            // point to the upstream conn's buffers.  Also, the
            // downstream conn might be in all sorts of states
            // (conn_read, write, mwrite, pause), and we want
            // to be careful about the downstream channel being
            // half written.
            //
            // The safest, but inefficient, thing to do then is
            // to close any conn_mwrite downstream conns.
            //
            ptd->stats.stats.tot_downstream_close_on_upstream_close++;

            int n = mcs_server_count(&d->mst);

            for (int i = 0; i < n; i++) {
                conn *downstream_conn = d->downstream_conns[i];
                if (downstream_conn != NULL &&
                    downstream_conn != NULL_CONN &&
                    downstream_conn->state == conn_mwrite) {
                    downstream_conn->msgcurr = 0;
                    downstream_conn->msgused = 0;
                    downstream_conn->iovused = 0;

                    cproxy_close_conn(downstream_conn);
                }
            }
        }
    }

    // Delink from wait queue.
    //
    ptd->waiting_any_downstream_head =
        conn_list_remove(ptd->waiting_any_downstream_head,
                         &ptd->waiting_any_downstream_tail,
                         c, NULL);
}

int delink_from_downstream_conns(conn *c) {
    downstream *d = c->extra;
    if (d == NULL) {
        return -1;
    }

    c->extra = NULL;

    int n = mcs_server_count(&d->mst);
    int k = -1; // Index of conn.

    for (int i = 0; i < n; i++) {
        if (d->downstream_conns[i] == c) {
            d->downstream_conns[i] = NULL;

            if (settings.verbose > 2) {
                moxi_log_write(
                        "<%d cproxy_on_close_downstream_conn quit_server\n",
                        c->sfd);
            }

            d->ptd->stats.stats.tot_downstream_quit_server++;

            mcs_server_st_quit(mcs_server_index(&d->mst, i), 1);
            assert(mcs_server_st_fd(mcs_server_index(&d->mst, i)) == -1);

            k = i;
        }
    }

    return k;
}

void cproxy_on_close_downstream_conn(conn *c) {
    assert(c != NULL);
    assert(c->sfd >= 0);
    assert(c->state == conn_closing);

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_on_close_downstream_conn\n", c->sfd);
    }

    downstream *d = c->extra;

    // Might have been set to NULL during cproxy_free_downstream().
    // Or, when a downstream conn is in the thread-based free pool, it
    // is not associated with any particular downstream.
    //
    if (d == NULL) {
        // TODO: See if we need to remove the downstream conn from the
        // thread-based free pool.  This shouldn't happen, but we
        // should then figure out how to put an assert() here.
        //
        return;
    }

    int k = delink_from_downstream_conns(c);

    proxy_td *ptd = d->ptd;
    assert(ptd);

    if (ptd->stats.stats.num_downstream_conn > 0) {
        ptd->stats.stats.num_downstream_conn--;
    }

    conn *uc_retry = NULL;

    if (d->upstream_conn != NULL &&
        d->downstream_used == 1) {
        // TODO: Revisit downstream close error handling.
        //       Should we propagate error when...
        //       - any downstream conn closes?
        //       - all downstream conns closes?
        //       - last downstream conn closes?  Current behavior.
        //
        if (d->upstream_suffix == NULL) {
            d->upstream_suffix = "SERVER_ERROR proxy downstream closed\r\n";
            d->upstream_suffix_len = 0;
            d->upstream_retry = 0;
        }

        // We sometimes see that drive_machine/transmit will not see
        // a closed connection error during conn_mwrite, possibly
        // due to non-blocking sockets.  Because of this, drive_machine
        // thinks it has a successful downstream request send and
        // moves the state forward trying to read a response from
        // the downstream conn (conn_new_cmd, conn_read, etc), and
        // only then do we finally see the conn close situation,
        // ending up here.  That is, drive_machine only
        // seems to move to conn_closing from conn_read.
        //
        // If we haven't received any reply yet, we retry based
        // on our cmd_retries counter.
        //
        // TODO: Reconsider retry behavior, is it right in all situations?
        //
        if (c->rcurr != NULL &&
            c->rbytes == 0 &&
            d->downstream_used_start == d->downstream_used &&
            d->downstream_used_start == 1 &&
            d->upstream_conn->next == NULL &&
            d->behaviors_arr != NULL) {
            if (k >= 0 && k < d->behaviors_num) {
                int retry_max = d->behaviors_arr[k].downstream_retry;
                if (d->upstream_conn->cmd_retries < retry_max) {
                    d->upstream_conn->cmd_retries++;
                    uc_retry = d->upstream_conn;
                    d->upstream_suffix = NULL;
                    d->upstream_suffix_len = 0;
                    d->upstream_retry = 0;
                }
            }
        }
    }

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

    // Setup a retry after unwinding the call stack.
    // We use the work_queue, because our caller, conn_close(),
    // is likely to blow away our fd if we try to reconnect
    // right now.
    //
    if (uc_retry != NULL) {
        if (settings.verbose > 2) {
            moxi_log_write("%d cproxy retrying\n", uc_retry->sfd);
        }

        ptd->stats.stats.tot_retry++;

        assert(uc_retry->thread);
        assert(uc_retry->thread->work_queue);

        work_send(uc_retry->thread->work_queue,
                  upstream_retry, ptd, uc_retry);
    }
}

void upstream_retry(void *data0, void *data1) {
    proxy_td *ptd = data0;
    assert(ptd);

    conn *uc = data1;
    assert(uc);

    cproxy_pause_upstream_for_downstream(ptd, uc);
}

void cproxy_add_downstream(proxy_td *ptd) {
    assert(ptd != NULL);
    assert(ptd->proxy != NULL);

    if (ptd->downstream_num < ptd->downstream_max) {
        if (settings.verbose > 2) {
            moxi_log_write("cproxy_add_downstream %d %d\n",
                    ptd->downstream_num,
                    ptd->downstream_max);
        }

        // The config/behaviors will be NULL if the
        // proxy is shutting down.
        //
        if (ptd->config != NULL &&
            ptd->behavior_pool.arr != NULL) {
            downstream *d =
                cproxy_create_downstream(ptd->config,
                                         ptd->config_ver,
                                         &ptd->behavior_pool);
            if (d != NULL) {
                d->ptd = ptd;
                ptd->downstream_tot++;
                ptd->downstream_num++;
                cproxy_release_downstream(d, true);
            } else {
                ptd->stats.stats.tot_downstream_create_failed++;
            }
        }
    } else {
        ptd->stats.stats.tot_downstream_max_reached++;
    }
}

downstream *cproxy_reserve_downstream(proxy_td *ptd) {
    assert(ptd != NULL);

    // Loop in case we need to clear out downstreams
    // that have outdated configs.
    //
    while (true) {
        downstream *d;

        d = ptd->downstream_released;
        if (d == NULL) {
            cproxy_add_downstream(ptd);
        }

        d = ptd->downstream_released;
        if (d == NULL) {
            return NULL;
        }

        ptd->downstream_released = d->next;

        assert(d->upstream_conn == NULL);
        assert(d->upstream_suffix == NULL);
        assert(d->upstream_suffix_len == 0);
        assert(d->upstream_retry == 0);
        assert(d->downstream_used == 0);
        assert(d->downstream_used_start == 0);
        assert(d->merger == NULL);
        assert(d->timeout_tv.tv_sec == 0);
        assert(d->timeout_tv.tv_usec == 0);
        assert(d->next_waiting == NULL);

        d->upstream_conn = NULL;
        d->upstream_suffix = NULL;
        d->upstream_suffix_len = 0;
        d->upstream_retry = 0;
        d->upstream_retries = 0;
        d->usec_start = 0;
        d->downstream_used = 0;
        d->downstream_used_start = 0;
        d->merger = NULL;
        d->timeout_tv.tv_sec = 0;
        d->timeout_tv.tv_usec = 0;
        d->next_waiting = NULL;

        if (cproxy_check_downstream_config(d)) {
            ptd->downstream_reserved =
                downstream_list_remove(ptd->downstream_reserved, d);
            ptd->downstream_released =
                downstream_list_remove(ptd->downstream_released, d);

            zstored_downstream_waiting_remove(d);

            d->next = ptd->downstream_reserved;
            ptd->downstream_reserved = d;

            ptd->stats.stats.tot_downstream_reserved++;

            return d;
        }

        cproxy_free_downstream(d);
    }
}

bool cproxy_release_downstream(downstream *d, bool force) {
    assert(d != NULL);
    assert(d->ptd != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("%d: release_downstream\n",
                       d->upstream_conn != NULL ?
                       d->upstream_conn->sfd : -1);
    }

    // Always release the timeout_event, even if we're going to retry,
    // to avoid pegging CPU with leaked timeout_events.
    //
    if (d->timeout_tv.tv_sec != 0 ||
        d->timeout_tv.tv_usec != 0) {
        evtimer_del(&d->timeout_event);
    }

    d->timeout_tv.tv_sec = 0;
    d->timeout_tv.tv_usec = 0;

    // If we need to retry the command, we do so here,
    // keeping the same downstream that would otherwise
    // be released.
    //
    if (!force &&
        d->upstream_retry > 0) {
        d->upstream_retry = 0;
        d->upstream_retries++;

        // But, we can stop retrying if we've tried each server twice.
        //
        int max_retries = cproxy_max_retries(d);

        if (d->upstream_retries <= max_retries) {
            if (settings.verbose > 2) {
                moxi_log_write("%d: release_downstream,"
                               " instead retrying %d, %d <= %d, %d\n",
                               d->upstream_conn->sfd,
                               d->upstream_retry,
                               d->upstream_retries, max_retries,
                               d->ptd->stats.stats.tot_retry);
            }

            d->ptd->stats.stats.tot_retry++;

            if (cproxy_forward(d) == true) {
                return true;
            } else {
                d->ptd->stats.stats.tot_downstream_propagate_failed++;

                propagate_error(d);
            }
        } else {
            if (settings.verbose > 2) {
                moxi_log_write("%d: release_downstream,"
                               " skipping retry %d, %d > %d\n",
                               d->upstream_conn->sfd,
                               d->upstream_retry,
                               d->upstream_retries, max_retries);
            }
        }
    }

    // Record reserved_time histogram timings.
    //
    if (d->usec_start > 0) {
        uint64_t ux = usec_now() - d->usec_start;

        d->ptd->stats.stats.tot_downstream_reserved_time += ux;

        if (d->ptd->stats.stats.max_downstream_reserved_time < ux) {
            d->ptd->stats.stats.max_downstream_reserved_time = ux;
        }

        if (d->upstream_retries > 0) {
            d->ptd->stats.stats.tot_retry_time += ux;

            if (d->ptd->stats.stats.max_retry_time < ux) {
                d->ptd->stats.stats.max_retry_time = ux;
            }
        }

        downstream_reserved_time_sample(&d->ptd->stats, ux);
    }

    d->ptd->stats.stats.tot_downstream_released++;

    // Delink upstream conns.
    //
    while (d->upstream_conn != NULL) {
        if (d->merger != NULL) {
            // TODO: Allow merger callback to be func pointer.
            //
            genhash_iter(d->merger,
                        protocol_stats_foreach_write,
                        d->upstream_conn);

            if (update_event(d->upstream_conn, EV_WRITE | EV_PERSIST)) {
                conn_set_state(d->upstream_conn, conn_mwrite);
            } else {
                d->ptd->stats.stats.err_oom++;
                cproxy_close_conn(d->upstream_conn);
            }
        }

        if (d->upstream_suffix != NULL) {
            // Do a last write on the upstream.  For example,
            // the upstream_suffix might be "END\r\n" or other
            // way to mark the end of a scatter-gather or
            // multiline response.
            //
            if (settings.verbose > 2) {
                if (d->upstream_suffix_len > 0) {
                    moxi_log_write("%d: release_downstream"
                                   " writing suffix binary: %d\n",
                                   d->upstream_conn->sfd,
                                   d->upstream_suffix_len);

                    cproxy_dump_header(d->upstream_conn->sfd,
                                       d->upstream_suffix);
                } else {
                    moxi_log_write("%d: release_downstream"
                                   " writing suffix ascii: %s\n",
                                   d->upstream_conn->sfd,
                                   d->upstream_suffix);
                }
            }

            int suffix_len = d->upstream_suffix_len;
            if (suffix_len == 0) {
                suffix_len = strlen(d->upstream_suffix);
            }

            if (add_iov(d->upstream_conn,
                        d->upstream_suffix,
                        suffix_len) == 0 &&
                update_event(d->upstream_conn, EV_WRITE | EV_PERSIST)) {
                conn_set_state(d->upstream_conn, conn_mwrite);
            } else {
                d->ptd->stats.stats.err_oom++;
                cproxy_close_conn(d->upstream_conn);
            }
        }

        conn *curr = d->upstream_conn;
        d->upstream_conn = d->upstream_conn->next;
        curr->next = NULL;
    }

    // Free extra hash tables.
    //
    if (d->multiget != NULL) {
        genhash_iter(d->multiget, multiget_foreach_free, d);
        genhash_free(d->multiget);
        d->multiget = NULL;
    }

    if (d->merger != NULL) {
        genhash_iter(d->merger, protocol_stats_foreach_free, NULL);
        genhash_free(d->merger);
        d->merger = NULL;
    }

    d->upstream_conn = NULL;
    d->upstream_suffix = NULL; // No free(), expecting a static string.
    d->upstream_suffix_len = 0;
    d->upstream_retry = 0;
    d->upstream_retries = 0;
    d->usec_start = 0;
    d->downstream_used = 0;
    d->downstream_used_start = 0;
    d->multiget = NULL;
    d->merger = NULL;

    int n = mcs_server_count(&d->mst);

    for (int i = 0; i < n; i++) {
        conn *dc = d->downstream_conns[i];
        d->downstream_conns[i] = NULL;
        if (dc != NULL) {
            zstored_release_downstream_conn(dc, false);
        }
    }

    // If this downstream still has the same configuration as our top-level
    // proxy config, go back onto the available, released downstream list.
    //
    if (cproxy_check_downstream_config(d) || force) {
        // TODO: Consider adding a downstream->prev backpointer
        //       or doubly-linked list to save on this scan.
        //
        d->ptd->downstream_reserved =
            downstream_list_remove(d->ptd->downstream_reserved, d);
        d->ptd->downstream_released =
            downstream_list_remove(d->ptd->downstream_released, d);

        zstored_downstream_waiting_remove(d);

        d->next = d->ptd->downstream_released;
        d->ptd->downstream_released = d;

        return true;
    }

    cproxy_free_downstream(d);

    return false;
}

void cproxy_free_downstream(downstream *d) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->upstream_conn == NULL);
    assert(d->multiget == NULL);
    assert(d->merger == NULL);
    assert(d->timeout_tv.tv_sec == 0);
    assert(d->timeout_tv.tv_usec == 0);

    if (settings.verbose > 2) {
        moxi_log_write("cproxy_free_downstream\n");
    }

    d->ptd->stats.stats.tot_downstream_freed++;

    d->ptd->downstream_reserved =
        downstream_list_remove(d->ptd->downstream_reserved, d);
    d->ptd->downstream_released =
        downstream_list_remove(d->ptd->downstream_released, d);

    zstored_downstream_waiting_remove(d);

    d->ptd->downstream_num--;
    assert(d->ptd->downstream_num >= 0);

    int n = mcs_server_count(&d->mst);

    if (d->downstream_conns != NULL) {
        for (int i = 0; i < n; i++) {
            if (d->downstream_conns[i] != NULL &&
                d->downstream_conns[i] != NULL_CONN) {
                d->downstream_conns[i]->extra = NULL;
            }
        }
    }

    // This will close sockets, which will force associated conn's
    // to go to conn_closing state.  Since we've already cleared
    // the conn->extra pointers, there's no extra release/free.
    //
    mcs_free(&d->mst);

    if (d->timeout_tv.tv_sec != 0 ||
        d->timeout_tv.tv_usec != 0) {
        evtimer_del(&d->timeout_event);
    }

    d->timeout_tv.tv_sec = 0;
    d->timeout_tv.tv_usec = 0;

    if (d->downstream_conns != NULL) {
        free(d->downstream_conns);
    }

    if (d->config != NULL) {
        free(d->config);
    }

    free(d);
}

/* The config input is something libmemcached can parse.
 * See mcs_server_st_parse().
 */
downstream *cproxy_create_downstream(char *config,
                                     uint32_t config_ver,
                                     proxy_behavior_pool *behavior_pool) {
    assert(config != NULL);
    assert(behavior_pool != NULL);

    downstream *d = (downstream *) calloc(1, sizeof(downstream));
    if (d != NULL &&
        config != NULL &&
        config[0] != '\0') {
        d->config        = strdup(config);
        d->config_ver    = config_ver;
        d->behaviors_num = behavior_pool->num;
        d->behaviors_arr = cproxy_copy_behaviors(behavior_pool->num,
                                                 behavior_pool->arr);

        // TODO: Handle non-uniform downstream protocols.
        //
        assert(IS_PROXY(behavior_pool->base.downstream_protocol));

        if (settings.verbose > 2) {
            moxi_log_write(
                    "cproxy_create_downstream: %s, %u, %u\n",
                    config, config_ver,
                    behavior_pool->base.downstream_protocol);
        }

        if (d->config != NULL &&
            d->behaviors_arr != NULL) {
            int nconns = init_mcs_st(&d->mst, d->config);
            if (nconns > 0) {
                d->downstream_conns = (conn **)
                    calloc(nconns, sizeof(conn *));
                if (d->downstream_conns != NULL) {
                    return d;
                }

                mcs_free(&d->mst);
            }
        }

        free(d->config);
        free(d->behaviors_arr);
        free(d);
    }

    return NULL;
}

int init_mcs_st(mcs_st *mst, char *config) {
    assert(mst);
    assert(config);

    if (mcs_create(mst, config) != NULL) {
        return mcs_server_count(mst);
    } else {
        if (settings.verbose > 1) {
            moxi_log_write("mcs_create failed: %s\n",
                    config);
        }
    }

    return 0;
}

/* See if the downstream config matches the top-level proxy config.
 */
bool cproxy_check_downstream_config(downstream *d) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);

    int rv = false;

    if (d->config_ver == d->ptd->config_ver) {
        rv = true;
    } else if (d->config != NULL &&
               d->ptd->config != NULL &&
               cproxy_equal_behaviors(d->behaviors_num,
                                      d->behaviors_arr,
                                      d->ptd->behavior_pool.num,
                                      d->ptd->behavior_pool.arr)) {
        // Parse the proxy/parent's config to see if we can
        // reuse our existing downstream connections.
        //
        mcs_st next;

        int n = init_mcs_st(&next, d->ptd->config);
        if (n > 0) {
            if (mcs_stable_update(&d->mst, &next)) {
                if (settings.verbose > 2) {
                    moxi_log_write("check_downstream_config stable update\n");
                }

                free(d->config);
                d->config     = strdup(d->ptd->config);
                d->config_ver = d->ptd->config_ver;
                rv = true;
            }

            mcs_free(&next);
        }
    }

    if (settings.verbose > 2) {
        moxi_log_write("check_downstream_config %u\n", rv);
    }

    return rv;
}

// Returns -1 if the connections aren't fully assigned and ready.
// In that case, the downstream has to wait for a downstream connection
// to get out of the conn_connecting state.
//
// The downstream connection might leave the conn_connecting state
// with an error (unable to connect).  That case is handled by
// tracking a NULL_CONN sentinel value.
//
// Also, in the -1 result case, the d->upstream_conn should remain in
// conn_pause state.
//
// A server_index of -1 means to connect all downstreams, as the
// caller probably needs to proxy a broadcast command.
//
int cproxy_connect_downstream(downstream *d, LIBEVENT_THREAD *thread,
                              int server_index) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->downstream_released != d); // Should not be in free list.
    assert(d->downstream_conns != NULL);
    assert(mcs_server_count(&d->mst) > 0);
    assert(thread != NULL);
    assert(thread->base != NULL);

    int s = 0; // Number connected.
    int n = mcs_server_count(&d->mst);
    mcs_server_st msst;
    mcs_server_st *msst_actual;

    assert(d->behaviors_num >= n);
    assert(d->behaviors_arr != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("%d: cproxy_connect_downstream server_index %d in %d\n",
                       d->upstream_conn->sfd, server_index, n);
    }

    int i = 0;

    if (server_index >= 0) {
        assert(server_index < n);
        i = server_index;
        n = server_index + 1;
    }

    for (; i < n; i++) {
        assert(IS_PROXY(d->behaviors_arr[i].downstream_protocol));

        msst_actual = mcs_server_index(&d->mst, i);

        // Connect to downstream servers, if not already.
        //
        // A NULL_CONN means that we tried to connect, but failed,
        // which is different than NULL (which means that we haven't
        // tried to connect yet).
        //
        if (d->downstream_conns[i] == NULL) {
            conn *c = d->upstream_conn;
            /*
             * mcmux compatiblity mode, one downstream struct will be associated
             * with downstream connection
             */
            if (c && c->peer_host && c->peer_port) {
                bzero(&msst, sizeof(mcs_server_st));
                memcpy(msst.hostname, c->peer_host, strlen(c->peer_host));
                msst.port = c->peer_port;
                msst.fd = -1;
                msst_actual = &msst;
            }

            bool downstream_conn_max_reached = false;

            d->downstream_conns[i] =
                zstored_acquire_downstream_conn(d, thread,
                                                msst_actual,
                                                &d->behaviors_arr[i],
                                                &downstream_conn_max_reached);
            if (d->downstream_conns[i] != NULL &&
                d->downstream_conns[i] != NULL_CONN &&
                d->downstream_conns[i]->state == conn_connecting) {
                return -1;
            }

            if (d->downstream_conns[i] == NULL &&
                downstream_conn_max_reached == true &&
                zstored_downstream_waiting_add(d, thread,
                                               msst_actual,
                                               &d->behaviors_arr[i]) == true) {
                return -1;
            }
        }

        if (d->downstream_conns[i] != NULL &&
            d->downstream_conns[i] != NULL_CONN) {
            s++;
        } else {
            mcs_server_st_quit(msst_actual, 1);
            d->downstream_conns[i] = NULL_CONN;
        }
    }

    return s;
}

conn *cproxy_connect_downstream_conn(downstream *d,
                                     LIBEVENT_THREAD *thread,
                                     mcs_server_st *msst,
                                     proxy_behavior *behavior) {
    assert(d);
    assert(d->ptd);
    assert(d->ptd->downstream_released != d); // Should not be in free list.
    assert(thread);
    assert(thread->base);
    assert(msst);
    assert(behavior);
    assert(mcs_server_st_hostname(msst) != NULL);
    assert(mcs_server_st_port(msst) > 0);
    assert(mcs_server_st_fd(msst) == -1);

    uint64_t start = 0;

    if (d->ptd->behavior_pool.base.time_stats) {
        start = usec_now();
    }

    int err = -1;
    int fd = mcs_connect(mcs_server_st_hostname(msst),
                         mcs_server_st_port(msst), &err, false);
    if (fd != -1) {
        if (settings.verbose > 2) {
            moxi_log_write("%d: cproxy_connect_downstream_conn %s:%d\n", fd,
                           mcs_server_st_hostname(msst),
                           mcs_server_st_port(msst));
        }

        conn *c = conn_new(fd, conn_pause, 0,
                           DATA_BUFFER_SIZE,
                           tcp_transport,
                           thread->base,
                           &cproxy_downstream_funcs, d);
        if (c != NULL ) {
            c->protocol = behavior->downstream_protocol;
            c->thread = thread;
            c->cmd_start_time = start;

            if (err == EINPROGRESS) {
                struct timeval connect_timeout;

                connect_timeout.tv_sec = 5;
                connect_timeout.tv_usec = 0;

                if (update_event_timed(c, EV_WRITE | EV_PERSIST,
                                       &connect_timeout)) {
                    conn_set_state(c, conn_connecting);

                    return c;
                } else {
                    d->ptd->stats.stats.err_oom++;
                }
            } else {
                if (downstream_connect_init(d, msst, behavior, c)) {
                    return c;
                }
            }

            cproxy_close_conn(c);
        } else {
            d->ptd->stats.stats.err_oom++;
        }
    } else {
        d->ptd->stats.stats.tot_downstream_connect_failed++;
    }

    return NULL;
}

bool downstream_connect_init(downstream *d, mcs_server_st *msst,
                             proxy_behavior *behavior, conn *c) {
    assert(c->thread != NULL);
    assert(c->host_ident != NULL);

    d->ptd->stats.stats.tot_downstream_connect++;

    zstored_error_count(c->thread, c->host_ident, false);

    if (c->cmd_start_time != 0 &&
        d->ptd->behavior_pool.base.time_stats) {
        downstream_connect_time_sample(&d->ptd->stats,
                                       usec_now() - c->cmd_start_time);
    }

    if (cproxy_auth_downstream(msst, behavior, c->sfd)) {
        d->ptd->stats.stats.tot_downstream_auth++;

        if (cproxy_bucket_downstream(msst, behavior, c->sfd)) {
            d->ptd->stats.stats.tot_downstream_bucket++;

            return true;
        } else {
            d->ptd->stats.stats.tot_downstream_bucket_failed++;
        }
    } else {
        d->ptd->stats.stats.tot_downstream_auth_failed++;
    }

    return false;
}

conn *cproxy_find_downstream_conn(downstream *d,
                                  char *key, int key_length,
                                  bool *self) {
    return cproxy_find_downstream_conn_ex(d, key, key_length, self, NULL);
}

conn *cproxy_find_downstream_conn_ex(downstream *d,
                                     char *key, int key_length,
                                     bool *self,
                                     int *vbucket) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(key != NULL);
    assert(key_length > 0);

    if (self != NULL) {
        *self = false;
    }

    int v = -1;
    int s = cproxy_server_index(d, key, key_length, &v);

    if (settings.verbose > 2) {
        moxi_log_write("%d: server_index %d, vbucket %d, conn %d\n", s, v,
                       (d->upstream_conn != NULL ?
                        d->upstream_conn->sfd : 0),
                       (d->downstream_conns[s] == NULL ?
                        0 :
                        (d->downstream_conns[s] == NULL_CONN ?
                         -1 :
                         d->downstream_conns[s]->sfd)));
    }

    if (s >= 0 &&
        s < (int) mcs_server_count(&d->mst) &&
        d->downstream_conns[s] != NULL &&
        d->downstream_conns[s] != NULL_CONN) {
        if (self != NULL &&
            settings.port > 0 &&
            settings.port == mcs_server_st_port(mcs_server_index(&d->mst, s)) &&
            strcmp(mcs_server_st_hostname(mcs_server_index(&d->mst, s)),
                   cproxy_hostname) == 0) {
            *self = true;
        }

        if (vbucket != NULL) {
            *vbucket = v;
        }

        return d->downstream_conns[s];
    }

    return NULL;
}

bool cproxy_prep_conn_for_write(conn *c) {
    if (c != NULL) {
        assert(c->item == NULL);
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

        if (add_msghdr(c) == 0) {
            return true;
        }

        if (settings.verbose > 1) {
            moxi_log_write("%d: cproxy_prep_conn_for_write failed\n",
                           c->sfd);
        }
    }

    return false;
}

bool cproxy_update_event_write(downstream *d, conn *c) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(c != NULL);

    if (!update_event(c, EV_WRITE | EV_PERSIST)) {
        d->ptd->stats.stats.err_oom++;
        cproxy_close_conn(c);

        return false;
    }

    return true;
}

/**
 * Do a hash through libmemcached to see which server (by index)
 * should hold a given key.
 */
int cproxy_server_index(downstream *d, char *key, size_t key_length,
                        int *vbucket) {
    assert(d != NULL);
    assert(key != NULL);
    assert(key_length > 0);

    if (mcs_server_count(&d->mst) <= 0) {
        return -1;
    }

    return (int) mcs_key_hash(&d->mst, key, key_length, vbucket);
}

void cproxy_assign_downstream(proxy_td *ptd) {
    assert(ptd != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("assign_downstream\n");
    }

    ptd->downstream_assigns++;

    uint64_t da = ptd->downstream_assigns;

    // Key loop that tries to reserve any available, released
    // downstream resources to waiting upstream conns.
    //
    // Remember the wait list tail when we start, in case more
    // upstream conns are tacked onto the wait list while we're
    // processing.  This helps avoid infinite loop where upstream
    // conns just keep on moving to the tail.
    //
    conn *tail = ptd->waiting_any_downstream_tail;
    bool  stop = false;

    while (ptd->waiting_any_downstream_head != NULL && !stop) {
        if (ptd->waiting_any_downstream_head == tail) {
            stop = true;
        }

        downstream *d = cproxy_reserve_downstream(ptd);
        if (d == NULL) {
            if (ptd->downstream_num <= 0) {
                // Absolutely no downstreams connected, so
                // might as well error out.
                //
                while (ptd->waiting_any_downstream_head != NULL) {
                    ptd->stats.stats.tot_downstream_propagate_failed++;

                    conn *uc = ptd->waiting_any_downstream_head;
                    ptd->waiting_any_downstream_head =
                        ptd->waiting_any_downstream_head->next;
                    if (ptd->waiting_any_downstream_head == NULL) {
                        ptd->waiting_any_downstream_tail = NULL;
                    }
                    uc->next = NULL;

                    upstream_error(uc);
                }
            }

            break; // If no downstreams are available, stop loop.
        }

        assert(d->upstream_conn == NULL);
        assert(d->downstream_used == 0);
        assert(d->downstream_used_start == 0);
        assert(d->multiget == NULL);
        assert(d->merger == NULL);
        assert(d->timeout_tv.tv_sec == 0);
        assert(d->timeout_tv.tv_usec == 0);

        // We have a downstream reserved, so assign the first
        // waiting upstream conn to it.
        //
        d->upstream_conn = ptd->waiting_any_downstream_head;
        ptd->waiting_any_downstream_head =
            ptd->waiting_any_downstream_head->next;
        if (ptd->waiting_any_downstream_head == NULL) {
            ptd->waiting_any_downstream_tail = NULL;
        }
        d->upstream_conn->next = NULL;

        ptd->stats.stats.tot_assign_downstream++;
        ptd->stats.stats.tot_assign_upstream++;

        // Add any compatible upstream conns to the downstream.
        // By compatible, for example, we mean multi-gets from
        // different upstreams so we can de-deplicate get keys.
        //
        conn *uc_last = d->upstream_conn;

        while (is_compatible_request(uc_last,
                                     ptd->waiting_any_downstream_head)) {
            uc_last->next = ptd->waiting_any_downstream_head;

            ptd->waiting_any_downstream_head =
                ptd->waiting_any_downstream_head->next;
            if (ptd->waiting_any_downstream_head == NULL) {
                ptd->waiting_any_downstream_tail = NULL;
            }

            uc_last = uc_last->next;
            uc_last->next = NULL;

            // Note: tot_assign_upstream - tot_assign_downstream
            // should get us how many requests we've piggybacked together.
            //
            ptd->stats.stats.tot_assign_upstream++;
        }

        if (settings.verbose > 2) {
            moxi_log_write("%d: assign_downstream, matched to upstream\n",
                    d->upstream_conn->sfd);
        }

        if (cproxy_forward(d) == false) {
            // TODO: This stat is incorrect, as we might reach here
            // when we have entire front cache hit or talk-to-self
            // optimization hit on multiget.
            //
            ptd->stats.stats.tot_downstream_propagate_failed++;

            // During cproxy_forward(), we might have recursed,
            // especially in error situation if a downstream
            // conn got closed and released.  Check for recursion
            // before we touch d anymore.
            //
            if (da != ptd->downstream_assigns) {
                ptd->stats.stats.tot_assign_recursion++;
                break;
            }

            propagate_error(d);

            cproxy_release_downstream(d, false);
        }
    }

    if (settings.verbose > 2) {
        moxi_log_write("assign_downstream, done\n");
    }
}

void propagate_error(downstream *d) {
    assert(d != NULL);

    while (d->upstream_conn != NULL) {
        conn *uc = d->upstream_conn;

        if (settings.verbose > 1) {
            moxi_log_write("%d: could not forward upstream to downstream\n",
                           uc->sfd);
        }

        upstream_error(uc);

        conn *curr = d->upstream_conn;
        d->upstream_conn = d->upstream_conn->next;
        curr->next = NULL;
    }
}

bool cproxy_forward(downstream *d) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->upstream_conn != NULL);

    if (settings.verbose > 2) {
        moxi_log_write(
                "%d: cproxy_forward prot %d to prot %d\n",
                d->upstream_conn->sfd,
                d->upstream_conn->protocol,
                d->ptd->behavior_pool.base.downstream_protocol);
    }

    if (IS_ASCII(d->upstream_conn->protocol)) {
        // ASCII upstream.
        //
        if (IS_ASCII(d->ptd->behavior_pool.base.downstream_protocol)) {
            return cproxy_forward_a2a_downstream(d);
        } else {
            return cproxy_forward_a2b_downstream(d);
        }
    } else {
        // BINARY upstream.
        //
        if (IS_BINARY(d->ptd->behavior_pool.base.downstream_protocol)) {
            return cproxy_forward_b2b_downstream(d);
        } else {
            // TODO: No binary upstream to ascii downstream support.
            //
            assert(0);
            return false;
        }
    }
}

bool cproxy_forward_or_error(downstream *d) {
    if (cproxy_forward(d) == false) {
        d->ptd->stats.stats.tot_downstream_propagate_failed++;
        propagate_error(d);
        cproxy_release_downstream(d, false);

        return false;
    }

    return true;
}

void upstream_error(conn *uc) {
    assert(uc);
    assert(uc->state == conn_pause);

    proxy_td *ptd = uc->extra;
    assert(ptd != NULL);

    if (IS_ASCII(uc->protocol)) {
        char *msg = "SERVER_ERROR proxy write to downstream\r\n";

        // Send an END on get/gets instead of generic SERVER_ERROR.
        //
        if (uc->cmd == -1 &&
            uc->cmd_start != NULL &&
            strncmp(uc->cmd_start, "get", 3) == 0) {
            msg = "END\r\n";
        }

        if (add_iov(uc, msg, strlen(msg)) == 0 &&
            update_event(uc, EV_WRITE | EV_PERSIST)) {
            conn_set_state(uc, conn_mwrite);
        } else {
            ptd->stats.stats.err_oom++;
            cproxy_close_conn(uc);
        }
    } else {
        assert(IS_BINARY(uc->protocol));

        write_bin_error(uc, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
    }
}

void cproxy_reset_upstream(conn *uc) {
    assert(uc != NULL);

    proxy_td *ptd = uc->extra;
    assert(ptd != NULL);

    conn_set_state(uc, conn_new_cmd);

    if (uc->rbytes <= 0) {
        if (!update_event(uc, EV_READ | EV_PERSIST)) {
            ptd->stats.stats.err_oom++;
            cproxy_close_conn(uc);
        }

        return; // Return either way.
    }

    // We may have already read incoming bytes into the uc's buffer,
    // so the issue is that libevent may never see (or expect) any
    // EV_READ events (and hence, won't fire event callbacks) for the
    // upstream connection.  This can leave the uc seemingly stuck,
    // never hitting drive_machine() loop.
    //
    if (settings.verbose > 2) {
        moxi_log_write("%d: cproxy_reset_upstream with bytes available: %d\n",
                       uc->sfd, uc->rbytes);
    }

    // So, we copy the drive_machine()/conn_new_cmd handling to
    // schedule uc into drive_machine() execution, where the uc
    // conn is likely to be writable.  We need to do this
    // because we're currently on the drive_machine() execution
    // loop for the downstream connection, not for the uc.
    //
    if (!update_event(uc, EV_WRITE | EV_PERSIST)) {
        if (settings.verbose > 0) {
            moxi_log_write("Couldn't update event\n");
        }

        conn_set_state(uc, conn_closing);
    }

    ptd->stats.stats.tot_reset_upstream_avail++;
}

bool cproxy_dettach_if_noreply(downstream *d, conn *uc) {
    if (uc->noreply) {
        uc->noreply        = false;
        d->upstream_conn   = NULL;
        d->upstream_suffix = NULL;
        d->upstream_suffix_len = 0;
        d->upstream_retry  = 0;

        cproxy_reset_upstream(uc);

        return true;
    }

    return false;
}

void cproxy_wait_any_downstream(proxy_td *ptd, conn *uc) {
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(ptd != NULL);
    assert(!ptd->waiting_any_downstream_tail ||
           !ptd->waiting_any_downstream_tail->next);

    // Add the upstream conn to the wait list.
    //
    uc->next = NULL;
    if (ptd->waiting_any_downstream_tail != NULL) {
        ptd->waiting_any_downstream_tail->next = uc;
    }
    ptd->waiting_any_downstream_tail = uc;
    if (ptd->waiting_any_downstream_head == NULL) {
        ptd->waiting_any_downstream_head = uc;
    }
}

void cproxy_release_downstream_conn(downstream *d, conn *c) {
    assert(c != NULL);
    assert(d != NULL);

    proxy_td *ptd = d->ptd;
    assert(ptd != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("%d: release_downstream_conn, downstream_used %d %d,"
                       " upstream %d\n",
                       c->sfd, d->downstream_used, d->downstream_used_start,
                       (d->upstream_conn != NULL ?
                        d->upstream_conn->sfd : 0));
    }

    d->downstream_used--;
    if (d->downstream_used <= 0) {
        // The downstream_used count might go < 0 when if there's
        // an early error and we decide to close the downstream
        // conn, before anything gets sent or before the
        // downstream_used was able to be incremented.
        //
        cproxy_release_downstream(d, false);
        cproxy_assign_downstream(ptd);
    }
}

void cproxy_on_pause_downstream_conn(conn *c) {
    assert(c != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_on_pause_downstream_conn\n",
                c->sfd);
    }

    downstream *d = c->extra;
    assert(d != NULL);
    assert(d->ptd != NULL);

    // Must update_event() before releasing the downstream conn,
    // because the release might call udpate_event(), too,
    // and we don't want to override its work.
    //
    if (update_event(c, 0)) {
        cproxy_release_downstream_conn(d, c);
    } else {
        d->ptd->stats.stats.err_oom++;
        cproxy_close_conn(c);
    }
}

void cproxy_pause_upstream_for_downstream(proxy_td *ptd, conn *upstream) {
    assert(ptd != NULL);
    assert(upstream != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("%d: pause_upstream_for_downstream\n",
                upstream->sfd);
    }

    conn_set_state(upstream, conn_pause);

    cproxy_wait_any_downstream(ptd, upstream);

    if (ptd->timeout_tv.tv_sec == 0 &&
        ptd->timeout_tv.tv_usec == 0) {
        cproxy_start_wait_queue_timeout(ptd, upstream);
    }

    cproxy_assign_downstream(ptd);
}

struct timeval cproxy_get_downstream_timeout(downstream *d, conn *c) {
    assert(d);

    struct timeval rv;

    if (c != NULL) {
        assert(d->behaviors_num > 0);
        assert(d->behaviors_arr != NULL);
        assert(d->downstream_conns != NULL);

        int i = downstream_conn_index(d, c);
        if (i >= 0 && i < d->behaviors_num) {
            rv = d->behaviors_arr[i].downstream_timeout;
            if (rv.tv_sec != 0 ||
                rv.tv_usec != 0) {
                return rv;
            }
        }
    }

    proxy_td *ptd = d->ptd;
    assert(ptd);

    rv = ptd->behavior_pool.base.downstream_timeout;

    return rv;
}

bool cproxy_start_wait_queue_timeout(proxy_td *ptd, conn *uc) {
    assert(ptd);
    assert(uc);
    assert(uc->thread);
    assert(uc->thread->base);

    ptd->timeout_tv = ptd->behavior_pool.base.wait_queue_timeout;
    if (ptd->timeout_tv.tv_sec != 0 ||
        ptd->timeout_tv.tv_usec != 0) {
        if (settings.verbose > 2) {
            moxi_log_write("wait_queue_timeout started\n");
        }

        evtimer_set(&ptd->timeout_event, wait_queue_timeout, ptd);

        event_base_set(uc->thread->base, &ptd->timeout_event);

        return evtimer_add(&ptd->timeout_event, &ptd->timeout_tv) == 0;
    }

    return true;
}

void wait_queue_timeout(const int fd,
                        const short which,
                        void *arg) {
    (void)fd;
    (void)which;
    proxy_td *ptd = arg;
    assert(ptd != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("wait_queue_timeout\n");
    }

    // This timer callback is invoked when an upstream conn
    // has been in the wait queue for too long.
    //
    if (ptd->timeout_tv.tv_sec != 0 ||
        ptd->timeout_tv.tv_usec != 0) {
        evtimer_del(&ptd->timeout_event);

        ptd->timeout_tv.tv_sec = 0;
        ptd->timeout_tv.tv_usec = 0;

        if (settings.verbose > 2) {
            moxi_log_write("wait_queue_timeout cleared\n");
        }

        struct timeval wqt = ptd->behavior_pool.base.wait_queue_timeout;

        // TODO: Millisecond capacity in 32-bit field not enough?
        //
        uint32_t wqt_msec = (wqt.tv_sec * 1000) +
                            (wqt.tv_usec / 1000);

        uint32_t cut_msec = msec_current_time - wqt_msec;

        // Run through all the old upstream conn's in
        // the wait queue, remove them, and emit errors
        // on them.  And then start a new timer if needed.
        //
        conn *uc_curr = ptd->waiting_any_downstream_head;
        while (uc_curr != NULL) {
            conn *uc = uc_curr;

            uc_curr = uc_curr->next;

            // Check if upstream conn is old and should be removed.
            //
            if (settings.verbose > 2) {
                moxi_log_write("wait_queue_timeout compare %u to %u cutoff\n",
                        uc->cmd_start_time, cut_msec);
            }

            if (uc->cmd_start_time <= cut_msec) {
                if (settings.verbose > 1) {
                    moxi_log_write("proxy_td_timeout sending error %d\n",
                            uc->sfd);
                }

                ptd->stats.stats.tot_wait_queue_timeout++;

                ptd->waiting_any_downstream_head =
                    conn_list_remove(ptd->waiting_any_downstream_head,
                                     &ptd->waiting_any_downstream_tail,
                                     uc, NULL); // TODO: O(N^2).

                upstream_error(uc);
            }
        }

        if (ptd->waiting_any_downstream_head != NULL) {
            cproxy_start_wait_queue_timeout(ptd,
                                            ptd->waiting_any_downstream_head);
        }
    }
}

rel_time_t cproxy_realtime(const time_t exptime) {
    // Input is a long...
    //
    // 0       | (0...REALIME_MAXDELTA] | (REALTIME_MAXDELTA...
    // forever | delta                  | unix_time
    //
    // Storage is an unsigned int.
    //
    // TODO: Handle resolution loss.
    //
    // The cproxy version of realtime doesn't do any
    // time math munging, just pass through.
    //
    return (rel_time_t) exptime;
}

void cproxy_close_conn(conn *c) {
    assert(c != NULL);

    if (c == NULL_CONN) {
        return;
    }

    conn_set_state(c, conn_closing);

    update_event(c, 0);

    // Run through drive_machine just once,
    // to go through close code paths.
    //
    drive_machine(c);
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
        char *suffix = cache_alloc(c->thread->suffix_cache);
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
 *  while (scan_tokens(command, tokens, max_tokens, NULL) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      command = tokens[ix].value;
 *  }
 */
size_t scan_tokens(char *command, token_t *tokens,
                   const size_t max_tokens,
                   int *command_len) {
    char *s, *e;
    size_t ntokens = 0;

    if (command_len != NULL) {
        *command_len = 0;
    }

    assert(command != NULL && tokens != NULL && max_tokens > 1);

    for (s = e = command; ntokens < max_tokens - 1; ++e) {
        if (*e == '\0' || *e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }
            if (*e == '\0') {
                if (command_len != NULL) {
                    *command_len = (e - command);
                }
                break; /* string end */
            }
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

/* Remove conn c from a conn list.
 * Returns the new head of the list.
 */
conn *conn_list_remove(conn *head, conn **tail, conn *c, bool *found) {
    conn *prev = NULL;
    conn *curr = head;

    if (found != NULL) {
        *found = false;
    }

    while (curr != NULL) {
        if (curr == c) {
            if (found != NULL) {
                *found = true;
            }

            if (tail != NULL &&
                *tail == curr) {
                *tail = prev;
            }

            if (prev != NULL) {
                assert(curr != head);
                prev->next = curr->next;
                curr->next = NULL;
                return head;
            }

            assert(curr == head);
            conn *r = curr->next;
            curr->next = NULL;
            return r;
        }

        prev = curr;
        curr = curr ->next;
    }

    return head;
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
                curr->next = NULL;
                return head;
            }

            assert(curr == head);
            downstream *r = curr->next;
            curr->next = NULL;
            return r;
        }

        prev = curr;
        curr = curr ->next;
    }

    return head;
}

/* Returns the new head of the list.
 */
downstream *downstream_list_waiting_remove(downstream *head,
                                           downstream **tail,
                                           downstream *d) {
    downstream *prev = NULL;
    downstream *curr = head;

    while (curr != NULL) {
        if (curr == d) {
            if (tail != NULL &&
                *tail == curr) {
                *tail = prev;
            }

            if (prev != NULL) {
                assert(curr != head);
                prev->next_waiting = curr->next_waiting;
                curr->next_waiting = NULL;
                return head;
            }

            assert(curr == head);
            downstream *r = curr->next_waiting;
            curr->next_waiting = NULL;
            return r;
        }

        prev = curr;
        curr = curr ->next_waiting;
    }

    return head;
}

/* Returns true if a candidate request is squashable
 * or de-duplicatable with an existing request, to
 * save on network hops.
 */
bool is_compatible_request(conn *existing, conn *candidate) {
    (void)existing;
    (void)candidate;

    // The not-my-vbucket error handling requires us to not
    // squash ascii multi-GET requests, due to reusing the
    // multiget-deduplication machinery during retries and
    // to simplify the later codepaths.
    /*
    assert(existing);
    assert(existing->state == conn_pause);
    assert(IS_PROXY(existing->protocol));

    if (IS_BINARY(existing->protocol)) {
        // TODO: Revisit multi-get squashing for binary another day.
        //
        return false;
    }

    assert(IS_ASCII(existing->protocol));

    if (candidate != NULL) {
        assert(IS_ASCII(candidate->protocol));
        assert(IS_PROXY(candidate->protocol));
        assert(candidate->state == conn_pause);

        // TODO: Allow gets (CAS) for de-duplication.
        //
        if (existing->cmd == -1 &&
            candidate->cmd == -1 &&
            existing->cmd_retries <= 0 &&
            candidate->cmd_retries <= 0 &&
            !existing->noreply &&
            !candidate->noreply &&
            strncmp(existing->cmd_start, "get ", 4) == 0 &&
            strncmp(candidate->cmd_start, "get ", 4) == 0) {
            assert(existing->item == NULL);
            assert(candidate->item == NULL);

            return true;
        }
    }
    */

    return false;
}

void downstream_timeout(const int fd,
                        const short which,
                        void *arg) {
    (void)fd;
    (void)which;

    downstream *d = arg;
    assert(d != NULL);
    assert(d->ptd != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("downstream_timeout\n");
    }

    // This timer callback is invoked when one or more of
    // the downstream conns must be really slow.  Handle by
    // closing downstream conns, which might help by
    // freeing up downstream resources.
    //
    if (d->timeout_tv.tv_sec != 0 ||
        d->timeout_tv.tv_usec != 0) {
        evtimer_del(&d->timeout_event);

        d->timeout_tv.tv_sec = 0;
        d->timeout_tv.tv_usec = 0;

        d->ptd->stats.stats.tot_downstream_timeout++;

        int n = mcs_server_count(&d->mst);

        for (int i = 0; i < n; i++) {
            if (d->downstream_conns[i] != NULL &&
                d->downstream_conns[i] != NULL_CONN) {
                cproxy_close_conn(d->downstream_conns[i]);
            }
            d->downstream_conns[i] = NULL;
        }
    }
}

bool cproxy_start_downstream_timeout(downstream *d, conn *c) {
    assert(d != NULL);
    assert(d->timeout_tv.tv_sec == 0);
    assert(d->timeout_tv.tv_usec == 0);
    assert(d->behaviors_num > 0);
    assert(d->behaviors_arr != NULL);

    struct timeval dt = cproxy_get_downstream_timeout(d, c);
    if (dt.tv_sec == 0 &&
        dt.tv_usec == 0) {
        return true;
    }

    conn *uc = d->upstream_conn;

    assert(uc != NULL);
    assert(uc->state == conn_pause);
    assert(uc->thread != NULL);
    assert(uc->thread->base != NULL);
    assert(IS_PROXY(uc->protocol));

    if (settings.verbose > 2) {
        moxi_log_write("%d: cproxy_start_downstream_timeout\n",
                       (c != NULL ? c->sfd : -1));
    }

    evtimer_set(&d->timeout_event, downstream_timeout, d);

    event_base_set(uc->thread->base, &d->timeout_event);

    d->timeout_tv.tv_sec  = dt.tv_sec;
    d->timeout_tv.tv_usec = dt.tv_usec;

    return (evtimer_add(&d->timeout_event, &d->timeout_tv) == 0);
}

bool cproxy_auth_downstream(mcs_server_st *server,
                            proxy_behavior *behavior,
                            int fd) {
    assert(server);
    assert(behavior);
    assert(fd != -1);

    char buf[3000];

    if (!IS_BINARY(behavior->downstream_protocol)) {
        return true;
    }

    const char *usr = mcs_server_st_usr(server) != NULL ?
        mcs_server_st_usr(server) : behavior->usr;
    const char *pwd = mcs_server_st_pwd(server) != NULL ?
        mcs_server_st_pwd(server) : behavior->pwd;

    int usr_len = strlen(usr);
    int pwd_len = strlen(pwd);
    if (usr_len <= 0 &&
        pwd_len <= 0) {
        return true; // When no usr & no pwd.
    }

    if (settings.verbose > 2) {
        moxi_log_write("cproxy_auth_downstream usr: %s pwd: (%d)\n",
                       usr, pwd_len);
    }

    if (usr_len <= 0 ||
        !IS_PROXY(behavior->downstream_protocol) ||
        (usr_len + pwd_len + 50 > (int) sizeof(buf))) {
        if (settings.verbose > 1) {
            moxi_log_write("auth failure args\n");
        }

        return false; // Probably misconfigured.
    }

    // The key should look like "PLAIN", or the sasl mech string.
    // The data should look like "\0usr\0pwd".  So, the body buf
    // should look like "PLAIN\0usr\0pwd".
    //
    // TODO: Allow binary passwords.
    //
    int buf_len = snprintf(buf, sizeof(buf), "PLAIN%c%s%c%s",
                           0, usr,
                           0, pwd);
    assert(buf_len == 7 + usr_len + pwd_len);

    protocol_binary_request_header req = { .bytes = {0} };

    req.request.magic    = PROTOCOL_BINARY_REQ;
    req.request.opcode   = PROTOCOL_BINARY_CMD_SASL_AUTH;
    req.request.keylen   = htons((uint16_t) 5); // 5 == strlen("PLAIN").
    req.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.request.bodylen  = htonl(buf_len);

    if (mcs_io_write(fd, (const char *) req.bytes,
                     sizeof(req.bytes)) != sizeof(req.bytes) ||
        mcs_io_write(fd, buf, buf_len) == -1) {
        mcs_io_reset(fd);

        if (settings.verbose > 1) {
            moxi_log_write("auth failure during write for %s (%d)\n",
                           usr, buf_len);
        }

        return false;
    }

    protocol_binary_response_header res = { .bytes = {0} };

    if (mcs_io_read(fd, &res.bytes,
                    sizeof(res.bytes)) == MCS_SUCCESS &&
        res.response.magic == PROTOCOL_BINARY_RES) {
        res.response.status  = ntohs(res.response.status);
        res.response.keylen  = ntohs(res.response.keylen);
        res.response.bodylen = ntohl(res.response.bodylen);

        // Swallow whatever body comes.
        //
        int len = res.response.bodylen;
        while (len > 0) {
            int amt = (len > (int) sizeof(buf) ? (int) sizeof(buf) : len);
            if (mcs_io_read(fd, buf, amt) != MCS_SUCCESS) {
                if (settings.verbose > 1) {
                    moxi_log_write("auth could not read response body (%d)\n",
                                   usr, amt);
                }

                return false;
            }

            len -= amt;
        }

        // The res status should be either...
        // - SUCCESS         - sasl aware server and good credentials.
        // - AUTH_ERROR      - wrong credentials.
        // - UNKNOWN_COMMAND - sasl-unaware server.
        //
        if (res.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            if (settings.verbose > 2) {
                moxi_log_write("auth_downstream success for %s\n", usr);
            }

            return true;
        }

        if (settings.verbose > 1) {
            moxi_log_write("auth_downstream failure for %s (%x)\n",
                           usr, res.response.status);
        }
    } else {
        if (settings.verbose > 1) {
            moxi_log_write("auth_downstream response error for %s\n",
                           usr);
        }
    }

    return false;
}

bool cproxy_bucket_downstream(mcs_server_st *server,
                              proxy_behavior *behavior,
                              int fd) {
    assert(server);
    assert(behavior);
    assert(IS_PROXY(behavior->downstream_protocol));
    assert(fd != -1);

    if (!IS_BINARY(behavior->downstream_protocol)) {
        return true;
    }

    int bucket_len = strlen(behavior->bucket);
    if (bucket_len <= 0) {
        return true; // When no bucket.
    }

    protocol_binary_request_header req = { .bytes = {0} };

    req.request.magic    = PROTOCOL_BINARY_REQ;
    req.request.opcode   = PROTOCOL_BINARY_CMD_BUCKET;
    req.request.keylen   = htons((uint16_t) bucket_len);
    req.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.request.bodylen  = htonl(bucket_len);

    if (mcs_io_write(fd, (const char *) req.bytes,
                     sizeof(req.bytes)) != sizeof(req.bytes) ||
        mcs_io_write(fd, behavior->bucket, bucket_len) == -1) {
        mcs_io_reset(fd);

        if (settings.verbose > 1) {
            moxi_log_write("bucket failure during write (%d)\n",
                    bucket_len);
        }

        return false;
    }

    protocol_binary_response_header res = { .bytes = {0} };
    if (mcs_io_read(fd, &res.bytes,
                    sizeof(res.bytes)) == MCS_SUCCESS &&
        res.response.magic == PROTOCOL_BINARY_RES) {
        res.response.status  = ntohs(res.response.status);
        res.response.keylen  = ntohs(res.response.keylen);
        res.response.bodylen = ntohl(res.response.bodylen);

        // Swallow whatever body comes.
        //
        char buf[300];

        int len = res.response.bodylen;
        while (len > 0) {
            int amt = (len > (int) sizeof(buf) ? (int) sizeof(buf) : len);
            if (mcs_io_read(fd, buf, amt) != MCS_SUCCESS) {
                return false;
            }
            len -= amt;
        }

        // The res status should be either...
        // - SUCCESS         - we got the bucket.
        // - AUTH_ERROR      - not allowed to use that bucket.
        // - UNKNOWN_COMMAND - bucket-unaware server.
        //
        if (res.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            if (settings.verbose > 2) {
                moxi_log_write("bucket_downstream success, %s\n",
                        behavior->bucket);
            }

            return true;
        }

        if (settings.verbose > 1) {
            moxi_log_write("bucket_downstream failure, %s (%x)\n",
                    behavior->bucket,
                    res.response.status);
        }
    }

    return false;
}

int cproxy_max_retries(downstream *d) {
    return mcs_server_count(&d->mst) * 2;
}

int downstream_conn_index(downstream *d, conn *c) {
    assert(d);

    int nconns = mcs_server_count(&d->mst);
    for (int i = 0; i < nconns; i++) {
        if (d->downstream_conns[i] == c) {
            return i;
        }
    }

    return -1;
}

/**
 * Optimization when we're talking with ourselves,
 * so we don't need to go through network hop,
 * for a simple one-liner command.
 */
void cproxy_optimize_to_self(downstream *d, conn *uc,
                                   char *command) {
    assert(d);
    assert(d->ptd);
    assert(uc);
    assert(uc->next == NULL);

    d->ptd->stats.stats.tot_optimize_self++;

    if (command != NULL &&
        settings.verbose > 2) {
        moxi_log_write("%d: optimize to self: %s\n",
                uc->sfd, command);
    }

    d->upstream_conn   = NULL;
    d->upstream_suffix = NULL;
    d->upstream_suffix_len = 0;

    cproxy_release_downstream(d, false);
}

void cproxy_upstream_state_change(conn *c, enum conn_states next_state) {
    assert(c != NULL);

    proxy_td *ptd = c->extra;
    if (ptd != NULL) {
        if (c->state == conn_pause) {
            ptd->stats.stats.tot_upstream_unpaused++;
        }
        if (next_state == conn_pause) {
            ptd->stats.stats.tot_upstream_paused++;
        }
    }
}

// -------------------------------------------------

bool cproxy_on_connect_downstream_conn(conn *c) {
    int k;

    assert(c != NULL);
    assert(c->host_ident);

    downstream *d = c->extra;
    assert(d != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("%d: cproxy_on_connect_downstream_conn for %s\n",
                       c->sfd, c->host_ident);
    }

    if (c->which == EV_TIMEOUT) {
        // TODO: Should have a stat for the connect timeout case.
        //
        if (settings.verbose) {
            moxi_log_write("%d: connection timed out: %s",
                           c->sfd, c->host_ident);
        }
        goto cleanup;
    }

    int       error = -1;
    socklen_t errsz = sizeof(error);

    /* Check if the connection completed */
    if (getsockopt(c->sfd, SOL_SOCKET, SO_ERROR, (void *) &error,
                   &errsz) == -1) {
        if (settings.verbose) {
            moxi_log_write("%d: connect error: %s, %s",
                           c->sfd, c->host_ident, strerror(error));
        }
        goto cleanup;
    }

    if (error) {
        if (settings.verbose) {
            moxi_log_write("%d: connect failed: %s, %s",
                           c->sfd, c->host_ident, strerror(error));
        }
        goto cleanup;
    }

    k = downstream_conn_index(d, c);
    if (k >= 0) {
        if (downstream_connect_init(d, mcs_server_index(&d->mst, k),
                                    &d->behaviors_arr[k], c)) {
            /* We are connected to the server now */
            if (settings.verbose > 2) {
                moxi_log_write("%d: connected to: %s\n", c->sfd, c->host_ident);
            }

            conn_set_state(c, conn_pause);
            update_event(c, 0);
            cproxy_forward_or_error(d);

            return true;
        }
    }

cleanup:
    d->ptd->stats.stats.tot_downstream_connect_failed++;

    k = delink_from_downstream_conns(c);
    if (k >= 0) {
        assert(d->downstream_conns[k] == NULL);

        d->downstream_conns[k] = NULL_CONN;
    }

    zstored_error_count(c->thread, c->host_ident, true);

    conn_set_state(c, conn_closing);
    update_event(c, 0);
    cproxy_forward_or_error(d);

    return false;
}

void downstream_reserved_time_sample(proxy_stats_td *pstd, uint64_t duration) {
    if (pstd->downstream_reserved_time_htgram == NULL) {
        pstd->downstream_reserved_time_htgram =
            cproxy_create_timing_histogram();
    }

    if (pstd->downstream_reserved_time_htgram != NULL) {
        htgram_incr(pstd->downstream_reserved_time_htgram, duration, 1);
    }
}

void downstream_connect_time_sample(proxy_stats_td *pstd, uint64_t duration) {
    if (pstd->downstream_connect_time_htgram == NULL) {
        pstd->downstream_connect_time_htgram =
            cproxy_create_timing_histogram();
    }

    if (pstd->downstream_connect_time_htgram != NULL) {
        htgram_incr(pstd->downstream_connect_time_htgram, duration, 1);
    }
}

// A histogram for tracking timings, such as for usec request timings.
//
HTGRAM_HANDLE cproxy_create_timing_histogram(void) {
    // TODO: Make histogram bins more configurable one day.
    //
    HTGRAM_HANDLE h1 = htgram_mk(2000, 100, 2.0, 20, NULL);
    HTGRAM_HANDLE h0 = htgram_mk(0, 100, 1.0, 20, h1);

    return h0;
}

zstored_downstream_conns *zstored_get_downstream_conns(LIBEVENT_THREAD *thread,
                                                       const char *host_ident) {
    assert(thread);
    assert(thread->base);

    genhash_t *conn_hash = thread->conn_hash;
    assert(conn_hash != NULL);

    zstored_downstream_conns *conns = genhash_find(conn_hash, host_ident);
    if (conns == NULL) {
        conns = calloc(1, sizeof(zstored_downstream_conns));
        if (conns != NULL) {
            conns->host_ident = strdup(host_ident);
            if (conns->host_ident != NULL) {
                genhash_store(conn_hash, conns->host_ident, conns);
            } else {
                free(conns);
                conns = NULL;
            }
        }
    }

    return conns;
}

void zstored_error_count(LIBEVENT_THREAD *thread,
                         const char *host_ident,
                         bool has_error) {
    zstored_downstream_conns *conns =
        zstored_get_downstream_conns(thread, host_ident);
    if (conns != NULL) {
        if (has_error) {
            conns->error_count++;
            conns->error_time = msec_current_time;
        } else {
            conns->error_count = 0;
            conns->error_time = 0;
        }

        if (settings.verbose > 2) {
            moxi_log_write("z_error, %s, %d, %d, %d, %d\n",
                           host_ident,
                           has_error,
                           conns->dc_acquired,
                           conns->error_count,
                           conns->error_time);
        }

        if (has_error) {
            // We reach here when a non-blocking connect() has failed.
            // The downstream conn is just going to be closed
            // rather than be released back to the thread->conn_hash,
            // so update the dc_acquired here.
            //
            assert(conns->dc_acquired > 0);
            conns->dc_acquired--;

            // When zero downstream conns are available, wake up all
            // waiting downstreams so they can proceed (possibly by
            // just returning ERROR's to upstream clients).
            //
            // TODO: Should have a stat for this case.
            //
            if (conns->dc_acquired <= 0 &&
                conns->dc == NULL) {
                while (conns->downstream_waiting_head != NULL) {
                    assert(conns->downstream_waiting_tail != NULL);

                    downstream *d_head = conns->downstream_waiting_head;

                    conns->downstream_waiting_head =
                        conns->downstream_waiting_head->next_waiting;
                    if (conns->downstream_waiting_head == NULL) {
                        conns->downstream_waiting_tail = NULL;
                    }

                    d_head->next_waiting = NULL;

                    cproxy_forward_or_error(d_head);
                }
            }
        }
    }
}

conn *zstored_acquire_downstream_conn(downstream *d,
                                      LIBEVENT_THREAD *thread,
                                      mcs_server_st *msst,
                                      proxy_behavior *behavior,
                                      bool *downstream_conn_max_reached) {
    assert(d);
    assert(d->ptd);
    assert(d->ptd->downstream_released != d); // Should not be in free list.
    assert(thread);
    assert(msst);
    assert(behavior);
    assert(mcs_server_st_hostname(msst) != NULL);
    assert(mcs_server_st_port(msst) > 0);
    assert(mcs_server_st_fd(msst) == -1);

    *downstream_conn_max_reached = false;

    d->ptd->stats.stats.tot_downstream_conn_acquired++;

    char host_ident_buf[300];
    format_host_ident(host_ident_buf, sizeof(host_ident_buf), msst,
                      behavior->downstream_protocol);

    conn *dc;

    zstored_downstream_conns *conns =
        zstored_get_downstream_conns(thread, host_ident_buf);
    if (conns != NULL) {
        dc = conns->dc;
        if (dc != NULL) {
            assert(dc->thread == thread);
            assert(strcmp(host_ident_buf, dc->host_ident) == 0);

            conns->dc_acquired++;
            conns->dc = dc->next;
            dc->next = NULL;

            assert(dc->extra == NULL);
            dc->extra = d;

            return dc;
        }

        if (behavior->connect_max_errors > 0 &&
            behavior->connect_max_errors < conns->error_count) {
            rel_time_t msecs_since_error =
                msec_current_time - conns->error_time;

            if (settings.verbose > 2) {
                moxi_log_write("zacquire_dc, %s, %d, %d, (%d)\n",
                               host_ident_buf,
                               conns->error_count,
                               conns->error_time,
                               msecs_since_error);
            }

            if ((behavior->cycle > 0) &&
                (behavior->connect_retry_interval > msecs_since_error)) {
                // TODO: Should have a stat for this retry-interval case.
                //
                return NULL;
            } else {
                conns->error_count = 0;
                conns->error_time = 0;
            }
        }

        if (behavior->downstream_conn_max > 0 &&
            behavior->downstream_conn_max <= conns->dc_acquired) {
            // TODO: Should have a stat when downstream_conn_max reached.
            //
            *downstream_conn_max_reached = true;

            return NULL;
        }
    }

    dc = cproxy_connect_downstream_conn(d, thread, msst, behavior);
    if (dc != NULL) {
        assert(dc->host_ident == NULL);
        dc->host_ident = strdup(host_ident_buf);
        if (conns != NULL) {
            conns->dc_acquired++;

            if (dc->state != conn_connecting) {
                conns->error_count = 0;
                conns->error_time = 0;
            }
        }
    } else {
        if (conns != NULL) {
            conns->error_count++;
            conns->error_time = msec_current_time;
        }
    }

    return dc;
}

// new fn by jsh
void zstored_release_downstream_conn(conn *dc, bool closing) {
    assert(dc != NULL);

    if (dc == NULL_CONN) {
        return;
    }

    downstream *d = dc->extra;
    assert(d != NULL);

    d->ptd->stats.stats.tot_downstream_conn_released++;

    if (settings.verbose > 2) {
        moxi_log_write("%d: release_downstream_conn, %s, (%d)"
                       " upstream %d\n",
                       dc->sfd, state_text(dc->state), closing,
                       (d->upstream_conn != NULL ?
                        d->upstream_conn->sfd : -1));
    }

    assert(dc->next == NULL);
    assert(dc->state == conn_pause);
    assert(dc->thread != NULL);
    assert(dc->host_ident != NULL);

    dc->extra = NULL;

    zstored_downstream_conns *conns =
        zstored_get_downstream_conns(dc->thread, dc->host_ident);
    if (conns != NULL) {
        assert(dc->next == NULL);
        dc->next = conns->dc;
        conns->dc = dc;
        assert(conns->dc_acquired > 0);
        conns->dc_acquired--;

        // Since one downstream conn was released, process a single
        // waiting downstream, if any.
        //
        downstream *d_head = conns->downstream_waiting_head;
        if (d_head != NULL) {
            assert(conns->downstream_waiting_tail != NULL);

            conns->downstream_waiting_head =
                conns->downstream_waiting_head->next_waiting;
            if (conns->downstream_waiting_head == NULL) {
                conns->downstream_waiting_tail = NULL;
            }
            d_head->next_waiting = NULL;

            cproxy_forward_or_error(d_head);
        }
    } else {
        cproxy_close_conn(dc);
    }
}

void zstored_downstream_waiting_remove(downstream *d) {
    // TODO: Need to remove the downstream if it is on
    // any conns->downstream_waiting_head/tail queues.
    //
    // Actually, the downstream should _not_ be on any queues in any
    // of this function's (current) callers, so we should check that
    // here with a stronger assert() than the following.
    //
    assert(d->next_waiting == NULL);
}

bool zstored_downstream_waiting_add(downstream *d, LIBEVENT_THREAD *thread,
                                    mcs_server_st *msst,
                                    proxy_behavior *behavior) {
    assert(thread != NULL);
    assert(d != NULL);
    assert(d->next_waiting == NULL);

    char host_ident_buf[300];
    format_host_ident(host_ident_buf, sizeof(host_ident_buf), msst,
                      behavior->downstream_protocol);

    zstored_downstream_conns *conns =
        zstored_get_downstream_conns(thread, host_ident_buf);
    if (conns != NULL) {
        assert(conns->dc == NULL);

        if (conns->downstream_waiting_head == NULL) {
            assert(conns->downstream_waiting_tail == NULL);
            conns->downstream_waiting_head = d;
        }
        if (conns->downstream_waiting_tail != NULL) {
            assert(conns->downstream_waiting_tail->next_waiting == NULL);
            conns->downstream_waiting_tail->next_waiting = d;
        }
        conns->downstream_waiting_tail = d;

        return true;
    }

    return false;
}

void format_host_ident(char *buf, int buf_len,
                       mcs_server_st *msst,
                       enum protocol host_protocol) {
    assert(msst != NULL);

    snprintf(buf, buf_len - 1, "%s:%d:%s:%s:%d",
             mcs_server_st_hostname(msst),
             mcs_server_st_port(msst),
             mcs_server_st_usr(msst),
             mcs_server_st_pwd(msst),
             (int) host_protocol);
}

