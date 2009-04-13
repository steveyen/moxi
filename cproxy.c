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

// From libmemcached.
//
memcached_return memcached_connect(memcached_server_st *ptr);
uint32_t         memcached_generate_hash(memcached_st *ptr,
                                         const char *key,
                                         size_t key_length);
void             memcached_quit_server(memcached_server_st *ptr,
                                       uint8_t io_death);

// Internal forward declarations.
//
downstream *downstream_list_remove(downstream *head, downstream *d);

// Function tables.
//
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

/* Main function to create a proxy struct.
 */
proxy *cproxy_create(char *name, int port,
                     char *config, uint32_t config_ver,
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
        p->config_ver = config_ver;
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
            for (int i = 1; i < p->thread_data_num; i++) {
                proxy_td *ptd = &p->thread_data[i];
                ptd->proxy = p;
                ptd->waiting_any_downstream_head = NULL;
                ptd->waiting_any_downstream_tail = NULL;
                ptd->downstream_reserved = NULL;
                ptd->downstream_released = NULL;
                ptd->downstream_tot = 0;
                ptd->downstream_num = 0;
                ptd->downstream_max = downstream_max;
                ptd->stats.num_upstream = 0;
                ptd->stats.tot_upstream = 0;
            }

            return p;
        }

        free(p->name);
        free(p->config);
    }

    free(p);

    return NULL;
}

/* Must be called on the main listener thread.
 */
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

/* Finds the proxy_td associated with a worker thread.
 */
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
    // lifecycle, on the worker thread, so it's a good place
    // to record the proxy_td into the conn->extra.
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

    ptd->stats.num_upstream++;
    ptd->stats.tot_upstream++;

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

    ptd->stats.num_upstream--;
    assert(ptd->stats.num_upstream >= 0);

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
    conn *curr = ptd->waiting_any_downstream_head;

    while (curr != NULL) {
        if (curr == c) {
            if (ptd->waiting_any_downstream_tail == curr)
                ptd->waiting_any_downstream_tail = prev;

            if (prev != NULL) {
                assert(curr != ptd->waiting_any_downstream_head);
                prev->next = curr->next;
                break;
            }

            assert(curr == ptd->waiting_any_downstream_head);
            ptd->waiting_any_downstream_head = curr->next;
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
    assert(ptd->proxy != NULL);

    if (ptd != NULL &&
        ptd->downstream_num < ptd->downstream_max) {
        if (settings.verbose > 1)
            fprintf(stderr, "cproxy_add_downstream\n");

        char *config = NULL;

        pthread_mutex_lock(&ptd->proxy->proxy_lock);

        if (ptd->proxy->config != NULL)
            config = strdup(ptd->proxy->config);

        uint32_t config_ver = ptd->proxy->config_ver;

        pthread_mutex_unlock(&ptd->proxy->proxy_lock);

        // The config will be NULL if the proxy is shutting down.
        //
        if (config != NULL) {
            downstream *d = cproxy_create_downstream(config, config_ver);
            if (d != NULL) {
                d->ptd = ptd;
                ptd->downstream_tot++;
                ptd->downstream_num++;
                cproxy_release_downstream(d, true);
            }

            free(config);
        }
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
    assert(d->ptd != NULL);
    assert(d->next == NULL);
    assert(d->upstream_conn == NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_free_downstream\n");

    d->ptd->downstream_num--;
    assert(d->ptd->downstream_num >= 0);

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
downstream *cproxy_create_downstream(char *config, uint32_t config_ver) {
    assert(config != NULL);

    downstream *d = (downstream *) calloc(1, sizeof(downstream));
    if (d != NULL) {
        d->config     = strdup(config);
        d->config_ver = config_ver;

        if (settings.verbose > 1)
            fprintf(stderr, "cproxy_create_downstream: %s, %u\n",
                    config, config_ver);

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
            } else {
                if (settings.verbose > 1)
                    fprintf(stderr, "mserver_parse failed: %s\n",
                            config);
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

    if (d->config_ver == d->ptd->proxy->config_ver) {
        rv = true;
    } else if (d->config != NULL &&
               d->ptd->proxy->config != NULL &&
               strcmp(d->config, d->ptd->proxy->config) == 0) {
        d->config_ver = d->ptd->proxy->config_ver;
        rv = true;
    }

    pthread_mutex_unlock(&d->ptd->proxy->proxy_lock);

    return rv;
}

int cproxy_connect_downstream(downstream *d, LIBEVENT_THREAD *thread) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->downstream_released != d); // Should not be in free list.
    assert(d->next == NULL);
    assert(d->downstream_conns != NULL);
    assert(memcached_server_count(&d->mst) > 0);
    assert(thread != NULL);
    assert(thread->base != NULL);

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

conn *cproxy_find_downstream_conn(downstream *d,
                                  char *key, int key_length) {
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
            fprintf(stderr,
                    "%d: cproxy_prep_conn_for_write failed\n", c->sfd);
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
    conn *tail = ptd->waiting_any_downstream_tail;
    bool  stop = false;

    while (ptd->waiting_any_downstream_head != NULL && !stop) {
        if (ptd->waiting_any_downstream_head == tail)
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
        d->upstream_conn = ptd->waiting_any_downstream_head;
        ptd->waiting_any_downstream_head =
            ptd->waiting_any_downstream_head->next;
        if (ptd->waiting_any_downstream_head == NULL)
            ptd->waiting_any_downstream_tail = NULL;
        d->upstream_conn->next = NULL;

        if (settings.verbose > 1)
            fprintf(stderr, "assign_downstream, matched to upstream %d\n",
                    d->upstream_conn->sfd);

        if (!cproxy_forward_downstream(d)) {
            // We reach here on error, so send error upstream.
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
                //       cproxy_wait_any_downstream(ptd, uc);
                //
                // Send an END on get/gets instead of generic SERVER_ERROR.
                //
                if (uc->cmd == -1 &&
                    uc->rcurr != NULL &&
                    strncmp(uc->rcurr, "get", 3) == 0)
                    out_string(uc, "END");
                else
                    out_string(uc, "SERVER_ERROR proxy write to downstream");

                update_event(uc, EV_WRITE | EV_PERSIST);
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "assign_downstream, done\n");
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
    // May need to use the work_queue to call drive_machine() on the uc?
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

void cproxy_wait_any_downstream(proxy_td *ptd, conn *uc) {
    assert(uc != NULL);
    assert(ptd != NULL);
    assert(!ptd->waiting_any_downstream_tail ||
           !ptd->waiting_any_downstream_tail->next);

    // Add the upstream conn to the wait list.
    //
    uc->next = NULL;
    if (ptd->waiting_any_downstream_tail != NULL)
        ptd->waiting_any_downstream_tail->next = uc;
    ptd->waiting_any_downstream_tail = uc;
    if (ptd->waiting_any_downstream_head == NULL)
        ptd->waiting_any_downstream_head = uc;
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
    cproxy_wait_any_downstream(ptd, upstream);
    cproxy_assign_downstream(ptd);
}

rel_time_t cproxy_realtime(const time_t exptime) {
    // Input is a long...
    //
    // 0       | (0...REALIME_MAXDELTA] | (REALTIME_MAXDELTA...
    // forever | delta                  | unix_time
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
size_t scan_tokens(char *command, token_t *tokens,
                   const size_t max_tokens) {
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

