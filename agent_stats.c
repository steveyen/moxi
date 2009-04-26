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

// From libmemcached.
//
uint32_t murmur_hash(const char *key, size_t length);

// Local declarations.
//
void on_memagent_get_stats(void *userdata,
                           void *opaque,
                           agent_add_stat add_stat);

void on_memagent_reset_stats(void *userdata);

static void request_stats(void *data0, void *data1);
static void request_stats_reset(void *data0, void *data1);

static void stats_collect(void *data0, void *data1);
static void stats_reset(void *data0, void *data1);

static void add_proxy_stats(proxy_stats *agg, proxy_stats *x);

void map_proxy_stats_foreach_free(gpointer key,
                                  gpointer value,
                                  gpointer user_data);

void map_proxy_stats_foreach_emit(gpointer key,
                                  gpointer value,
                                  gpointer user_data);

struct add_stat_emit {
    agent_add_stat  add_stat;
    void           *opaque;
    int             thread;
};

/* This callback is invoked by memagent on a memagent thread
 * when it wants proxy stats.
 *
 * We use the work_queues to retrieve the info, so that normal
 * runtime has fewer locks, at the cost of scatter/gather
 * complexity to handle the proxy stats request.
 */
void on_memagent_get_stats(void *userdata, void *opaque,
                           agent_add_stat add_stat) {
    proxy_main *m = userdata;
    assert(m);
    assert(m->nthreads > 1);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread);
    assert(mthread->work_queue);

    char buf[100];

#define more_stat(spec, key, val) \
    sprintf(buf, spec, val);      \
    add_stat(opaque, key, buf);

    more_stat("%u", "nthreads",
              m->nthreads);
    more_stat("%u", "downstream_max",
              m->downstream_max);
    more_stat("%llu", "configs",
              m->stat_configs);
    more_stat("%llu", "config_fails",
              m->stat_config_fails);
    more_stat("%llu", "proxy_starts",
              m->stat_proxy_starts);
    more_stat("%llu", "proxy_start_fails",
              m->stat_proxy_start_fails);
    more_stat("%llu", "proxy_existings",
              m->stat_proxy_existings);
    more_stat("%llu", "proxy_shutdowns",
              m->stat_proxy_shutdowns);

    // Alloc here so the main listener thread has less work.
    //
    work_collect *ca = calloc(m->nthreads, sizeof(work_collect));
    if (ca != NULL) {
        int i;

        for (i = 1; i < m->nthreads; i++) {
            GHashTable *map_proxy_stats =
                g_hash_table_new(g_str_hash,
                                 g_str_equal);
            if (map_proxy_stats != NULL)
                work_collect_init(&ca[i], -1, map_proxy_stats);
            else
                break;
        }

        // Continue on the main listener thread.
        //
        if (i >= m->nthreads &&
            work_send(mthread->work_queue, request_stats, m, ca)) {
            // Wait for all the stats collecting to finish.
            //
            for (i = 1; i < m->nthreads; i++) {
                work_collect_wait(&ca[i]);

                GHashTable *map_proxy_stats = ca[i].data;
                if (map_proxy_stats != NULL) {
                    struct add_stat_emit emit;

                    emit.add_stat  = add_stat;
                    emit.opaque    = opaque;
                    emit.thread    = i;

                    g_hash_table_foreach(map_proxy_stats,
                                         map_proxy_stats_foreach_emit,
                                         &emit);
                }
            }
        }

        for (i = 1; i < m->nthreads; i++) {
            GHashTable *map_proxy_stats = ca[i].data;
            if (map_proxy_stats != NULL) {
                g_hash_table_foreach(map_proxy_stats,
                                     map_proxy_stats_foreach_free,
                                     NULL);
                g_hash_table_destroy(map_proxy_stats);
            }
        }

        free(ca);
    }

    add_stat(opaque, NULL, NULL);
}

/* Must be invoked on the main listener thread.
 *
 * Puts stats gathering work on every worker thread's work_queue.
 */
static void request_stats(void *data0, void *data1) {
    proxy_main *m = data0;
    assert(m);
    assert(m->nthreads > 1);

    work_collect *ca = data1;
    assert(ca);

    assert(is_listen_thread());

    int sent   = 0;
    int nproxy = 0;

    for (proxy *p = m->proxy_head; p != NULL; p = p->next)
        nproxy++;

    // Starting at 1 because 0 is the main listen thread.
    //
    for (int i = 1; i < m->nthreads; i++) {
        work_collect *c = &ca[i];

        work_collect_count(c, nproxy);

        if (nproxy > 0) {
            LIBEVENT_THREAD *t = thread_by_index(i);
            assert(t);
            assert(t->work_queue);

            for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
                proxy_td *ptd = &p->thread_data[i];
                if (ptd != NULL &&
                    work_send(t->work_queue, stats_collect, ptd, c)) {
                    sent++;
                }
            }
        }
    }

    // Normally, no need to wait for the worker threads to finish,
    // as the workers will signal using work_collect_one().
    //
    // TODO: If sent is too small, then some proxies were disabled?
    //       Need to decrement count?
    //
    // TODO: Might want to block here until worker threads are done,
    //       so that concurrent reconfigs don't cause issues.
    //
    // In the case when config/config_ver changes might already
    // be inflight, as long as they're not removing proxies,
    // we're ok.  New proxies that happen afterwards are fine, too.
}

static void stats_collect(void *data0, void *data1) {
    proxy_td *ptd = data0;
    assert(ptd);
    assert(ptd->proxy);

    work_collect *c = data1;
    assert(c);

    assert(is_listen_thread() == false); // Expecting a worker thread.

    GHashTable *map_proxy_stats = c->data;
    assert(map_proxy_stats != NULL);

    if (ptd->proxy->name != NULL) {
        char *key = malloc(strlen(ptd->proxy->name) + 50);
        if (key != NULL) {
            sprintf(key, "%d:%s", ptd->proxy->port, ptd->proxy->name);

            proxy_stats *ps = g_hash_table_lookup(map_proxy_stats, key);
            if (ps == NULL) {
                ps = calloc(1, sizeof(proxy_stats));
                if (ps != NULL) {
                    g_hash_table_insert(map_proxy_stats, key, ps);
                    key = NULL;
                }
            }

            if (ps != NULL)
                add_proxy_stats(ps, &ptd->stats);

            if (key != NULL)
                free(key);
        }
    }

    work_collect_one(c);
}

static void add_proxy_stats(proxy_stats *agg, proxy_stats *x) {
    assert(agg);
    assert(x);

    agg->num_upstream += x->num_upstream;
    agg->tot_upstream += x->tot_upstream;

    agg->num_downstream_conn += x->num_downstream_conn;
    agg->tot_downstream_conn += x->tot_downstream_conn;
    agg->tot_downstream_released += x->tot_downstream_released;
    agg->tot_downstream_reserved += x->tot_downstream_reserved;
    agg->tot_downstream_freed    += x->tot_downstream_freed;
    agg->tot_downstream_quit_server   += x->tot_downstream_quit_server;
    agg->tot_downstream_max_reached   += x->tot_downstream_max_reached;
    agg->tot_downstream_create_failed += x->tot_downstream_create_failed;
    agg->tot_assign_downstream    += x->tot_assign_downstream;
    agg->tot_assign_upstream      += x->tot_assign_upstream;
    agg->tot_reset_upstream_avail += x->tot_reset_upstream_avail;
    agg->tot_oom   += x->tot_oom;
    agg->tot_retry += x->tot_retry;
}

void map_proxy_stats_foreach_free(gpointer key,
                                  gpointer value,
                                  gpointer user_data) {
    assert(key != NULL);
    free(key);

    assert(value != NULL);
    free(value);
}

void map_proxy_stats_foreach_emit(gpointer key,
                                  gpointer value,
                                  gpointer user_data) {
    char *name = key;
    assert(name != NULL);

    proxy_stats *ps = value;
    assert(ps != NULL);

    struct add_stat_emit *emit = user_data;
    assert(user_data != NULL);

    char buf_key[200];
    char buf_val[100];

#define more_thread_stat(key, val)                         \
    sprintf(buf_key, "%u:%s-%s", emit->thread, name, key); \
    sprintf(buf_val, "%llu", val);                         \
    emit->add_stat(emit->opaque, buf_key, buf_val);

    more_thread_stat("num_upstream",
                     ps->num_upstream);
    more_thread_stat("tot_upstream",
                     ps->tot_upstream);
    more_thread_stat("num_downstream_conn",
                     ps->num_downstream_conn);
    more_thread_stat("tot_downstream_conn",
                     ps->tot_downstream_conn);
    more_thread_stat("tot_downstream_released",
                     ps->tot_downstream_released);
    more_thread_stat("tot_downstream_reserved",
                     ps->tot_downstream_reserved);
    more_thread_stat("tot_downstream_freed",
                     ps->tot_downstream_freed);
    more_thread_stat("tot_downstream_quit_server",
                     ps->tot_downstream_quit_server);
    more_thread_stat("tot_downstream_max_reached",
                     ps->tot_downstream_max_reached);
    more_thread_stat("tot_downstream_create_failed",
                     ps->tot_downstream_create_failed);
    more_thread_stat("tot_assign_downstream",
                     ps->tot_assign_downstream);
    more_thread_stat("tot_assign_upstream",
                     ps->tot_assign_upstream);
    more_thread_stat("tot_reset_upstream_avail",
                     ps->tot_reset_upstream_avail);
    more_thread_stat("tot_oom",
                     ps->tot_oom);
    more_thread_stat("tot_retry",
                     ps->tot_retry);
}

/* This callback is invoked by memagent on a memagent thread
 * when it wants proxy stats.
 *
 * We use the work_queues to retrieve the info, so that normal
 * runtime has fewer locks, at the cost of scatter/gather
 * complexity to handle the proxy stats request.
 */
void on_memagent_reset_stats(void *userdata) {
    proxy_main *m = userdata;
    assert(m);
    assert(m->nthreads > 1);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread);
    assert(mthread->work_queue);

    // Alloc here so the main listener thread has less work.
    //
    work_collect *ca = calloc(m->nthreads, sizeof(work_collect));
    if (ca != NULL) {
        int i;

        for (i = 1; i < m->nthreads; i++) {
            work_collect_init(&ca[i], -1, NULL);
        }

        // Continue on the main listener thread.
        //
        if (i >= m->nthreads &&
            work_send(mthread->work_queue, request_stats_reset, m, ca)) {
            // Wait for all resets to finish.
            //
            for (i = 1; i < m->nthreads; i++) {
                work_collect_wait(&ca[i]);
            }
        }

        free(ca);
    }
}

/* Must be invoked on the main listener thread.
 *
 * Puts stats reset work on every worker thread's work_queue.
 */
static void request_stats_reset(void *data0, void *data1) {
    proxy_main *m = data0;
    assert(m);
    assert(m->nthreads > 1);

    work_collect *ca = data1;
    assert(ca);

    assert(is_listen_thread());

    int sent   = 0;
    int nproxy = 0;

    for (proxy *p = m->proxy_head; p != NULL; p = p->next)
        nproxy++;

    // Starting at 1 because 0 is the main listen thread.
    //
    for (int i = 1; i < m->nthreads; i++) {
        work_collect *c = &ca[i];

        work_collect_count(c, nproxy);

        if (nproxy > 0) {
            LIBEVENT_THREAD *t = thread_by_index(i);
            assert(t);
            assert(t->work_queue);

            for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
                proxy_td *ptd = &p->thread_data[i];
                if (ptd != NULL &&
                    work_send(t->work_queue, stats_reset, ptd, c)) {
                    sent++;
                }
            }
        }
    }

    // Normally, no need to wait for the worker threads to finish,
    // as the workers will signal using work_collect_one().
    //
    // TODO: If sent is too small, then some proxies were disabled?
    //       Need to decrement count?
    //
    // TODO: Might want to block here until worker threads are done,
    //       so that concurrent reconfigs don't cause issues.
    //
    // In the case when config/config_ver changes might already
    // be inflight, as long as they're not removing proxies,
    // we're ok.  New proxies that happen afterwards are fine, too.
}

static void stats_reset(void *data0, void *data1) {
    proxy_td *ptd = data0;
    assert(ptd);

    work_collect *c = data1;
    assert(c);

    assert(is_listen_thread() == false); // Expecting a worker thread.

    cproxy_reset_stats(&ptd->stats);

    work_collect_one(c);
}

