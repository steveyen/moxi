/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "conflate.h"
#include "cproxy.h"
#include "work.h"
#include "agent.h"

// From libmemcached.
//
uint32_t murmur_hash(const char *key, size_t length);

static void main_stats_collect(void *data0, void *data1);
static void work_stats_collect(void *data0, void *data1);

static void main_stats_reset(void *data0, void *data1);
static void work_stats_reset(void *data0, void *data1);

static void add_proxy_stats(proxy_stats *agg, proxy_stats *x);

void map_proxy_stats_foreach_free(gpointer key,
                                  gpointer value,
                                  gpointer user_data);

void map_proxy_stats_foreach_emit(gpointer key,
                                  gpointer value,
                                  gpointer user_data);

void map_proxy_stats_foreach_merge(gpointer key,
                                   gpointer value,
                                   gpointer user_data);

struct add_stat_emit {
    conflate_add_stat add_stat;
    void             *opaque;
};

struct main_stats_collect_info {
    proxy_main       *m;
    conflate_add_stat add_stat;
    void             *opaque;
};

/* This callback is invoked by conflate on a conflate thread
 * when it wants proxy stats.
 *
 * We use the work_queues to retrieve the info, so that normal
 * runtime has fewer locks, at the cost of scatter/gather
 * complexity to handle the proxy stats request.
 */
void on_conflate_get_stats(void *userdata, void *opaque,
                           char *type, kvpair_t *form,
                           conflate_add_stat add_stat) {
    proxy_main *m = userdata;
    assert(m);
    assert(m->nthreads > 1);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread);
    assert(mthread->work_queue);

    char buf[800];

#define more_stat(spec, key, val)          \
    snprintf(buf, sizeof(buf), spec, val); \
    add_stat(opaque, key, buf);

    more_stat("%s", "version",
              VERSION);
    more_stat("%u", "nthreads",
              m->nthreads);
    more_stat("%u", "cycle",
              m->behavior.cycle);
    more_stat("%u", "default_downstream_max",
              m->behavior.downstream_max);
    more_stat("%u", "default_downstream_weight",
              m->behavior.downstream_weight);
    more_stat("%u", "default_downstream_retry",
              m->behavior.downstream_retry);
    more_stat("%u", "default_downstream_protocol",
              m->behavior.downstream_protocol);
    more_stat("%ld", "default_downstream_timeout", // In millisecs.
              m->behavior.downstream_timeout.tv_sec * 1000 +
              m->behavior.downstream_timeout.tv_usec / 1000);
    more_stat("%ld", "default_wait_queue_timeout", // In millisecs.
              m->behavior.wait_queue_timeout.tv_sec * 1000 +
              m->behavior.wait_queue_timeout.tv_usec / 1000);
    more_stat("%u", "front_cache_lifespan",
              m->behavior.front_cache_lifespan);
    more_stat("%s", "front_cache_spec",
              m->behavior.front_cache_spec);
    more_stat("%llu", "configs",
              (long long unsigned int) m->stat_configs);
    more_stat("%llu", "config_fails",
              (long long unsigned int) m->stat_config_fails);
    more_stat("%llu", "proxy_starts",
              (long long unsigned int) m->stat_proxy_starts);
    more_stat("%llu", "proxy_start_fails",
              (long long unsigned int) m->stat_proxy_start_fails);
    more_stat("%llu", "proxy_existings",
              (long long unsigned int) m->stat_proxy_existings);
    more_stat("%llu", "proxy_shutdowns",
              (long long unsigned int) m->stat_proxy_shutdowns);

    // Alloc here so the main listener thread has less work.
    //
    work_collect *ca = calloc(m->nthreads, sizeof(work_collect));
    if (ca != NULL) {
        int i;

        for (i = 1; i < m->nthreads; i++) {
            // Each thread gets its own collection hashmap, which
            // is keyed by each proxy's "binding:name", and whose
            // values are proxy_stats.
            //
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
        struct main_stats_collect_info msci = {
            .m        = m,
            .add_stat = add_stat,
            .opaque   = opaque
        };

        if (i >= m->nthreads &&
            work_send(mthread->work_queue, main_stats_collect, &msci, ca)) {
            // Wait for all the stats collecting to finish.
            //
            for (i = 1; i < m->nthreads; i++) {
                work_collect_wait(&ca[i]);
            }

            assert(m->nthreads > 0);

            GHashTable *end_proxy_stats = ca[1].data;
            if (end_proxy_stats != NULL) {
                // Skip the first worker thread (index 1)'s results,
                // because that's where we'll aggregate final results.
                //
                for (i = 2; i < m->nthreads; i++) {
                    GHashTable *map_proxy_stats = ca[i].data;
                    if (map_proxy_stats != NULL) {
                        g_hash_table_foreach(map_proxy_stats,
                                             map_proxy_stats_foreach_merge,
                                             end_proxy_stats);
                    }
                }

                struct add_stat_emit emit;

                emit.add_stat = add_stat;
                emit.opaque   = opaque;

                g_hash_table_foreach(end_proxy_stats,
                                     map_proxy_stats_foreach_emit,
                                     &emit);
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

void map_proxy_stats_foreach_merge(gpointer key,
                                   gpointer value,
                                   gpointer user_data) {
    GHashTable *end_proxy_stats = user_data;
    if (key != NULL &&
        end_proxy_stats != NULL) {
        proxy_stats *cur_ps = (proxy_stats *) value;
        proxy_stats *end_ps =
            g_hash_table_lookup(end_proxy_stats,
                                key);
        if (cur_ps != NULL &&
            end_ps != NULL) {
            add_proxy_stats(end_ps, cur_ps);
        }
    }
}

/* Must be invoked on the main listener thread.
 *
 * Puts stats gathering work on every worker thread's work_queue.
 */
static void main_stats_collect(void *data0, void *data1) {
    struct main_stats_collect_info *msci = data0;
    assert(msci);
    assert(msci->add_stat);

    proxy_main *m = msci->m;
    assert(m);
    assert(m->nthreads > 1);

    work_collect *ca = data1;
    assert(ca);

    assert(is_listen_thread());

    int sent   = 0;
    int nproxy = 0;

    char bufk[100];
    char bufv[4000];

#define emit_s(num, key, val)                        \
    snprintf(bufk, sizeof(bufk), "%u:%s", num, key); \
    msci->add_stat(msci->opaque, bufk, val);

#define emit_f(num, key, fmtv, val)                  \
    snprintf(bufv, sizeof(bufv), fmtv, val);         \
    emit_s(num, key, bufv);

    for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
        nproxy++;

        pthread_mutex_lock(&p->proxy_lock);

        emit_f(p->port, "port", "%u", p->port);
        emit_s(p->port, "name",   p->name);
        emit_s(p->port, "config", p->config);
        emit_f(p->port, "config_ver",    "%u", p->config_ver);
        emit_f(p->port, "behaviors_num", "%u", p->behaviors_num);

        for (int i = 0; i < p->behaviors_num; i++) {
            char bufb[100];

#define emit_xs(key, val)                                  \
            snprintf(bufb, sizeof(bufb), "%u:%s", i, key); \
            emit_s(p->port, bufb, val);

#define emit_xf(key, fmtv, val)                            \
            snprintf(bufb, sizeof(bufb), "%u:%s", i, key); \
            emit_f(p->port, bufb, fmtv, val);

            emit_xs("usr",        p->behaviors[i].usr);
            emit_xs("host",       p->behaviors[i].host);
            emit_xf("port", "%u", p->behaviors[i].port);
            emit_xs("bucket",     p->behaviors[i].bucket);

            emit_xf("downstream_max",
                   "%u", p->behaviors[i].downstream_max);
            emit_xf("downstream_weight",
                   "%u", p->behaviors[i].downstream_weight);
            emit_xf("downstream_protocol",
                   "%u", p->behaviors[i].downstream_protocol);
            emit_xf("downstream_timeout", "%ld", // In millisecs.
                    p->behaviors[i].downstream_timeout.tv_sec * 1000 +
                    p->behaviors[i].downstream_timeout.tv_usec / 1000);
            emit_xf("wait_queue_timeout", "%ld", // In millisecs.
                    p->behaviors[i].wait_queue_timeout.tv_sec * 1000 +
                    p->behaviors[i].wait_queue_timeout.tv_usec / 1000);
        }

        emit_f(p->port, "listening",
             "%llu", (long long unsigned int) p->listening);
        emit_f(p->port, "listening_failed",
             "%llu", (long long unsigned int) p->listening_failed);

        pthread_mutex_unlock(&p->proxy_lock);

        // Emit front_cache stats.
        //
        pthread_mutex_lock(p->front_cache.lock);

        emit_f(p->port, "front_cache_tot_get_hits",
               "%llu",
               (long long unsigned int) p->front_cache.tot_get_hits);
        emit_f(p->port, "front_cache_tot_get_expires",
               "%llu",
               (long long unsigned int) p->front_cache.tot_get_expires);
        emit_f(p->port, "front_cache_tot_get_misses",
               "%llu",
               (long long unsigned int) p->front_cache.tot_get_misses);
        emit_f(p->port, "front_cache_tot_adds",
               "%llu",
               (long long unsigned int) p->front_cache.tot_adds);
        emit_f(p->port, "front_cache_tot_add_skips",
               "%llu",
               (long long unsigned int) p->front_cache.tot_add_skips);

        pthread_mutex_unlock(p->front_cache.lock);
    }

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
                    work_send(t->work_queue, work_stats_collect, ptd, c)) {
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

static void work_stats_collect(void *data0, void *data1) {
    proxy_td *ptd = data0;
    assert(ptd);

    proxy *p = ptd->proxy;
    assert(p);

    work_collect *c = data1;
    assert(c);

    assert(is_listen_thread() == false); // Expecting a worker thread.

    GHashTable *map_proxy_stats = c->data;
    assert(map_proxy_stats != NULL);

    pthread_mutex_lock(&p->proxy_lock);
    bool locked = true;

    if (p->name != NULL) {
        int   key_len = strlen(p->name) + 50;
        char *key_buf = malloc(key_len);
        if (key_buf != NULL) {
            snprintf(key_buf, key_len, "%d:%s", p->port, p->name);

            pthread_mutex_unlock(&p->proxy_lock);
            locked = false;

            proxy_stats *ps = g_hash_table_lookup(map_proxy_stats, key_buf);
            if (ps == NULL) {
                ps = calloc(1, sizeof(proxy_stats));
                if (ps != NULL) {
                    g_hash_table_insert(map_proxy_stats, key_buf, ps);
                    key_buf = NULL;
                }
            }

            if (ps != NULL)
                add_proxy_stats(ps, &ptd->stats);

            if (key_buf != NULL)
                free(key_buf);
        }
    }

    if (locked)
        pthread_mutex_unlock(&p->proxy_lock);

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
    agg->tot_downstream_quit_server    += x->tot_downstream_quit_server;
    agg->tot_downstream_max_reached    += x->tot_downstream_max_reached;
    agg->tot_downstream_create_failed  += x->tot_downstream_create_failed;
    agg->tot_downstream_connect        += x->tot_downstream_connect;
    agg->tot_downstream_connect_failed += x->tot_downstream_connect_failed;
    agg->tot_downstream_auth           += x->tot_downstream_auth;
    agg->tot_downstream_auth_failed    += x->tot_downstream_auth_failed;
    agg->tot_downstream_bucket         += x->tot_downstream_bucket;
    agg->tot_downstream_bucket_failed  += x->tot_downstream_bucket_failed;
    agg->tot_downstream_propagate_failed +=
        x->tot_downstream_propagate_failed;
    agg->tot_downstream_close_on_upstream_close +=
        x->tot_downstream_close_on_upstream_close;
    agg->tot_downstream_timeout   += x->tot_downstream_timeout;
    agg->tot_wait_queue_timeout   += x->tot_wait_queue_timeout;
    agg->tot_assign_downstream    += x->tot_assign_downstream;
    agg->tot_assign_upstream      += x->tot_assign_upstream;
    agg->tot_assign_recursion     += x->tot_assign_recursion;
    agg->tot_reset_upstream_avail += x->tot_reset_upstream_avail;
    agg->tot_multiget_keys        += x->tot_multiget_keys;
    agg->tot_multiget_keys_dedupe += x->tot_multiget_keys_dedupe;
    agg->tot_retry += x->tot_retry;
    agg->err_oom   += x->err_oom;
    agg->err_upstream_write_prep   += x->err_upstream_write_prep;
    agg->err_downstream_write_prep += x->err_downstream_write_prep;
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
    assert(emit != NULL);
    assert(emit->add_stat != NULL);

    char buf_key[200];
    char buf_val[100];

#define more_thread_stat(key, val)                  \
    snprintf(buf_key, sizeof(buf_key),              \
             "%s-%s", name, key);                   \
    snprintf(buf_val, sizeof(buf_val),              \
             "%llu", (long long unsigned int) val); \
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
    more_thread_stat("tot_downstream_connect",
                     ps->tot_downstream_connect);
    more_thread_stat("tot_downstream_connect_failed",
                     ps->tot_downstream_connect_failed);
    more_thread_stat("tot_downstream_auth",
                     ps->tot_downstream_auth);
    more_thread_stat("tot_downstream_auth_failed",
                     ps->tot_downstream_auth_failed);
    more_thread_stat("tot_downstream_bucket",
                     ps->tot_downstream_bucket);
    more_thread_stat("tot_downstream_bucket_failed",
                     ps->tot_downstream_bucket_failed);
    more_thread_stat("tot_downstream_propagate_failed",
                     ps->tot_downstream_propagate_failed);
    more_thread_stat("tot_downstream_close_on_upstream_close",
                     ps->tot_downstream_close_on_upstream_close);
    more_thread_stat("tot_downstream_timeout",
                     ps->tot_downstream_timeout);
    more_thread_stat("tot_wait_queue_timeout",
                     ps->tot_wait_queue_timeout);
    more_thread_stat("tot_assign_downstream",
                     ps->tot_assign_downstream);
    more_thread_stat("tot_assign_upstream",
                     ps->tot_assign_upstream);
    more_thread_stat("tot_assign_recursion",
                     ps->tot_assign_recursion);
    more_thread_stat("tot_reset_upstream_avail",
                     ps->tot_reset_upstream_avail);
    more_thread_stat("tot_multiget_keys",
                     ps->tot_multiget_keys);
    more_thread_stat("tot_multiget_keys_dedupe",
                     ps->tot_multiget_keys_dedupe);
    more_thread_stat("tot_retry",
                     ps->tot_retry);
    more_thread_stat("err_oom",
                     ps->err_oom);
    more_thread_stat("err_upstream_write_prep",
                     ps->err_upstream_write_prep);
    more_thread_stat("err_downstream_write_prep",
                     ps->err_downstream_write_prep);
}

/* This callback is invoked by conflate on a conflate thread
 * when it wants proxy stats.
 *
 * We use the work_queues to scatter the request across our
 * threads, so that normal runtime has fewer locks at the
 * cost of infrequent reset complexity.
 */
void on_conflate_reset_stats(void *userdata,
                             char *type, kvpair_t *form) {
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
            work_send(mthread->work_queue, main_stats_reset, m, ca)) {
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
static void main_stats_reset(void *data0, void *data1) {
    proxy_main *m = data0;
    assert(m);
    assert(m->nthreads > 1);

    work_collect *ca = data1;
    assert(ca);

    assert(is_listen_thread());

    m->stat_configs = 0;
    m->stat_config_fails = 0;
    m->stat_proxy_starts = 0;
    m->stat_proxy_start_fails = 0;
    m->stat_proxy_existings = 0;
    m->stat_proxy_shutdowns = 0;

    int sent   = 0;
    int nproxy = 0;

    for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
        nproxy++;

        // We don't clear p->listening because it's meant to
        // increase and decrease.
        //
        p->listening_failed = 0;
    }

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
                    work_send(t->work_queue, work_stats_reset, ptd, c)) {
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

static void work_stats_reset(void *data0, void *data1) {
    proxy_td *ptd = data0;
    assert(ptd);

    work_collect *c = data1;
    assert(c);

    assert(is_listen_thread() == false); // Expecting a worker thread.

    cproxy_reset_stats(&ptd->stats);

    work_collect_one(c);
}

