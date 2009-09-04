/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
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

// Local declarations.
//
static void add_stat_prefix(const void *dump_opaque,
                            const char *prefix,
                            const char *key,
                            const char *val);

static void add_stat_prefix_ase(const char *key, const uint16_t klen,
                                const char *val, const uint32_t vlen,
                                const void *cookie);

static void main_stats_collect(void *data0, void *data1);
static void work_stats_collect(void *data0, void *data1);

static void main_stats_reset(void *data0, void *data1);
static void work_stats_reset(void *data0, void *data1);

static void add_proxy_stats_td(proxy_stats_td *agg,
                               proxy_stats_td *x);
static void add_proxy_stats(proxy_stats *agg,
                            proxy_stats *x);
static void add_stats_cmd(proxy_stats_cmd *agg,
                          proxy_stats_cmd *x);

void map_pstd_foreach_free(const void *key,
                           const void *value,
                           void *user_data);
void map_pstd_foreach_emit(const void *key,
                           const void *value,
                           void *user_data);

void map_pstd_foreach_merge(const void *key,
                            const void *value,
                            void *user_data);

struct main_stats_collect_info {
    proxy_main           *m;
    conflate_form_result *result;
    char                 *prefix;

    char *type;
    bool  do_settings;
    bool  do_stats;
    bool  do_zeros;
};

static char *cmd_names[] = { // Keep sync'ed with enum_stats_cmd.
    "get",
    "get_key",
    "set",
    "add",
    "replace",
    "delete",
    "append",
    "prepend",
    "incr",
    "decr",
    "flush_all",
    "cas",
    "stats",
    "stats_reset",
    "version",
    "verbosity",
    "quit",
    "ERROR"
};

static char *cmd_type_names[] = { // Keep sync'ed with enum_stats_cmd_type.
    "regular",
    "quiet"
};

/* This callback is invoked by conflate on a conflate thread
 * when it wants proxy stats.
 *
 * We use the work_queues to retrieve the info, so that normal
 * runtime has fewer locks, at the cost of scatter/gather
 * complexity to handle the proxy stats request.
 */
enum conflate_mgmt_cb_result on_conflate_get_stats(void *userdata,
                                                   conflate_handle_t *handle,
                                                   const char *cmd,
                                                   bool direct,
                                                   kvpair_t *form,
                                                   conflate_form_result *r)
{
    assert(STATS_CMD_last      == sizeof(cmd_names) / sizeof(char *));
    assert(STATS_CMD_TYPE_last == sizeof(cmd_type_names) / sizeof(char *));

    proxy_main *m = userdata;
    assert(m);
    assert(m->nthreads > 1);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread);
    assert(mthread->work_queue);

    char *type = get_simple_kvpair_val(form, "-subtype-");
    bool do_all = (type == NULL ||
                   strlen(type) <= 0 ||
                   strcmp(type, "all") == 0);

    struct main_stats_collect_info msci = {
        .m        = m,
        .result   = r,
        .prefix   = "",
        .type     = type,
        .do_settings = (do_all || strcmp(type, "settings") == 0),
        .do_stats    = (do_all || strcmp(type, "stats") == 0),
        .do_zeros    = (type != NULL &&
                        strcmp(type, "all") == 0) // Only when explicit "all".
    };

    char buf[800];

#define more_stat(spec, key, val)              \
    if (msci.do_zeros || val) {                \
        snprintf(buf, sizeof(buf), spec, val); \
        conflate_add_field(r, key, buf);       \
    }

    more_stat("%s", "main_version",
              VERSION);
    more_stat("%u", "main_nthreads",
              m->nthreads);

    if (msci.do_settings) {
        conflate_add_field(r, "main_hostname", cproxy_hostname);

        cproxy_dump_behavior_ex(&m->behavior, "main_behavior", 2,
                                add_stat_prefix, &msci);
    }

    if (msci.do_stats) {
        more_stat("%llu", "main_configs",
                  (long long unsigned int) m->stat_configs);
        more_stat("%llu", "main_config_fails",
                  (long long unsigned int) m->stat_config_fails);
        more_stat("%llu", "main_proxy_starts",
                  (long long unsigned int) m->stat_proxy_starts);
        more_stat("%llu", "main_proxy_start_fails",
                  (long long unsigned int) m->stat_proxy_start_fails);
        more_stat("%llu", "main_proxy_existings",
                  (long long unsigned int) m->stat_proxy_existings);
        more_stat("%llu", "main_proxy_shutdowns",
                  (long long unsigned int) m->stat_proxy_shutdowns);
    }

#undef more_stat

    if (msci.do_settings) {
        struct main_stats_collect_info ase = msci;
        ase.prefix = "memcached_settings";

        process_stat_settings(add_stat_prefix_ase, &ase);
    }

    if (msci.do_stats) {
        struct main_stats_collect_info ase = msci;
        ase.prefix = "memcached_stats";

        server_stats(add_stat_prefix_ase, &ase);
    }

    // Alloc here so the main listener thread has less work.
    //
    work_collect *ca = calloc(m->nthreads, sizeof(work_collect));
    if (ca != NULL) {
        int i;

        for (i = 1; i < m->nthreads; i++) {
            // Each thread gets its own collection hashmap, which
            // is keyed by each proxy's "binding:name", and whose
            // values are proxy_stats_td.
            //
            genhash_t *map_pstd = genhash_init(128, strhash_ops);
            if (map_pstd != NULL)
                work_collect_init(&ca[i], -1, map_pstd);
            else
                break;
        }

        // Continue on the main listener thread.
        //
        if (i >= m->nthreads &&
            work_send(mthread->work_queue, main_stats_collect,
                      &msci, ca)) {
            // Wait for all the stats collecting to finish.
            //
            for (i = 1; i < m->nthreads; i++) {
                work_collect_wait(&ca[i]);
            }

            assert(m->nthreads > 0);

            if (msci.do_stats) {
                genhash_t *end_pstd = ca[1].data;
                if (end_pstd != NULL) {
                    // Skip the first worker thread (index 1)'s results,
                    // because that's where we'll aggregate final results.
                    //
                    for (i = 2; i < m->nthreads; i++) {
                        genhash_t *map_pstd = ca[i].data;
                        if (map_pstd != NULL) {
                            genhash_iter(map_pstd, map_pstd_foreach_merge,
                                         end_pstd);
                        }
                    }

                    genhash_iter(end_pstd, map_pstd_foreach_emit, &msci);
                }
            }

            for (i = 1; i < m->nthreads; i++) {
                genhash_t *map_pstd = ca[i].data;
                if (map_pstd != NULL) {
                    genhash_iter(map_pstd, map_pstd_foreach_free, NULL);
                    genhash_free(map_pstd);
                    map_pstd = NULL;
                }
            }

            free(ca);
        }
    }

    return RV_OK;
}

void map_pstd_foreach_merge(const void *key,
                            const void *value,
                            void *user_data) {
    genhash_t *map_end_pstd = user_data;
    if (key != NULL &&
        map_end_pstd != NULL) {
        proxy_stats_td *cur_pstd = (proxy_stats_td *) value;
        proxy_stats_td *end_pstd = genhash_find(map_end_pstd, key);
        if (cur_pstd != NULL &&
            end_pstd != NULL) {
            add_proxy_stats_td(end_pstd, cur_pstd);
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
    assert(msci->result);

    proxy_main *m = msci->m;
    assert(m);
    assert(m->nthreads > 1);

    work_collect *ca = data1;
    assert(ca);

    assert(is_listen_thread());

    struct main_stats_collect_info ase = *msci;
    ase.prefix = "";

    int sent   = 0;
    int nproxy = 0;

    char bufk[200];
    char bufv[4000];

    for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
        nproxy++;

#define emit_s(key, val)                               \
        snprintf(bufk, sizeof(bufk), "%u:%s:%s",       \
                 p->port,                              \
                 p->name != NULL ? p->name : "", key); \
        conflate_add_field(msci->result, bufk, val);

#define emit_f(key, fmtv, val)                   \
        snprintf(bufv, sizeof(bufv), fmtv, val); \
        emit_s(key, bufv);

        pthread_mutex_lock(&p->proxy_lock);

        emit_f("port",          "%u", p->port);
        emit_s("name",                p->name);
        emit_s("config",              p->config);
        emit_f("config_ver",    "%u", p->config_ver);
        emit_f("behaviors_num", "%u", p->behavior_pool.num);

        if (msci->do_settings) {
            snprintf(bufk, sizeof(bufk),
                     "%u:%s:behavior", p->port, p->name);

            cproxy_dump_behavior_ex(&p->behavior_pool.base, bufk, 1,
                                    add_stat_prefix, &ase);

            for (int i = 0; i < p->behavior_pool.num; i++) {
                snprintf(bufk, sizeof(bufk),
                         "%u:%s:behavior-%u", p->port, p->name, i);

                cproxy_dump_behavior_ex(&p->behavior_pool.arr[i], bufk, 0,
                                        add_stat_prefix, &ase);
            }
        }

        if (msci->do_stats) {
            emit_f("listening",
                   "%llu", (long long unsigned int) p->listening);
            emit_f("listening_failed",
                   "%llu", (long long unsigned int) p->listening_failed);
        }

        pthread_mutex_unlock(&p->proxy_lock);

        // Emit front_cache stats.
        //
        if (msci->do_stats) {
            pthread_mutex_lock(p->front_cache.lock);

            if (p->front_cache.map != NULL)
                emit_f("front_cache_size", "%u", genhash_size(p->front_cache.map));

            emit_f("front_cache_max",
                   "%u", p->front_cache.max);
            emit_f("front_cache_oldest_live",
                   "%u", p->front_cache.oldest_live);

            emit_f("front_cache_tot_get_hits",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_get_hits);
            emit_f("front_cache_tot_get_expires",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_get_expires);
            emit_f("front_cache_tot_get_misses",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_get_misses);
            emit_f("front_cache_tot_get_bytes",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_get_bytes);
            emit_f("front_cache_tot_adds",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_adds);
            emit_f("front_cache_tot_add_skips",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_add_skips);
            emit_f("front_cache_tot_add_fails",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_add_fails);
            emit_f("front_cache_tot_add_bytes",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_add_bytes);
            emit_f("front_cache_tot_deletes",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_deletes);
            emit_f("front_cache_tot_evictions",
                   "%llu",
                   (long long unsigned int) p->front_cache.tot_evictions);

            pthread_mutex_unlock(p->front_cache.lock);
        }
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

    genhash_t *map_pstd = c->data;
    assert(map_pstd != NULL);

    pthread_mutex_lock(&p->proxy_lock);
    bool locked = true;

    if (p->name != NULL) {
        int   key_len = strlen(p->name) + 50;
        char *key_buf = malloc(key_len);
        if (key_buf != NULL) {
            snprintf(key_buf, key_len, "%d:%s", p->port, p->name);

            pthread_mutex_unlock(&p->proxy_lock);
            locked = false;

            proxy_stats_td *pstd = genhash_find(map_pstd, key_buf);
            if (pstd == NULL) {
                pstd = calloc(1, sizeof(proxy_stats_td));
                if (pstd != NULL) {
                    genhash_update(map_pstd, key_buf, pstd);
                    key_buf = NULL;
                }
            }

            if (pstd != NULL) {
                add_proxy_stats_td(pstd, &ptd->stats);
            }

            if (key_buf != NULL)
                free(key_buf);
        }
    }

    if (locked)
        pthread_mutex_unlock(&p->proxy_lock);

    work_collect_one(c);
}

static void add_proxy_stats_td(proxy_stats_td *agg,
                               proxy_stats_td *x) {
    assert(agg);
    assert(x);

    add_proxy_stats(&agg->stats, &x->stats);

    for (int j = 0; j < STATS_CMD_TYPE_last; j++) {
        for (int k = 0; k < STATS_CMD_last; k++) {
            add_stats_cmd(&agg->stats_cmd[j][k],
                          &x->stats_cmd[j][k]);
        }
    }
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
    agg->tot_multiget_bytes_dedupe += x->tot_multiget_bytes_dedupe;
    agg->tot_optimize_sets        += x->tot_optimize_sets;
    agg->tot_optimize_self        += x->tot_optimize_self;
    agg->tot_retry += x->tot_retry;
    agg->err_oom   += x->err_oom;
    agg->err_upstream_write_prep   += x->err_upstream_write_prep;
    agg->err_downstream_write_prep += x->err_downstream_write_prep;
}

static void add_stats_cmd(proxy_stats_cmd *agg,
                          proxy_stats_cmd *x) {
    assert(agg);
    assert(x);

    agg->seen        += x->seen;
    agg->hits        += x->hits;
    agg->misses      += x->misses;
    agg->read_bytes  += x->read_bytes;
    agg->write_bytes += x->write_bytes;
    agg->cas         += x->cas;
}

void map_pstd_foreach_free(const void *key,
                           const void *value,
                           void *user_data) {
    assert(key != NULL);
    free((void*)key);

    assert(value != NULL);
    free((void*)value);
}

void map_pstd_foreach_emit(const void *k,
                           const void *value,
                           void *user_data) {

    const char *name = (const char*)k;
    assert(name != NULL);

    proxy_stats_td *pstd = (proxy_stats_td *)value;
    assert(pstd != NULL);

    const struct main_stats_collect_info *emit = user_data;
    assert(emit != NULL);
    assert(emit->result);

    char buf_key[200];
    char buf_val[100];

#define more_stat(key, val)                             \
    if (emit->do_zeros || val) {                        \
        snprintf(buf_key, sizeof(buf_key),              \
                 "%s:stats_%s", name, key);             \
        snprintf(buf_val, sizeof(buf_val),              \
                 "%llu", (long long unsigned int) val); \
        conflate_add_field(emit->result, buf_key, buf_val); \
    }

    more_stat("num_upstream",
              pstd->stats.num_upstream);
    more_stat("tot_upstream",
              pstd->stats.tot_upstream);
    more_stat("num_downstream_conn",
              pstd->stats.num_downstream_conn);
    more_stat("tot_downstream_conn",
              pstd->stats.tot_downstream_conn);
    more_stat("tot_downstream_released",
              pstd->stats.tot_downstream_released);
    more_stat("tot_downstream_reserved",
              pstd->stats.tot_downstream_reserved);
    more_stat("tot_downstream_freed",
              pstd->stats.tot_downstream_freed);
    more_stat("tot_downstream_quit_server",
              pstd->stats.tot_downstream_quit_server);
    more_stat("tot_downstream_max_reached",
              pstd->stats.tot_downstream_max_reached);
    more_stat("tot_downstream_create_failed",
              pstd->stats.tot_downstream_create_failed);
    more_stat("tot_downstream_connect",
              pstd->stats.tot_downstream_connect);
    more_stat("tot_downstream_connect_failed",
              pstd->stats.tot_downstream_connect_failed);
    more_stat("tot_downstream_auth",
              pstd->stats.tot_downstream_auth);
    more_stat("tot_downstream_auth_failed",
              pstd->stats.tot_downstream_auth_failed);
    more_stat("tot_downstream_bucket",
              pstd->stats.tot_downstream_bucket);
    more_stat("tot_downstream_bucket_failed",
              pstd->stats.tot_downstream_bucket_failed);
    more_stat("tot_downstream_propagate_failed",
              pstd->stats.tot_downstream_propagate_failed);
    more_stat("tot_downstream_close_on_upstream_close",
              pstd->stats.tot_downstream_close_on_upstream_close);
    more_stat("tot_downstream_timeout",
              pstd->stats.tot_downstream_timeout);
    more_stat("tot_wait_queue_timeout",
              pstd->stats.tot_wait_queue_timeout);
    more_stat("tot_assign_downstream",
              pstd->stats.tot_assign_downstream);
    more_stat("tot_assign_upstream",
              pstd->stats.tot_assign_upstream);
    more_stat("tot_assign_recursion",
              pstd->stats.tot_assign_recursion);
    more_stat("tot_reset_upstream_avail",
              pstd->stats.tot_reset_upstream_avail);
    more_stat("tot_multiget_keys",
              pstd->stats.tot_multiget_keys);
    more_stat("tot_multiget_keys_dedupe",
              pstd->stats.tot_multiget_keys_dedupe);
    more_stat("tot_multiget_bytes_dedupe",
              pstd->stats.tot_multiget_bytes_dedupe);
    more_stat("tot_optimize_sets",
              pstd->stats.tot_optimize_sets);
    more_stat("tot_optimize_self",
              pstd->stats.tot_optimize_self);
    more_stat("tot_retry",
              pstd->stats.tot_retry);
    more_stat("err_oom",
              pstd->stats.err_oom);
    more_stat("err_upstream_write_prep",
              pstd->stats.err_upstream_write_prep);
    more_stat("err_downstream_write_prep",
              pstd->stats.err_downstream_write_prep);

#define more_cmd_stat(type, cmd, key, val)                       \
    if (val != 0) {                                              \
        snprintf(buf_key, sizeof(buf_key),                       \
                 "%s:stats_cmd_%s_%s_%s", name, type, cmd, key); \
        snprintf(buf_val, sizeof(buf_val),                       \
                 "%llu", (long long unsigned int) val);          \
        conflate_add_field(emit->result, buf_key, buf_val);      \
    }

    for (int j = 0; j < STATS_CMD_TYPE_last; j++) {
        for (int k = 0; k < STATS_CMD_last; k++) {
            more_cmd_stat(cmd_type_names[j], cmd_names[k],
                          "seen",
                          pstd->stats_cmd[j][k].seen);
            more_cmd_stat(cmd_type_names[j], cmd_names[k],
                          "hits",
                          pstd->stats_cmd[j][k].hits);
            more_cmd_stat(cmd_type_names[j], cmd_names[k],
                          "misses",
                          pstd->stats_cmd[j][k].misses);
            more_cmd_stat(cmd_type_names[j], cmd_names[k],
                          "read_bytes",
                          pstd->stats_cmd[j][k].read_bytes);
            more_cmd_stat(cmd_type_names[j], cmd_names[k],
                          "write_bytes",
                          pstd->stats_cmd[j][k].write_bytes);
            more_cmd_stat(cmd_type_names[j], cmd_names[k],
                          "cas",
                          pstd->stats_cmd[j][k].cas);
        }
    }
}

/* This callback is invoked by conflate on a conflate thread
 * when it wants proxy stats.
 *
 * We use the work_queues to scatter the request across our
 * threads, so that normal runtime has fewer locks at the
 * cost of infrequent reset complexity.
 */
enum conflate_mgmt_cb_result on_conflate_reset_stats(void *userdata,
                                                     conflate_handle_t *handle,
                                                     const char *cmd,
                                                     bool direct,
                                                     kvpair_t *form,
                                                     conflate_form_result *r) {
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

    return RV_OK;
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

        mcache_reset_stats(&p->front_cache);
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

    cproxy_reset_stats_td(&ptd->stats);

    work_collect_one(c);
}

static void add_stat_prefix(const void *dump_opaque,
                            const char *prefix,
                            const char *key,
                            const char *val) {
    const struct main_stats_collect_info *ase = dump_opaque;
    assert(ase);

    char buf[2000];
    snprintf(buf, sizeof(buf), "%s_%s", prefix, key);

    conflate_add_field(ase->result, buf, val);
}

static void add_stat_prefix_ase(const char *key, const uint16_t klen,
                                const char *val, const uint32_t vlen,
                                const void *cookie) {
    const struct main_stats_collect_info *ase = cookie;
    assert(ase);

    add_stat_prefix(cookie, ase->prefix, key, val);
}
