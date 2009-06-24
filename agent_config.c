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
#include "agent.h"

// Integration with libconflate.
//
static void update_ptd_config(void *data0, void *data1);
static bool update_str_config(char **curr, char *next, char *descrip);

static bool update_behaviors_config(proxy_behavior **curr,
                                    int  *curr_num,
                                    proxy_behavior  *next,
                                    int   next_num,
                                    char *descrip);

char *parse_kvs_servers(char *prefix,
                        char *pool_name,
                        kvpair_t *kvs,
                        char **servers,
                        proxy_behavior_pool *behavior_pool);

char **parse_kvs_behavior(kvpair_t *kvs,
                          char *prefix,
                          char *name,
                          proxy_behavior *behavior);

static void agent_logger(void *userdata,
                         enum conflate_log_level lvl,
                         const char *msg, ...)
{
    char *n = NULL;
    bool v = false;

    switch(lvl) {
    case FATAL: n = "FATAL"; v = settings.verbose > 0; break;
    case ERROR: n = "ERROR"; v = settings.verbose > 0; break;
    case WARN:  n = "WARN";  v = settings.verbose > 1; break;
    case INFO:  n = "INFO";  v = settings.verbose > 1; break;
    case DEBUG: n = "DEBUG"; v = settings.verbose > 2; break;
    }
    if (!v) {
        return;
    }

    char fmt[strlen(msg) + 16];
    snprintf(fmt, sizeof(fmt), "%s: %s\n", n, msg);

    va_list ap;
    va_start(ap, msg);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
}

static void init_extensions(void)
{
    conflate_register_mgmt_cb("client_stats", "Retrieve stats from moxi",
                              on_conflate_get_stats);
    conflate_register_mgmt_cb("reset_stats", "Reset moxi stats",
                              on_conflate_reset_stats);
    conflate_register_mgmt_cb("ping_test", "Perform a ping test",
                              on_conflate_ping_test);
}

/** The cfg_str looks like...
 *
 *    apikey=jidname@jhostname%jpassword,config=config,host=host
 *      or...
 *    jidname@jhostname%jpassword,config=config,host=host
 *
 *  Only the apikey is needed, so it can also look like...
 *
 *    jidname@jhostname%jpassword
 */
int cproxy_init_agent(char *cfg_str,
                      proxy_behavior behavior,
                      int nthreads) {
    init_extensions();

    if (cfg_str == NULL) {
        fprintf(stderr, "missing cfg\n");
        exit(EXIT_FAILURE);
    }

    int cfg_len = strlen(cfg_str);
    if (cfg_len <= 0) {
        fprintf(stderr, "empty cfg\n");
        exit(EXIT_FAILURE);
    }

    char *buff;

    if (strncmp(cfg_str, "apikey=", 7) == 0) {
        buff = trimstrdup(cfg_str);
    } else {
        buff = calloc(cfg_len + 50, sizeof(char));
        if (buff != NULL) {
            snprintf(buff, cfg_len + 50, "apikey=%s", cfg_str);
        }
        buff = trimstr(buff);
    }

    char *next = buff;

    int rv = 0;

    while (next != NULL) {
        char *jid    = NULL;
        char *jpw    = NULL;
        char *config = NULL;
        char *host   = NULL;

        char *cur = trimstr(strsep(&next, ";"));
        while (cur != NULL) {
            char *key_val = trimstr(strsep(&cur, ","));
            if (key_val != NULL) {
                char *key = trimstr(strsep(&key_val, "="));
                char *val = trimstr(key_val);

                if (val != NULL) {
                    if (wordeq(key, "apikey")) {
                        jid = strsep(&val, "%");
                        jpw = val;

                        if (settings.verbose > 1) {
                            fprintf(stderr, "cproxy_init jid %s\n",
                                    jid);
                        }
                    }
                    if (wordeq(key, "config")) {
                        config = val;
                    }
                    if (wordeq(key, "host")) {
                        host = val;
                    }
                }
            }
        }

        if (jid == NULL ||
            strlen(jid) <= 0) {
            fprintf(stderr, "missing conflate id\n");
            exit(EXIT_FAILURE);
        }

        if (jpw == NULL ||
            strlen(jpw) <= 0) {
            fprintf(stderr, "missing conflate password\n");
            exit(EXIT_FAILURE);
        }

        int config_alloc = 0;
        if (config == NULL) {
            config_alloc = strlen(jid) + 100;
            config = calloc(config_alloc, 1);
            if (config != NULL) {
                snprintf(config, config_alloc,
                         "/var/tmp/moxi_%s.cfg", jid);
            } else {
                fprintf(stderr, "conflate config buf alloc\n");
                exit(EXIT_FAILURE);
            }
        }

        if (cproxy_init_agent_start(jid, jpw, config, host,
                                    behavior,
                                    nthreads) != NULL) {
            rv++;
        }

        if (config_alloc > 0 &&
            config != NULL) {
            free(config);
        }
    }

    free(buff);

    return rv;
}

proxy_main *cproxy_init_agent_start(char *jid,
                                    char *jpw,
                                    char *config_path,
                                    char *host,
                                    proxy_behavior behavior,
                                    int nthreads) {
    assert(jid);
    assert(jpw);
    assert(config_path);

    if (settings.verbose > 1) {
        fprintf(stderr, "cproxy_init_agent_start\n");;
    }

    proxy_main *m = calloc(1, sizeof(proxy_main));
    if (m != NULL) {
        m->proxy_head = NULL;
        m->nthreads   = nthreads;
        m->behavior   = behavior;

        m->stat_configs      = 0;
        m->stat_config_fails = 0;
        m->stat_proxy_starts      = 0;
        m->stat_proxy_start_fails = 0;
        m->stat_proxy_existings   = 0;
        m->stat_proxy_shutdowns   = 0;

        conflate_config_t config;

        memset(&config, 0, sizeof(config));

        init_conflate(&config);

        // Different jid's possible for production, staging, etc.
        config.jid  = jid;  // "customer@stevenmb.local"
        config.pass = jpw;  // "password"
        config.host = host; // "localhost"
        config.software   = PACKAGE;
        config.version    = VERSION;
        config.save_path  = config_path;
        config.userdata   = m;
        config.new_config = on_conflate_new_config;
        config.log        = agent_logger;

        if (start_conflate(config)) {
            if (settings.verbose > 1) {
                fprintf(stderr, "cproxy_init done\n");
            }

            return m;
        }

        free(m);
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "cproxy could not start conflate\n");
    }

    return NULL;
}

void on_conflate_new_config(void *userdata, kvpair_t *config) {
    assert(config != NULL);

    proxy_main *m = userdata;
    assert(m != NULL);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread != NULL);

    if (settings.verbose > 1) {
        fprintf(stderr, "agent_config ocnc on_conflate_new_config\n");
    }

    kvpair_t *copy = dup_kvpair(config);
    if (copy != NULL) {
        if (!work_send(mthread->work_queue, cproxy_on_new_config, m, copy) &&
            settings.verbose > 1) {
            fprintf(stderr, "work_send failed\n");
        }
    } else {
        if (settings.verbose > 1) {
            fprintf(stderr, "agent_config ocnc failed dup_kvpair\n");
        }
    }
}

void cproxy_on_new_config(void *data0, void *data1) {
    proxy_main *m = data0;
    assert(m);

    kvpair_t *kvs = data1;
    assert(kvs);
    assert(is_listen_thread());

    m->stat_configs++;

    uint32_t max_config_ver = 0;

    for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
        pthread_mutex_lock(&p->proxy_lock);
        if (max_config_ver < p->config_ver) {
            max_config_ver = p->config_ver;
        }
        pthread_mutex_unlock(&p->proxy_lock);
    }

    uint32_t new_config_ver = max_config_ver + 1;

    if (settings.verbose > 1) {
        fprintf(stderr, "conc new_config_ver %u\n", new_config_ver);
    }

    // The kvs key-multivalues look roughly like...
    //
    //  pool-customer1-a
    //    svrname3
    //  pool-customer1-b
    //    svrname1
    //    svrname2
    //  svr-svrname1
    //    host=mc1.foo.net
    //    port=11211
    //    weight=1
    //    bucket=buck1
    //    usr=test1
    //    pwd=password
    //  svr-svrnameX
    //    host=mc2.foo.net
    //    port=11211
    //  behavior-customer1-a
    //    wait_queue_timeout=1000
    //    downstream_max=10
    //  behavior-customer1-b
    //    wait_queue_timeout=1000
    //    downstream_max=10
    //  pool_drain-customer1-b
    //    svrname1
    //    svrname3
    //  pools
    //    customer1-a
    //    customer1-b
    //  bindings
    //    11221
    //    11331
    //
    char **pools    = get_key_values(kvs, "pools");
    char **bindings = get_key_values(kvs, "bindings");

    if (pools == NULL) {
        goto fail;
    }

    int npools    = 0;
    int nbindings = 0;

    while (pools && pools[npools])
        npools++;

    while (bindings && bindings[nbindings])
        nbindings++;

    if (nbindings > 0 &&
        nbindings != npools) {
        if (settings.verbose > 1) {
            fprintf(stderr, "npools does not match nbindings\n");
        }
        goto fail;
    }

    char **behavior_kvs = get_key_values(kvs, "behavior");
    if (behavior_kvs != NULL) {
        // Update the default behavior.
        //
        proxy_behavior m_behavior = m->behavior;

        for (int k = 0; behavior_kvs[k]; k++) {
            char *bstr = trimstrdup(behavior_kvs[k]);
            if (bstr != NULL) {
                cproxy_parse_behavior_key_val_str(bstr, &m_behavior);
                free(bstr);
            }
        }

        m->behavior = m_behavior;
    }

    for (int i = 0; i < npools; i++) {
        char *pool_name = skipspace(pools[i]);
        if (pool_name != NULL &&
            pool_name[0] != '\0') {
            char buf[200];

            snprintf(buf, sizeof(buf), "pool-%s", pool_name);

            char **servers = get_key_values(kvs, trimstr(buf));
            if (servers != NULL) {
                // Parse proxy-level behavior.
                //
                proxy_behavior proxyb = m->behavior;

                if (parse_kvs_behavior(kvs, "behavior", pool_name, &proxyb)) {
                    if (settings.verbose > 1) {
                        cproxy_dump_behavior(&proxyb,
                                             "conc proxy_behavior", 1);
                    }
                }

                // The legacy way to get a port is through the bindings,
                // but they're also available as an inheritable
                // proxy_behavior field of port_listen.
                //
                int pool_port = proxyb.port_listen;

                if (i < nbindings &&
                    bindings != NULL &&
                    bindings[i])
                    pool_port = atoi(skipspace(bindings[i]));

                if (pool_port > 0) {
                    // Number of servers in this pool.
                    //
                    int s = 0;
                    while (servers[s])
                        s++;

                    if (s > 0) {
                        // Parse server-level behaviors, so we'll have an
                        // array of behaviors, one entry for each server.
                        //
                        proxy_behavior_pool behavior_pool = {
                            .base = proxyb,
                            .num  = s,
                            .arr  = calloc(s, sizeof(proxy_behavior))
                        };

                        if (behavior_pool.arr != NULL) {
                            char *config_str =
                                parse_kvs_servers("svr", pool_name, kvs,
                                                  servers, &behavior_pool);
                            if (config_str != NULL &&
                                config_str[0] != '\0') {
                                if (settings.verbose > 1) {
                                    fprintf(stderr, "conc config: %s\n",
                                            config_str);
                                }

                                cproxy_on_new_pool(m, pool_name, pool_port,
                                                   config_str, new_config_ver,
                                                   &behavior_pool);

                                free(config_str);
                            }

                            free(behavior_pool.arr);
                        } else {
                            if (settings.verbose > 1) {
                                fprintf(stderr, "oom on re-config malloc\n");;
                            }
                            goto fail;
                        }
                    } else {
                        // Note: ignore when no servers for an existing pool.
                        // Because the config_ver won't be updated, we'll
                        // fall into the empty_pool code path below.
                    }
                } else {
                    if (settings.verbose > 1) {
                        fprintf(stderr, "conc missing pool port\n");
                    }
                    goto fail;
                }
            } else {
                // Note: ignore when no servers for an existing pool.
                // Because the config_ver won't be updated, we'll
                // fall into the empty_pool code path below.
            }
        } else {
            if (settings.verbose > 1) {
                fprintf(stderr, "conc missing pool name\n");
            }
            goto fail;
        }
    }

    // If there were any proxies that weren't updated in the
    // previous loop, we need to shut them down.  We mark the
    // proxy->config as NULL, and cproxy_check_downstream_config()
    // will catch it.
    //
    // TODO: Close any listening conns for the proxy?
    // TODO: Close any upstream conns for the proxy?
    // TODO: We still need to free proxy memory, after all its
    //       proxy_td's and downstreams are closed, and no more
    //       upstreams are pointed at the proxy.
    //
    proxy_behavior_pool empty_pool;
    memset(&empty_pool, 0, sizeof(proxy_behavior_pool));

    empty_pool.base = m->behavior;
    empty_pool.num  = 0;
    empty_pool.arr  = NULL;

    for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
        bool  down = false;
        int   port = 0;
        char *name = NULL;

        pthread_mutex_lock(&p->proxy_lock);

        if (p->config_ver != new_config_ver) {
            down = true;

            assert(p->port > 0);
            assert(p->name != NULL);

            port = p->port;
            name = strdup(p->name);
        }

        pthread_mutex_unlock(&p->proxy_lock);

        if (down) {
            cproxy_on_new_pool(m, name, port, NULL, new_config_ver,
                               &empty_pool);
        }

        if (name != NULL) {
            free(name);
        }
    }

    free_kvpair(kvs);
    return;

 fail:
    m->stat_config_fails++;
    free_kvpair(kvs);

    if (settings.verbose > 1) {
        fprintf(stderr, "conc failed config %llu\n", m->stat_config_fails);
    }
}

/**
 * A name and port uniquely identify a proxy.
 */
void cproxy_on_new_pool(proxy_main *m,
                        char *name, int port,
                        char *config,
                        uint32_t config_ver,
                        proxy_behavior_pool *behavior_pool) {
    assert(m);
    assert(name != NULL);
    assert(port >= 0);
    assert(is_listen_thread());

    // See if we've already got a proxy running with that name and port,
    // and create one if needed.
    //
    bool found = false;

    proxy *p = m->proxy_head;
    while (p != NULL && !found) {
        pthread_mutex_lock(&p->proxy_lock);

        assert(p->port > 0);
        assert(p->name != NULL);

        found = ((p->port == port) &&
                 (strcmp(p->name, name) == 0));

        pthread_mutex_unlock(&p->proxy_lock);

        if (found) {
            break;
        }

        p = p->next;
    }

    if (p == NULL) {
        p = cproxy_create(name, port,
                          config,
                          config_ver,
                          behavior_pool,
                          m->nthreads);
        if (p != NULL) {
            p->next = m->proxy_head;
            m->proxy_head = p;

            int n = cproxy_listen(p);
            if (n > 0) {
                if (settings.verbose > 1) {
                    fprintf(stderr,
                            "cproxy_listen success %u to %s with %d conns\n",
                            p->port, p->config, n);
                }
                m->stat_proxy_starts++;
            } else {
                if (settings.verbose > 1) {
                    fprintf(stderr,
                            "cproxy_listen failed on %u to %s\n",
                            p->port, p->config);
                }
                m->stat_proxy_start_fails++;
            }
        }
    } else {
        if (settings.verbose > 1) {
            fprintf(stderr, "conp existing config change %u\n",
                    p->port);
        }

        bool changed  = false;
        bool shutdown = false;

        // Turn off the front_cache while we're reconfiguring.
        //
        mcache_stop(&p->front_cache);
        matcher_stop(&p->front_cache_matcher);
        matcher_stop(&p->front_cache_unmatcher);

        matcher_stop(&p->optimize_set_matcher);

        pthread_mutex_lock(&p->proxy_lock);

        if (settings.verbose > 1) {
            if (p->config && config &&
                strcmp(p->config, config) != 0) {
                fprintf(stderr,
                        "conp config changed from %s to %s\n",
                        p->config, config);
            }
        }

        changed =
            update_str_config(&p->config, config,
                              "conp config changed") ||
            changed;

        changed =
            (cproxy_equal_behavior(&p->behavior_pool.base,
                                   &behavior_pool->base) == false) ||
            changed;

        p->behavior_pool.base = behavior_pool->base;

        changed =
            update_behaviors_config(&p->behavior_pool.arr,
                                    &p->behavior_pool.num,
                                    behavior_pool->arr,
                                    behavior_pool->num,
                                    "conp behaviors changed") ||
            changed;

        if (p->config != NULL &&
            p->behavior_pool.arr != NULL) {
            m->stat_proxy_existings++;
        } else {
            m->stat_proxy_shutdowns++;
            shutdown = true;
        }

        assert(config_ver != p->config_ver);

        p->config_ver = config_ver;

        pthread_mutex_unlock(&p->proxy_lock);

        if (settings.verbose > 1) {
            fprintf(stderr, "conp changed %s, shutdown %s\n",
                    changed ? "true" : "false",
                    shutdown ? "true" : "false");
        }

        // Restart the front_cache, if necessary.
        //
        if (shutdown == false) {
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
        }

        // Send update across worker threads, avoiding locks.
        //
        work_collect wc = {0};
        work_collect_init(&wc, m->nthreads - 1, NULL);

        for (int i = 1; i < m->nthreads; i++) {
            LIBEVENT_THREAD *t = thread_by_index(i);
            assert(t);
            assert(t->work_queue);

            proxy_td *ptd = &p->thread_data[i];
            if (t &&
                t->work_queue) {
                work_send(t->work_queue, update_ptd_config, ptd, &wc);
            }
        }

        work_collect_wait(&wc);
    }
}

// ----------------------------------------------------------

static void update_ptd_config(void *data0, void *data1) {
    proxy_td *ptd = data0;
    assert(ptd);

    proxy *p = ptd->proxy;
    assert(p);

    work_collect *c = data1;
    assert(c);

    assert(is_listen_thread() == false); // Expecting a worker thread.

    pthread_mutex_lock(&p->proxy_lock);

    bool changed = false;
    int  port = p->port;
    int  prev = ptd->config_ver;

    if (ptd->config_ver != p->config_ver) {
        ptd->config_ver = p->config_ver;

        changed =
            update_str_config(&ptd->config, p->config, NULL) ||
            changed;

        ptd->behavior_pool.base = p->behavior_pool.base;

        changed =
            update_behaviors_config(&ptd->behavior_pool.arr,
                                    &ptd->behavior_pool.num,
                                    p->behavior_pool.arr,
                                    p->behavior_pool.num,
                                    NULL) ||
            changed;
    }

    pthread_mutex_unlock(&p->proxy_lock);

    // Restart the key_stats, if necessary.
    //
    if (changed) {
        mcache_stop(&ptd->key_stats);
        matcher_stop(&ptd->key_stats_matcher);
        matcher_stop(&ptd->key_stats_unmatcher);

        if (ptd->config != NULL) {
            if (ptd->behavior_pool.base.key_stats_max > 0 &&
                ptd->behavior_pool.base.key_stats_lifespan > 0) {
                mcache_start(&ptd->key_stats,
                             ptd->behavior_pool.base.key_stats_max);

                if (strlen(ptd->behavior_pool.base.key_stats_spec) > 0) {
                    matcher_start(&ptd->key_stats_matcher,
                                  ptd->behavior_pool.base.key_stats_spec);
                }

                if (strlen(ptd->behavior_pool.base.key_stats_unspec) > 0) {
                    matcher_start(&ptd->key_stats_unmatcher,
                                  ptd->behavior_pool.base.key_stats_unspec);
                }
            }
        }

        if (settings.verbose > 1) {
            fprintf(stderr, "update_ptd_config %u, %u to %u\n",
                    port, prev, ptd->config_ver);
        }
    } else {
        if (settings.verbose > 1) {
            fprintf(stderr, "update_ptd_config %u, %u = %u no change\n",
                    port, prev, ptd->config_ver);
        }
    }

    work_collect_one(c);
}

// ----------------------------------------------------------

static bool update_str_config(char **curr, char *next, char *descrip) {
    bool rv = false;

    if ((*curr != NULL) &&
        (next == NULL ||
         strcmp(*curr, next) != 0)) {
        free(*curr);
        *curr = NULL;

        rv = true;

        if (descrip != NULL &&
            settings.verbose > 1) {
            fprintf(stderr, "%s\n", descrip);
        }
    }
    if (*curr == NULL && next != NULL) {
        *curr = trimstrdup(next);
    }

    return rv;
}

static bool update_behaviors_config(proxy_behavior **curr,
                                    int  *curr_num,
                                    proxy_behavior  *next,
                                    int   next_num,
                                    char *descrip) {
    bool rv = false;

    if ((*curr != NULL) &&
        (next == NULL ||
         cproxy_equal_behaviors(*curr_num,
                                *curr,
                                next_num,
                                next) == false)) {
        free(*curr);
        *curr = NULL;
        *curr_num = 0;

        rv = true;

        if (descrip != NULL &&
            settings.verbose > 1) {
            fprintf(stderr, "%s\n", descrip);
        }
    }
    if (*curr == NULL && next != NULL) {
        *curr = cproxy_copy_behaviors(next_num,
                                      next);
        *curr_num = next_num;
    }

    return rv;
}

// ----------------------------------------------------------

/**
 * Parse server-level behaviors from a pool into a given
 * array of behaviors, one entry for each server.
 *
 * An example prefix is "svr".
 */
char *parse_kvs_servers(char *prefix,
                        char *pool_name,
                        kvpair_t *kvs,
                        char **servers,
                        proxy_behavior_pool *behavior_pool) {
    assert(prefix);
    assert(pool_name);
    assert(kvs);
    assert(servers);
    assert(behavior_pool);
    assert(behavior_pool->arr);

    if (behavior_pool->num <= 0) {
        return NULL;
    }

    // Create a config string that libmemcached likes.
    // See memcached_servers_parse().
    //
    int   config_len = 200;
    char *config_str = calloc(config_len, 1);

    for (int j = 0; servers[j]; j++) {
        assert(j < behavior_pool->num);

        // Inherit default behavior.
        //
        behavior_pool->arr[j] = behavior_pool->base;

        parse_kvs_behavior(kvs, prefix, servers[j],
                           &behavior_pool->arr[j]);

        // Grow config string for libmemcached.
        //
        int x = 40 + // For port and weight.
            strlen(config_str) +
            strlen(behavior_pool->arr[j].host);
        if (config_len < x) {
            config_len = 2 * (config_len + x);
            config_str = realloc(config_str, config_len);
        }

        char *config_end = config_str + strlen(config_str);
        if (config_end != config_str) {
            *config_end++ = ',';
        }

        if (strlen(behavior_pool->arr[j].host) > 0 &&
            behavior_pool->arr[j].port > 0) {
            snprintf(config_end,
                     config_len - (config_end - config_str),
                     "%s:%u",
                     behavior_pool->arr[j].host,
                     behavior_pool->arr[j].port);
        } else {
            if (settings.verbose > 1) {
                fprintf(stderr,
                        "missing host:port for svr-%s in %s\n",
                        servers[j], pool_name);
            }
        }

        if (behavior_pool->arr[j].downstream_weight > 0) {
            config_end = config_str + strlen(config_str);
            snprintf(config_end,
                     config_len - (config_end - config_str),
                     ":%u",
                     behavior_pool->arr[j].downstream_weight);
        }

        if (settings.verbose > 1) {
            cproxy_dump_behavior(&behavior_pool->arr[j],
                                 "pks", 0);
        }
    }

    return config_str;
}

// ----------------------------------------------------------

/**
 * Parse a "[prefix]-[name]" configuration section into a behavior.
 */
char **parse_kvs_behavior(kvpair_t *kvs,
                          char *prefix,
                          char *name,
                          proxy_behavior *behavior) {
    assert(kvs);
    assert(prefix);
    assert(name);
    assert(behavior);

    char key[800];

    snprintf(key, sizeof(key), "%s-%s", prefix, name);

    char **props = get_key_values(kvs, key);
    for (int k = 0; props && props[k]; k++) {
        char *key_val = trimstrdup(props[k]);
        if (key_val != NULL) {
            cproxy_parse_behavior_key_val_str(key_val, behavior);
            free(key_val);
        }
    }

    return props;
}

// ----------------------------------------------------------

char **get_key_values(kvpair_t *kvs, char *key) {
    kvpair_t *x = find_kvpair(kvs, key);
    if (x != NULL) {
        return x->values;
    }
    return NULL;
}

