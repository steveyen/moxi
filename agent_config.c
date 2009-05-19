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
    if (!v)
        return;

    char fmt[strlen(msg) + 16];
    snprintf(fmt, sizeof(fmt), "%s: %s\n", n, msg);

    va_list ap;
    va_start(ap, msg);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
}

int cproxy_init_agent(char *cfg_str,
                      proxy_behavior behavior,
                      int nthreads) {
    assert(cfg_str);

    char *buff = strdup(cfg_str);
    char *next = buff;

    // Each sec (or section) looks like...
    //
    //   apikey=jidname@jhostname%jpassword,config=config,host=host
    //
    // Only the apikey is needed.
    //
    int rv = 0;

    while (next != NULL) {
        char *jid    = NULL;
        char *jpw    = NULL;
        char *config = "/tmp/memscale.cfg"; // TODO: Revisit.
        char *host   = "localhost";         // TODO: Revisit.

        char *cur = strsep(&next, ";");
        while (cur != NULL) {
            char *key_val = strsep(&cur, ",");
            if (key_val != NULL) {
                char *key = strsep(&key_val, "=");
                char *val = key_val;

                if (settings.verbose > 1)
                    fprintf(stderr, "cproxy_init kv %s %s\n", key, val);

                if (val != NULL) {
                    if (strcmp(key, "apikey") == 0) {
                        jid = strsep(&val, "%");
                        jpw = val;

                        if (settings.verbose > 1)
                            fprintf(stderr, "cproxy_init apikey %s %s\n",
                                    jid, jpw);
                    }
                    if (strcmp(key, "config") == 0)
                        config = val;
                    if (strcmp(key, "host") == 0)
                        host = val;
                }
            }
        }

        if (jid == NULL) {
            fprintf(stderr, "missing conflate id\n");
            exit(EXIT_FAILURE);
        } else if (jpw == NULL) {
            fprintf(stderr, "missing conflate password\n");
            exit(EXIT_FAILURE);
        } else if (config == NULL) {
            fprintf(stderr, "missing config\n");
            exit(EXIT_FAILURE);
        } else if (host == NULL) {
            fprintf(stderr, "missing host\n");
            exit(EXIT_FAILURE);
        } else {
            if (cproxy_init_agent_start(jid, jpw, config, host,
                                        behavior,
                                        nthreads) == 0)
                rv++;
        }
    }

    free(buff);

    return rv;
}

int cproxy_init_agent_start(char *jid,
                            char *jpw,
                            char *config_path,
                            char *host,
                            proxy_behavior behavior,
                            int nthreads) {
    assert(jid);
    assert(jpw);
    assert(config_path);
    assert(host);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_init_agent_start\n");;

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
        config.software   = "memscale";
        config.version    = VERSION;
        config.save_path  = config_path;
        config.userdata   = m;
        config.new_config  = on_conflate_new_config;
        config.get_stats   = on_conflate_get_stats;
        config.reset_stats = on_conflate_reset_stats;
        config.ping_test   = on_conflate_ping_test;
        config.log         = agent_logger;

        if (start_conflate(config)) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init done\n");

            return 0;
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy could not start conflate\n");

    return 1;
}

void on_conflate_new_config(void *userdata, kvpair_t *config) {
    assert(config != NULL);

    proxy_main *m = userdata;
    assert(m != NULL);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "agent_config on_conflate_new_config\n");

    kvpair_t *copy = dup_kvpair(config);
    if (copy != NULL) {
        work_send(mthread->work_queue, cproxy_on_new_config, m, copy);
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
        if (max_config_ver < p->config_ver)
            max_config_ver = p->config_ver;
        pthread_mutex_unlock(&p->proxy_lock);
    }

    uint32_t new_config_ver = max_config_ver + 1;

    // The kvs key-multivalues looks roughly like...
    //
    //  pool-customer1-b
    //    svrname1
    //    svrname2
    //  pool-customer1-a
    //    svrname3
    //  svr-svrname1
    //    host=mc1.foo.net
    //    port=11211
    //    weight=1
    //    bucket=buck1
    //    usr=test1
    //    pwd=password
    //  pools
    //    customer1-a
    //    customer1-b
    //  bindings
    //    11221
    //    11331
    //
    char **pools = get_key_values(kvs, "pools");
    if (pools == NULL)
        goto fail;

    char **bindings = get_key_values(kvs, "bindings");
    if (bindings == NULL)
        goto fail;

    int npools = 0;
    while (pools[npools])
        npools++;

    int nbindings = 0;
    while (bindings[nbindings])
        nbindings++;

    if (nbindings != npools) {
        if (settings.verbose > 1)
            fprintf(stderr, "npools does not match nbindings\n");
        goto fail;
    }

    char **behavior_kvs = get_key_values(kvs, "behavior");
    if (behavior_kvs != NULL) {
        // Update the default behavior.
        //
        proxy_behavior m_behavior = m->behavior;

        for (int k = 0; behavior_kvs[k]; k++) {
            cproxy_parse_behavior_key_val_str(behavior_kvs[k],
                                              &m_behavior);
        }

        m->behavior = m_behavior;
    }

    for (int i = 0; i < npools; i++) {
        assert(pools[i]);
        assert(bindings[i]);

        if (pools[i] != NULL &&
            bindings[i] != NULL) {
            char *pool_name = pools[i];
            int   pool_port = atoi(bindings[i]);
            char  pool_key[200];

            snprintf(pool_key, sizeof(pool_key),
                     "pool-%s", pool_name);

            char **servers = get_key_values(kvs, pool_key);

            assert(pool_name);
            assert(pool_port > 0);

            if (servers != NULL &&
                pool_port > 0) {
                // Create a config string that libmemcached likes.
                // See memcached_servers_parse().
                //
                int   config_len = 200;
                char *config_str = calloc(config_len, 1);

                // Number of servers in this pool.
                //
                int s = 0;
                while (servers[s])
                    s++;

                // Array of behaviors, one entry for each server.
                //
                proxy_behavior *behaviors =
                    calloc(s, sizeof(proxy_behavior));

                if (config_str != NULL &&
                    behaviors != NULL) {
                    for (int j = 0; servers[j]; j++) {
                        char svr_key[800];

                        snprintf(svr_key, sizeof(svr_key),
                                 "svr-%s", servers[j]);

                        // Inherit default behavior.
                        //
                        behaviors[j] = m->behavior;

                        char **props = get_key_values(kvs, svr_key);
                        for (int k = 0; props && props[k]; k++) {
                            cproxy_parse_behavior_key_val_str(props[k],
                                                              &behaviors[j]);
                        }

                        int x = 40 + // For port and weight.
                            strlen(config_str) +
                            strlen(behaviors[j].host);
                        if (config_len < x) {
                            config_len = 2 * (config_len + x);
                            config_str = realloc(config_str, config_len);
                        }

                        char *config_end = config_str + strlen(config_str);
                        if (config_end != config_str)
                            *config_end++ = ',';

                        if (strlen(behaviors[j].host) > 0 &&
                            behaviors[j].port > 0) {
                            snprintf(config_end,
                                     config_len - (config_end - config_str),
                                     "%s:%u",
                                     behaviors[j].host,
                                     behaviors[j].port);
                        } else {
                            if (settings.verbose > 1)
                                fprintf(stderr,
                                        "missing host:port for %s in %s\n",
                                        svr_key, pool_name);

                            goto fail;
                        }

                        if (behaviors[j].downstream_weight > 0) {
                            config_end = config_str + strlen(config_str);
                            snprintf(config_end,
                                     config_len - (config_end - config_str),
                                     ":%u",
                                     behaviors[j].downstream_weight);
                        }

                        if (settings.verbose > 1) {
                            cproxy_dump_behavior(&behaviors[j],
                                                 "on_new_config");
                        }
                    }

                    if (settings.verbose > 1)
                        fprintf(stderr, "agent_config: %s\n",
                                config_str);

                    cproxy_on_new_pool(m, pool_name, pool_port,
                                       config_str, new_config_ver,
                                       s, behaviors);

                    free(config_str);
                    free(behaviors);
                } else {
                    if (settings.verbose > 1)
                        fprintf(stderr, "oom on re-config malloc\n");;
                    goto fail;
                }
            } else {
                if (settings.verbose > 1)
                    fprintf(stderr, "missing servers or port\n");
                goto fail;
            }
        } else {
            if (settings.verbose > 1)
                fprintf(stderr, "missing ports or bindings\n");
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
    for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
        bool down = false;
        int  port = 0;

        pthread_mutex_lock(&p->proxy_lock);

        if (p->config_ver != new_config_ver) {
            down = true;
            port = p->port;
        }

        pthread_mutex_unlock(&p->proxy_lock);

        if (down)
            cproxy_on_new_pool(m, NULL, port, NULL, new_config_ver,
                               0, NULL);
    }

    free_kvpair(kvs);
    return;

 fail:
    m->stat_config_fails++;
    free_kvpair(kvs);
}

void cproxy_on_new_pool(proxy_main *m,
                        char *name, int port,
                        char *config,
                        uint32_t config_ver,
                        int   behaviors_num,
                        proxy_behavior *behaviors) {
    assert(m);
    assert(port >= 0);
    assert(is_listen_thread());

    // See if we've already got a proxy running on the port,
    // and create one if needed.
    //
    proxy *p = m->proxy_head;
    while (p != NULL &&
           p->port != port)
        p = p->next;

    if (p == NULL) {
        p = cproxy_create(name, port,
                          config,
                          config_ver,
                          behaviors_num,
                          behaviors,
                          m->nthreads);
        if (p != NULL) {
            p->next = m->proxy_head;
            m->proxy_head = p;

            int n = cproxy_listen(p);
            if (n > 0) {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "cproxy_listen success %u to %s with %d conns\n",
                            p->port, p->config, n);
                m->stat_proxy_starts++;
            } else {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "cproxy_listen failed on %u to %s\n",
                            p->port, p->config);
                m->stat_proxy_start_fails++;
            }
        }
    } else {
        if (settings.verbose > 1)
            fprintf(stderr, "cproxy main existing config change %u\n",
                    p->port);

        pthread_mutex_lock(&p->proxy_lock);

        if (settings.verbose > 1) {
            if (p->name && name &&
                strcmp(p->name, name) != 0)
                fprintf(stderr,
                        "cproxy main name changed from %s to %s\n",
                        p->name, name);

            if (p->config && config &&
                strcmp(p->config, config) != 0)
                fprintf(stderr,
                        "cproxy main config changed from %s to %s\n",
                        p->config, config);
        }

        if ((p->name != NULL) &&
            (name == NULL ||
             strcmp(p->name, name) != 0)) {
            free(p->name);
            p->name = NULL;

            if (settings.verbose > 1)
                fprintf(stderr, "agent_config name changed\n");
        }
        if (p->name == NULL && name != NULL) {
            p->name = strdup(name);
        }

        if ((p->config != NULL) &&
            (config == NULL ||
             strcmp(p->config, config) != 0)) {
            free(p->config);
            p->config = NULL;

            if (settings.verbose > 1)
                fprintf(stderr, "agent_config config changed\n");
        }
        if (p->config == NULL && config != NULL) {
            p->config = strdup(config);
        }

        if ((p->behaviors != NULL) &&
            (behaviors == NULL ||
             cproxy_equal_behaviors(p->behaviors_num,
                                    p->behaviors,
                                    behaviors_num,
                                    behaviors) == false)) {
            free(p->behaviors);
            p->behaviors = NULL;
            p->behaviors_num = 0;

            if (settings.verbose > 1)
                fprintf(stderr, "agent_config behaviors changed\n");
        }
        if (p->behaviors == NULL && behaviors != NULL) {
            p->behaviors = cproxy_copy_behaviors(behaviors_num,
                                                 behaviors);
            p->behaviors_num = behaviors_num;
        }

        if (p->name != NULL &&
            p->config != NULL &&
            p->behaviors != NULL)
            m->stat_proxy_existings++;
        else
            m->stat_proxy_shutdowns++;

        assert(config_ver != p->config_ver);

        p->config_ver = config_ver;

        pthread_mutex_unlock(&p->proxy_lock);
    }
}

// ----------------------------------------------------------

char **get_key_values(kvpair_t *kvs, char *key) {
    kvpair_t *x = find_kvpair(kvs, key);
    if (x != NULL)
        return x->values;
    return NULL;
}

