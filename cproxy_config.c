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

// Integration with memagent.
//
void on_memagent_new_config(void *userdata, kvpair_t *config);
void on_memagent_get_stats(void *userdata, void *opaque,
                           agent_add_stat add_stat);

void cproxy_on_new_config(void *data0, void *data1);

void cproxy_on_new_pool(proxy_main *m,
                        char *name, int port,
                        char *config, uint32_t config_ver);

char **get_key_values(kvpair_t *kvs, char *key);

kvpair_t *copy_kvpairs(kvpair_t *orig);

static int cproxy_init_string(const char *cfg, int nthreads,
                              int downstream_max);

static int cproxy_init_agent(char *jid, char *jpw,
                             char *config, char *host,
                             int nthreads, int downstream_max);

int cproxy_init(const char *cfg, int nthreads,
                int downstream_max) {
    assert(nthreads > 1); // Main + at least one worker.
    assert(nthreads == settings.num_threads);
    assert(downstream_max > 0);

    if (cfg == NULL ||
        strlen(cfg) <= 0)
        return 0;

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_init %s\n", cfg);

    if (strchr(cfg, '@') == NULL) // Not jid format.
        return cproxy_init_string(cfg, nthreads, downstream_max);

    char *buff = strdup(cfg);
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

        // TODO: Better config/init error handling.
        //
        if (jid == NULL) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init missing jid\n");
        } else if (jpw == NULL) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init missing jpw\n");
        } else if (config == NULL) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init missing config\n");
        } else if (host == NULL) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init missing host\n");
        } else {
            if (cproxy_init_agent(jid, jpw, config, host,
                                  nthreads, downstream_max) == 0)
                rv++;
        }
    }

    free(buff);

    return rv;
}

static int cproxy_init_agent(char *jid, char *jpw,
                             char *config_path, char *host,
                             int nthreads, int downstream_max) {
    assert(jid);
    assert(jpw);
    assert(config_path);
    assert(host);

    proxy_main *m = calloc(1, sizeof(proxy_main));
    if (m != NULL) {
        m->proxy_head     = NULL;
        m->nthreads       = nthreads;
        m->downstream_max = downstream_max;

        m->stat_configs      = 0;
        m->stat_config_fails = 0;
        m->stat_proxy_starts      = 0;
        m->stat_proxy_start_fails = 0;
        m->stat_proxy_existings   = 0;
        m->stat_proxy_shutdowns   = 0;

        agent_config_t config; // Immutable.

        memset(&config, 0, sizeof(config));

        // Different jid's possible for production, staging, etc.
        config.jid  = jid;  // "customer@stevenmb.local"
        config.pass = jpw;  // "password"
        config.host = host; // "localhost" // TODO: XMPP server, for dev.
        config.software   = "memscale";
        config.version    = "0.1";       // TODO: Version.
        config.save_path  = config_path; // "/tmp/memscale.cfg"
        config.userdata   = m;
        config.new_config = on_memagent_new_config;
        config.get_stats  = on_memagent_get_stats;

        if (start_agent(config)) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init done\n");

            return 0;
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy could not start memagent\n");

    return 1;
}

void on_memagent_new_config(void *userdata, kvpair_t *config) {
    assert(config != NULL);

    proxy_main *m = userdata;
    assert(m != NULL);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread != NULL);

    kvpair_t *copy = copy_kvpairs(config);
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
    // 	customer1-b
    // 	    localhost:11311
    // 	    localhost:11312
    // 	customer1-a
    // 	    localhost:11211
    // 	-bindings-
    // 	    11221
    // 	    11331
    // 	-pools-
    // 	    customer1-a
    // 	    customer1-b
    //
    char **pools = get_key_values(kvs, "-pools-");
    if (pools == NULL)
        goto fail;

    char **bindings = get_key_values(kvs, "-bindings-");
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

    for (int i = 0; i < npools; i++) {
        assert(pools[i]);
        assert(bindings[i]);

        if (pools[i] != NULL &&
            bindings[i] != NULL) {
            char  *pool_name = pools[i];
            int    pool_port = atoi(bindings[i]);
            char **servers   = get_key_values(kvs, pool_name);

            assert(pool_name);
            assert(pool_port >= 0);
            assert(servers);

            // Create a config string that libmemcached likes,
            // first by counting up buffer size needed.
            //
            int n = 0;

            for (int j = 0; servers[j]; j++)
                n = n + strlen(servers[j]) + 50;

            char *config = calloc(n, 1);
            if (config != NULL) {
                for (int j = 0; servers[j]; j++) {
                    char *cur = config + strlen(config); // TODO: O(N^2).

                    if (j == 0)
                        sprintf(cur, "%s", servers[j]);
                    else
                        sprintf(cur, ",%s", servers[j]);
                }

                cproxy_on_new_pool(m, pool_name, pool_port,
                                   config, new_config_ver);

                free(config);
            }
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
        bool  down   = false;
        int   port   = 0;
        char *name   = NULL;
        char *config = NULL;

        pthread_mutex_lock(&p->proxy_lock);

        if (p->config_ver != new_config_ver) {
            down = true;
            port = p->port;

            if (p->name != NULL)
                name = strdup(p->name);
            if (p->config != NULL)
                config = strdup(p->config);
        }

        pthread_mutex_unlock(&p->proxy_lock);

        if (down)
            cproxy_on_new_pool(m, NULL, port, NULL, new_config_ver);

        if (name != NULL)
            free(name);

        if (config != NULL)
            free(config);
    }

    free_kvpair(kvs);
    return;

 fail:
    m->stat_config_fails++;
    free_kvpair(kvs);
}

void cproxy_on_new_pool(proxy_main *m,
                        char *name, int port,
                        char *config, uint32_t config_ver) {
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
        p = cproxy_create(name, port, config, config_ver,
                          m->nthreads, m->downstream_max);
        if (p != NULL) {
            p->next = m->proxy_head;
            m->proxy_head = p;

            int n = cproxy_listen(p);
            if (n > 0) {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "cproxy_listen success on %u to %s with %d conns\n",
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
        }
        if (p->name == NULL && name != NULL)
            p->name = strdup(name);

        if ((p->config != NULL) &&
            (config == NULL ||
             strcmp(p->config, config) != 0)) {
            free(p->config);
            p->config = NULL;
        }
        if (p->config == NULL && config != NULL)
            p->config = strdup(config);

        if (p->name != NULL &&
            p->config != NULL)
            m->stat_proxy_existings++;
        else
            m->stat_proxy_shutdowns++;

        assert(config_ver != p->config_ver);

        p->config_ver = config_ver;

        pthread_mutex_unlock(&p->proxy_lock);
    }
}

// ----------------------------------------------------------

static int cproxy_init_string(const char *cfg, int nthreads,
                              int downstream_max) {
    /* cfg should look like "local_port=host:port,host:port;local_port=host:port"
     * like "11222=memcached1.foo.net:11211"  This means local port 11222
     * will be a proxy to downstream memcached server running at
     * host memcached1.foo.net on port 11211.
     */
    assert(nthreads > 1); // Main + at least one worker.
    assert(nthreads == settings.num_threads);
    assert(downstream_max > 0);

    if (cfg == NULL ||
        strlen(cfg) <= 0)
        return 0;

    char *buff;
    char *next;
    char *proxy_name = "default";
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

        proxy *p = cproxy_create(proxy_name, proxy_port, proxy_sect, 0,
                                 nthreads, downstream_max);
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

    return 0;
}

char **get_key_values(kvpair_t *kvs, char *key) {
    assert(kvs);
    assert(key);

    while (kvs != NULL) {
        assert(kvs->key);

        if (strcmp(kvs->key, key) == 0)
            return kvs->values;

        kvs = kvs->next;
    }

    return NULL;
}

kvpair_t *copy_kvpairs(kvpair_t *orig) {
    if (orig != NULL) {
        kvpair_t *copy = mk_kvpair(orig->key, orig->values);
        if (copy != NULL) {
            copy->next = copy_kvpairs(orig->next);
            return copy;
        }
    }
    return NULL;
}

