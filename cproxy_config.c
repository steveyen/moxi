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

int cproxy_init(const char *cfg, int nthreads,
                int default_downstream_max) {
    assert(nthreads > 1); // Main + at least one worker.
    assert(nthreads == settings.num_threads);
    assert(default_downstream_max > 0);

    proxy_main *m = calloc(1, sizeof(proxy_main));
    if (m != NULL) {
        m->proxy_head             = NULL;
        m->nthreads               = nthreads;
        m->default_downstream_max = default_downstream_max;

        // Different jid's for production, staging, etc.
        m->config.jid = "customer@stevenmb.local";
        m->config.pass = "password";
        m->config.host = "localhost"; // TODO: XMPP server host, for dev.
        m->config.software = "memscale";
        m->config.version = "0.1";
        m->config.save_path = "/tmp/memscale.db";
        m->config.userdata = m;
        m->config.new_serverlist = on_memagent_new_serverlists;
        m->config.get_stats = on_memagent_get_stats;

        if (start_agent(m->config)) {
            if (settings.verbose > 1)
                fprintf(stderr, "cproxy_init done\n");

            return 0;
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy could not start memagent\n");

    return 1;
}

void on_memagent_new_serverlists(void *userdata,
                                 memcached_server_list_t **lists) {
    assert(lists != NULL);

    proxy_main *m = userdata;
    assert(m != NULL);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "on_memagent_new_serverlist\n");

    int n = 0;
    while (lists[n])
        n++;

    memcached_server_list_t **lists_copy =
        calloc(n, sizeof(memcached_server_list_t *));
    if (lists_copy != NULL) {
        int i;

        for (i = 0; i < n; i++) {
            lists_copy[i] = copy_server_list(lists[i]);
            if (lists_copy[i] == NULL)
                break;
        }

        if (i >= n &&
            work_send(mthread->work_queue,
                      cproxy_on_new_serverlists,
                      m, lists_copy))
            return; // Success.

        for (i = 0; i < n; i++) {
            if (lists_copy[i] != NULL)
                free_server_list(lists_copy[i]);
        }

        free(lists_copy);
    }
}

void cproxy_on_new_serverlists(void *data0, void *data1) {
    proxy_main *m = data0;
    assert(m);
    memcached_server_list_t **lists = data1;
    assert(lists);
    assert(is_listen_thread());

    uint32_t max_config_ver = 0;

    for (proxy *p = m->proxy_head; p != NULL; p = p->next)
        if (max_config_ver < p->config_ver)
            max_config_ver = p->config_ver;

    uint32_t new_config_ver = max_config_ver + 1;

    for (int i = 0; lists[i]; i++) {
        cproxy_on_new_serverlist(m, lists[i], new_config_ver);
        free_server_list(lists[i]);
    }

    for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
        if (p->config_ver != new_config_ver) {
            // TODO: Shutdown old proxies.
        }
    }

    free(lists);
}

void cproxy_on_new_serverlist(proxy_main *m,
                              memcached_server_list_t *list,
                              uint32_t config_ver) {
    assert(m);
    assert(list);
    assert(list->servers);
    assert(is_listen_thread());

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
    if (cfg == NULL)
        return;

    for (int j = 0; list->servers[j]; j++) {
        memcached_server_t *server = list->servers[j];
        char *cur = cfg + strlen(cfg);
        if (j == 0)
            sprintf(cur, "%s:%u", server->host, server->port);
        else
            sprintf(cur, ",%s:%u", server->host, server->port);
    }

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy main has new cfg: %s (bound to %d) %u\n",
                cfg, list->binding, config_ver);

    // See if we've already got a proxy running on the port,
    // and create one if needed.
    //
    proxy *p = m->proxy_head;
    while (p != NULL &&
           p->port != list->binding)
        p = p->next;

    if (p == NULL) {
        if (settings.verbose > 1)
            fprintf(stderr, "cproxy main creating new proxy for %s on %d\n",
                    cfg, list->binding);

        p = cproxy_create(list->name, list->binding, cfg, config_ver,
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
            fprintf(stderr, "cproxy main handling existing config change %u\n",
                    p->port);

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
                fprintf(stderr,
                        "cproxy main config changed from %s to %s\n",
                        p->config, cfg);

            free(p->config);
            p->config = cfg;
            cfg = NULL;
        }

        assert(config_ver != p->config_ver);

        p->config_ver = config_ver;

        pthread_mutex_unlock(&p->proxy_lock);
    }

    if (cfg != NULL)
        free(cfg);
}

