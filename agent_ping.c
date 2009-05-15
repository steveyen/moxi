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

// From libmemcached.
//
memcached_return memcached_connect(memcached_server_st *ptr);
memcached_return memcached_version(memcached_st *ptr);

// Local declarations.
//
void ping_server(char *pool,
                 char *server,
                 proxy_behavior *behavior,
                 void *opaque,
                 conflate_add_ping_report add_report);

void on_conflate_ping_test(void *userdata, void *opaque,
                           kvpair_t *form,
                           conflate_add_ping_report add_report) {
    assert(userdata);
    assert(add_report);

    // The form key-multivalues looks roughly like...
    //
    //  pool-customer1-b
    //    svrname1
    //    svrname2
    //  pool-customer1-a
    //    svrname3
    //  svr-svrname1
    //    key1=val1
    //    key2=val2
    //  pools
    //    customer1-a
    //    customer1-b
    //
    if (form != NULL) {
        char   svr_key[200];
        char   pool_key[200];
        char **pools = get_key_values(form, "pools");

        for (int i = 0; pools != NULL && pools[i]; i++) {
            snprintf(pool_key, sizeof(pool_key),
                     "pool-%s", pools[i]);

            char **servers = get_key_values(form, pool_key);
            for (int j = 0; servers != NULL && servers[j]; j++) {
                snprintf(svr_key, sizeof(svr_key),
                         "svr-%s", servers[j]);

                proxy_behavior behavior;

                memset(&behavior, 0, sizeof(behavior));

                char **props = get_key_values(form, svr_key);
                for (int k = 0; props && props[k]; k++) {
                    cproxy_parse_behavior_key_val_str(props[k],
                                                      &behavior);
                }

                ping_server(pools[i], servers[j], &behavior,
                            opaque, add_report);
            }
        }
    }

    add_report(opaque, NULL, NULL);
}

void ping_server(char *pool,
                 char *server,
                 proxy_behavior *behavior,
                 void *opaque,
                 conflate_add_ping_report add_report) {
    kvpair_t *kvr = mk_kvpair(pool, NULL);
    kvr = NULL;

    memcached_st mst;

    if (memcached_create(&mst) != NULL) {
        memcached_behavior_set(&mst, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);

        memcached_server_st *mservers;

        mservers = memcached_servers_parse(server);
        if (mservers != NULL) {
            memcached_server_push(&mst, mservers);
            memcached_server_list_free(mservers);
            mservers = NULL;

            int nconns = memcached_server_count(&mst);

            for (int i = 0; i < nconns; i++) {
                struct timeval timer_start;
                gettimeofday(&timer_start, NULL);

                memcached_return rc = memcached_connect(&mst.hosts[i]);
                if (rc == MEMCACHED_SUCCESS) {
                    struct timeval timer_conn;
                    gettimeofday(&timer_conn, NULL);

                    if (cproxy_auth_downstream(&mst.hosts[i],
                                               behavior)) {
                        struct timeval timer_auth;
                        gettimeofday(&timer_auth, NULL);
                    }
                }
            }

            for (int i = 0; i < 5; i++) {
                struct timeval timer_version_pre;
                gettimeofday(&timer_version_pre, NULL);

                memcached_version(&mst);

                struct timeval timer_version_post;
                gettimeofday(&timer_version_post, NULL);
            }
        }

        memcached_free(&mst);
    }
}

