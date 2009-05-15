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
    //    localhost:11311
    //    localhost:11312
    //  pool-customer1-a
    //    localhost:11211
    //  svr-localhost:11311
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
}

