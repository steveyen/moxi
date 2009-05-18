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
void ping_server(char *server_name,
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
    //  servers
    //    svrname1
    //    svrname2
    //  svr-svrname1
    //    host=mc1.foo.net
    //    port=11211
    //    bucket=buck1
    //    usr=test1
    //    pwd=password
    //  svr-svrname2
    //    host=mc2.foo.net
    //    port=11211
    //    bucket=buck1
    //    usr=test1
    //    pwd=password
    //
    if (form != NULL) {
        char   server_key[200];
        char **servers = get_key_values(form, "servers");

        for (int j = 0; servers != NULL && servers[j]; j++) {
            snprintf(server_key, sizeof(server_key),
                     "svr-%s", servers[j]);

            if (settings.verbose > 1)
                fprintf(stderr, "ping_test %s\n",
                        server_key);

            proxy_behavior behavior;

            memset(&behavior, 0, sizeof(behavior));

            char **props = get_key_values(form, server_key);
            for (int k = 0; props && props[k]; k++) {
                cproxy_parse_behavior_key_val_str(props[k],
                                                  &behavior);
            }

            ping_server(servers[j], &behavior,
                        opaque, add_report);
        }
    }

    add_report(opaque, NULL, NULL);
}

void ping_server(char *server_name,
                 proxy_behavior *behavior,
                 void *opaque,
                 conflate_add_ping_report add_report) {
    assert(server_name);
    assert(behavior);
    assert(add_report);

    if (strlen(behavior->host) <= 0 ||
        behavior->port <= 0)
        return;

    kvpair_t *kvr = mk_kvpair(server_name, NULL);
    if (kvr == NULL)
        return;

    memcached_st         mst;
    memcached_server_st *mservers;

    char buf[300];

#define tv_report(name, val)                           \
    snprintf(buf, sizeof(buf), "%s=%llu-%llu", name,   \
            (long long unsigned int) ((val).tv_sec),   \
            (long long unsigned int) ((val).tv_usec)); \
    add_kvpair_value(kvr, buf);

    if (memcached_create(&mst) != NULL) {
        memcached_behavior_set(&mst, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);

        snprintf(buf, sizeof(buf),
                 "%s:%u",
                 behavior->host,
                 behavior->port);

        mservers = memcached_servers_parse(buf);
        if (mservers != NULL) {
            memcached_server_push(&mst, mservers);
            memcached_server_list_free(mservers);
            mservers = NULL;

            int nconns = memcached_server_count(&mst);
            bool vers  = false;

            for (int i = 0; i < nconns; i++) {
                if (settings.verbose > 1)
                    fprintf(stderr, "ping_test connecting %d\n", i);

                struct timeval tv_start;
                gettimeofday(&tv_start, NULL);
                tv_report("tv_start", tv_start);

                memcached_return rc = memcached_connect(&mst.hosts[i]);
                if (rc == MEMCACHED_SUCCESS) {
                    struct timeval tv_conn;
                    gettimeofday(&tv_conn, NULL);
                    tv_report("tv_conn", tv_conn);

                    if (cproxy_auth_downstream(&mst.hosts[i],
                                               behavior) &&
                        cproxy_bucket_downstream(&mst.hosts[i],
                                                 behavior)) {
                        struct timeval tv_auth;
                        gettimeofday(&tv_auth, NULL);
                        tv_report("tv_auth", tv_auth);

                        // Only bother with version if at least one
                        // server is authorized.
                        //
                        vers = true;
                    }
                }
            }

            // TODO: Need a better ping test here.
            // TODO: Hardcoded iteration here.
            // TODO: Set a few small & big values, and get them.
            //
            for (int i = 0; vers && i < 5; i++) {
                struct timeval tv_version_pre;
                gettimeofday(&tv_version_pre, NULL);
                tv_report("tv_version_pre", tv_version_pre);

                memcached_version(&mst);

                struct timeval tv_version_post;
                gettimeofday(&tv_version_post, NULL);
                tv_report("tv_version_post", tv_version_post);
            }
        }

        memcached_free(&mst);
    }
}

