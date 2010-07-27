/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"
#include "agent.h"
#include "log.h"

// Local declarations.
//
struct ping_test_recipe {
    char *name;
    int keysize;
    int valsize;
    int iterations;
};

static void ping_server(char *server_name,
                        struct ping_test_recipe *recipes,
                        proxy_behavior *behavior,
                        conflate_form_result *r);

static int charlistlen(char **in) {
    int rv = 0;
    while (in[rv]) {
        rv++;
    }
    return rv;
}

static bool starts_with(const char* needle, const char *haystack)
{
    return strncmp(needle, haystack, strlen(needle)) == 0;
}

static void load_ping_recipe(char **params,
                             struct ping_test_recipe *out)
{
    assert(params);
    assert(out);

    for (int i = 0; params[i]; i++) {
        int value = 0;
        char *p = NULL;

        p = strchr(params[i], '=');
        assert(p);

        if (!safe_strtol(p+1, &value)) {
            moxi_log_write("Failed to parse ``%s'' as number\n", p+1);
            assert(false);
        }

        if (starts_with("ksize=", params[i])) {
            out->keysize = value;
        } else if (starts_with("vsize=", params[i])) {
            out->valsize = value;
        } else if (starts_with("iterations=", params[i])) {
            out->iterations = value;
        } else {
            if (settings.verbose > 1) {
                moxi_log_write("Unknown recipe property:  %s\n", params[i]);
            }
        }
    }
}

enum conflate_mgmt_cb_result on_conflate_ping_test(void *userdata,
                                                   conflate_handle_t *handle,
                                                   const char *cmd,
                                                   bool direct,
                                                   kvpair_t *form,
                                                   conflate_form_result *r)
                           {
    assert(userdata);

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
    //  tests
    //    test1
    //    test2
    //  def-test1
    //    ksize=16
    //    vsize=16
    //    iterations=500
    //  def-test2
    //    ksize=64
    //    vsize=524288
    //    iterations=50
    //
    if (!form) {
        return RV_BADARG;
    }

    char detail_key[200];

    // Discover test configuration.
    char **tests = get_key_values(form, "tests");
    int nrecipes = charlistlen(tests);
    struct ping_test_recipe recipes[nrecipes+1];
    memset(recipes, 0x00, sizeof(struct ping_test_recipe) * (nrecipes+1));
    for (int j = 0; j < nrecipes; j++) {
        snprintf(detail_key, sizeof(detail_key), "def-%s", tests[j]);
        recipes[j].name = strdup(detail_key);
        assert(recipes[j].name);
        load_ping_recipe(get_key_values(form, detail_key), &recipes[j]);
    }

    // Initialize each server and run the tests
    char **servers = get_key_values(form, "servers");
    for (int j = 0; servers != NULL && servers[j]; j++) {
        snprintf(detail_key, sizeof(detail_key),
                 "svr-%s", servers[j]);

        if (settings.verbose > 1) {
            moxi_log_write("ping_test %s\n", detail_key);
        }

        proxy_behavior behavior;

        memset(&behavior, 0, sizeof(behavior));

        char **props = get_key_values(form, detail_key);
        for (int k = 0; props && props[k]; k++) {
            cproxy_parse_behavior_key_val_str(props[k], &behavior);
        }

        ping_server(servers[j], recipes, &behavior, r);
    }

    /* The recipe memory allocations */
    for (int j = 0; j < nrecipes; j++) {
        free(recipes[j].name);
    }

    return RV_OK;
}

#ifndef MOXI_USE_VBUCKET
static void perform_ping_test(struct ping_test_recipe recipe,
                              memcached_st *mst,
                              struct moxi_stats *out, int *failures)
{
    double *timing_results = calloc(recipe.iterations, sizeof(double));
    assert(timing_results);

    struct timeval timing = { 0, 0 };

    char *key = calloc(recipe.keysize, sizeof(char));
    assert(key);
    char *value = calloc(recipe.valsize, sizeof(char));
    assert(value);

    /* Key is all 't's...just because */
    memset(key, 't', recipe.keysize);
    /* Value is a random bunch of stuff */
    for (int i = 0; i < recipe.valsize; i++) {
        value[i] = random() & 0xff;
    }

    if (memcached_set(mst,
                      key, recipe.keysize,
                      value, recipe.valsize,
                      0, 0) != MEMCACHED_SUCCESS) {
        /* XXX: Failure */
    }

    for (int i = 0 ; i < recipe.iterations; i++) {
        struct timeval tv_pre = { 0, 0 } , tv_post = { 0, 0 };
        size_t retrieved_len = 0;
        uint32_t flags = 0;
        memcached_return error;

        gettimeofday(&tv_pre, NULL);

        char *retrieved = memcached_get(mst,
                                        key, recipe.keysize,
                                        &retrieved_len, &flags,
                                        &error);

        gettimeofday(&tv_post, NULL);
        timeval_subtract(&timing, &tv_post, &tv_pre);
        timing_results[i] = timeval_to_double(timing);

        if (retrieved) {
            free(retrieved);
        } else {
            (*failures)++;
        }
    }

    compute_stats(out, timing_results, recipe.iterations);
    free(timing_results);
    free(key);
    free(value);
}
#endif // !MOXI_USE_VBUCKET

static void ping_server(char *server_name,
                        struct ping_test_recipe *recipes,
                        proxy_behavior *behavior,
                        conflate_form_result *r) {
#ifndef MOXI_USE_VBUCKET
    assert(server_name);
    assert(behavior);
    assert(r);

    if (strlen(behavior->host) <= 0 ||
        behavior->port <= 0)
        return;

    memcached_st         mst;
    memcached_server_st *mservers;

    struct timeval timing;

    conflate_next_fieldset(r);
    conflate_add_field(r, "-set-", server_name);

    char  buf[300] = { 0x00 };

#define dbl_report(name, dval)                  \
    snprintf(buf, sizeof(buf), "%f", dval);     \
    conflate_add_field(r, name, buf);

#define int_report(name, ival)                  \
    snprintf(buf, sizeof(buf), "%d", ival);     \
    conflate_add_field(r, name, buf);

#define tv_report(name, mark, val)                  \
    timeval_subtract(&timing, &val, &mark);         \
    dbl_report(name, timeval_to_double(timing));

#define stat_report(buf, buflen, name, type, dval)  \
    snprintf(buf, buflen, "%s_%s", name, type);     \
    dbl_report(buf, dval);

    if (memcached_create(&mst) != NULL) {
        memcached_behavior_set(&mst, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
        memcached_behavior_set(&mst, MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);

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
            bool connected  = false;

            for (int i = 0; i < nconns; i++) {
                if (settings.verbose > 1)
                    moxi_log_write("ping_test connecting %d\n", i);

                struct timeval start;
                gettimeofday(&start, NULL);

                memcached_return rc = memcached_connect(&mst.servers[i]);
                if (rc == MEMCACHED_SUCCESS) {
                    struct timeval tv_conn;
                    gettimeofday(&tv_conn, NULL);
                    tv_report("conn", start, tv_conn);

                    if (cproxy_auth_downstream(&mst.servers[i],
                                               behavior) &&
                        cproxy_bucket_downstream(&mst.servers[i],
                                                 behavior)) {
                        struct timeval tv_auth;
                        gettimeofday(&tv_auth, NULL);
                        tv_report("auth", tv_conn, tv_auth);

                        // Flag whether to proceed if we connected
                        connected = true;
                    }
                }
            }

            if (connected) {
                for (int j = 0; recipes[j].name; j++) {
                    struct moxi_stats recipe_stats = { 0.0 };
                    int failures = 0;

                    perform_ping_test(recipes[j], &mst,
                                      &recipe_stats, &failures);
                    int vlen = strlen(recipes[j].name) + 8;
                    char val_name[vlen];

                    stat_report(val_name, vlen, recipes[j].name,
                                "min", recipe_stats.min);
                    stat_report(val_name, vlen, recipes[j].name,
                                "avg", recipe_stats.avg);
                    stat_report(val_name, vlen, recipes[j].name,
                                "max", recipe_stats.max);
                    stat_report(val_name, vlen, recipes[j].name,
                                "stddev", recipe_stats.stddev);
                    stat_report(val_name, vlen, recipes[j].name,
                                "95th", recipe_stats.ninetyfifth);


                    snprintf(val_name, vlen, "%s_fail", recipes[j].name);
                    int_report(val_name, failures);
                }
            }
        } else {
            conflate_add_field(r, "error", "Didn't work. :(");
        }

        memcached_free(&mst);
    }
#endif // !MOXI_USE_VBUCKET
}

