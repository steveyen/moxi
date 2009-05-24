/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <math.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"

void front_cache_foreach_free(gpointer key,
                              gpointer value,
                              gpointer user_data);

void front_cache_start(proxy *p, proxy_behavior *behavior) {
    assert(p);
    assert(p->front_cache == NULL);
    assert(behavior);

    pthread_mutex_lock(&p->front_cache_lock);

    if (behavior->front_cache_lifespan > 0 &&
        strlen(behavior->front_cache_spec) > 0) {
        p->front_cache = g_hash_table_new_full(skey_hash,
                                               skey_equal,
                                               helper_g_free,
                                               NULL);
        matcher_init(&p->front_cache_matcher,
                     behavior->front_cache_spec);
    } else {
        p->front_cache = NULL;
    }

    pthread_mutex_unlock(&p->front_cache_lock);
}

void front_cache_stop(proxy *p) {
    assert(p);

    pthread_mutex_lock(&p->front_cache_lock);

    matcher_free(&p->front_cache_matcher);

    if (p->front_cache != NULL) {
        g_hash_table_foreach(p->front_cache,
                             front_cache_foreach_free,
                             NULL);
        g_hash_table_destroy(p->front_cache);
        p->front_cache = NULL;
    }

    pthread_mutex_unlock(&p->front_cache_lock);
}

void front_cache_foreach_free(gpointer key,
                              gpointer value,
                              gpointer user_data) {
    assert(key);
    free(key);

    assert(value);
    item_remove((item *) value);
}

