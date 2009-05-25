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

void mcache_item_free(gpointer value);

void mcache_init(mcache *m, bool multithreaded) {
    assert(m);

    m->map         = NULL;
    m->oldest_live = 0;

    if (multithreaded) {
        m->lock = malloc(sizeof(pthread_mutex_t));
        if (m->lock != NULL)
            pthread_mutex_init(m->lock, NULL);
    } else {
        m->lock = NULL;
    }

    mcache_reset_stats(m);
}

void mcache_reset_stats(mcache *m) {
    assert(m);

    if (m->lock)
        pthread_mutex_lock(m->lock);

    m->tot_get_hits = 0;
    m->tot_get_expires = 0;
    m->tot_get_misses = 0;
    m->tot_adds = 0;
    m->tot_add_skips = 0;

    if (m->lock)
        pthread_mutex_unlock(m->lock);
}

void mcache_start(mcache *m) {
    assert(m);
    assert(m->map == NULL);

    if (m->lock)
        pthread_mutex_lock(m->lock);

    m->map = g_hash_table_new_full(skey_hash,
                                   skey_equal,
                                   helper_g_free,
                                   mcache_item_free);
    m->oldest_live = 0;

    if (m->lock)
        pthread_mutex_unlock(m->lock);
}

bool mcache_started(mcache *m) {
    assert(m);

    if (m->lock)
        pthread_mutex_lock(m->lock);

    bool rv = m->map != NULL;

    if (m->lock)
        pthread_mutex_unlock(m->lock);

    return rv;
}

void mcache_stop(mcache *m) {
    assert(m);

    if (m->lock)
        pthread_mutex_lock(m->lock);

    if (m->map != NULL) {
        g_hash_table_destroy(m->map);
        m->map = NULL;
    }

    m->oldest_live = 0;

    if (m->lock)
        pthread_mutex_unlock(m->lock);
}

item *mcache_get(mcache *m, char *key, int key_len,
                 uint32_t curr_time) {
    assert(key);

    if (m == NULL)
        return NULL;

    if (m->lock)
        pthread_mutex_lock(m->lock);

    if (m->map != NULL) {
        item *it = g_hash_table_lookup(m->map, key);
        if (it != NULL) {
            assert(it->nkey == key_len);
            assert(strncmp(ITEM_key(it), key, it->nkey) == 0);

            // TODO: Need configurable cache oldest_live
            // mark to implement fast FLUSH_ALL.
            //
            if (it->exptime >= curr_time &&
                it->exptime >= m->oldest_live) {
                // TODO: Stats for front cache hit.
                //
                it->refcount++; // TODO: Need locking here?

                m->tot_get_hits++;

                if (m->lock)
                    pthread_mutex_unlock(m->lock);

                if (settings.verbose > 1)
                    fprintf(stderr,
                            "mcache hit: %s\n", key);

                return it;
            }

            m->tot_get_expires++;

            if (settings.verbose > 1)
                fprintf(stderr,
                        "mcache expire: %s\n", key);

            // Handle item expiry.
            //
            // TODO: Stats for mcache expiry.
            // TODO: Track mcache size.
            //
            g_hash_table_remove(m->map, key);
        } else {
            m->tot_get_misses++;
        }
    }

    if (m->lock)
        pthread_mutex_unlock(m->lock);

    return NULL;
}

void mcache_add(mcache *m, item *it,
                uint32_t lifespan,
                uint32_t curr_time) {
    assert(it);

    if (m == NULL)
        return;

    // TODO: Our lock areas are too wide.
    //
    if (m->lock)
        pthread_mutex_lock(m->lock);

    if (m->map != NULL) {
        // The ITEM_key is not NULL or space terminated,
        // and we need a copy, too, for hashtable ownership.
        //
        char *key_buf = malloc(it->nkey + 1);
        if (key_buf != NULL) {
            memcpy(key_buf, ITEM_key(it), it->nkey);
            key_buf[it->nkey] = '\0';

            // TODO: Would be nice if there was a g_hash_table_add().
            //
            if (g_hash_table_lookup(m->map,
                                    key_buf) == NULL) {
                // TODO: Need configurable L1 cache expiry.
                //
                it->exptime = curr_time + lifespan;

                it->refcount++; // TODO: Need item lock here?

                g_hash_table_insert(m->map, key_buf, it);

                m->tot_adds++;

                if (settings.verbose > 1)
                    fprintf(stderr,
                            "mcache add: %s\n", key_buf);
            } else {
                m->tot_add_skips++;

                if (settings.verbose > 1)
                    fprintf(stderr,
                            "mcache add-skip: %s\n", key_buf);

                free(key_buf);
            }
        }
    }

    if (m->lock)
        pthread_mutex_unlock(m->lock);
}

void mcache_delete(mcache *m, char *key, int key_len) {
    assert(key);
    assert(key_len > 0);
    assert(key[key_len] == '\0' ||
           key[key_len] == ' ');

    if (m == NULL)
        return;

    if (m->lock)
        pthread_mutex_lock(m->lock);

    if (m->map != NULL) {
        // Handle item expiry.
        //
        // TODO: Stats for mcache expiry.
        // TODO: Track mcache size.
        //
        g_hash_table_remove(m->map, key);
    }

    if (m->lock)
        pthread_mutex_unlock(m->lock);
}

void mcache_flush_all(mcache *m, uint32_t msec_exp) {
    if (m == NULL)
        return;

    if (m->lock)
        pthread_mutex_lock(m->lock);

    if (m->map != NULL) {
        g_hash_table_remove_all(m->map);

        m->oldest_live = msec_exp;
    }

    if (m->lock)
        pthread_mutex_unlock(m->lock);
}

void mcache_item_free(gpointer value) {
    if (value != NULL)
        item_remove((item *) value);
}

