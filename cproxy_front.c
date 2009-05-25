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
void mcache_item_unlink(mcache *m, item *it);
void mcache_item_touch(mcache *m, item *it);

void mcache_init(mcache *m, bool multithreaded) {
    assert(m);

    m->map         = NULL;
    m->max         = 0;
    m->lru_head    = NULL;
    m->lru_tail    = NULL;
    m->oldest_live = 0;

    if (multithreaded) {
        m->lock = malloc(sizeof(pthread_mutex_t));
        if (m->lock != NULL) {
            pthread_mutex_init(m->lock, NULL);
        }
    } else {
        m->lock = NULL;
    }

    mcache_reset_stats(m);
}

void mcache_reset_stats(mcache *m) {
    assert(m);

    if (m->lock)
        pthread_mutex_lock(m->lock);

    m->tot_get_hits    = 0;
    m->tot_get_expires = 0;
    m->tot_get_misses  = 0;
    m->tot_adds        = 0;
    m->tot_add_skips   = 0;
    m->tot_add_fails   = 0;
    m->tot_deletes     = 0;
    m->tot_evictions   = 0;

    if (m->lock)
        pthread_mutex_unlock(m->lock);
}

void mcache_start(mcache *m, uint32_t max) {
    assert(m);

    if (m->lock)
        pthread_mutex_lock(m->lock);

    assert(m->map == NULL);
    assert(m->max == 0);
    assert(m->lru_head == NULL);
    assert(m->lru_tail == NULL);
    assert(m->oldest_live == 0);

    m->map = g_hash_table_new_full(skey_hash,
                                   skey_equal,
                                   helper_g_free,
                                   mcache_item_free);
    if (m->map != NULL) {
        m->max         = max;
        m->lru_head    = NULL;
        m->lru_tail    = NULL;
        m->oldest_live = 0;
    }

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

    if (m->map != NULL)
        g_hash_table_destroy(m->map);

    m->map         = NULL;
    m->max         = 0;
    m->lru_head    = NULL;
    m->lru_tail    = NULL;
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
        item *it = (item *) g_hash_table_lookup(m->map, key);
        if (it != NULL) {
            assert(it->nkey == key_len);
            assert(strncmp(ITEM_key(it), key, it->nkey) == 0);

            mcache_item_unlink(m, it);

            if (it->exptime >= curr_time &&
                it->exptime >= m->oldest_live) {
                mcache_item_touch(m, it);

                it->refcount++; // TODO: Need locking here?

                m->tot_get_hits++;

                if (m->lock)
                    pthread_mutex_unlock(m->lock);

                if (settings.verbose > 1)
                    fprintf(stderr,
                            "mcache hit: %s\n", key);

                return it;
            }

            // Handle item expiration.
            //
            m->tot_get_expires++;

            if (settings.verbose > 1)
                fprintf(stderr,
                        "mcache expire: %s\n", key);

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
    assert(it->next == NULL);
    assert(it->prev == NULL);

    if (m == NULL)
        return;

    // TODO: Our lock areas are possibly too wide.
    //
    if (m->lock)
        pthread_mutex_lock(m->lock);

    if (m->map != NULL) {
        // Evict some items if necessary.
        //
        for (int i = 0; m->lru_tail != NULL && i < 20; i++) {
            if (g_hash_table_size(m->map) < m->max)
                break;

            item *last_it = m->lru_tail;
            mcache_item_unlink(m, last_it);

            char buf[KEY_MAX_LENGTH + 10];
            memcpy(buf, ITEM_key(last_it), last_it->nkey);
            buf[last_it->nkey] = '\0';

            g_hash_table_remove(m->map, buf);

            m->tot_evictions++;
        }

        if (g_hash_table_size(m->map) < m->max) {
            // The ITEM_key is not NULL or space terminated,
            // and we need a copy, too, for hashtable ownership.
            //
            char *key_buf = malloc(it->nkey + 1);
            if (key_buf != NULL) {
                memcpy(key_buf, ITEM_key(it), it->nkey);
                key_buf[it->nkey] = '\0';

                item *existing =
                    (item *) g_hash_table_lookup(m->map, key_buf);
                if (existing != NULL) {
                    mcache_item_unlink(m, existing);
                    mcache_item_touch(m, existing);

                    m->tot_add_skips++;

                    if (settings.verbose > 1)
                        fprintf(stderr,
                                "mcache add-skip: %s\n", key_buf);

                    free(key_buf);
                } else {
                    it->exptime = curr_time + lifespan;

                    it->refcount++; // TODO: Need item lock here?

                    g_hash_table_insert(m->map, key_buf, it);

                    m->tot_adds++;

                    if (settings.verbose > 1)
                        fprintf(stderr,
                                "mcache add: %s\n", key_buf);
                }
            } else {
                m->tot_add_fails++;
            }
        } else {
            m->tot_add_fails++;
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
        item *existing = (item *) g_hash_table_lookup(m->map, key);
        if (existing != NULL) {
            mcache_item_unlink(m, existing);

            g_hash_table_remove(m->map, key);

            m->tot_deletes++;
        }
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

        m->lru_head = NULL;
        m->lru_tail = NULL;

        m->oldest_live = msec_exp;
    }

    if (m->lock)
        pthread_mutex_unlock(m->lock);
}

void mcache_item_free(gpointer value) {
    if (value != NULL)
        item_remove((item *) value);
}

void mcache_item_unlink(mcache *m, item *it) {
    assert(m);
    assert(it);

    if (m->lru_head == it)
        m->lru_head = it->next;

    if (m->lru_tail == it)
        m->lru_tail = it->prev;

    if (it->next != NULL)
        it->next->prev = it->prev;

    if (it->prev != NULL)
        it->prev->next = it->next;

    it->next = NULL;
    it->prev = NULL;
}

/**
 * Push the item onto the head of the lru list.
 */
void mcache_item_touch(mcache *m, item *it) {
    assert(m);
    assert(it);
    assert(it->next == NULL);
    assert(it->prev == NULL);

    if (m->lru_head != NULL)
        m->lru_head->prev = it;
    it->next = m->lru_head;
    m->lru_head = it;
    if (m->lru_tail == NULL)
        m->lru_tail = it;
}
