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

static char *item_key(void *it);
static int item_key_len(void *it);
static int item_len(void *it);
static void item_add_ref(void *it);
static void item_dec_ref(void *it);
static void *item_get_next(void *it);
static void item_set_next(void *it, void *next);
static void *item_get_prev(void *it);
static void item_set_prev(void *it, void *prev);
static uint32_t item_get_exptime(void *it);
static void item_set_exptime(void *it, uint32_t exptime);

void mcache_item_unlink(mcache *m, void *it);
void mcache_item_touch(mcache *m, void *it);

mcache_funcs mcache_item_funcs = {
    .item_key         = item_key,
    .item_key_len     = item_key_len,
    .item_len         = item_len,
    .item_add_ref     = item_add_ref,
    .item_dec_ref     = item_dec_ref,
    .item_get_next    = item_get_next,
    .item_set_next    = item_set_next,
    .item_get_prev    = item_get_prev,
    .item_set_prev    = item_set_prev,
    .item_get_exptime = item_get_exptime,
    .item_set_exptime = item_set_exptime
};

void mcache_init(mcache *m, bool multithreaded,
                 mcache_funcs *funcs) {
    assert(m);
    assert(funcs);

    m->funcs       = funcs;
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
    m->tot_get_bytes   = 0;
    m->tot_adds        = 0;
    m->tot_add_skips   = 0;
    m->tot_add_fails   = 0;
    m->tot_add_bytes   = 0;
    m->tot_deletes     = 0;
    m->tot_evictions   = 0;

    if (m->lock)
        pthread_mutex_unlock(m->lock);
}

void mcache_start(mcache *m, uint32_t max) {
    assert(m);

    if (m->lock)
        pthread_mutex_lock(m->lock);

    assert(m->funcs);
    assert(m->map == NULL);
    assert(m->max == 0);
    assert(m->lru_head == NULL);
    assert(m->lru_tail == NULL);
    assert(m->oldest_live == 0);

    m->map = g_hash_table_new_full(skey_hash,
                                   skey_equal,
                                   helper_g_free,
                                   m->funcs->item_dec_ref);
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

void *mcache_get(mcache *m, char *key, int key_len,
                 uint32_t curr_time) {
    assert(key);

    if (m == NULL)
        return NULL;

    assert(m->funcs);

    if (m->lock)
        pthread_mutex_lock(m->lock);

    if (m->map != NULL) {
        void *it = g_hash_table_lookup(m->map, key);
        if (it != NULL) {
            mcache_item_unlink(m, it);

            uint32_t exptime = m->funcs->item_get_exptime(it);
            if (exptime >= curr_time &&
                exptime >= m->oldest_live) {
                mcache_item_touch(m, it);

                m->funcs->item_add_ref(it); // TODO: Need lock here?

                m->tot_get_hits++;
                m->tot_get_bytes += m->funcs->item_len(it);

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

void mcache_add(mcache *m, void *it,
                uint32_t lifespan,
                uint32_t curr_time) {
    assert(it);
    assert(m->funcs);
    assert(m->funcs->item_get_next(it) == NULL);
    assert(m->funcs->item_get_prev(it) == NULL);

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

            void *last_it = m->lru_tail;
            mcache_item_unlink(m, last_it);

            int  len = m->funcs->item_key_len(last_it);
            char buf[KEY_MAX_LENGTH + 10];
            memcpy(buf, m->funcs->item_key(last_it), len);
            buf[len] = '\0';

            g_hash_table_remove(m->map, buf);

            m->tot_evictions++;
        }

        if (g_hash_table_size(m->map) < m->max) {
            // The ITEM_key is not NULL or space terminated,
            // and we need a copy, too, for hashtable ownership.
            //
            int   key_len = m->funcs->item_key_len(it);
            char *key_buf = malloc(key_len + 1);
            if (key_buf != NULL) {
                memcpy(key_buf, m->funcs->item_key(it), key_len);
                key_buf[key_len] = '\0';

                void *existing =
                    (void *) g_hash_table_lookup(m->map, key_buf);
                if (existing != NULL) {
                    mcache_item_unlink(m, existing);
                    mcache_item_touch(m, existing);

                    m->tot_add_skips++;

                    if (settings.verbose > 1)
                        fprintf(stderr,
                                "mcache add-skip: %s\n", key_buf);

                    free(key_buf);
                } else {
                    m->funcs->item_set_exptime(it, curr_time + lifespan);
                    m->funcs->item_add_ref(it); // TODO: Need item lock here?

                    g_hash_table_insert(m->map, key_buf, it);

                    m->tot_adds++;
                    m->tot_add_bytes += m->funcs->item_len(it);

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
        void *existing = (void *) g_hash_table_lookup(m->map, key);
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

void mcache_item_unlink(mcache *m, void *it) {
    assert(m);
    assert(m->funcs);
    assert(it);

    if (m->lru_head == it)
        m->lru_head = m->funcs->item_get_next(it);

    if (m->lru_tail == it)
        m->lru_tail = m->funcs->item_get_prev(it);

    void *next = m->funcs->item_get_next(it);
    if (next != NULL)
        m->funcs->item_set_prev(next, m->funcs->item_get_prev(it));

    void *prev = m->funcs->item_get_prev(it);
    if (prev != NULL)
        m->funcs->item_set_next(prev, m->funcs->item_get_next(it));

    m->funcs->item_set_next(it, NULL);
    m->funcs->item_set_prev(it, NULL);
}

/**
 * Push the item onto the head of the lru list.
 */
void mcache_item_touch(mcache *m, void *it) {
    assert(m);
    assert(m->funcs);
    assert(m->funcs->item_get_next(it) == NULL);
    assert(m->funcs->item_get_prev(it) == NULL);
    assert(it);

    if (m->lru_head != NULL)
        m->funcs->item_set_prev(m->lru_head, it);
    m->funcs->item_set_next(it, m->lru_head);
    m->lru_head = it;
    if (m->lru_tail == NULL)
        m->lru_tail = it;
}

// -------------------------------------------------

static char *item_key(void *it) {
    item *i = it;
    assert(i);
    return ITEM_key(i);
}


static int item_key_len(void *it) {
    item *i = it;
    assert(i);
    return i->nkey;
}

static int item_len(void *it) {
    item *i = it;
    assert(i);
    return i->nbytes;
}

static void item_add_ref(void *it) {
    item *i = it;
    if (i != NULL)
        i->refcount++; // TODO: Need item lock here?
}

static void item_dec_ref(void *it) {
    item *i = it;
    if (i != NULL)
        item_remove(i);
}

static void *item_get_next(void *it) {
    item *i = it;
    assert(i);
    return i->next;
}

static void item_set_next(void *it, void *next) {
    item *i = it;
    assert(i);
    i->next = (item *) next;
}

static void *item_get_prev(void *it) {
    item *i = it;
    assert(i);
    return i->prev;
}

static void item_set_prev(void *it, void *prev) {
    item *i = it;
    assert(i);
    i->prev = (item *) prev;
}

static uint32_t item_get_exptime(void *it) {
    item *i = it;
    assert(i);
    return i->exptime;
}

static void item_set_exptime(void *it, uint32_t exptime) {
    item *i = it;
    assert(i);
    i->exptime = exptime;
}

