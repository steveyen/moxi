/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <glib.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"

// From libmemcached.
//
uint32_t murmur_hash(const char *key, size_t length);

// Key may be zero or space terminated.
//
size_t multiget_key_len(const char *key) {
    assert(key);

    char *x = (char *) key;
    while (*x != ' ' && *x != '\0')
        x++;

    return x - key;
}

guint multiget_key_hash(gconstpointer v) {
    assert(v);

    const char *key = v;
    size_t      len = multiget_key_len(key);

    return murmur_hash(key, len);
}

gboolean multiget_key_equal(gconstpointer v1, gconstpointer v2) {
    assert(v1);
    assert(v2);

    const char *k1 = v1;
    const char *k2 = v2;

    size_t n1 = multiget_key_len(k1);
    size_t n2 = multiget_key_len(k2);

    return (n1 == n2 && strncmp(k1, k2, n1) == 0);
}

/* Callback to g_hash_table_foreach that frees the multiget_entry list.
 */
void multiget_foreach_free(gpointer key,
                           gpointer value,
                           gpointer user_data) {
    multiget_entry *entry = value;
    assert(entry != NULL);

    while (entry != NULL) {
        multiget_entry *curr = entry;
        entry = entry->next;
        free(curr);
    }
}

/* Callback to g_hash_table_foreach that clears out multiget_entries
 * which have the given upstream conn (passed as user_data).
 */
void multiget_remove_upstream(gpointer key,
                              gpointer value,
                              gpointer user_data) {
    multiget_entry *entry = value;
    assert(entry != NULL);

    conn *uc = user_data;
    assert(uc != NULL);

    while (entry != NULL) {
        // Just clear the slots, because glib hash table API
        // doesn't allow for key/value modifications during iteration.
        //
        if (entry->upstream_conn == uc) {
            entry->upstream_conn = NULL;
            entry->opaque = 0;
        }

        entry = entry ->next;
    }
}

