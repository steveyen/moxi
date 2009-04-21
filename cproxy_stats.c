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

// From libmemcached.
//
uint32_t murmur_hash(const char *key, size_t length);

// Local declarations.
//
static void add_proxy_stats(proxy_stats *agg, proxy_stats *x);

static void request_stats(void *data0, void *data1);
static void collect_stats(void *data0, void *data1);

// Protocol STATS command handling.
//
char *protocol_stats_keys_first = "pid ";
char *protocol_stats_keys_smallest =
    "uptime "
    "time "
    "version "
    "pointer_size "
    "limit_maxbytes "
    "accepting_conns "
    ":chunk_size "
    ":chunk_per_page "
    ":age "; // TODO: Should age merge be largest, not smallest?

bool protocol_stats_merge_sum(char *v1, int v1len,
                              char *v2, int v2len,
                              char *out, int outlen);
bool protocol_stats_merge_smallest(char *v1, int v1len,
                                   char *v2, int v2len,
                                   char *out, int outlen);

int count_dot_pair(char *x, int xlen, char *y, int ylen);
int count_dot(char *x, int len);

/* This callback is invoked by memagent on a memagent thread
 * when it wants proxy stats.
 *
 * We use the work_queues to retrieve the info, so that normal
 * runtime has fewer locks, at the cost of scatter/gather
 * complexity.
 *
 * TODO: We're currently gathering the reverse of what we
 *       probably want -- thread-based stats rather than
 *       proxy-based stats.  Need to flip it over.
 */
void on_memagent_get_stats(void *userdata, void *opaque,
                           agent_add_stat add_stat) {
    proxy_main *m = userdata;
    assert(m);
    assert(m->nthreads > 1);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread);
    assert(mthread->work_queue);

    work_collect *ca = calloc(m->nthreads, sizeof(work_collect));
    if (ca != NULL) {
        int i;

        for (i = 1; i < m->nthreads; i++) {
            proxy_stats *ps = calloc(1, sizeof(proxy_stats));
            if (ps != NULL)
                work_collect_init(&ca[i], -1, ps);
            else
                break;
        }

        // Continue on the main listener thread.
        //
        if (i >= m->nthreads &&
            work_send(mthread->work_queue, request_stats, m, ca)) {
            // Wait for all the stats collecting to finish.
            //
            for (i = 1; i < m->nthreads; i++)
                work_collect_wait(&ca[i]);

            char buf0[100];
            char buf1[100];

#define more_stat(spec, key, val) \
    sprintf(buf0, spec, val);     \
    add_stat(opaque, key, buf0);

#define more_thread_stat(thread_id, spec, key, val) \
    sprintf(buf1, "%u:%s", thread_id, key);         \
    more_stat(spec, buf1, val);

            more_stat("%u", "nthreads",               m->nthreads);
            more_stat("%u", "default_downstream_max", m->default_downstream_max);

            for (i = 1; i < m->nthreads; i++) {
                more_thread_stat(i, "%u", "hello", 100);
            }
        }

        for (i = 1; i < m->nthreads; i++) {
            if (ca[i].data != NULL)
                free(ca[i].data);
        }

        free(ca);
    }

    add_stat(opaque, NULL, NULL);
}

/* Must be invoked on the main listener thread.
 *
 * Puts stats gathering work on every worker thread's work_queue.
 */
static void request_stats(void *data0, void *data1) {
    proxy_main *m = data0;
    assert(m);
    assert(m->nthreads > 1);

    work_collect *ca = data1;
    assert(ca);

    assert(is_listen_thread());

    int sent   = 0;
    int nproxy = 0;

    for (proxy *p = m->proxy_head; p != NULL; p = p->next)
        nproxy++;

    // Starting at 1 because 0 is the main listen thread.
    //
    for (int i = 1; i < m->nthreads; i++) {
        work_collect *c = &ca[i];

        work_collect_count(c, nproxy);

        if (nproxy > 0) {
            LIBEVENT_THREAD *t = thread_by_index(i);
            assert(t);
            assert(t->work_queue);

            for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
                proxy_td *ptd = &p->thread_data[i];
                if (ptd != NULL &&
                    work_send(t->work_queue, collect_stats, ptd, c)) {
                    sent++;
                }
            }
        }
    }

    // Normally, no need to wait for the worker threads to finish,
    // as the workers will signal using work_collect_one().
    //
    // TODO: If sent is too small, then some proxies were disabled?
    //       Need to decrement count?
    //
    // TODO: Might want to block here until worker threads are done,
    //       so that concurrent reconfigs don't cause issues.
    //
    // In the case when config/config_ver changes might already
    // be inflight, as long as they're not removing proxies,
    // we're ok.  New proxies that happen afterwards are fine, too.
}

static void collect_stats(void *data0, void *data1) {
    proxy_td *ptd = data0;
    assert(ptd);

    work_collect *c = data1;
    assert(c);

    assert(is_listen_thread() == false); // Expecting a worker thread.

    add_proxy_stats((proxy_stats *) c->data, &ptd->stats);

    work_collect_one(c);
}

static void add_proxy_stats(proxy_stats *agg, proxy_stats *x) {
    assert(agg);
    assert(x);

    agg->num_upstream += x->num_upstream;
    agg->tot_upstream += x->tot_upstream;
    agg->tot_downstream_released += x->tot_downstream_released;
    agg->tot_downstream_reserved += x->tot_downstream_reserved;
}

// Special STATS value merging rules, instead of the
// default to just sum the values.  Note the trailing space.
//
#define MAX_TOKENS     5
#define NAME_TOKEN     1
#define VALUE_TOKEN    2
#define MERGE_BUF_SIZE 300

bool protocol_stats_merge(GHashTable *merger, char *line) {
    assert(merger != NULL);
    assert(line != NULL);

    int nline = strlen(line); // Ex: "STATS uptime 123455"
    if (nline <= 0 ||
        nline >= MERGE_BUF_SIZE)
        return false;

    token_t tokens[MAX_TOKENS];
    size_t  ntokens = scan_tokens(line, tokens, MAX_TOKENS);

    if (ntokens != 4) // 3 + 1 for the terminal token.
        return false;

    char *name     = tokens[NAME_TOKEN].value;
    int   name_len = tokens[NAME_TOKEN].length;
    if (name == NULL ||
        name_len <= 0 ||
        tokens[VALUE_TOKEN].value == NULL ||
        tokens[VALUE_TOKEN].length >= MERGE_BUF_SIZE)
        return false;

    char *key = name + name_len;       // Key part for merge rule lookup.
    while (key >= name && *key != ':') // Scan for last colon.
        key--;
    if (key < name)
        key = name;
    int key_len = name_len - (key - name);
    if (key_len > 0 &&
        key_len < MERGE_BUF_SIZE) {
        char buf_key[MERGE_BUF_SIZE];
        char buf_val[MERGE_BUF_SIZE];
        char buf_end[MERGE_BUF_SIZE];

        char *prev = (char *) g_hash_table_lookup(merger, name);
        if (prev == NULL) {
            char *hval = strdup(line);
            g_hash_table_insert(merger,
                                hval + (name - line),
                                hval);
            return true;
        }

        strncpy(buf_key, key, key_len);
        buf_key[key_len] = '\0';

        if (strstr(protocol_stats_keys_first, buf_key) != NULL) {
            return true;
        }

        token_t prev_tokens[MAX_TOKENS];
        size_t  prev_ntokens = scan_tokens(prev, prev_tokens, MAX_TOKENS);

        if (prev_ntokens != 4)
            return true;

        bool ok;

        if (strstr(protocol_stats_keys_smallest, buf_key) != NULL) {
            ok = protocol_stats_merge_smallest(prev_tokens[VALUE_TOKEN].value,
                                               prev_tokens[VALUE_TOKEN].length,
                                               tokens[VALUE_TOKEN].value,
                                               tokens[VALUE_TOKEN].length,
                                               buf_val, MERGE_BUF_SIZE);
        } else {
            ok = protocol_stats_merge_sum(prev_tokens[VALUE_TOKEN].value,
                                          prev_tokens[VALUE_TOKEN].length,
                                          tokens[VALUE_TOKEN].value,
                                          tokens[VALUE_TOKEN].length,
                                          buf_val, MERGE_BUF_SIZE);
        }

        if (ok) {
            int prefix_len = tokens[VALUE_TOKEN].value - line;

            strncpy(buf_end, line, prefix_len);
            strcpy(buf_end + prefix_len, buf_val);

            char *hval = strdup(buf_end);
            g_hash_table_insert(merger,
                                hval + (name - line),
                                hval);

            free(prev);
        }

        // Note, if we couldn't merge, then just keep
        // the previous value.
        //
        return true;
    }

    return false;
}

bool protocol_stats_merge_sum(char *v1, int v1len,
                              char *v2, int v2len,
                              char *out, int outlen) {
    int dot = count_dot_pair(v1, v1len, v2, v2len);
    if (dot > 0) {
        float v1f = strtof(v1, NULL);
        float v2f = strtof(v2, NULL);
        sprintf(out, "%f", v1f + v2f);
        return true;
    } else {
        int32_t v1i;
        int32_t v2i;

        if (safe_strtol(v1, &v1i) &&
            safe_strtol(v2, &v2i)) {
            sprintf(out, "%d", v1i + v2i);
            return true;
        }
    }

    return false;
}

bool protocol_stats_merge_smallest(char *v1, int v1len,
                                   char *v2, int v2len,
                                   char *out, int outlen) {
    int dot = count_dot_pair(v1, v1len, v2, v2len);
    if (dot > 0) {
        float v1f = strtof(v1, NULL);
        float v2f = strtof(v2, NULL);
        sprintf(out, "%f", (v1f > v2f ? v1f : v2f));
        return true;
    } else {
        int32_t v1i;
        int32_t v2i;

        if (safe_strtol(v1, &v1i) &&
            safe_strtol(v2, &v2i)) {
            sprintf(out, "%d", (v1i > v2i ? v1i : v2i));
            return true;
        }
    }

    return false;
}

size_t protocol_stats_key_len(const char *key) {
    assert(key);

    char *x = (char *) key;
    while (*x != ' ' && *x != '\0')
        x++;

    return x - key;
}

guint protocol_stats_key_hash(gconstpointer v) {
    assert(v);

    const char *key = v;
    size_t      len = protocol_stats_key_len(key);

    return murmur_hash(key, len);
}

gboolean protocol_stats_key_equal(gconstpointer v1, gconstpointer v2) {
    assert(v1);
    assert(v2);

    const char *k1 = v1;
    const char *k2 = v2;

    size_t n1 = protocol_stats_key_len(k1);
    size_t n2 = protocol_stats_key_len(k2);

    return (n1 == n2 && strncmp(k1, k2, n1) == 0);
}

/* Callback to g_hash_table_foreach that frees the multiget_entry list.
 */
void protocol_stats_foreach_free(gpointer key,
                                 gpointer value,
                                 gpointer user_data) {
    assert(value != NULL);
    free(value);
}

void protocol_stats_foreach_write(gpointer key,
                                  gpointer value,
                                  gpointer user_data) {
    char *line = (char *) value;
    assert(line != NULL);

    conn *uc = (conn *) user_data;
    assert(uc != NULL);

    int nline = strlen(line);
    if (nline > 0) {
        item *it = item_alloc("s", 1, 0, 0, nline + 2);
        if (it != NULL) {
            strncpy(ITEM_data(it), line, nline);
            strncpy(ITEM_data(it) + nline, "\r\n", 2);

            if (add_conn_item(uc, it)) {
                add_iov(uc, ITEM_data(it), nline + 2);
                return;
            }

            item_remove(it);
        }
    }
}

int count_dot_pair(char *x, int xlen, char *y, int ylen) {
    int xdot = count_dot(x, xlen);
    int ydot = count_dot(y, ylen);

    assert(xdot == ydot);

    return (xdot > ydot ? xdot : ydot);
}

int count_dot(char *x, int len) { // Number of '.' chars in a string.
    int dot = 0;

    for (char *end = x + len; x < end; x++) {
        if (*x == '.')
            dot++;
    }

    return dot;
}

