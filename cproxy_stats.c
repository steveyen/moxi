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

// From libmemcached.
//
uint32_t murmur_hash(const char *key, size_t length);

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

// Special STATS value merging rules, instead of the
// default to just sum the values.  Note the trailing space.
//
#define MAX_TOKENS     5
#define PREFIX_TOKEN   0
#define NAME_TOKEN     1
#define VALUE_TOKEN    2
#define MERGE_BUF_SIZE 300

bool protocol_stats_merge_line(GHashTable *merger, char *line) {
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
        tokens[VALUE_TOKEN].length <= 0 ||
        tokens[VALUE_TOKEN].length >= MERGE_BUF_SIZE)
        return false;

    return protocol_stats_merge_name_val(merger,
                                         tokens[PREFIX_TOKEN].value,
                                         tokens[PREFIX_TOKEN].length,
                                         name, name_len,
                                         tokens[VALUE_TOKEN].value,
                                         tokens[VALUE_TOKEN].length);
}

// TODO: The stats merge assumes an ascii upstream.
//
bool protocol_stats_merge_name_val(GHashTable *merger,
                                   char *prefix,
                                   int   prefix_len,
                                   char *name,
                                   int   name_len,
                                   char *val,
                                   int   val_len) {
    assert(merger);
    assert(name);
    assert(val);

    char *key = name + name_len;       // Key part for merge rule lookup.
    while (key >= name && *key != ':') // Scan for last colon.
        key--;
    if (key < name)
        key = name;
    int key_len = name_len - (key - name);
    if (key_len > 0 &&
        key_len < MERGE_BUF_SIZE) {
        char buf_name[MERGE_BUF_SIZE];
        char buf_key[MERGE_BUF_SIZE];
        char buf_val[MERGE_BUF_SIZE];

        strncpy(buf_name, name, name_len);
        buf_name[name_len] = '\0';

        char *prev = (char *) g_hash_table_lookup(merger, buf_name);
        if (prev == NULL) {
            char *hval = malloc(prefix_len + 1 +
                                name_len + 1 +
                                val_len + 1);
            if (hval != NULL) {
                memcpy(hval, prefix, prefix_len);
                hval[prefix_len] = ' ';

                memcpy(hval + prefix_len + 1, name, name_len);
                hval[prefix_len + 1 + name_len] = ' ';

                memcpy(hval + prefix_len + 1 + name_len + 1, val, val_len);
                hval[prefix_len + 1 + name_len + 1 + val_len] = '\0';

                g_hash_table_insert(merger,
                                    hval + prefix_len + 1,
                                    hval);
            }

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

        strncpy(buf_val, val, val_len);
        buf_val[val_len] = '\0';

        bool ok;

        if (strstr(protocol_stats_keys_smallest, buf_key) != NULL) {
            ok = protocol_stats_merge_smallest(prev_tokens[VALUE_TOKEN].value,
                                               prev_tokens[VALUE_TOKEN].length,
                                               buf_val, val_len,
                                               buf_val, MERGE_BUF_SIZE);
        } else {
            ok = protocol_stats_merge_sum(prev_tokens[VALUE_TOKEN].value,
                                          prev_tokens[VALUE_TOKEN].length,
                                          buf_val, val_len,
                                          buf_val, MERGE_BUF_SIZE);
        }

        if (ok) {
            int   vlen = strlen(buf_val);
            char *hval = malloc(prefix_len + 1 +
                                name_len + 1 +
                                vlen + 1);
            if (hval != NULL) {
                memcpy(hval, prefix, prefix_len);
                hval[prefix_len] = ' ';

                memcpy(hval + prefix_len + 1, name, name_len);
                hval[prefix_len + 1 + name_len] = ' ';

                strcpy(hval + prefix_len + 1 + name_len + 1, buf_val);
                hval[prefix_len + 1 + name_len + 1 + vlen] = '\0';

                g_hash_table_insert(merger,
                                    hval + prefix_len + 1,
                                    hval);

                free(prev);
            }
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

// ----------------------------------------

void cproxy_reset_stats_td(proxy_stats_td *pstd) {
    assert(pstd);

    cproxy_reset_stats(&pstd->stats);

    for (int j = 0; j < STATS_CMD_TYPE_last; j++) {
        for (int k = 0; k < STATS_CMD_last; k++) {
            cproxy_reset_stats_cmd(&pstd->stats_cmd[j][k]);
        }
    }
}

void cproxy_reset_stats(proxy_stats *ps) {
    assert(ps);

    // Only clear the tot_xxx stats, not the num_xxx ones.
    //
    ps->tot_upstream = 0;
    ps->tot_downstream_conn = 0;
    ps->tot_downstream_released = 0;
    ps->tot_downstream_reserved = 0;
    ps->tot_downstream_freed = 0;
    ps->tot_downstream_quit_server = 0;
    ps->tot_downstream_max_reached = 0;
    ps->tot_downstream_create_failed = 0;
    ps->tot_downstream_connect = 0;
    ps->tot_downstream_connect_failed = 0;
    ps->tot_downstream_auth = 0;
    ps->tot_downstream_auth_failed = 0;
    ps->tot_downstream_bucket = 0;
    ps->tot_downstream_bucket_failed = 0;
    ps->tot_downstream_propagate_failed = 0;
    ps->tot_downstream_close_on_upstream_close = 0;
    ps->tot_downstream_timeout = 0;
    ps->tot_wait_queue_timeout = 0;
    ps->tot_assign_downstream = 0;
    ps->tot_assign_upstream = 0;
    ps->tot_assign_recursion = 0;
    ps->tot_reset_upstream_avail = 0;
    ps->tot_retry = 0;
    ps->tot_multiget_keys = 0;
    ps->tot_multiget_keys_dedupe = 0;
    ps->tot_optimize_sets = 0;
    ps->err_oom = 0;
    ps->err_upstream_write_prep = 0;
    ps->err_downstream_write_prep = 0;
}

void cproxy_reset_stats_cmd(proxy_stats_cmd *sc) {
    assert(sc);
    memset(sc, 0, sizeof(proxy_stats_cmd));
}

