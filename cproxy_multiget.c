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

bool multiget_ascii_downstream(downstream *d, conn *uc,
    int (*emit_start)(conn *c, char *cmd, int cmd_len),
    int (*emit_skey)(conn *c, char *skey, int skey_len),
    int (*emit_end)(conn *c),
    mcache *front_cache) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(d->multiget == NULL);
    assert(uc != NULL);
    assert(uc->noreply == false);

    proxy_td *ptd = d->ptd;
    assert(ptd != NULL);

    int nwrite = 0;
    int nconns = memcached_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        if (d->downstream_conns[i] != NULL &&
            cproxy_prep_conn_for_write(d->downstream_conns[i]) == false) {
            d->ptd->stats.err_downstream_write_prep++;
            cproxy_close_conn(d->downstream_conns[i]);
            return false;
        }
    }

    if (uc->next != NULL) {
        // More than one upstream conn, so we need a hashtable
        // to track keys for de-deplication.
        //
        d->multiget = g_hash_table_new(skey_hash,
                                       skey_equal);
        if (settings.verbose > 1)
            fprintf(stderr, "cproxy multiget hash table new\n");
    }

    // Snapshot the volatile only once.
    //
    uint32_t msec_current_time_snapshot = msec_current_time;

    int   uc_num = 0;
    conn *uc_cur = uc;

    while (uc_cur != NULL) {
        assert(uc_cur->cmd == -1);
        assert(uc_cur->item == NULL);
        assert(uc_cur->state == conn_pause);
        assert(IS_ASCII(uc_cur->protocol));
        assert(IS_PROXY(uc_cur->protocol));

        char *command = uc_cur->cmd_start;
        assert(command != NULL);

        char *space = strchr(command, ' ');
        assert(space > command);

        int cmd_len = space - command;
        assert(cmd_len == 3 || cmd_len == 4); // Either get or gets.

        if (settings.verbose > 1)
            fprintf(stderr, "forward multiget %s (%d %d)\n",
                    command, cmd_len, uc_num);

        while (space != NULL) {
            char *key = space + 1;
            char *next_space = strchr(key, ' ');
            int   key_len;

            if (next_space != NULL)
                key_len = next_space - key;
            else
                key_len = strlen(key);

            // This key_len check helps skip consecutive spaces.
            //
            if (key_len > 0) {
                ptd->stats.tot_multiget_keys++;

                // Handle a front cache hit by queuing response.
                //
                item *it = mcache_get(front_cache, key, key_len,
                                      msec_current_time_snapshot);
                if (it != NULL) {
                    assert(it->nkey == key_len);
                    assert(strncmp(ITEM_key(it), key, it->nkey) == 0);

                    cproxy_upstream_ascii_item_response(it, uc_cur);

                    // The refcount was inc'ed by mcache_get() for us.
                    //
                    item_remove(it);

                    goto loop_next;
                }

                bool self = false;

                conn *c = cproxy_find_downstream_conn(d, key, key_len,
                                                      &self);
                if (c != NULL) {
                    // See if we've already requested this key via
                    // the multiget hash table, in order to
                    // de-deplicate repeated keys.
                    //
                    bool first_request = true;

                    if (d->multiget != NULL) {
                        multiget_entry *entry =
                            calloc(1, sizeof(multiget_entry));
                        if (entry != NULL) {
                            entry->upstream_conn = uc_cur;
                            entry->opaque = 0;
                            entry->next = g_hash_table_lookup(d->multiget, key);

                            g_hash_table_insert(d->multiget, key, entry);

                            if (entry->next != NULL)
                                first_request = false;
                        } else {
                            // TODO: Handle out of multiget entry memory.
                        }
                    }

                    if (first_request) {
                        assert(c->item == NULL);
                        assert(c->state == conn_pause);
                        assert(IS_PROXY(c->protocol));
                        assert(c->ilist != NULL);
                        assert(c->isize > 0);

                        c->icurr = c->ilist;
                        c->ileft = 0;

                        if (uc_num <= 0 &&
                            c->msgused <= 1 &&
                            c->msgbytes <= 0) {
                            emit_start(c, command, cmd_len);
                        }

                        // Provide the preceding space as optimization
                        // for ascii-to-ascii configuration.
                        //
                        emit_skey(c, key - 1, key_len + 1);
                    } else {
                        ptd->stats.tot_multiget_keys_dedupe++;

                        if (settings.verbose > 1) {
                            char buf[KEY_MAX_LENGTH + 10];
                            memcpy(buf, key, key_len);
                            buf[key_len] = '\0';

                            fprintf(stderr,
                                    "%d cproxy multiget dedpue: %s\n",
                                    uc_cur->sfd, buf);
                        }
                    }
                } else {
                    // TODO: Handle when downstream conn is down.
                }
            }

        loop_next:
            space = next_space;
        }

        uc_num++;
        uc_cur = uc_cur->next;
    }

    for (int i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL &&
            (c->msgused > 1 ||
             c->msgbytes > 0)) {
            emit_end(c);

            conn_set_state(c, conn_mwrite);
            c->write_and_go = conn_new_cmd;

            if (update_event(c, EV_WRITE | EV_PERSIST)) {
                nwrite++;

                if (uc->noreply) {
                    c->write_and_go = conn_pause;
                }
            } else {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "Couldn't update cproxy write event\n");

                d->ptd->stats.err_oom++;
                cproxy_close_conn(c);
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "forward multiget nwrite %d out of %d\n",
                nwrite, nconns);

    d->downstream_used_start = nwrite;
    d->downstream_used       = nwrite;

    if (cproxy_dettach_if_noreply(d, uc) == false) {
        d->upstream_suffix = "END\r\n";

        cproxy_start_downstream_timeout(d, NULL);
    }

    return nwrite > 0;
}

void multiget_ascii_downstream_response(downstream *d, item *it) {
    assert(d);
    assert(it);
    assert(it->nkey > 0);
    assert(ITEM_key(it) != NULL);

    proxy_td *ptd = d->ptd;
    assert(ptd);

    proxy *p = ptd->proxy;
    assert(p);

    pthread_mutex_lock(&p->proxy_lock);
    uint32_t front_cache_lifespan = p->behavior_head.front_cache_lifespan;
    pthread_mutex_unlock(&p->proxy_lock);

    if (front_cache_lifespan > 0) {
        if (matcher_started(&p->front_cache_matcher) == false ||
            matcher_check(&p->front_cache_matcher,
                          ITEM_key(it), it->nkey)) {
            mcache_add(&p->front_cache, it, front_cache_lifespan,
                       msec_current_time);
        }
    }

    if (d->multiget != NULL) {
        // The ITEM_key is not NULL or space terminated.
        //
        char key_buf[KEY_MAX_LENGTH + 10];
        assert(it->nkey <= KEY_MAX_LENGTH);
        memcpy(key_buf, ITEM_key(it), it->nkey);
        key_buf[it->nkey] = '\0';

        multiget_entry *entry =
            g_hash_table_lookup(d->multiget, key_buf);

        while (entry != NULL) {
            // The upstream might have been closed mid-request.
            //
            conn *uc = entry->upstream_conn;
            if (uc != NULL)
                cproxy_upstream_ascii_item_response(it, uc);

            entry = entry->next;
        }
    } else {
        conn *uc = d->upstream_conn;
        while (uc != NULL) {
            cproxy_upstream_ascii_item_response(it, uc);
            uc = uc->next;
        }
    }
}
