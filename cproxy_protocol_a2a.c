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
#include "work.h"

// Internal declarations.
//
#define KEY_TOKEN  1
#define MAX_TOKENS 8

int a2a_multiget_start(conn *c, char *cmd, int cmd_len);
int a2a_multiget_skey(conn *c, char *skey, int skey_len);
int a2a_multiget_end(conn *c);

void cproxy_init_a2a() {
    // Nothing right now.
}

void cproxy_process_a2a_downstream(conn *c, char *line) {
    assert(c != NULL);
    assert(c->next == NULL);
    assert(c->extra != NULL);
    assert(c->cmd == -1);
    assert(c->item == NULL);
    assert(line != NULL);
    assert(line == c->rcurr);
    assert(IS_ASCII(c->protocol));
    assert(IS_PROXY(c->protocol));

    if (settings.verbose > 1)
        fprintf(stderr, "<%d cproxy_process_a2a_downstream %s\n",
                c->sfd, line);

    downstream *d = c->extra;

    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);

    if (strncmp(line, "VALUE ", 6) == 0) {
        token_t      tokens[MAX_TOKENS];
        size_t       ntokens;
        unsigned int flags;
        int          vlen;
        uint64_t     cas = CPROXY_NOT_CAS;

        ntokens = scan_tokens(line, tokens, MAX_TOKENS);
        if (ntokens >= 5 && // Accounts for extra termimation token.
            ntokens <= 6 &&
            tokens[KEY_TOKEN].length <= KEY_MAX_LENGTH &&
            safe_strtoul(tokens[2].value, (uint32_t *) &flags) &&
            safe_strtoul(tokens[3].value, (uint32_t *) &vlen)) {
            char  *key  = tokens[KEY_TOKEN].value;
            size_t nkey = tokens[KEY_TOKEN].length;

            item *it = item_alloc(key, nkey, flags, 0, vlen + 2);
            if (it != NULL) {
                if (ntokens == 5 ||
                    safe_strtoull(tokens[4].value, &cas)) {
                    ITEM_set_cas(it, cas);

                    c->item = it;
                    c->ritem = ITEM_data(it);
                    c->rlbytes = it->nbytes;
                    c->cmd = -1;

                    conn_set_state(c, conn_nread);

                    return; // Success.
                } else {
                    if (settings.verbose > 1)
                        fprintf(stderr, "cproxy could not parse cas\n");
                }
            } else {
                if (settings.verbose > 1)
                    fprintf(stderr, "cproxy could not item_alloc size %u\n",
                            vlen + 2);
            }

            if (it != NULL)
                item_remove(it);
            it = NULL;

            c->sbytes = vlen + 2; // Number of bytes to swallow.

            conn_set_state(c, conn_swallow);

            // Note, eventually, we'll see an END later.
        } else {
            // We don't know how much to swallow, so close the downstream.
            // The conn_closing should release the downstream,
            // which should write a suffix/error to the upstream.
            //
            conn_set_state(c, conn_closing);
        }
    } else if (strncmp(line, "END", 3) == 0) {
        conn_set_state(c, conn_pause);
    } else if (strncmp(line, "OK", 2) == 0) {
        conn_set_state(c, conn_pause);

        // TODO: Handle flush_all's expiration parameter against
        // the front_cache.
        //
        // TODO: We flush the front_cache too often, inefficiently
        // on every downstream flush_all OK response, rather than
        // on just the last flush_all OK response.
        //
        conn *uc = d->upstream_conn;
        if (uc != NULL &&
            uc->cmd_start != NULL &&
            strncmp(uc->cmd_start, "flush_all", 9) == 0) {
            mcache_flush_all(&d->ptd->proxy->front_cache, 0);
        }
    } else if (strncmp(line, "STAT ", 5) == 0 ||
               strncmp(line, "ITEM ", 5) == 0 ||
               strncmp(line, "PREFIX ", 7) == 0) {
        assert(d->merger != NULL);

        conn *uc = d->upstream_conn;
        if (uc != NULL) {
            assert(uc->next == NULL);

            if (protocol_stats_merge_line(d->merger, line) == false) {
                // Forward the line as-is if we couldn't merge it.
                //
                int nline = strlen(line);

                item *it = item_alloc("s", 1, 0, 0, nline + 2);
                if (it != NULL) {
                    strncpy(ITEM_data(it), line, nline);
                    strncpy(ITEM_data(it) + nline, "\r\n", 2);

                    if (add_conn_item(uc, it)) {
                        add_iov(uc, ITEM_data(it), nline + 2);

                        it = NULL;
                    }

                    if (it != NULL)
                        item_remove(it);
                }
            }
        }

        conn_set_state(c, conn_new_cmd);
    } else {
        conn_set_state(c, conn_pause);

        // The upstream conn might be NULL when closed already
        // or while handling a noreply.
        //
        conn *uc = d->upstream_conn;
        if (uc != NULL) {
            assert(uc->next == NULL);

            out_string(uc, line);

            if (!update_event(uc, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "Can't update upstream write event\n");

                d->ptd->stats.err_oom++;
                cproxy_close_conn(uc);
            }

            cproxy_del_front_cache_key_ascii_response(d, line,
                                                      uc->cmd_start);
        }
    }
}

/* We get here after reading the value in a VALUE reply.
 * The item is ready in c->item.
 */
void cproxy_process_a2a_downstream_nread(conn *c) {
    assert(c != NULL);

    if (settings.verbose > 1)
        fprintf(stderr,
                "<%d cproxy_process_a2a_downstream_nread %d %d\n",
                c->sfd, c->ileft, c->isize);

    downstream *d = c->extra;
    assert(d != NULL);

    item *it = c->item;
    assert(it != NULL);

    // Clear c->item because we either move it to the upstream or
    // item_remove() it on error.
    //
    c->item = NULL;

    conn_set_state(c, conn_new_cmd);

    // pthread_mutex_lock(&c->thread->stats.mutex);
    // c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    // pthread_mutex_unlock(&c->thread->stats.mutex);

    multiget_ascii_downstream_response(d, it);

    item_remove(it);
}

/* Do the actual work of forwarding the command from an
 * upstream ascii conn to its assigned ascii downstream.
 */
bool cproxy_forward_a2a_downstream(downstream *d) {
    assert(d != NULL);

    conn *uc = d->upstream_conn;

    assert(uc != NULL);
    assert(uc->state == conn_pause);
    assert(uc->cmd_start != NULL);
    assert(uc->thread != NULL);
    assert(uc->thread->base != NULL);
    assert(IS_ASCII(uc->protocol));
    assert(IS_PROXY(uc->protocol));

    if (cproxy_connect_downstream(d, uc->thread) > 0) {
        assert(d->downstream_conns != NULL);

        if (uc->cmd == -1) {
            return cproxy_forward_a2a_simple_downstream(d, uc->cmd_start, uc);
        } else {
            return cproxy_forward_a2a_item_downstream(d, uc->cmd, uc->item, uc);
        }
    }

    return false;
}

/* Forward a simple one-liner command downstream.
 * For example, get, incr/decr, delete, etc.
 * The response, though, might be a simple line or
 * multiple VALUE+END lines.
 */
bool cproxy_forward_a2a_simple_downstream(downstream *d,
                                          char *command, conn *uc) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->item == NULL);
    assert(d->multiget == NULL);
    assert(d->merger == NULL);

    if (strncmp(command, "get", 3) == 0) {
        // Only use front_cache for 'get', not for 'gets'.
        //
        mcache *front_cache =
            (command[3] == ' ') ? &d->ptd->proxy->front_cache : NULL;

        return multiget_ascii_downstream(d, uc,
                                         a2a_multiget_start,
                                         a2a_multiget_skey,
                                         a2a_multiget_end,
                                         front_cache);
    }

    assert(uc->next == NULL);

    if (strncmp(command, "flush_all", 9) == 0)
        return cproxy_broadcast_a2a_downstream(d, command, uc,
                                               "OK\r\n");

    if (strncmp(command, "stats", 5) == 0) {
        if (strncmp(command + 5, " reset", 6) == 0)
            return cproxy_broadcast_a2a_downstream(d, command, uc,
                                                   "RESET\r\n");

        if (cproxy_broadcast_a2a_downstream(d, command, uc,
                                            "END\r\n")) {
            d->merger = g_hash_table_new(skey_hash,
                                         skey_equal);
            return true;
        } else {
            return false;
        }
    }

    token_t  tokens[MAX_TOKENS];
    size_t   ntokens = scan_tokens(command, tokens, MAX_TOKENS);
    char    *key     = tokens[KEY_TOKEN].value;
    int      key_len = tokens[KEY_TOKEN].length;

    if (ntokens <= 1) { // This was checked long ago, while parsing
        assert(false);  // the upstream conn.
        return false;
    }

    // Assuming we're already connected to downstream.
    //
    bool self = false;

    conn *c = cproxy_find_downstream_conn(d, key, key_len,
                                          &self);
    if (c != NULL) {
        if (cproxy_prep_conn_for_write(c)) {
            assert(c->state == conn_pause);

            out_string(c, command);

            if (settings.verbose > 1)
                fprintf(stderr, "forwarding to %d, noreply %d\n",
                        c->sfd, uc->noreply);

            if (update_event(c, EV_WRITE | EV_PERSIST)) {
                d->downstream_used_start = 1;
                d->downstream_used       = 1;

                if (cproxy_dettach_if_noreply(d, uc) == false) {
                    cproxy_start_downstream_timeout(d, c);
                } else {
                    c->write_and_go = conn_pause;

                    // Do mcache_delete() here only during a noreply,
                    // otherwise for with-reply requests, we could
                    // be in a race with other clients repopulating
                    // the front_cache.  For with-reply requests, we
                    // clear the front_cache when we get a success reply.
                    //
                    mcache_delete(&d->ptd->proxy->front_cache, key, key_len);
                }

                return true;
            }

            if (settings.verbose > 1)
                fprintf(stderr, "Couldn't update cproxy write event\n");

            d->ptd->stats.err_oom++;
            cproxy_close_conn(c);
        } else {
            d->ptd->stats.err_downstream_write_prep++;
            cproxy_close_conn(c);
        }
    }

    return false;
}

int a2a_multiget_start(conn *c, char *cmd, int cmd_len) {
    return add_iov(c, cmd, cmd_len);
}

/* An skey is a space prefixed key string.
 */
int a2a_multiget_skey(conn *c, char *skey, int skey_len) {
    return add_iov(c, skey, skey_len);
}

int a2a_multiget_end(conn *c) {
    return add_iov(c, "\r\n", 2);
}

/* Used for broadcast commands, like flush_all or stats.
 */
bool cproxy_broadcast_a2a_downstream(downstream *d,
                                     char *command,
                                     conn *uc,
                                     char *suffix) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(uc->item == NULL);

    int nwrite = 0;
    int nconns = memcached_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL) {
            if (cproxy_prep_conn_for_write(c)) {
                assert(c->state == conn_pause);

                out_string(c, command);

                if (update_event(c, EV_WRITE | EV_PERSIST)) {
                    nwrite++;

                    if (uc->noreply) {
                        c->write_and_go = conn_pause;
                    }
                } else {
                    if (settings.verbose > 1)
                        fprintf(stderr,
                                "Update cproxy write event failed\n");

                    d->ptd->stats.err_oom++;
                    cproxy_close_conn(c);
                }
            } else {
                d->ptd->stats.err_downstream_write_prep++;
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
        d->upstream_suffix = suffix;

        cproxy_start_downstream_timeout(d, NULL);
    } else {
        // TODO: Handle flush_all's expiration parameter against
        // the front_cache.
        //
        if (strncmp(command, "flush_all", 9) == 0) {
            mcache_flush_all(&d->ptd->proxy->front_cache, 0);
        }
    }

    return nwrite > 0;
}

/* Forward an upstream command that came with item data,
 * like set/add/replace/etc.
 */
bool cproxy_forward_a2a_item_downstream(downstream *d, short cmd,
                                        item *it, conn *uc) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(it != NULL);
    assert(uc != NULL);
    assert(uc->next == NULL);

    // Assuming we're already connected to downstream.
    //
    bool self = false;

    conn *c = cproxy_find_downstream_conn(d, ITEM_key(it), it->nkey,
                                          &self);
    if (c != NULL) {
        if (cproxy_prep_conn_for_write(c)) {
            assert(c->state == conn_pause);

            char *verb = nread_text(cmd);
            assert(verb != NULL);

            char *str_flags   = ITEM_suffix(it);
            char *str_length  = strchr(str_flags + 1, ' ');
            int   len_flags   = str_length - str_flags;
            int   len_length  = it->nsuffix - len_flags - 2;
            char *str_exptime = add_conn_suffix(c);
            char *str_cas     = (cmd == NREAD_CAS ? add_conn_suffix(c) : NULL);

            if (str_flags != NULL &&
                str_length != NULL &&
                len_flags > 1 &&
                len_length > 1 &&
                str_exptime != NULL &&
                (cmd != NREAD_CAS ||
                 str_cas != NULL)) {
                sprintf(str_exptime, " %u", it->exptime);

                if (str_cas != NULL)
                    sprintf(str_cas, " %llu",
                            (unsigned long long) ITEM_get_cas(it));

                if (add_iov(c, verb, strlen(verb)) == 0 &&
                    add_iov(c, ITEM_key(it), it->nkey) == 0 &&
                    add_iov(c, str_flags, len_flags) == 0 &&
                    add_iov(c, str_exptime, strlen(str_exptime)) == 0 &&
                    add_iov(c, str_length, len_length) == 0 &&
                    (str_cas == NULL ||
                     add_iov(c, str_cas, strlen(str_cas)) == 0) &&
                    (uc->noreply == false ||
                     add_iov(c, " noreply", 8) == 0) &&
                    add_iov(c, ITEM_data(it) - 2, it->nbytes + 2) == 0) {
                    conn_set_state(c, conn_mwrite);
                    c->write_and_go = conn_new_cmd;

                    if (update_event(c, EV_WRITE | EV_PERSIST)) {
                        d->downstream_used_start = 1;
                        d->downstream_used       = 1;

                        if (cproxy_dettach_if_noreply(d, uc) == false) {
                            cproxy_start_downstream_timeout(d, c);

                            // During a synchronous (with-reply) SET,
                            // handle fire-&-forget SET optimization.
                            //
                            if (cmd == NREAD_SET &&
                                cproxy_optimize_set_ascii(d, uc,
                                                          ITEM_key(it),
                                                          it->nkey)) {
                                d->ptd->stats.tot_optimize_sets++;
                            }
                        } else {
                            c->write_and_go = conn_pause;

                            mcache_delete(&d->ptd->proxy->front_cache,
                                          ITEM_key(it), it->nkey);
                        }

                        return true;
                    }
                }

                d->ptd->stats.err_oom++;
                cproxy_close_conn(c);
            } else {
                // TODO: Handle this weird error case.
            }
        } else {
            d->ptd->stats.err_downstream_write_prep++;
            cproxy_close_conn(c);
        }

        if (settings.verbose > 1)
            fprintf(stderr, "Proxy item write out of memory");
    }

    return false;
}

