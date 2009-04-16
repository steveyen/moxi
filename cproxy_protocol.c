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

#define NOT_CAS -1

#define COMMAND_TOKEN    0
#define KEY_TOKEN        1
#define MAX_TOKENS       8

void cproxy_process_upstream_ascii(conn *c, char *line) {
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
        fprintf(stderr, "<%d cproxy_process_upstream_ascii %s\n",
                c->sfd, line);

    /* For commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */
    if (!cproxy_prep_conn_for_write(c)) {
        out_string(c, "SERVER_ERROR out of memory preparing response");
        return;
    }

    proxy_td *ptd = c->extra;

    assert(ptd != NULL);

    token_t tokens[MAX_TOKENS];
    size_t  ntokens = scan_tokens(line, tokens, MAX_TOKENS);
    char   *cmd     = tokens[COMMAND_TOKEN].value;
    int     comm;

    if (ntokens >= 3 &&
        (strncmp(cmd, "get", 3) == 0)) {

        // Handles get and gets.
        //
        cproxy_pause_upstream_for_downstream(ptd, c);

    } else if ((ntokens == 6 || ntokens == 7) &&
               ((strncmp(cmd, "add", 3) == 0     && (comm = NREAD_ADD)) ||
                (strncmp(cmd, "set", 3) == 0     && (comm = NREAD_SET)) ||
                (strncmp(cmd, "replace", 7) == 0 && (comm = NREAD_REPLACE)) ||
                (strncmp(cmd, "prepend", 7) == 0 && (comm = NREAD_PREPEND)) ||
                (strncmp(cmd, "append", 6) == 0  && (comm = NREAD_APPEND)) )) {

        process_update_command(c, tokens, ntokens, comm, false);

    } else if ((ntokens == 7 || ntokens == 8) &&
               (strncmp(cmd, "cas", 3) == 0 && (comm = NREAD_CAS))) {

        process_update_command(c, tokens, ntokens, comm, true);

    } else if ((ntokens == 4 || ntokens == 5) &&
               (strncmp(cmd, "incr", 4) == 0 ||
                strncmp(cmd, "decr", 4) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

    } else if (ntokens >= 3 && ntokens <= 4 &&
               (strncmp(cmd, "delete", 6) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

    } else if (ntokens >= 2 && ntokens <= 4 &&
               (strncmp(cmd, "flush_all", 9) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

    } else if (ntokens >= 2 &&
               (strncmp(cmd, "stats", 5) == 0)) {

        cproxy_pause_upstream_for_downstream(ptd, c);

    } else if (ntokens == 2 &&
               (strncmp(cmd, "version", 7) == 0)) {

        out_string(c, "VERSION " VERSION);

    } else if ((ntokens == 3 || ntokens == 4) &&
               (strncmp(cmd, "verbosity", 9) == 0)) {

        process_verbosity_command(c, tokens, ntokens);

    } else if (ntokens == 2 &&
               (strncmp(cmd, "quit", 4) == 0)) {

        conn_set_state(c, conn_closing);

    } else {
        out_string(c, "ERROR");
    }
}

/* We get here after reading the value in set/add/replace
 * commands. The command has been stored in c->cmd, and
 * the item is ready in c->item.
 */
void cproxy_process_upstream_ascii_nread(conn *c) {
    assert(c != NULL);

    item *it = c->item;

    assert(it != NULL);

    // pthread_mutex_lock(&c->thread->stats.mutex);
    // c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    // pthread_mutex_unlock(&c->thread->stats.mutex);

    if (strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) == 0) {
        proxy_td *ptd = c->extra;

        assert(ptd != NULL);

        cproxy_pause_upstream_for_downstream(ptd, c);
    } else
        out_string(c, "CLIENT_ERROR bad data chunk");
}

void cproxy_process_downstream_ascii(conn *c, char *line) {
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
        fprintf(stderr, "<%d cproxy_process_downstream_ascii %s\n",
                c->sfd, line);

    downstream *d = c->extra;

    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->next == NULL);

    if (strncmp(line, "VALUE ", 6) == 0) {
        token_t      tokens[MAX_TOKENS];
        size_t       ntokens;
        unsigned int flags;
        int          vlen;
        uint64_t     cas = NOT_CAS;

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
    } else if (strncmp(line, "END", 3) == 0 ||
               strncmp(line, "OK", 2) == 0) {
        conn_set_state(c, conn_pause);
    } else if (strncmp(line, "STAT ", 5) == 0 ||
               strncmp(line, "ITEM ", 5) == 0 ||
               strncmp(line, "PREFIX ", 7) == 0) {
        conn *uc = d->upstream_conn;
        if (uc != NULL) {
            // TODO: This only works for simple, single-target proxies.
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

        conn_set_state(c, conn_new_cmd);
    } else {
        conn_set_state(c, conn_pause);

        // The upstream conn might be NULL when closed already
        // or while handling a noreply.
        //
        conn *uc = d->upstream_conn;
        if (uc != NULL) {
            out_string(uc, line);

            if (!update_event(uc, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 1)
                    fprintf(stderr,
                            "Can't update upstream write event\n");

                cproxy_close_conn(uc);
            }
        }
    }
}

/* We get here after reading the value in a VALUE reply.
 * The item is ready in c->item.
 */
void cproxy_process_downstream_ascii_nread(conn *c) {
    assert(c != NULL);

    if (settings.verbose > 1)
        fprintf(stderr,
                "<%d cproxy_process_downstream_ascii_nread %d %d\n",
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

    conn *uc = d->upstream_conn;
    if (uc != NULL) {
        assert(uc->funcs != NULL);
        assert(IS_ASCII(uc->protocol));
        assert(IS_PROXY(uc->protocol));

        if (strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) == 0) {
            uint64_t cas = ITEM_get_cas(it);
            if (cas == NOT_CAS) {
                if (add_conn_item(uc, it)) {
                    if (add_iov(uc, "VALUE ", 6) == 0 &&
                        add_iov(uc, ITEM_key(it), it->nkey) == 0 &&
                        add_iov(uc, ITEM_suffix(it),
                                it->nsuffix + it->nbytes) == 0) {
                        if (settings.verbose > 1)
                            fprintf(stderr,
                                    "<%d cproxy downstream ascii success\n",
                                    c->sfd);

                        return; // Success.
                    }

                    it = NULL;
                }
            } else {
                char *suffix = add_conn_suffix(uc);
                if (suffix != NULL) {
                    sprintf(suffix, " %llu\r\n", (unsigned long long) cas);

                    if (add_conn_item(uc, it)) {
                        if (add_iov(uc, "VALUE ", 6) == 0 &&
                            add_iov(uc, ITEM_key(it), it->nkey) == 0 &&
                            add_iov(uc, ITEM_suffix(it),
                                    it->nsuffix - 2) == 0 &&
                            add_iov(uc, suffix, strlen(suffix)) == 0 &&
                            add_iov(uc, ITEM_data(it), it->nbytes) == 0) {
                            if (settings.verbose > 1)
                                fprintf(stderr,
                                        "<%d cproxy downstream ascii ok\n",
                                        c->sfd);

                            return; // Success.
                        }

                        it = NULL;
                    }
                }
            }

            // TODO: Need to clean up half-written add_iov()'s.
            //       Consider closing the upstream_conn?
            //
            if (settings.verbose > 1)
                fprintf(stderr, "proxy out of response memory");
        } else {
            if (settings.verbose > 1)
                fprintf(stderr, "unexpected downstream data block");
        }
    } else {
        if (settings.verbose > 1)
            fprintf(stderr, "proxy upstream seems closed already");
    }

    if (it != NULL)
        item_remove(it);
}

/* Do the actual work of forwarding the command from an
 * upstream conn to its assigned downstream.
 */
bool cproxy_forward_downstream(downstream *d) {
    assert(d != NULL);

    conn *uc = d->upstream_conn;

    assert(uc != NULL);
    assert(uc->state == conn_pause);
    assert(uc->rcurr != NULL);
    assert(uc->thread != NULL);
    assert(uc->thread->base != NULL);
    assert(IS_ASCII(uc->protocol));
    assert(IS_PROXY(uc->protocol));

    if (cproxy_connect_downstream(d, uc->thread) > 0) {
        assert(d->downstream_conns != NULL);

        if (uc->cmd == -1) {
            return cproxy_forward_simple_downstream(d, uc->rcurr, uc);
        } else {
            return cproxy_forward_item_downstream(d, uc->cmd, uc->item, uc);
        }
    }

    return false;
}

/* Forward a simple one-liner command downstream.
 * For example, get, incr/decr, delete, etc.
 * The response, though, might be a simple line or
 * multiple VALUE+END lines.
 */
bool cproxy_forward_simple_downstream(downstream *d,
                                      char *command, conn *uc) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->item == NULL);

    if (strncmp(command, "get", 3) == 0)
        return cproxy_forward_multiget_downstream(d, command, uc);

    if (strncmp(command, "flush_all", 9) == 0)
        return cproxy_broadcast_downstream(d, command, uc, "OK\r\n");

    if (strncmp(command, "stats", 5) == 0) {
        if (strncmp(command + 5, " reset", 6) == 0)
            return cproxy_broadcast_downstream(d, command, uc, "RESET\r\n");

        return cproxy_broadcast_downstream(d, command, uc, "END\r\n");
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
    conn *c = cproxy_find_downstream_conn(d, key, key_len);
    if (c != NULL &&
        cproxy_prep_conn_for_write(c)) {
        assert(c->state == conn_pause);

        out_string(c, command);

        if (settings.verbose > 1)
            fprintf(stderr, "forwarding to %d, noreply %d\n",
                    c->sfd, uc->noreply);

        if (update_event(c, EV_WRITE | EV_PERSIST)) {
            d->downstream_used_start = 1; // TODO: Need timeout?
            d->downstream_used       = 1;

            if (cproxy_dettach_if_noreply(d, uc))
                c->write_and_go = conn_pause;

            return true;
        }

        if (settings.verbose > 1)
            fprintf(stderr, "Couldn't update cproxy write event\n");

        cproxy_close_conn(c);
    }

    return false;
}

bool cproxy_forward_multiget_downstream(downstream *d,
                                        char *command, conn *uc) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->item == NULL);

    int nwrite = 0;
    int nconns = memcached_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        if (d->downstream_conns[i] != NULL) {
            cproxy_prep_conn_for_write(d->downstream_conns[i]);
            assert(d->downstream_conns[i]->state == conn_pause);
        }
    }

    char *space = strchr(command, ' ');
    assert(space > command);

    int cmd_len = space - command;
    assert(cmd_len == 3 || cmd_len == 4); // Either get or gets.

    if (settings.verbose > 1)
        fprintf(stderr, "forward multiget %s (%d)\n", command, cmd_len);

    while (space != NULL) {
        char *key = space + 1;
        char *next_space = strchr(key, ' ');
        int   key_len;

        if (next_space != NULL)
            key_len = next_space - key;
        else
            key_len = strlen(key);

        // This key_len check helps skips consecutive spaces.
        //
        if (key_len > 0) {
            conn *c = cproxy_find_downstream_conn(d, key, key_len);
            if (c != NULL) {
                assert(c->item == NULL);
                assert(c->state == conn_pause);
                assert(IS_ASCII(c->protocol));
                assert(IS_PROXY(c->protocol));
                assert(c->ilist != NULL);
                assert(c->isize > 0);

                c->icurr = c->ilist;
                c->ileft = 0;

                if (c->msgused <= 1 &&
                    c->msgbytes <= 0) {
                    add_iov(c, command, cmd_len);
                }

                // Write the key, including the preceding space.
                //
                add_iov(c, key - 1, key_len + 1);
            } else {
                // TODO: Handle when downstream conn is down.
            }
        }

        space = next_space;
    }

    for (int i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL &&
            (c->msgused > 1 ||
             c->msgbytes > 0)) {
            add_iov(c, "\r\n", 2);

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

                cproxy_close_conn(c);
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "forward multiget nwrite %d out of %d\n",
                nwrite, nconns);

    d->downstream_used_start = nwrite; // TODO: Need timeout?
    d->downstream_used       = nwrite;

    if (cproxy_dettach_if_noreply(d, uc) == false)
        d->upstream_suffix = "END\r\n";

    return nwrite > 0;
}

/* Used for broadcast commands, like flush_all or stats.
 */
bool cproxy_broadcast_downstream(downstream *d, char *command, conn *uc,
                                 char *suffix) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->item == NULL);

    int nwrite = 0;
    int nconns = memcached_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL &&
            cproxy_prep_conn_for_write(c)) {
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

                cproxy_close_conn(c);
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "forward multiget nwrite %d out of %d\n",
                nwrite, nconns);

    d->downstream_used_start = nwrite; // TODO: Need timeout?
    d->downstream_used       = nwrite;

    if (cproxy_dettach_if_noreply(d, uc) == false)
        d->upstream_suffix = suffix;

    return nwrite > 0;
}

/* Forward an upstream command that came with item data,
 * like set/add/replace/etc.
 */
bool cproxy_forward_item_downstream(downstream *d, short cmd,
                                    item *it, conn *uc) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(it != NULL);
    assert(uc != NULL);

    // Assuming we're already connected to downstream.
    //
    conn *c = cproxy_find_downstream_conn(d, ITEM_key(it), it->nkey);
    if (c != NULL &&
        cproxy_prep_conn_for_write(c)) {
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
                    d->downstream_used_start = 1; // TODO: Need timeout?
                    d->downstream_used       = 1;

                    if (cproxy_dettach_if_noreply(d, uc))
                        c->write_and_go = conn_pause;

                    return true;
                }
            }
        }

        if (settings.verbose > 1)
            fprintf(stderr, "Proxy item write out of memory");

        // TODO: Need better out-of-memory behavior.
    }

    return false;
}

