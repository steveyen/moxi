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
#define COMMAND_TOKEN 0
#define MAX_TOKENS    8

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

    // Snapshot rcurr, because the caller, try_read_command(), changes it.
    //
    c->cmd_start      = c->rcurr;
    c->cmd_start_time = current_time;
    c->cmd_retries    = 0;

    proxy_td *ptd = c->extra;
    assert(ptd != NULL);

    /* For commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */
    if (!cproxy_prep_conn_for_write(c)) {
        ptd->stats.err_upstream_write_prep++;
        conn_set_state(c, conn_closing);
        return;
    }

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
    assert(c->next == NULL);

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

void cproxy_upstream_ascii_item_response(item *it, conn *uc) {
    assert(it != NULL);
    assert(uc != NULL);
    assert(uc->state == conn_pause);
    assert(uc->funcs != NULL);
    assert(IS_ASCII(uc->protocol));
    assert(IS_PROXY(uc->protocol));

    if (strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) == 0) {
        // TODO: Need to clean up half-written add_iov()'s.
        //       Consider closing the upstream_conns?
        //
        uint64_t cas = ITEM_get_cas(it);
        if (cas == CPROXY_NOT_CAS) {
            if (add_conn_item(uc, it)) {
                it->refcount++;

                if (add_iov(uc, "VALUE ", 6) == 0 &&
                    add_iov(uc, ITEM_key(it), it->nkey) == 0 &&
                    add_iov(uc, ITEM_suffix(it),
                            it->nsuffix + it->nbytes) == 0) {
                    if (settings.verbose > 1)
                        fprintf(stderr,
                                "<%d cproxy ascii item response success\n",
                                uc->sfd);
                }
            }
        } else {
            char *suffix = add_conn_suffix(uc);
            if (suffix != NULL) {
                sprintf(suffix, " %llu\r\n", (unsigned long long) cas);

                if (add_conn_item(uc, it)) {
                    it->refcount++;

                    if (add_iov(uc, "VALUE ", 6) == 0 &&
                        add_iov(uc, ITEM_key(it), it->nkey) == 0 &&
                        add_iov(uc, ITEM_suffix(it),
                                it->nsuffix - 2) == 0 &&
                        add_iov(uc, suffix, strlen(suffix)) == 0 &&
                        add_iov(uc, ITEM_data(it), it->nbytes) == 0) {
                        if (settings.verbose > 1)
                            fprintf(stderr,
                                    "<%d cproxy ascii item response ok\n",
                                    uc->sfd);
                    }
                }
            }
        }
    } else {
        if (settings.verbose > 1)
            fprintf(stderr, "unexpected downstream data block");
    }
}

