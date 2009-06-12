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
    c->cmd_start_time = msec_current_time;
    c->cmd_retries    = 0;

    proxy_td *ptd = c->extra;
    assert(ptd != NULL);

    /* For commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */
    if (!cproxy_prep_conn_for_write(c)) {
        ptd->stats.stats.err_upstream_write_prep++;
        conn_set_state(c, conn_closing);
        return;
    }

    int     cmd_len = 0;
    token_t tokens[MAX_TOKENS];
    size_t  ntokens = scan_tokens(line, tokens, MAX_TOKENS, &cmd_len);
    char   *cmd     = tokens[COMMAND_TOKEN].value;
    int     cmdx    = -1;
    int     cmd_st  = STATS_CMD_TYPE_REGULAR;
    int     comm;

#define SEEN(cmd_id, is_cas, cmd_len)                           \
    cmd_st = c->noreply ?                                       \
        STATS_CMD_TYPE_QUIET : STATS_CMD_TYPE_REGULAR;          \
    ptd->stats.stats_cmd[cmd_st][cmd_id].seen++;                \
    ptd->stats.stats_cmd[cmd_st][cmd_id].read_bytes += cmd_len; \
    if (is_cas)                                                 \
        ptd->stats.stats_cmd[cmd_st][cmd_id].cas++;

    if (ntokens >= 3 &&
        (strncmp(cmd, "get", 3) == 0)) {

        // Handles get and gets.
        //
        cproxy_pause_upstream_for_downstream(ptd, c);

        // The cmd_len from scan_tokens might not include
        // all the keys, so cmd_len might not == strlen(command).
        // Handle read_bytes during multiget broadcast.
        //
        SEEN(STATS_CMD_GET, cmd[3] == 's', 0);

    } else if ((ntokens == 6 || ntokens == 7) &&
               ((strncmp(cmd, "add", 3) == 0 &&
                 (comm = NREAD_ADD) &&
                 (cmdx = STATS_CMD_ADD)) ||
                (strncmp(cmd, "set", 3) == 0 &&
                 (comm = NREAD_SET) &&
                 (cmdx = STATS_CMD_SET)) ||
                (strncmp(cmd, "replace", 7) == 0 &&
                 (comm = NREAD_REPLACE) &&
                 (cmdx = STATS_CMD_REPLACE)) ||
                (strncmp(cmd, "prepend", 7) == 0 &&
                 (comm = NREAD_PREPEND) &&
                 (cmdx = STATS_CMD_PREPEND)) ||
                (strncmp(cmd, "append", 6) == 0 &&
                 (comm = NREAD_APPEND) &&
                 (cmdx = STATS_CMD_APPEND)))) {
        assert(c->item == NULL);
        c->item = NULL;

        process_update_command(c, tokens, ntokens, comm, false);

        if (cmdx >= 0) {
            item *it = c->item;
            if (it != NULL) {
                SEEN(cmdx, false, cmd_len + it->nbytes);
            } else {
                SEEN(cmdx, false, cmd_len);
                ptd->stats.stats_cmd[cmd_st][cmdx].misses++;
            }
        }

    } else if ((ntokens == 7 || ntokens == 8) &&
               (strncmp(cmd, "cas", 3) == 0 && (comm = NREAD_CAS))) {
        assert(c->item == NULL);
        c->item = NULL;

        process_update_command(c, tokens, ntokens, comm, true);

        item *it = c->item;
        if (it != NULL) {
            SEEN(STATS_CMD_CAS, true, cmd_len + it->nbytes);
        } else {
            SEEN(STATS_CMD_CAS, true, cmd_len);
            ptd->stats.stats_cmd[cmd_st][STATS_CMD_CAS].misses++;
        }

    } else if ((ntokens == 4 || ntokens == 5) &&
               (strncmp(cmd, "incr", 4) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

        SEEN(STATS_CMD_INCR, false, cmd_len);

    } else if ((ntokens == 4 || ntokens == 5) &&
               (strncmp(cmd, "decr", 4) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

        SEEN(STATS_CMD_DECR, false, cmd_len);

    } else if (ntokens >= 3 && ntokens <= 4 &&
               (strncmp(cmd, "delete", 6) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

        SEEN(STATS_CMD_DELETE, false, cmd_len);

    } else if (ntokens >= 2 && ntokens <= 4 &&
               (strncmp(cmd, "flush_all", 9) == 0)) {

        set_noreply_maybe(c, tokens, ntokens);
        cproxy_pause_upstream_for_downstream(ptd, c);

        SEEN(STATS_CMD_FLUSH_ALL, false, cmd_len);

    } else if (ntokens == 3 &&
               (strcmp(cmd, "stats reset") == 0)) {

        cproxy_pause_upstream_for_downstream(ptd, c);

        SEEN(STATS_CMD_STATS_RESET, false, cmd_len);

    } else if (ntokens == 2 &&
               (strcmp(cmd, "stats") == 0)) {

        // Even though we've coded to handle advanced stats
        // like stats cachedump, prevent those here to avoid
        // locking downstream servers.
        //
        cproxy_pause_upstream_for_downstream(ptd, c);

        SEEN(STATS_CMD_STATS, false, cmd_len);

    } else if (ntokens == 2 &&
               (strncmp(cmd, "version", 7) == 0)) {

        out_string(c, "VERSION " VERSION);

        SEEN(STATS_CMD_VERSION, false, cmd_len);

    } else if ((ntokens == 3 || ntokens == 4) &&
               (strncmp(cmd, "verbosity", 9) == 0)) {

        process_verbosity_command(c, tokens, ntokens);

        SEEN(STATS_CMD_VERBOSITY, false, cmd_len);

    } else if (ntokens == 2 &&
               (strncmp(cmd, "quit", 4) == 0)) {

        conn_set_state(c, conn_closing);

        SEEN(STATS_CMD_QUIT, false, cmd_len);

    } else {
        out_string(c, "ERROR");

        SEEN(STATS_CMD_ERROR, false, cmd_len);
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

/**
 * @param cas_emit  1: emit CAS.
 *                  0: do not emit CAS.
 *                 -1: data driven.
 */
void cproxy_upstream_ascii_item_response(item *it, conn *uc,
                                         int cas_emit) {
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
        if ((cas_emit == 0) ||
            (cas_emit < 0 &&
             cas == CPROXY_NOT_CAS)) {
            if (add_conn_item(uc, it)) {
                it->refcount++; // TODO: Need item lock here?

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
                    it->refcount++; // TODO: Need item lock here?

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

/**
 * When we're sending an ascii response line back upstream to
 * an ascii protocol client, keep the front_cache sync'ed.
 */
void cproxy_del_front_cache_key_ascii_response(downstream *d,
                                               char *response,
                                               char *command) {
    assert(d);
    assert(d->ptd);
    assert(d->ptd->proxy);
    assert(response);

    if (!mcache_started(&d->ptd->proxy->front_cache))
        return;

    // TODO: Not sure if we need all these checks, or just
    // clear the cache item no matter what.
    //
    if (strncmp(response, "DELETED", 7) == 0 ||
        strncmp(response, "STORED", 6) == 0 ||
        strncmp(response, "EXISTS", 6) == 0 ||
        strncmp(response, "NOT_FOUND", 9) == 0 ||
        strncmp(response, "NOT_STORED", 10) == 0 ||
        strncmp(response, "ERROR", 5) == 0 ||
        strncmp(response, "SERVER_ERROR", 12) == 0 ||
        (response[0] == '-') ||
        (response[0] >= '0' && response[0] <= '9')) {
        cproxy_del_front_cache_key_ascii(d, command);
    }
}

void cproxy_del_front_cache_key_ascii(downstream *d,
                                      char *command) {
    assert(d);
    assert(d->ptd);
    assert(d->ptd->proxy);

    if (mcache_started(&d->ptd->proxy->front_cache)) {
        char *spc = strchr(command, ' ');
        if (spc != NULL) {
            char *key = spc + 1;
            int   key_len = skey_len(key);
            if (key_len > 0) {
                mcache_delete(&d->ptd->proxy->front_cache,
                              key, key_len);

                if (settings.verbose > 1)
                    fprintf(stderr, "front_cache del %s\n", key);
            }
        }
    }
}

/**
 * Depending on our configuration, we can optimize SET's
 * on certain keys by making them fire-and-forget and
 * immediately transmitting a success response to the
 * upstream client.
 */
bool cproxy_optimize_set_ascii(downstream *d, conn *uc,
                               char *key, int key_len) {
    assert(d);
    assert(d->ptd);
    assert(d->ptd->proxy);
    assert(uc);
    assert(uc->next == NULL);

    if (matcher_check(&d->ptd->proxy->optimize_set_matcher,
                      key, key_len, false)) {
        d->upstream_conn = NULL;
        d->upstream_suffix = NULL;

        out_string(uc, "STORED");

        if (!update_event(uc, EV_WRITE | EV_PERSIST)) {
            if (settings.verbose > 1)
                fprintf(stderr,
                        "Can't update upstream write event\n");

            d->ptd->stats.stats.err_oom++;
            cproxy_close_conn(uc);
        }

        return true;
    }

    return false;
}

/**
 * Optimization when we're talking with ourselves,
 * so we don't need to go through network hop,
 * for a simple one-liner command.
 */
void cproxy_optimize_to_self(downstream *d, conn *uc,
                             char *command) {
    assert(d);
    assert(d->ptd);
    assert(uc);
    assert(uc->next == NULL);

    d->ptd->stats.stats.tot_optimize_self++;

    if (command != NULL &&
        settings.verbose > 1)
        fprintf(stderr, "optimize to self: %s\n", command);

    d->upstream_conn   = NULL;
    d->upstream_suffix = NULL;

    cproxy_release_downstream(d, false);
}
