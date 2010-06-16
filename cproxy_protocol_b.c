/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"

void cproxy_process_upstream_binary(conn *c) {
    assert(c != NULL);
    assert(c->next == NULL);
    assert(c->extra != NULL);
    assert(c->item == NULL);
    assert(IS_BINARY(c->protocol));
    assert(IS_PROXY(c->protocol));

    proxy_td *ptd = c->extra;
    assert(ptd != NULL);

    if (settings.verbose > 2) {
        fprintf(stderr, "<%d cproxy_process_upstream_binary\n",
                c->sfd);
    }

    if (!cproxy_prep_conn_for_write(c)) {
        ptd->stats.stats.err_upstream_write_prep++;
        conn_set_state(c, conn_closing);
        return;
    }

    c->cmd_curr       = -1;
    c->cmd_start      = NULL;
    c->cmd_start_time = msec_current_time;
    c->cmd_retries    = 0;

    int extlen = c->binary_header.request.extlen;
    int keylen = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen;

    if (c->cmd == PROTOCOL_BINARY_CMD_NOOP &&
        extlen == 0 && keylen == 0 && bodylen == 0) {
        c->noreply = false;

        // For a NOOP, we have received everything.
        //
        cproxy_pause_upstream_for_downstream(ptd, c);
    } else {
        // Because binary protocol is very regular, we can
        // reuse a lot of existing dispatch machinery.
        //
        dispatch_bin_command(c);
    }
}

/* We get here after reading the key + extras values,
 * and possibly an item.
 */
void cproxy_process_upstream_binary_nread(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);
    assert(c->next == NULL);

    if (settings.verbose > 2) {
        fprintf(stderr, "<%d cproxy_process_upstream_binary_nread\n",
                c->sfd);
    }

    // pthread_mutex_lock(&c->thread->stats.mutex);
    // c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    // pthread_mutex_unlock(&c->thread->stats.mutex);

    proxy_td *ptd = c->extra;
    assert(ptd != NULL);

    if (c->substate == bin_reading_set_header) {
        // Have the existing machinery do the item_alloc, etc.
        //
        if (settings.verbose > 2) {
            fprintf(stderr, "<%d cproxy_process_upstream_binary_nread bin_reading_set_header\n",
                    c->sfd);
        }

        assert(c->item == NULL);

        complete_nread_binary(c);

        return;
    }

    // At this point, we have the key, extras, and item (if they
    // were provided) all received.
    //
    if (c->noreply) {
        if (settings.verbose > 2) {
            fprintf(stderr, "<%d cproxy_process_upstream_binary_nread corking quiet command %x\n",
                    c->sfd, c->cmd);
        }

        // Hold onto or 'cork' all the binary quiet commands
        // until there's a later non-quiet command.
        //
        cproxy_binary_cork_cmd(c);

        conn_set_state(c, conn_new_cmd);

        return;
    }

    assert(c->item == NULL || ((item *) c->item)->refcount == 1);

    cproxy_pause_upstream_for_downstream(ptd, c);
}

void cproxy_binary_cork_cmd(conn *c) {
    // TODO: Save the quiet binary command for later uncorking.
    //
    assert(false);
}

void cproxy_binary_uncork_cmds(downstream *d, conn *c) {
    assert(false);
}

void cproxy_process_downstream_binary(conn *c) {
    downstream *d = c->extra;
    assert(d != NULL);
    assert(d->upstream_conn != NULL);

    if (IS_ASCII(d->upstream_conn->protocol)) {
        cproxy_process_a2b_downstream(c);
    } else {
        cproxy_process_b2b_downstream(c);
    }
}

void cproxy_process_downstream_binary_nread(conn *c) {
    downstream *d = c->extra;
    assert(d != NULL);
    assert(d->upstream_conn != NULL);

    if (IS_ASCII(d->upstream_conn->protocol)) {
        cproxy_process_a2b_downstream_nread(c);
    } else {
        cproxy_process_b2b_downstream_nread(c);
    }
}

