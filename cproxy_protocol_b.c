/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"
#include "log.h"

void cproxy_process_upstream_binary(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);
    assert(c->next == NULL);
    assert(c->item == NULL);
    assert(IS_BINARY(c->protocol));
    assert(IS_PROXY(c->protocol));

    proxy_td *ptd = c->extra;
    assert(ptd != NULL);

    if (!cproxy_prep_conn_for_write(c)) {
        ptd->stats.stats.err_upstream_write_prep++;
        conn_set_state(c, conn_closing);
        return;
    }

    c->cmd_curr       = -1;
    c->cmd_start      = NULL;
    c->cmd_start_time = msec_current_time;
    c->cmd_retries    = 0;

    int      extlen  = c->binary_header.request.extlen;
    int      keylen  = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen;

    assert(bodylen >= keylen + extlen);

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_process_upstream_binary %x %d %d %u\n",
                c->sfd, c->cmd, extlen, keylen, bodylen);
    }

    process_bin_noreply(c); // Map quiet c->cmd values into non-quiet.

    if (c->cmd == PROTOCOL_BINARY_CMD_VERSION ||
        c->cmd == PROTOCOL_BINARY_CMD_QUIT) {
        dispatch_bin_command(c);
        return;
    }

    // Alloc an item and continue with an rest-of-body nread if
    // necessary.  The item will hold the entire request message
    // (the header + body).
    //
    char *ikey    = "u";
    int   ikeylen = 1;

    c->item = item_alloc(ikey, ikeylen, 0, 0,
                         sizeof(c->binary_header) + bodylen);
    if (c->item != NULL) {
        item *it = c->item;
        void *rb = c->rcurr;

        assert(it->refcount == 1);

        memcpy(ITEM_data(it), rb, sizeof(c->binary_header));

        if (bodylen > 0) {
            c->ritem = ITEM_data(it) + sizeof(c->binary_header);
            c->rlbytes = bodylen;
            c->substate = bin_read_set_value;

            conn_set_state(c, conn_nread);
        } else {
            // Since we have no body bytes, we can go immediately to
            // the nread completed processing step.
            //
            cproxy_pause_upstream_for_downstream(ptd, c);
        }
    } else {
        ptd->stats.stats.err_oom++;
        cproxy_close_conn(c);
    }
}

/* We get here after reading the header+body into an item.
 */
void cproxy_process_upstream_binary_nread(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);
    assert(c->next == NULL);
    assert(c->cmd_start == NULL);
    assert(IS_BINARY(c->protocol));
    assert(IS_PROXY(c->protocol));

    protocol_binary_request_header *header =
        (protocol_binary_request_header *) &c->binary_header;

    int      extlen  = header->request.extlen;
    int      keylen  = header->request.keylen;
    uint32_t bodylen = header->request.bodylen;

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_process_upstream_binary_nread %x %d %d %u\n",
                c->sfd, c->cmd, extlen, keylen, bodylen);
    }

    // pthread_mutex_lock(&c->thread->stats.mutex);
    // c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    // pthread_mutex_unlock(&c->thread->stats.mutex);

    proxy_td *ptd = c->extra;
    assert(ptd != NULL);

    if (c->noreply) {
        if (settings.verbose > 2) {
            moxi_log_write("<%d cproxy_process_upstream_binary_nread "
                    "corking quiet command %x %d\n",
                    c->sfd, c->cmd, (c->corked != NULL));
        }

        // TODO: We currently don't support binary FLUSHQ.
        //
        // Rather than having the downstream connections get
        // into a wonky state, prevent it.
        //
        if (header->request.opcode == PROTOCOL_BINARY_CMD_FLUSHQ) {
            // Note: don't use cproxy_close_conn(c), as it goes
            // through the drive_machine() loop again.
            //
            // cproxy_close_conn(c);

            conn_set_state(c, conn_closing);

            return;
        }

        // Hold onto or 'cork' all the binary quiet commands
        // until there's a later non-quiet command.
        //
        if (cproxy_binary_cork_cmd(c)) {
            conn_set_state(c, conn_new_cmd);
        } else {
            ptd->stats.stats.err_oom++;
            cproxy_close_conn(c);
        }

        return;
    }

    assert(c->item == NULL || ((item *) c->item)->refcount == 1);

    cproxy_pause_upstream_for_downstream(ptd, c);
}

static int bin_cmd_append(bin_cmd **head, bin_cmd *bc) {
    assert(head != NULL);
    assert(bc != NULL);

    bin_cmd *tail = *head;

    int n = 1;

    while (tail != NULL) {
        n++;
        if (tail->next == NULL) {
            tail->next = bc;
            return n;
        }
        tail = tail->next;
    }

    *head = bc;

    return n; // Returns number of items in list.
}

bool cproxy_binary_cork_cmd(conn *c) {
    // Save the quiet binary command for later uncorking.
    //
    assert(c != NULL);
    assert(c->item != NULL);

    bin_cmd *bc = calloc(1, sizeof(bin_cmd));
    if (bc != NULL) {
        // Transferred the item refcount from c->item to the bin_cmd.
        //
        bc->request_item = c->item;
        c->item = NULL;

        int ncorked = bin_cmd_append(&c->corked, bc);

        if (settings.verbose > 2) {
            moxi_log_write("%d: cproxy_binary_cork_cmd, ncorked %d %d\n",
                    c->sfd, ncorked, (c->corked != NULL));
        }

        return true;
    }

    if (settings.verbose > 2) {
        moxi_log_write("%d: cproxy_binary_cork_cmd failed\n",
                c->sfd);
    }

    return false;
}

void cproxy_binary_uncork_cmds(downstream *d, conn *uc) {
    assert(d != NULL);
    assert(uc != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("%d: cproxy_binary_uncork_cmds\n",
                uc->sfd);
    }

    int n = 0;

    while (uc->corked != NULL) {
        bin_cmd *next = uc->corked->next;

        item *it = uc->corked->request_item;
        if (it != NULL) {
            b2b_forward_item(uc, d, it);
            n++;
        }

        if (uc->corked->request_item != NULL) {
            item_remove(uc->corked->request_item);
        }

        if (uc->corked->response_item != NULL) {
            item_remove(uc->corked->response_item);
        }

        free(uc->corked);
        uc->corked = next;
    }

    if (settings.verbose > 2) {
        moxi_log_write("%d: cproxy_binary_uncork_cmds, uncorked %d\n",
                uc->sfd, n);
    }
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

void cproxy_dump_header(int prefix, char *bb) {
    if (settings.verbose > 2) {
        char buf[200];

        int prefix_len = snprintf(buf, sizeof(buf), "%d   ", prefix);
        int start = prefix_len;

        for (int ii = 0; ii < sizeof(protocol_binary_request_header); ++ii) {
            if (ii > 0 && ii % 4 == 0) {
                buf[start] = '\n';
                buf[start + 1] = '\0';
                moxi_log_write(buf);

                start = prefix_len;
            }

            start += snprintf(buf + start, sizeof(buf) - start,
                              " 0x%02x", (unsigned char) bb[ii]);
        }

        buf[start] = '\n';
        buf[start + 1] = '\0';

        moxi_log_write(buf);
    }
}

bool cproxy_binary_ignore_reply(conn *c, protocol_binary_response_header *header, item *it) {
    if (c->noreply &&
        OPAQUE_IGNORE_REPLY == ntohl(header->response.opaque)) {
        // Handle when the client sent an ascii noreply command,
        // and we now need to eat the binary error responses.
        // So, drop the current response (should be an error response)
        // and go to read the next response message.
        //
        if (settings.verbose > 2) {
            moxi_log_write("<%d cproxy_process_a2b_downstream_response OPAQUE_IGNORE_REPLY, "
                    "cmd: %x, status: %x, ignoring reply\n",
                    c->sfd, header->response.opcode, header->response.status);
        }

        conn_set_state(c, conn_new_cmd);

        if (it != NULL) {
            item_remove(it);
        }

        return true;
    }

    return false;
}
