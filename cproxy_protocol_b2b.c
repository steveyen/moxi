/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <math.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"

// Internal declarations.
//
static protocol_binary_request_noop req_noop = {
    .bytes = {0}
};

bool b2b_forward(conn *uc, downstream *d, conn *c, bool self, int vbucket);

void cproxy_init_b2b() {
    memset(&req_noop, 0, sizeof(req_noop));

    req_noop.message.header.request.magic    = PROTOCOL_BINARY_REQ;
    req_noop.message.header.request.opcode   = PROTOCOL_BINARY_CMD_NOOP;
    req_noop.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
}

/* Do the actual work of forwarding the command from an
 * upstream ascii conn to its assigned binary downstream.
 */
bool cproxy_forward_b2b_downstream(downstream *d) {
    assert(d != NULL);
    assert(d->ptd != NULL);

    conn *uc = d->upstream_conn;

    if (settings.verbose > 2) {
        fprintf(stderr, "%d: cproxy_forward_b2b_downstream %x\n",
                uc->sfd, uc->cmd);
    }

    assert(uc != NULL);
    assert(uc->state == conn_pause);
    assert(uc->cmd_start == NULL);
    assert(uc->thread != NULL);
    assert(uc->thread->base != NULL);
    assert(IS_BINARY(uc->protocol));
    assert(IS_PROXY(uc->protocol));

    if (cproxy_connect_downstream(d, uc->thread) > 0) {
        assert(d->downstream_conns != NULL);

        // TODO: Now would be the time to uncork.
        //
        // cproxy_binary_uncork_cmds(d, uc);

        if (uc->cmd == PROTOCOL_BINARY_CMD_FLUSH ||
            uc->cmd == PROTOCOL_BINARY_CMD_NOOP ||
            uc->cmd == PROTOCOL_BINARY_CMD_STAT) {
            return cproxy_broadcast_b2b_downstream(d, uc);
        }

        return cproxy_forward_b2b_simple_downstream(d, uc);
    }

    return false;
}

/* A simple command includes a key, for hashing.
 */
bool cproxy_forward_b2b_simple_downstream(downstream *d, conn *uc) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(d->downstream_used_start == 0);
    assert(d->downstream_used == 0);
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(uc->noreply == false);
    assert(d->multiget == NULL);
    assert(d->merger == NULL);

    // Assuming we're already connected to downstream.
    //
    // TODO: Optimize to self codepath.
    //
    char *key = binary_get_key(uc);
    int  nkey = uc->binary_header.request.keylen;

    if (settings.verbose > 2) {
        fprintf(stderr, "%d: cproxy_forward_b2b_simple_downstream %x\n",
                uc->sfd, uc->cmd);
    }

    assert(key != NULL);
    assert(nkey > 0);

    bool self = false;
    int  vbucket = -1;

    conn *c = cproxy_find_downstream_conn_ex(d, key, nkey, &self, &vbucket);
    if (c != NULL &&
        b2b_forward(uc, d, c, self, vbucket) == true) {
        d->downstream_used_start = 1;
        d->downstream_used = 1;

        cproxy_start_downstream_timeout(d, c);

        return true;
    }

    return false;
}

bool b2b_forward(conn *uc, downstream *d, conn *c, bool self, int vbucket) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(uc->noreply == false);
    assert(c != NULL);

    if (settings.verbose > 2) {
        fprintf(stderr, "%d: b2b_forward %x to %d\n",
                uc->sfd, uc->cmd, c->sfd);
    }

    if (cproxy_prep_conn_for_write(c)) {
        assert(c->state == conn_pause);

        protocol_binary_request_header *req = binary_get_request(uc);
        if (req != NULL) {
            assert(req->request.bodylen >= (uc->binary_header.request.keylen +
                                            uc->binary_header.request.extlen));

            if (add_iov(c, req,
                        sizeof(uc->binary_header) +
                        uc->binary_header.request.keylen +
                        uc->binary_header.request.extlen) == 0) {
                item *it = uc->item;
                if (it != NULL) {
                    assert(it->refcount == 1);
                    assert(ITEM_data(it) != NULL);

                    if (add_conn_item(c, it) == true) {
                        it->refcount++; // The uc keeps its refcount++, and we need our own.

                        if (settings.verbose > 2) {
                            fprintf(stderr, "%d: b2b_forward %x to %d with item_len %d\n",
                                    uc->sfd, uc->cmd, c->sfd, it->nbytes);
                        }

                        if (add_iov(c, ITEM_data(it), it->nbytes - 2) != 0) {
                            goto error_oom;
                        }
                    }
                }

                conn_set_state(c, conn_mwrite);
                c->write_and_go = conn_new_cmd;

                if (update_event(c, EV_WRITE | EV_PERSIST)) {
                    if (settings.verbose > 2) {
                        fprintf(stderr, "%d: b2b_forward %x to %d success\n",
                                uc->sfd, uc->cmd, c->sfd);
                    }

                    return true;
                }
            }
        }

    error_oom:
        d->ptd->stats.stats.err_oom++;
        cproxy_close_conn(c);
    } else {
        d->ptd->stats.stats.err_downstream_write_prep++;
        cproxy_close_conn(c);
    }

    return false;
}

/* Used for broadcast commands, like no-op, flush_all or stats.
 */
bool cproxy_broadcast_b2b_downstream(downstream *d, conn *uc) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(d->downstream_used_start == 0);
    assert(d->downstream_used == 0);
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(uc->item == NULL);
    assert(uc->noreply == false);

    int nwrite = 0;
    int nconns = mcs_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL &&
            b2b_forward(uc, d, c, false, -1) == true) {
            nwrite++;
        }
    }

    if (settings.verbose > 2) {
        fprintf(stderr, "%d: b2b broadcast nwrite %d out of %d\n",
                uc->sfd, nwrite, nconns);
    }

    if (nwrite > 0) {
        d->downstream_used_start = nwrite;
        d->downstream_used       = nwrite;

        cproxy_start_downstream_timeout(d, NULL);

        return true;
    }

    return false;
}

/* Called when we receive a binary response header from
 * a downstream server, via try_read_command()/drive_machine().
 */
void cproxy_process_b2b_downstream(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);
    assert(c->next == NULL);
    assert(c->item == NULL);
    assert(IS_BINARY(c->protocol));
    assert(IS_PROXY(c->protocol));
    assert(c->substate == bin_no_state);

    downstream *d = c->extra;
    assert(d);

    int      extlen  = c->binary_header.request.extlen;
    int      keylen  = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen;

    if (settings.verbose > 2) {
        fprintf(stderr, "<%d cproxy_process_b2b_downstream %x %d %d %u\n",
                c->sfd, c->cmd, extlen, keylen, bodylen);
    }

    assert(bodylen >= keylen + extlen);

    process_bin_noreply(c); // Map quiet c->cmd values into non-quiet.

    // Our approach is to read everything we can before
    // getting into big switch/case statements for the
    // actual processing.
    //
    // Alloc an item and continue with an rest-of-body nread if
    // necessary.  The item will hold the entire response message
    // (the header + body).
    //
    char *ikey    = "q";
    int   ikeylen = 1;

    assert(ikey);
    assert(ikeylen > 0);

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
            cproxy_process_b2b_downstream_nread(c);
        }
    } else {
        d->ptd->stats.stats.err_oom++;
        cproxy_close_conn(c);
    }
}

/* We reach here after nread'ing a header+body into an item.
 */
void cproxy_process_b2b_downstream_nread(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);
    assert(c->next == NULL);
    assert(c->cmd_start == NULL);
    assert(IS_BINARY(c->protocol));
    assert(IS_PROXY(c->protocol));

    protocol_binary_response_header *header =
        (protocol_binary_response_header *) &c->binary_header;

    int      extlen  = header->response.extlen;
    int      keylen  = header->response.keylen;
    uint32_t bodylen = header->response.bodylen;

    if (settings.verbose > 2) {
        fprintf(stderr, "<%d cproxy_process_b2b_downstream_nread %x %d %d %u\n",
                c->sfd, c->cmd, extlen, keylen, bodylen);
    }

    downstream *d = c->extra;
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);

    item *it = c->item;
    assert(it != NULL);
    assert(it->refcount == 1);

    c->item = NULL;

    conn *uc = d->upstream_conn;
    if (uc != NULL) {
        if (settings.verbose > 2) {
            fprintf(stderr, "<%d cproxy_process_b2b_downstream_nread writing %u\n",
                    c->sfd, it->nbytes);

            unsigned char *bb = (unsigned char *) ITEM_data(it);
            fprintf(stderr, ">%d Write upstream binary protocol data:",
                    uc->sfd);
            for (int ii = 0; ii < sizeof(protocol_binary_response_header); ++ii) {
                if (ii % 4 == 0) {
                    fprintf(stderr, "\n<%d   ", uc->sfd);
                }
                fprintf(stderr, " 0x%02x", bb[ii]);
            }
            fprintf(stderr, "\n");
        }

        if (add_conn_item(uc, it) == true) {
            if (add_iov(uc, ITEM_data(it), it->nbytes) != 0) {
                goto error_oom;
            }

            it = NULL; // The uc now owns the item's refcount.
        } else {
            goto error_oom;
        }
    }

    if (settings.verbose > 2) {
        fprintf(stderr, "%d: cproxy_process_b2b_downstream_nread pausing\n",
                c->sfd);
    }

    if (it != NULL) {
        item_remove(it);
    }

    if (c->noreply) {
        conn_set_state(c, conn_new_cmd);
    } else {
        conn_set_state(c, conn_pause);

        if (settings.verbose > 2) {
            fprintf(stderr, "%d: cproxy_process_b2b_downstream_nread pausing\n",
                    c->sfd);
        }

        if (uc != NULL) {
            conn_set_state(uc, conn_mwrite);
            c->write_and_go = conn_new_cmd;

            if (!update_event(uc, EV_WRITE | EV_PERSIST)) {
                goto error_oom;
            }
        }
    }

    return;

 error_oom:
    if (settings.verbose > 1) {
        fprintf(stderr,
                "ERROR: Can't write event\n");
    }

    d->ptd->stats.stats.err_oom++;
    cproxy_close_conn(c);
}

