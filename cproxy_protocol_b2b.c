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
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(d->downstream_used_start == 0);
    assert(d->downstream_used == 0);
    assert(d->multiget == NULL);
    assert(d->merger == NULL);

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
    assert(uc->noreply == false);
    assert(IS_BINARY(uc->protocol));
    assert(IS_PROXY(uc->protocol));

    if (cproxy_connect_downstream(d, uc->thread) > 0) {
        assert(d->downstream_conns != NULL);

        int nconns = mcs_server_count(&d->mst);

        for (int i = 0; i < nconns; i++) {
            conn *c = d->downstream_conns[i];
            if (c != NULL) {
                assert(c->state == conn_pause);
                assert(c->item == NULL);

                if (cproxy_prep_conn_for_write(c) == false) {
                    d->ptd->stats.stats.err_downstream_write_prep++;
                    cproxy_close_conn(c);

                    return false;
                }
            }
        }

        // Uncork the saved-up quiet binary commands.
        //
        cproxy_binary_uncork_cmds(d, uc);

        if (uc->cmd == PROTOCOL_BINARY_CMD_FLUSH ||
            uc->cmd == PROTOCOL_BINARY_CMD_NOOP ||
            uc->cmd == PROTOCOL_BINARY_CMD_STAT) {
            return cproxy_broadcast_b2b_downstream(d, uc);
        }

        return cproxy_forward_b2b_simple_downstream(d, uc);
    }

    if (settings.verbose > 2) {
        fprintf(stderr,
                "%d: cproxy_forward_b2b_downstream connect failed\n",
                uc->sfd);
    }

    return false;
}

/* A simple command includes a key, for hashing.
 */
bool cproxy_forward_b2b_simple_downstream(downstream *d, conn *uc) {
    return b2b_forward_item(uc, d, uc->item);
}

bool b2b_forward_item(conn *uc, downstream *d, item *it) {
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(uc->noreply == false);
    assert(it != NULL);

    protocol_binary_request_header *req =
        (protocol_binary_request_header *) ITEM_data(it);

    char *key    = ((char *) req) + sizeof(*req) + req->request.extlen;
    int   keylen = ntohs(req->request.keylen);

    if (settings.verbose > 2) {
        char buf[300];
        memcpy(buf, key, keylen);
        buf[keylen] = '\0';

        fprintf(stderr,
                "%d: b2b_forward_item nbytes %u, extlen %d, keylen %d opcode %x key (%s)",
                uc->sfd, it->nbytes, req->request.extlen, keylen, req->request.opcode, buf);

        cproxy_dump_header(uc->sfd, (char *) req);
    }

    assert(key != NULL);
    assert(keylen > 0);

    bool self = false;
    int  vbucket = -1;

    conn *c = cproxy_find_downstream_conn_ex(d, key, keylen,
                                             &self, &vbucket);
    if (c != NULL) {
        if (b2b_forward_item_vbucket(uc, d, it, c, self, vbucket) == true) {
            d->downstream_used_start = 1;
            d->downstream_used = 1;

            cproxy_start_downstream_timeout(d, c);

            return true;
        }
    }

    if (settings.verbose > 2) {
        fprintf(stderr,
                "%d: b2b_forward_item failed (%d)\n",
                uc->sfd, (c != NULL));
    }

    return false;
}

bool b2b_forward_item_vbucket(conn *uc, downstream *d, item *it,
                              conn *c, bool self, int vbucket) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(uc->noreply == false);
    assert(c != NULL);

    // Assuming we're already connected to downstream.
    //
    // TODO: Optimize to self codepath.
    //
    if (settings.verbose > 2) {
        fprintf(stderr,
                "%d: b2b_forward_item_vbucket %x to %d, vbucket %d\n",
                uc->sfd, uc->cmd, c->sfd, vbucket);
    }

    protocol_binary_request_header *req =
        (protocol_binary_request_header *) ITEM_data(it);

    if (vbucket >= 0) {
        req->request.reserved = htons(vbucket);
    }

    if (add_conn_item(c, it) == true) {
        // The caller keeps its refcount, and we need our own.
        //
        it->refcount++;

        if (add_iov(c, ITEM_data(it), it->nbytes) == 0) {
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

    d->ptd->stats.stats.err_oom++;
    cproxy_close_conn(c);

    return false;
}

/* Used for broadcast commands, like no-op, flush_all or stats.
 */
bool cproxy_broadcast_b2b_downstream(downstream *d, conn *uc) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(uc->noreply == false);

    int nwrite = 0;
    int nconns = mcs_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL &&
            b2b_forward_item_vbucket(uc, d, uc->item, c,
                                     false, -1) == true) {
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

    c->cmd_curr       = -1;
    c->cmd_start      = NULL;
    c->cmd_start_time = msec_current_time;
    c->cmd_retries    = 0;

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
    int      status  = header->response.status;

    if (settings.verbose > 2) {
        fprintf(stderr,
                "<%d cproxy_process_b2b_downstream_nread %x %d %d %u %d %x\n",
                c->sfd, c->cmd, extlen, keylen, bodylen, c->noreply, status);
    }

    downstream *d = c->extra;
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);

    // TODO: Need to handle quiet binary command error response,
    //       in the right order.
    // TODO: Need to handle not-my-vbucket error during a quiet cmd.
    //
    conn *uc = d->upstream_conn;
    item *it = c->item;
    assert(it != NULL);
    assert(it->refcount == 1);

    if (c->noreply) {
        conn_set_state(c, conn_new_cmd);
    } else {
        conn_set_state(c, conn_pause);

        // If the client is still there, we should handle
        // a not-my-vbucket error with a possible retry.
        //
        if (uc != NULL &&
            status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET) {
            if (settings.verbose > 2) {
                fprintf(stderr,
                        "<%d cproxy_process_b2b_downstream_nread not-my-vbucket, "
                        "cmd: %x %d\n",
                        c->sfd, header->response.opcode, uc->item != NULL);
            }

            assert(uc->item != NULL);

            protocol_binary_request_header *req =
                (protocol_binary_request_header *) ITEM_data((item *) uc->item);

            int vbucket = ntohs(req->request.reserved);
            int sindex = downstream_conn_index(d, c);

            if (settings.verbose > 2) {
                fprintf(stderr,
                        "<%d cproxy_process_b2b_downstream_nread not-my-vbucket, "
                        "cmd: %x not multi-key get, sindex %d, vbucket %d, retries %d\n",
                        c->sfd, header->response.opcode,
                        sindex, vbucket, uc->cmd_retries);
            }

            mcs_server_invalid_vbucket(&d->mst, sindex, vbucket);

            // As long as the upstream is still open and we haven't
            // retried too many times already.
            //
            int max_retries = (mcs_server_count(&d->mst) * 2);

            if (uc->cmd_retries < max_retries) {
                uc->cmd_retries++;

                // TODO: Add a stats counter here for this case.
                //
                d->upstream_retry++;

                return;
            }

            if (settings.verbose > 2) {
                fprintf(stderr,
                        "%d: cproxy_process_b2b_downstream_nread not-my-vbucket, "
                        "cmd: %x skipping retry %d >= %d\n",
                        c->sfd, header->response.opcode, uc->cmd_retries,
                        max_retries);
            }
        }
    }

    if (uc != NULL) {
        if (settings.verbose > 2) {
            fprintf(stderr,
                    "<%d cproxy_process_b2b_downstream_nread got %u",
                    c->sfd, it->nbytes);

            cproxy_dump_header(c->sfd, ITEM_data(it));
        }

        if (add_conn_item(uc, it) == true) {
            it->refcount++;

            if (add_iov(uc, ITEM_data(it), it->nbytes) == 0 &&
                cproxy_update_event_write(d, uc) == true) {
                conn_set_state(uc, conn_mwrite);
                goto done;
            }
        }

        d->ptd->stats.stats.err_oom++;
        cproxy_close_conn(uc);
    }

 done:
    item_remove(c->item);
    c->item = NULL;
}

