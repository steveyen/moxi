/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <assert.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"
#include "log.h"

static void cproxy_sasl_plain_auth(conn *c, char *req_bytes);

static proxy *cproxy_find_proxy_by_plain_auth(proxy_main *m,
                                              const char *usr,
                                              int usrlen,
                                              const char *pwd,
                                              int pwdlen);

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
            if (c->binary_header.request.opcode == PROTOCOL_BINARY_CMD_SASL_LIST_MECHS) {
                // TODO: One day handle more than just PLAIN sasl auth.
                //
                write_bin_response(c, "PLAIN", 0, 0, strlen("PLAIN"));
                return;
            }

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

    if (header->request.opcode == PROTOCOL_BINARY_CMD_SASL_AUTH) {
        item *it = c->item;
        assert(it);

        cproxy_sasl_plain_auth(c, (char *) ITEM_data(it));
        return;
    }

    if (header->request.opcode == PROTOCOL_BINARY_CMD_SASL_STEP) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        return;
    }

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

        for (size_t ii = 0; ii < sizeof(protocol_binary_request_header); ++ii) {
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

static void cproxy_sasl_plain_auth(conn *c, char *req_bytes) {
    proxy_td *ptd = c->extra;
    assert(ptd != NULL);
    assert(ptd->proxy != NULL);
    assert(ptd->proxy->main != NULL);

    // Authenticate an upstream connection.
    //
    protocol_binary_request_header *req =
        (protocol_binary_request_header *) req_bytes;

    char *key     = ((char *) req) + sizeof(*req) + req->request.extlen;
    int   keylen  = ntohs(req->request.keylen);
    int   bodylen = ntohl(req->request.bodylen);

    // The key is the sasl mech.
    //
    if (keylen != 5 ||
        memcmp(key, "PLAIN", 5) != 0) { // 5 == strlen("PLAIN").
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        return;
    }

    char     *clientin    = key + keylen;
    unsigned  clientinlen = bodylen - keylen - req->request.extlen;

    // The clientin string looks like "[authzid]\0username\0password".
    //
    while (clientinlen > 0 && clientin[0] != '\0') {
        // Skip authzid.
        //
        clientin++;
        clientinlen--;
    }

    if (clientinlen > 2 && clientinlen < 128 && clientin[0] == '\0') {
        const char *username = clientin + 1;
        char        password[256];

        int uslen = strlen(username);
        int pwlen = clientinlen - 2 - uslen;

        // Note that we don't allow auth'ing with an empty password.
        // So, once you're sasl auth'ed, you can't sasl auth again
        // back to some empty password proxy/bucket, such as to the
        // default bucket.
        //
        if (pwlen > 0 && pwlen < sizeof(password)) {
            memcpy(password, clientin + 2 + uslen, pwlen);
            password[pwlen] = '\0';

            proxy *p = cproxy_find_proxy_by_plain_auth(ptd->proxy->main,
                                                       username, uslen,
                                                       password, pwlen);
            if (p != NULL) {
                proxy_td *ptd_target = cproxy_find_thread_data(p, pthread_self());
                if (ptd_target != NULL) {
                    c->extra = ptd_target;

                    write_bin_response(c, "Authenticated", 0, 0,
                                       strlen("Authenticated"));

                    if (settings.verbose > 2) {
                        moxi_log_write("<%d sasl authenticated for %s\n",
                                       c->sfd, username);
                    }

                    return;
                } else {
                    if (settings.verbose > 2) {
                        moxi_log_write("<%d sasl auth failed on ptd for %s\n",
                                       c->sfd, username);
                    }
                }
            } else {
                if (settings.verbose > 2) {
                    moxi_log_write("<%d sasl auth failed for %s (%d)\n",
                                   c->sfd, username, pwlen);
                }
            }
        } else {
            if (settings.verbose > 2) {
                moxi_log_write("<%d sasl auth failed for %s with empty password\n",
                               c->sfd, username);
            }
        }
    } else {
        if (settings.verbose > 2) {
            moxi_log_write("<%d sasl auth failed with malformed PLAIN data\n",
                           c->sfd);
        }
    }

    // TODO: If authentication failed, we should consider
    // reassigning the connection to the NULL_BUCKET.
    //
    write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
}

// Find an appropriate proxy struct or NULL.
//
static proxy *cproxy_find_proxy_by_plain_auth(proxy_main *m,
                                              const char *usr,
                                              int usrlen,
                                              const char *pwd,
                                              int pwdlen) {
    proxy *found = NULL;

    pthread_mutex_lock(&m->proxy_main_lock);

    for (proxy *p = m->proxy_head; p != NULL && found == NULL; p = p->next) {
        pthread_mutex_lock(&p->proxy_lock);
        if (strncmp(p->behavior_pool.base.usr, usr, usrlen) == 0 &&
            strncmp(p->behavior_pool.base.pwd, pwd, pwdlen) == 0) {
            found = p;
        }
        pthread_mutex_unlock(&p->proxy_lock);
    }

    pthread_mutex_unlock(&m->proxy_main_lock);

    return found;
}
