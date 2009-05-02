/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <math.h>
#include <glib.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"

#ifndef HAVE_HTONLL
extern uint64_t ntohll(uint64_t);
extern uint64_t htonll(uint64_t);
#endif

// Internal declarations.
//
#define NOT_CAS -1

#define CMD_TOKEN  0
#define KEY_TOKEN  1
#define MAX_TOKENS 9

void cproxy_a2b_item_response(item *it, conn *uc);

// A2B means ascii-to-binary (or, ascii upstream and binary downstream).
//
struct A2BSpec {
    char *line;

    protocol_binary_command cmd;
    protocol_binary_command cmdq;

    int     size;         // Number of bytes in request header.
    token_t tokens[MAX_TOKENS];
    int     ntokens;
    bool    noreply_allowed;
    int     num_optional; // Number of optional arguments in cmd.
    bool    broadcast;    // True if cmd does scatter/gather.
};

// The arguments are carefully named with unique first characters.
//
struct A2BSpec a2b_specs[] = {
    { .line = "set <key> <flags> <exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_SET,
      .cmdq = PROTOCOL_BINARY_CMD_SETQ,
      .size = sizeof(protocol_binary_request_set)
    },
    { .line = "add <key> <flags> <exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_ADD,
      .cmdq = PROTOCOL_BINARY_CMD_ADDQ,
      .size = sizeof(protocol_binary_request_add)
    },
    { .line = "replace <key> <flags> <exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_REPLACE,
      .cmdq = PROTOCOL_BINARY_CMD_REPLACEQ,
      .size = sizeof(protocol_binary_request_replace)
    },
    { .line = "append <key> <skip_flags> <skip_exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_APPEND,
      .cmdq = PROTOCOL_BINARY_CMD_APPENDQ,
      .size = sizeof(protocol_binary_request_append)
    },
    { .line = "prepend <key> <skip_flags> <skip_exptime> <bytes> [noreply]" ,
      .cmd  = PROTOCOL_BINARY_CMD_PREPEND,
      .cmdq = PROTOCOL_BINARY_CMD_PREPENDQ,
      .size = sizeof(protocol_binary_request_prepend)
    },
    { .line = "cas <key> <flags> <exptime> <bytes> <cas> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_SET,
      .cmdq = PROTOCOL_BINARY_CMD_SETQ,
      .size = sizeof(protocol_binary_request_set)
    },
    { .line = "delete <key> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_DELETE,
      .cmdq = PROTOCOL_BINARY_CMD_DELETEQ,
      .size = sizeof(protocol_binary_request_delete)
    },
    { .line = "incr <key> <value> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_INCREMENT,
      .cmdq = PROTOCOL_BINARY_CMD_INCREMENTQ,
      .size = sizeof(protocol_binary_request_incr)
    },
    { .line = "decr <key> <value> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_DECREMENT,
      .cmdq = PROTOCOL_BINARY_CMD_DECREMENTQ,
      .size = sizeof(protocol_binary_request_decr)
    },
    { .line = "flush_all [xpiration] [noreply]", // TODO: noreply tricky here.
      .cmd  = PROTOCOL_BINARY_CMD_FLUSH,
      .cmdq = PROTOCOL_BINARY_CMD_FLUSHQ,
      .size = sizeof(protocol_binary_request_flush),
      .broadcast = true
    },
    { .line = "get <key>*",
      .cmd  = PROTOCOL_BINARY_CMD_GETK,
      .cmdq = PROTOCOL_BINARY_CMD_GETKQ,
      .size = sizeof(protocol_binary_request_getk)
    },
    { .line = "gets <key>*",
      .cmd  = PROTOCOL_BINARY_CMD_GETK,
      .cmdq = PROTOCOL_BINARY_CMD_GETKQ,
      .size = sizeof(protocol_binary_request_getk)
    },
    { .line = "stats [args]*",
      .cmd  = PROTOCOL_BINARY_CMD_STAT,
      .cmdq = PROTOCOL_BINARY_CMD_NOOP,
      .size = sizeof(protocol_binary_request_stats),
      .broadcast = true
    },
    { 0 } // NULL sentinel.
};

GHashTable *a2b_spec_map = NULL; // Key: command string, value: A2BSpec.
int         a2b_size_max = 0;    // Max header + extra frame bytes size.

int a2b_fill_request(token_t *cmd_tokens,
                     int      cmd_ntokens,
                     item    *it, // Might be NULL.
                     protocol_binary_request_header *header,
                     uint8_t **key);

bool a2b_fill_request_token(struct A2BSpec *spec,
                            int      cur_token,
                            token_t *cmd_tokens,
                            int      cmd_ntokens,
                            protocol_binary_request_header *header,
                            uint8_t **key);

void cproxy_init_a2b() {
    if (a2b_spec_map == NULL) {
        a2b_spec_map = g_hash_table_new(skey_hash, skey_equal);
        if (a2b_spec_map == NULL)
            return; // TODO: Better oom error handling.

        // Run through the a2b_specs to populate the a2b_spec_map.
        //
        int i = 0;
        while (true) {
            struct A2BSpec *spec = &a2b_specs[i];
            if (spec->line == NULL)
                break;

            spec->ntokens = scan_tokens(spec->line,
                                        spec->tokens,
                                        MAX_TOKENS);
            assert(spec->ntokens > 2);

            int noreply_index = spec->ntokens - 2;
            if (spec->tokens[noreply_index].value &&
                strcmp(spec->tokens[noreply_index].value,
                       "[noreply]") == 0)
                spec->noreply_allowed = true;
            else
                spec->noreply_allowed = false;

            spec->num_optional = 0;
            for (int j = 0; j < spec->ntokens; j++) {
                if (spec->tokens[j].value &&
                    spec->tokens[j].value[0] == '[')
                    spec->num_optional++;
            }

            if (a2b_size_max < spec->size)
                a2b_size_max = spec->size;

            g_hash_table_insert(a2b_spec_map,
                                spec->tokens[CMD_TOKEN].value,
                                spec);

            i = i + 1;
        }
    }
}

int a2b_fill_request(token_t *cmd_tokens,
                     int      cmd_ntokens,
                     item    *it, // Might be NULL.
                     protocol_binary_request_header *header,
                     uint8_t **key) {
    assert(header);
    assert(cmd_tokens);
    assert(cmd_ntokens > 1);
    assert(cmd_tokens[CMD_TOKEN].value);
    assert(cmd_tokens[CMD_TOKEN].length > 0);
    assert(key);
    assert(a2b_spec_map);

    struct A2BSpec *spec = g_hash_table_lookup(a2b_spec_map,
                                               cmd_tokens[CMD_TOKEN].value);
    if (spec != NULL) {
        if (cmd_ntokens >= (spec->ntokens - spec->num_optional) &&
            cmd_ntokens <= (spec->ntokens)) {
            header->request.magic  = PROTOCOL_BINARY_REQ;
            header->request.opcode = spec->cmd;

            // Start at 1 to skip the CMD_TOKEN.
            //
            for (int i = 1; i < cmd_ntokens - 1; i++) {
                if (a2b_fill_request_token(spec, i,
                                           cmd_tokens, cmd_ntokens,
                                           header, key) == false)
                    return 0;
            }

            return spec->size; // Success.
        }
    }

    return 0;
}


bool a2b_fill_request_token(struct A2BSpec *spec,
                            int      cur_token,
                            token_t *cmd_tokens,
                            int      cmd_ntokens,
                            protocol_binary_request_header *header,
                            uint8_t **key) {
    assert(header);
    assert(spec);
    assert(spec->tokens);
    assert(spec->ntokens > 1);
    assert(spec->tokens[cur_token].value);
    assert(cur_token > 0);
    assert(cur_token < cmd_ntokens);
    assert(cur_token < spec->ntokens);
    assert(key);

    uint64_t delta;

    char t = spec->tokens[cur_token].value[1];
    switch (t) {
    case 'k': // key
        assert(key);
        *key = (uint8_t *) cmd_tokens[cur_token].value;
        header->request.keylen =
            htons((uint16_t) cmd_tokens[cur_token].length);
        break;
    case 'v': // value (for incr/decr)
        delta = 0;
        if (safe_strtoull(cmd_tokens[cur_token].value, &delta)) {
            protocol_binary_request_incr *req =
                (protocol_binary_request_incr *) header;
            header->request.extlen   = 20;
            header->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
            req->message.body.delta = htonll(delta);
            req->message.body.initial = 0;
            req->message.body.expiration = 0;
        } else {
            // TODO: Send back better error.
            return false;
        }
        break;
    case 'n': // noreply
        if (strncmp("noreply",
                    cmd_tokens[cur_token].value,
                    cmd_tokens[cur_token].length) == 0) {
            header->request.opcode = spec->cmdq;
        } else {
            // TODO: Send back better error.
            return false;
        }
        break;
    case 'x': // xpiration (for flush_all)
        // TODO.
        return false;
        break;
    case 'a': // args (for stats)
        // TODO.
        return false;
        break;

    // The above are handled by looking at the item struct.
    //
    // case 'f': // FALLTHRU, flags
    // case 'e': // FALLTHRU, exptime
    // case 'b': // FALLTHRU, bytes
    // case 's': // FALLTHRU, skip_xxx
    // case 'c': // FALLTHRU, cas
    //
    default:
        break;
    }

    return true;
}

void cproxy_process_a2b_downstream(conn *c, char *line) {
    assert(c != NULL);
    assert(c->next == NULL);
    assert(c->extra != NULL);
    assert(c->cmd == -1);
    assert(c->item == NULL);
    assert(line != NULL);
    assert(line == c->rcurr);
    assert(IS_BINARY(c->protocol));
    assert(IS_PROXY(c->protocol));

    if (settings.verbose > 1)
        fprintf(stderr, "<%d cproxy_process_a2b_downstream %s\n",
                c->sfd, line);

    downstream *d = c->extra;

    assert(d != NULL);
    assert(d->ptd != NULL);

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
        assert(d->merger != NULL);

        conn *uc = d->upstream_conn;
        if (uc != NULL) {
            assert(uc->next == NULL);

            if (protocol_stats_merge(d->merger, line) == false) {
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

                d->ptd->stats.tot_oom++;
                cproxy_close_conn(uc);
            }
        }
    }
}

/* We get here after reading the value in a VALUE reply.
 * The item is ready in c->item.
 */
void cproxy_process_a2b_downstream_nread(conn *c) {
    assert(c != NULL);

    if (settings.verbose > 1)
        fprintf(stderr,
                "<%d cproxy_process_a2b_downstream_nread %d %d\n",
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

    if (d->multiget != NULL) {
        char key_buf[KEY_MAX_LENGTH + 10];

        memcpy(key_buf, ITEM_key(it), it->nkey);
        key_buf[it->nkey] = '\0';

        multiget_entry *entry =
            g_hash_table_lookup(d->multiget, key_buf);

        while (entry != NULL) {
            // The upstream might be NULL if it was closed mid-request.
            //
            if (entry->upstream_conn != NULL)
                cproxy_a2b_item_response(it, entry->upstream_conn);

            entry = entry->next;
        }
    } else {
        conn *uc = d->upstream_conn;
        while (uc != NULL) {
            cproxy_a2b_item_response(it, uc);
            uc = uc->next;
        }
    }

    item_remove(it);
}

/* Do the actual work of forwarding the command from an
 * upstream ascii conn to its assigned binary downstream.
 */
bool cproxy_forward_a2b_downstream(downstream *d) {
    assert(d != NULL);

    conn *uc = d->upstream_conn;

    assert(uc != NULL);
    assert(uc->state == conn_pause);
    assert(uc->cmd_ascii != NULL);
    assert(uc->thread != NULL);
    assert(uc->thread->base != NULL);
    assert(IS_ASCII(uc->protocol));
    assert(IS_PROXY(uc->protocol));

    if (cproxy_connect_downstream(d, uc->thread) > 0) {
        assert(d->downstream_conns != NULL);

        if (uc->cmd == -1) {
            return cproxy_forward_a2b_simple_downstream(d, uc->cmd_ascii, uc);
        } else {
            return cproxy_forward_a2b_item_downstream(d, uc->cmd, uc->item, uc);
        }
    }

    return false;
}

/* Forward a simple one-liner ascii command to a binary downstream.
 * For example, get, incr/decr, delete, etc.
 * The response, though, might be a simple line or
 * multiple VALUE+END lines.
 */
bool cproxy_forward_a2b_simple_downstream(downstream *d,
                                          char *command, conn *uc) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->item == NULL);
    assert(d->multiget == NULL);
    assert(d->merger == NULL);

    if (strncmp(command, "get", 3) == 0)
        return cproxy_forward_a2b_multiget_downstream(d, uc);

    assert(uc->next == NULL);

    if (strncmp(command, "flush_all", 9) == 0)
        return cproxy_broadcast_a2b_downstream(d, command, uc,
                                               "OK\r\n");

    if (strncmp(command, "stats", 5) == 0) {
        if (strncmp(command + 5, " reset", 6) == 0)
            return cproxy_broadcast_a2b_downstream(d, command, uc,
                                                   "RESET\r\n");

        if (cproxy_broadcast_a2b_downstream(d, command, uc,
                                            "END\r\n")) {
            d->merger = g_hash_table_new(protocol_stats_key_hash,
                                         protocol_stats_key_equal);
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
    conn *c = cproxy_find_downstream_conn(d, key, key_len);
    if (c != NULL &&
        cproxy_prep_conn_for_write(c)) {
        assert(c->state == conn_pause);

        item *it = item_alloc("b", 1, 0, 0, a2b_size_max);
        if (it != NULL) {
            protocol_binary_request_header *header =
                (protocol_binary_request_header *) ITEM_data(it);

            uint8_t *key_out = NULL;

            memset(header, 0, a2b_size_max);

            int size = a2b_fill_request(tokens, ntokens, NULL,
                                        header, &key_out);
            if (size > 0) {
                assert(key     == (char *) key_out);
                assert(key_len == header->request.keylen);
                assert(header->request.bodylen == 0);

                header->request.bodylen =
                    htonl(header->request.keylen +
                          header->request.extlen);

                if (add_conn_item(c, it)) {
                    add_iov(c, header, size);

                    if (settings.verbose > 1)
                        fprintf(stderr, "forwarding a2b to %d, noreply %d\n",
                                c->sfd, uc->noreply);

                    if (update_event(c, EV_WRITE | EV_PERSIST)) {
                        d->downstream_used_start = 1; // TODO: Need timeout?
                        d->downstream_used       = 1;

                        if (cproxy_dettach_if_noreply(d, uc) == false) {
                            cproxy_start_downstream_timeout(d);
                        } else {
                            c->write_and_go = conn_pause;
                        }

                        return true;
                    }
                }
            }

            item_remove(it);
        }

        if (settings.verbose > 1)
            fprintf(stderr, "Couldn't update cproxy write event\n");

        d->ptd->stats.tot_oom++;
        cproxy_close_conn(c);
    }

    return false;
}

bool cproxy_forward_a2b_multiget_downstream(downstream *d, conn *uc) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->downstream_conns != NULL);
    assert(d->multiget == NULL);
    assert(uc != NULL);
    assert(uc->noreply == false);

    int nwrite = 0;
    int nconns = memcached_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        if (d->downstream_conns[i] != NULL) {
            cproxy_prep_conn_for_write(d->downstream_conns[i]);
            assert(d->downstream_conns[i]->state == conn_pause);
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

    int   uc_num = 0;
    conn *uc_cur = uc;

    while (uc_cur != NULL) {
        assert(uc_cur->cmd == -1);
        assert(uc_cur->item == NULL);
        assert(uc_cur->state == conn_pause);
        assert(IS_ASCII(uc_cur->protocol));
        assert(IS_PROXY(uc_cur->protocol));

        char *command = uc_cur->cmd_ascii;
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

            // This key_len check helps skips consecutive spaces.
            //
            if (key_len > 0) {
                // See if we've already requested this key via
                // the multiget hash table, in order to
                // de-deplicate repeated keys.
                //
                bool first_request = true;

                if (d->multiget != NULL) {
                    multiget_entry *entry = calloc(1, sizeof(multiget_entry));
                    if (entry != NULL) {
                        entry->upstream_conn = uc_cur;
                        entry->next = g_hash_table_lookup(d->multiget, key);

                        g_hash_table_insert(d->multiget, key, entry);

                        if (entry->next != NULL)
                            first_request = false;
                    } else {
                        // TODO: Handle out of multiget entry memory.
                    }
                }

                if (first_request) {
                    conn *c = cproxy_find_downstream_conn(d, key, key_len);
                    if (c != NULL) {
                        assert(c->item == NULL);
                        assert(c->state == conn_pause);
                        assert(IS_BINARY(c->protocol));
                        assert(IS_PROXY(c->protocol));
                        assert(c->ilist != NULL);
                        assert(c->isize > 0);

                        c->icurr = c->ilist;
                        c->ileft = 0;

                        if (uc_num <= 0 &&
                            c->msgused <= 1 &&
                            c->msgbytes <= 0) {
                            add_iov(c, command, cmd_len);

                            // TODO: Handle out of iov memory.
                        }

                        // Write the key, including the preceding space.
                        //
                        add_iov(c, key - 1, key_len + 1);
                    } else {
                        // TODO: Handle when downstream conn is down.
                    }
                } else {
                    if (settings.verbose > 1) {
                        char buf[KEY_MAX_LENGTH + 10];
                        memcpy(buf, key, key_len);
                        buf[key_len] = '\0';

                        fprintf(stderr, "%d cproxy multiget squash: %s\n",
                                uc_cur->sfd, buf);
                    }
                }
            }

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

                d->ptd->stats.tot_oom++;
                cproxy_close_conn(c);
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "forward multiget nwrite %d out of %d\n",
                nwrite, nconns);

    d->downstream_used_start = nwrite; // TODO: Need timeout?
    d->downstream_used       = nwrite;

    if (cproxy_dettach_if_noreply(d, uc) == false) {
        d->upstream_suffix = "END\r\n";

        cproxy_start_downstream_timeout(d);
    }

    return nwrite > 0;
}

/* Used for broadcast commands, like flush_all or stats.
 */
bool cproxy_broadcast_a2b_downstream(downstream *d,
                                     char *command,
                                     conn *uc,
                                     char *suffix) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->next == NULL);
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

                d->ptd->stats.tot_oom++;
                cproxy_close_conn(c);
            }
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "forward multiget nwrite %d out of %d\n",
                nwrite, nconns);

    d->downstream_used_start = nwrite; // TODO: Need timeout?
    d->downstream_used       = nwrite;

    if (cproxy_dettach_if_noreply(d, uc) == false) {
        d->upstream_suffix = suffix;

        cproxy_start_downstream_timeout(d);
    }

    return nwrite > 0;
}

/* Forward an upstream command that came with item data,
 * like set/add/replace/etc.
 */
bool cproxy_forward_a2b_item_downstream(downstream *d, short cmd,
                                        item *it, conn *uc) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->downstream_conns != NULL);
    assert(it != NULL);
    assert(uc != NULL);
    assert(uc->next == NULL);

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

                    if (cproxy_dettach_if_noreply(d, uc) == false) {
                        cproxy_start_downstream_timeout(d);
                    } else {
                        c->write_and_go = conn_pause;
                    }

                    return true;
                }

                d->ptd->stats.tot_oom++;
                cproxy_close_conn(c);
            }
        }

        if (settings.verbose > 1)
            fprintf(stderr, "Proxy item write out of memory");

        // TODO: Need better out-of-memory behavior.
    }

    return false;
}

void cproxy_a2b_item_response(item *it, conn *uc) {
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
        if (cas == NOT_CAS) {
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

