/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <assert.h>
#include <math.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"
#include "log.h"

// Internal declarations.
//
static protocol_binary_request_noop req_noop = {
    .bytes = {0}
};

#define CMD_TOKEN  0
#define KEY_TOKEN  1
#define MAX_TOKENS 9

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

// The a2b_specs are immutable after init.
//
// The arguments are carefully named with unique first characters.
//
struct A2BSpec a2b_specs[] = {
    { .line = "set <key> <flags> <exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_SET,
      .cmdq = PROTOCOL_BINARY_CMD_SETQ,
      // The size should be...
      //   sizeof(protocol_binary_request_header) [24 bytes] +
      //   sizeof(protocol_binary_request_set.message.body) [8 bytes]
      .size = sizeof(protocol_binary_request_header) + 8
    },
    { .line = "add <key> <flags> <exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_ADD,
      .cmdq = PROTOCOL_BINARY_CMD_ADDQ,
      // The size should be...
      //   sizeof(protocol_binary_request_header) [24 bytes] +
      //   sizeof(protocol_binary_request_add.message.body) [8 bytes]
      .size = sizeof(protocol_binary_request_header) + 8
    },
    { .line = "replace <key> <flags> <exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_REPLACE,
      .cmdq = PROTOCOL_BINARY_CMD_REPLACEQ,
      // The size should be...
      //   sizeof(protocol_binary_request_header) [24 bytes] +
      //   sizeof(protocol_binary_request_replace.message.body) [8 bytes]
      .size = sizeof(protocol_binary_request_header) + 8
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
      // The size should be...
      //   sizeof(protocol_binary_request_header) [24 bytes] +
      //   sizeof(protocol_binary_request_set.message.body) [8 bytes]
      .size = sizeof(protocol_binary_request_header) + 8
    },
    { .line = "delete <key> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_DELETE,
      .cmdq = PROTOCOL_BINARY_CMD_DELETEQ,
      .size = sizeof(protocol_binary_request_delete)
    },
    { .line = "incr <key> <value> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_INCREMENT,
      .cmdq = PROTOCOL_BINARY_CMD_INCREMENTQ,
      // The size should be...
      //   sizeof(protocol_binary_request_header) [24 bytes] +
      //   sizeof(protocol_binary_request_incr.message.body) [20 bytes]
      .size = sizeof(protocol_binary_request_header) + 20
    },
    { .line = "decr <key> <value> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_DECREMENT,
      .cmdq = PROTOCOL_BINARY_CMD_DECREMENTQ,
      // The size should be...
      //   sizeof(protocol_binary_request_header) [24 bytes] +
      //   sizeof(protocol_binary_request_decr.message.body) [20 bytes]
      .size = sizeof(protocol_binary_request_header) + 20
    },
    { .line = "flush_all [xpiration] [noreply]", // TODO: noreply tricky here.
      .cmd  = PROTOCOL_BINARY_CMD_FLUSH,
      .cmdq = PROTOCOL_BINARY_CMD_FLUSHQ,
      // The size should be...
      //   sizeof(protocol_binary_request_header) [24 bytes] +
      //   sizeof(protocol_binary_request_flush.message.body) [4 bytes]
      .size = sizeof(protocol_binary_request_header) + 4,
      .broadcast = true
    },
    { .line = "get <key>*", // Multi-key GET/GETS
      .cmd  = PROTOCOL_BINARY_CMD_GETKQ,
      .cmdq = -1,
      .size = sizeof(protocol_binary_request_header)
    },
    { .line = "get <key>", // Single-key GET/GETS.
      .cmd  = PROTOCOL_BINARY_CMD_GETK,
      .cmdq = -1,
      .size = sizeof(protocol_binary_request_header)
    },
    { .line = "stats [args]*",
      .cmd  = PROTOCOL_BINARY_CMD_STAT,
      .cmdq = -1,
      .size = sizeof(protocol_binary_request_stats),
      .broadcast = true
    },
    { .line = 0 } // NULL sentinel.
};

// These are immutable after init.
//
struct A2BSpec *a2b_spec_map[0x80] = {0}; // Lookup table by A2BSpec->cmd.
int             a2b_size_max = 0;         // Max header + extra frame bytes.

int a2b_fill_request(short    cmd,
                     token_t *cmd_tokens,
                     int      cmd_ntokens,
                     bool     noreply,
                     protocol_binary_request_header *header,
                     uint8_t **out_key,
                     uint16_t *out_keylen,
                     uint8_t  *out_extlen);

bool a2b_fill_request_token(struct A2BSpec *spec,
                            int      cur_token,
                            token_t *cmd_tokens,
                            int      cmd_ntokens,
                            protocol_binary_request_header *header,
                            uint8_t **out_key,
                            uint16_t *out_keylen,
                            uint8_t  *out_extlen);

void a2b_process_downstream_response(conn *c);

int a2b_multiget_start(conn *c, char *cmd, int cmd_len);
int a2b_multiget_skey(conn *c, char *skey, int skey_len, int vbucket, int key_index);
int a2b_multiget_end(conn *c);

void a2b_set_opaque(conn *c, protocol_binary_request_header *header, bool noreply);

bool a2b_not_my_vbucket(conn *uc, conn *c,
                        protocol_binary_response_header *header);

void cproxy_init_a2b() {
    memset(&req_noop, 0, sizeof(req_noop));

    req_noop.message.header.request.magic    = PROTOCOL_BINARY_REQ;
    req_noop.message.header.request.opcode   = PROTOCOL_BINARY_CMD_NOOP;
    req_noop.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;

    // Run through the a2b_specs to populate the a2b_spec_map.
    //
    int i = 0;
    while (true) {
        struct A2BSpec *spec = &a2b_specs[i];
        if (spec->line == NULL) {
            break;
        }

        spec->ntokens = scan_tokens(spec->line,
                                    spec->tokens,
                                    MAX_TOKENS, NULL);
        assert(spec->ntokens > 2);

        int noreply_index = spec->ntokens - 2;
        if (spec->tokens[noreply_index].value &&
            strcmp(spec->tokens[noreply_index].value,
                   "[noreply]") == 0) {
            spec->noreply_allowed = true;
        } else {
            spec->noreply_allowed = false;
        }

        spec->num_optional = 0;
        for (int j = 0; j < spec->ntokens; j++) {
            if (spec->tokens[j].value &&
                spec->tokens[j].value[0] == '[') {
                spec->num_optional++;
            }
        }

        if (a2b_size_max < spec->size) {
            a2b_size_max = spec->size;
        }

        assert(spec->cmd < (sizeof(a2b_spec_map) /
                            sizeof(struct A2BSpec *)));

        a2b_spec_map[spec->cmd] = spec;

        i = i + 1;
    }
}

int a2b_fill_request(short    cmd,
                     token_t *cmd_tokens,
                     int      cmd_ntokens,
                     bool     noreply,
                     protocol_binary_request_header *header,
                     uint8_t **out_key,
                     uint16_t *out_keylen,
                     uint8_t  *out_extlen) {
    assert(header);
    assert(cmd_tokens);
    assert(cmd_ntokens > 1);
    assert(cmd_tokens[CMD_TOKEN].value);
    assert(cmd_tokens[CMD_TOKEN].length > 0);
    assert(out_key);
    assert(out_keylen);
    assert(out_extlen);

    struct A2BSpec *spec = a2b_spec_map[cmd];
    if (spec != NULL) {
        if (cmd_ntokens >= (spec->ntokens - spec->num_optional) &&
            cmd_ntokens <= (spec->ntokens)) {
            header->request.magic = PROTOCOL_BINARY_REQ;

            if (noreply) {
                assert(spec->cmd != (protocol_binary_command) -1);

                header->request.opcode = spec->cmdq;
                header->request.opaque = htonl(OPAQUE_IGNORE_REPLY);

                if (settings.verbose > 2) {
                    moxi_log_write("a2b_fill_request OPAQUE_IGNORE_REPLY, cmdq: %x\n",
                            spec->cmdq);
                }
            } else {
                header->request.opcode = spec->cmd;
            }

            // Start at 1 to skip the CMD_TOKEN.
            //
            for (int i = 1; i < cmd_ntokens - 1; i++) {
                if (a2b_fill_request_token(spec, i,
                                           cmd_tokens, cmd_ntokens,
                                           header,
                                           out_key,
                                           out_keylen,
                                           out_extlen) == false) {
                    return 0;
                }
            }

            return spec->size; // Success.
        }
    } else {
        if (settings.verbose > 2) {
            moxi_log_write("a2b_fill_request unknown cmd: %x\n", cmd);
        }
    }

    return 0;
}

bool a2b_fill_request_token(struct A2BSpec *spec,
                            int      cur_token,
                            token_t *cmd_tokens,
                            int      cmd_ntokens,
                            protocol_binary_request_header *header,
                            uint8_t **out_key,
                            uint16_t *out_keylen,
                            uint8_t  *out_extlen) {
    (void)cmd_ntokens;
    assert(header);
    assert(spec);
    assert(spec->tokens);
    assert(spec->ntokens > 1);
    assert(spec->tokens[cur_token].value);
    assert(cur_token > 0);
    assert(cur_token < cmd_ntokens);
    assert(cur_token < spec->ntokens);

    uint64_t delta;

    if (settings.verbose > 2) {
        moxi_log_write("a2b_fill_request_token %s\n",
                spec->tokens[cur_token].value);
    }

    char t = spec->tokens[cur_token].value[1];
    switch (t) {
    case 'k': // key
        assert(out_key);
        assert(out_keylen);
        *out_key    = (uint8_t *) cmd_tokens[cur_token].value;
        *out_keylen = (uint16_t)  cmd_tokens[cur_token].length;
        header->request.keylen =
            htons((uint16_t) cmd_tokens[cur_token].length);
        break;

    case 'v': // value (for incr/decr)
        delta = 0;
        if (safe_strtoull(cmd_tokens[cur_token].value, &delta)) {
            assert(out_extlen);

            header->request.extlen   = *out_extlen = 20;
            header->request.datatype = PROTOCOL_BINARY_RAW_BYTES;

            protocol_binary_request_incr *req =
                (protocol_binary_request_incr *) header;

            req->message.body.delta = htonll(delta);
            req->message.body.initial = 0;
            req->message.body.expiration = 0xffffffff;
        } else {
            // TODO: Send back better error.
            return false;
        }
        break;

    case 'x': { // xpiration (for flush_all)
        int32_t exptime_int = 0;
        time_t  exptime = 0;

        if (safe_strtol(cmd_tokens[cur_token].value, &exptime_int)) {
            /* Ubuntu 8.04 breaks when I pass exptime to safe_strtol */
            exptime = exptime_int;

            header->request.extlen   = *out_extlen = 4;
            header->request.datatype = PROTOCOL_BINARY_RAW_BYTES;

            protocol_binary_request_flush *req =
                (protocol_binary_request_flush *) header;

            req->message.body.expiration = htonl(exptime);
        }
        break;
    }

    case 'a': // args (for stats)
        assert(out_key);
        assert(out_keylen);
        *out_key    = (uint8_t *) cmd_tokens[cur_token].value;
        *out_keylen = (uint16_t)  cmd_tokens[cur_token].length;
        header->request.keylen =
            htons((uint16_t) cmd_tokens[cur_token].length);
        break;

    // The noreply was handled in a2b_fill_request().
    //
    // case 'n': // noreply
    //
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

/* Called when we receive a binary response header from
 * a downstream server, via try_read_command()/drive_machine().
 */
void cproxy_process_a2b_downstream(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);
    assert(c->next == NULL);
    assert(c->item == NULL);
    assert(IS_BINARY(c->protocol));
    assert(IS_PROXY(c->protocol));

    // Snapshot rcurr, because the caller, try_read_command(), changes it.
    //
    c->cmd_start = c->rcurr;

    protocol_binary_response_header *header =
        (protocol_binary_response_header *) &c->binary_header;

    header->response.status = (uint16_t) ntohs(header->response.status);

    assert(header->response.magic == (uint8_t) PROTOCOL_BINARY_RES);
    assert(header->response.opcode == c->cmd);

    process_bin_noreply(c); // Map quiet c->cmd values into non-quiet.

    int      extlen  = header->response.extlen;
    int      keylen  = header->response.keylen;
    uint32_t bodylen = header->response.bodylen;

    assert(bodylen >= (uint32_t) keylen + extlen);

    // Our approach is to read everything we can before
    // getting into big switch/case statements for the
    // actual processing.
    //
    // If status is non-zero (an err code), then bodylen should be small.
    // If status is 0, then bodylen might be for a huge item during
    // a GET family of response.
    //
    // If bodylen > extlen + keylen, then we should nread
    // the ext+key and set ourselves up for a later item nread.
    //
    // We overload the meaning of the conn substates...
    // - bin_reading_get_key means do nread for ext and key data.
    // - bin_read_set_value means do nread for item data.
    //
    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_process_a2b_downstream %x %d %d %u\n",
                c->sfd, c->cmd, extlen, keylen, bodylen);
    }

    if (keylen > 0 || extlen > 0) {
        // One reason we reach here is during a
        // GET/GETQ/GETK/GETKQ hit response, because extlen
        // will be > 0 for the flags.
        //
        // Also, we reach here during a GETK miss response, since
        // keylen will be > 0.  Oddly, a GETK miss response will have
        // a non-zero status of PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
        // but won't have any extra error message string.
        //
        // Also, we reach here during a STAT response, with
        // keylen > 0, extlen == 0, and bodylen == keylen.
        //
        assert(c->cmd == PROTOCOL_BINARY_CMD_GET ||
               c->cmd == PROTOCOL_BINARY_CMD_GETK ||
               c->cmd == PROTOCOL_BINARY_CMD_STAT);

        bin_read_key(c, bin_reading_get_key, extlen);
    } else {
        assert(keylen == 0 && extlen == 0);

        if (bodylen > 0) {
            // We reach here on error response, version response,
            // or incr/decr responses, which all have only (relatively
            // small) body bytes, and with no ext bytes and no key bytes.
            //
            // For example, error responses will have 0 keylen,
            // 0 extlen, with an error message string for the body.
            //
            // We'll just reuse the key-reading code path, rather
            // than allocating an item.
            //
            assert(header->response.status != 0 ||
                   c->cmd == PROTOCOL_BINARY_CMD_VERSION ||
                   c->cmd == PROTOCOL_BINARY_CMD_INCREMENT ||
                   c->cmd == PROTOCOL_BINARY_CMD_DECREMENT);

            bin_read_key(c, bin_reading_get_key, bodylen);
        } else {
            assert(keylen == 0 && extlen == 0 && bodylen == 0);

            // We have the entire response in the header,
            // such as due to a general success response,
            // including a no-op response.
            //
            a2b_process_downstream_response(c);
        }
    }
}

/* We reach here after nread'ing a ext+key or item.
 */
void cproxy_process_a2b_downstream_nread(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);
    assert(c->next == NULL);
    assert(c->cmd_start != NULL);
    assert(IS_BINARY(c->protocol));
    assert(IS_PROXY(c->protocol));

    downstream *d = c->extra;
    assert(d);

    protocol_binary_response_header *header =
        (protocol_binary_response_header *) &c->binary_header;

    int      extlen  = header->response.extlen;
    int      keylen  = header->response.keylen;
    uint32_t bodylen = header->response.bodylen;

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_process_a2b_downstream_nread %d %d, cmd %x %d %d\n",
                c->sfd, c->ileft, c->isize, c->cmd, c->substate,
                header->response.status);
    }

    if (c->substate == bin_reading_get_key &&
        header->response.status == 0 &&
        (c->cmd == PROTOCOL_BINARY_CMD_GET ||
         c->cmd == PROTOCOL_BINARY_CMD_GETK ||
         c->cmd == PROTOCOL_BINARY_CMD_STAT)) {
        if (settings.verbose > 2) {
            moxi_log_write("<%d cproxy_process_a2b_downstream_nread %d %d %x get/getk/stat\n",
                    c->sfd, c->ileft, c->isize, c->cmd);
        }

        assert(c->item == NULL);

        // Alloc an item and continue with an item nread.
        // We item_alloc() even if vlen is 0, so that later
        // code can assume an item exists.
        //
        char *key   = binary_get_key(c);
        int   vlen  = bodylen - (keylen + extlen);
        int   flags = 0;

        assert(key);
        assert(keylen > 0);
        assert(vlen >= 0);

        if (c->cmd == PROTOCOL_BINARY_CMD_GET ||
            c->cmd == PROTOCOL_BINARY_CMD_GETK) {
            protocol_binary_response_get *response_get =
                (protocol_binary_response_get *) binary_get_request(c);

            assert(extlen == sizeof(response_get->message.body));

            flags = ntohl(response_get->message.body.flags);
        }

        item *it = item_alloc(key, keylen, flags, 0, vlen + 2);
        if (it != NULL) {
            c->item = it;
            c->ritem = ITEM_data(it);
            c->rlbytes = vlen;
            c->substate = bin_read_set_value;

            uint64_t cas = CPROXY_NOT_CAS;

            conn *uc = d->upstream_conn;
            if (uc != NULL &&
                uc->cmd_start != NULL &&
                strncmp(uc->cmd_start, "gets ", 5) == 0) {
                cas = header->response.cas;
            }

            ITEM_set_cas(it, cas);

            conn_set_state(c, conn_nread);
        } else {
            d->ptd->stats.stats.err_oom++;
            cproxy_close_conn(c);
        }
    } else {
        a2b_process_downstream_response(c);
    }
}

static void a2b_out_error(conn *uc, uint16_t status) {
    switch (status) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
        out_string(uc, "OK");
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
        out_string(uc, "NOT_FOUND");
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
        out_string(uc, "EXISTS");
        break;
    case PROTOCOL_BINARY_RESPONSE_E2BIG:
        out_string(uc, "SERVER_ERROR a2b e2big");
        break;
    case PROTOCOL_BINARY_RESPONSE_EINVAL:
        out_string(uc, "SERVER_ERROR a2b einval");
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
        out_string(uc, "NOT_STORED");
        break;
    case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
        out_string(uc, "SERVER_ERROR a2b delta_badval");
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET:
        out_string(uc, "SERVER_ERROR a2b not_my_vbucket");
        break;
    case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
        out_string(uc, "SERVER_ERROR a2b auth_error");
        break;
    case PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE:
        out_string(uc, "SERVER_ERROR a2b auth_continue");
        break;
    case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
        out_string(uc, "SERVER_ERROR a2b unknown");
        break;
    case PROTOCOL_BINARY_RESPONSE_ENOMEM:
        out_string(uc, "SERVER_ERROR a2b out of memory");
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED:
        out_string(uc, "SERVER_ERROR a2b not supported");
        break;
    case PROTOCOL_BINARY_RESPONSE_EINTERNAL:
        out_string(uc, "SERVER_ERROR a2b einternal");
        break;
    case PROTOCOL_BINARY_RESPONSE_EBUSY:
        out_string(uc, "SERVER_ERROR a2b ebusy");
        break;
    default:
        out_string(uc, "SERVER_ERROR a2b error");
        break;
    }
}

/* Invoked when we have read a complete downstream binary response,
 * including header, ext, key, and item data, as appropriate.
 */
void a2b_process_downstream_response(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);
    assert(c->next == NULL);
    assert(c->cmd_start != NULL);
    assert(IS_BINARY(c->protocol));
    assert(IS_PROXY(c->protocol));

    protocol_binary_response_header *header =
        (protocol_binary_response_header *) &c->binary_header;

    uint32_t extlen  = header->response.extlen;
    uint32_t keylen  = header->response.keylen;
    uint32_t bodylen = header->response.bodylen;
    uint16_t status  = header->response.status;

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_process_a2b_downstream_response, cmd: %x, item: %d, status: %d\n",
                c->sfd, c->cmd, (c->item != NULL), status);
    }

    // We reach here when we have the entire response,
    // including header, ext, key, and possibly item data.
    // Now we can get into big switch/case processing.
    //
    downstream *d = c->extra;
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);

    item *it = c->item;

    // Clear c->item because we either move it to the upstream or
    // item_remove() it on error.
    //
    c->item = NULL;

    if (cproxy_binary_ignore_reply(c, header, it)) {
        return;
    }

    conn *uc = d->upstream_conn;

    // Handle not-my-vbucket error response.
    //
    if (status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET) {
        assert(it == NULL);

        if (a2b_not_my_vbucket(uc, c, header)) {
            return;
        }
    }

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_GETK:
        if (settings.verbose > 2) {
            moxi_log_write("%d: cproxy_process_a2b_downstream_response GETK "
                    "noreply: %d\n", c->sfd, c->noreply);
        }

        if (c->noreply == false) {
            // Single-key GET/GETS.
            //
            if (status == 0) {
                assert(it != NULL);
                assert(it->nbytes >= 2);
                assert(keylen > 0);
                assert(extlen > 0);

                if (bodylen >= keylen + extlen) {
                    *(ITEM_data(it) + it->nbytes - 2) = '\r';
                    *(ITEM_data(it) + it->nbytes - 1) = '\n';

                    multiget_ascii_downstream_response(d, it);
                } else {
                    assert(false); // TODO.
                }

                item_remove(it);
            }

            conn_set_state(c, conn_pause);

            cproxy_update_event_write(d, uc);

            return;
        }

        // Multi-key GET/GETS.
        //
        // We should keep processing for a non-quiet
        // terminating response (NO-OP).
        //
        conn_set_state(c, conn_new_cmd);

        if (status != 0) {
            assert(it == NULL);

            if (status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT) {
                return; // Swallow miss response.
            }

            // TODO: Handle error case.  Should we pause the conn
            //       or keep looking for more responses?
            //
            assert(false);
            return;
        }

        assert(status == 0);
        assert(it != NULL);
        assert(it->nbytes >= 2);
        assert(keylen > 0);
        assert(extlen > 0);

        if (bodylen >= keylen + extlen) {
            *(ITEM_data(it) + it->nbytes - 2) = '\r';
            *(ITEM_data(it) + it->nbytes - 1) = '\n';

            multiget_ascii_downstream_response(d, it);
        } else {
            assert(false); // TODO.
        }

        item_remove(it);
        break;

    case PROTOCOL_BINARY_CMD_FLUSH:
        conn_set_state(c, conn_pause);

        // TODO: Handle flush_all's expiration parameter against
        // the front_cache.
        //
        // TODO: We flush the front_cache too often, inefficiently
        // on every downstream FLUSH response, rather than on
        // just the last FLUSH response.
        //
        if (uc != NULL) {
            mcache_flush_all(&d->ptd->proxy->front_cache, 0);
        }
        break;

    case PROTOCOL_BINARY_CMD_NOOP:
        conn_set_state(c, conn_pause);
        break;

    case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_APPEND:
    case PROTOCOL_BINARY_CMD_PREPEND:
        conn_set_state(c, conn_pause);

        if (uc != NULL) {
            assert(uc->next == NULL);

            switch (status) {
            case PROTOCOL_BINARY_RESPONSE_SUCCESS:
                out_string(uc, "STORED");
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
                if (c->cmd == PROTOCOL_BINARY_CMD_ADD) {
                    out_string(uc, "NOT_STORED");
                } else {
                    out_string(uc, "EXISTS");
                }
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
                if (c->cmd == PROTOCOL_BINARY_CMD_REPLACE) {
                    out_string(uc, "NOT_STORED");
                } else {
                    out_string(uc, "NOT_FOUND");
                }
                break;
            default:
                a2b_out_error(uc, status);
                break;
            }

            cproxy_del_front_cache_key_ascii(d, uc->cmd_start);

            cproxy_update_event_write(d, uc);
        }
        break;

    case PROTOCOL_BINARY_CMD_DELETE:
        conn_set_state(c, conn_pause);

        if (uc != NULL) {
            assert(uc->next == NULL);

            switch (status) {
            case 0:
                out_string(uc, "DELETED");
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
                out_string(uc, "EXISTS");
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
            case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
            case PROTOCOL_BINARY_RESPONSE_ENOMEM: // TODO.
            default:
                out_string(uc, "NOT_FOUND");
                break;
            }

            cproxy_del_front_cache_key_ascii(d, uc->cmd_start);

            cproxy_update_event_write(d, uc);
        }
        break;

    case PROTOCOL_BINARY_CMD_INCREMENT: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_DECREMENT:
        conn_set_state(c, conn_pause);

        if (uc != NULL) {
            assert(uc->next == NULL);

            // TODO: Any weird alignment/padding issues on different
            //       platforms in this cast to worry about here?
            //
            protocol_binary_response_incr *response_incr =
                (protocol_binary_response_incr *) c->cmd_start;

            switch (status) {
            case 0: {
                char *s = add_conn_suffix(uc);
                if (s != NULL) {
                    uint64_t v = mc_swap64(response_incr->message.body.value);
                    sprintf(s, "%llu", (unsigned long long) v);
                    out_string(uc, s);
                } else {
                    d->ptd->stats.stats.err_oom++;
                    cproxy_close_conn(uc);
                }
                break;
            }
            case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS: // Due to CAS.
                out_string(uc, "EXISTS");
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
                out_string(uc, "NOT_FOUND");
                break;
            case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
                out_string(uc, "NOT_STORED");
                break;
            case PROTOCOL_BINARY_RESPONSE_ENOMEM: // TODO.
            default:
                out_string(uc, "SERVER_ERROR a2b arith error");
                break;
            }

            cproxy_del_front_cache_key_ascii(d, uc->cmd_start);

            cproxy_update_event_write(d, uc);
        }
        break;

    case PROTOCOL_BINARY_CMD_STAT:
        assert(c->noreply == false);

        if (keylen > 0) {
            assert(it != NULL); // Holds the stat value.
            assert(it->nbytes > 2);
            assert(bodylen > keylen);
            assert(d->merger != NULL);

            if (uc != NULL) {
                assert(uc->next == NULL);

                // TODO: Handle ITEM and PREFIX.
                //
                protocol_stats_merge_name_val(d->merger,
                                              "STAT", 4,
                                              ITEM_key(it), it->nkey,
                                              ITEM_data(it), it->nbytes - 2);
            }

            item_remove(it);
            conn_set_state(c, conn_new_cmd);
        } else {
            // Handle the stats terminator.
            //
            assert(it == NULL);
            assert(bodylen == 0);
            conn_set_state(c, conn_pause);
        }
        break;

    case PROTOCOL_BINARY_CMD_VERSION:
    case PROTOCOL_BINARY_CMD_QUIT:
    default:
        assert(false); // TODO: Handled unexpected responses.
        break;
    }
}

/* Do the actual work of forwarding the command from an
 * upstream ascii conn to its assigned binary downstream.
 */
bool cproxy_forward_a2b_downstream(downstream *d) {
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
            return cproxy_forward_a2b_simple_downstream(d, uc->cmd_start, uc);
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
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(command != NULL);
    assert(uc != NULL);
    assert(uc->item == NULL);
    assert(uc->cmd_curr != (protocol_binary_command) -1);
    assert(d->merger == NULL);

    // Handles multi-key get and gets.
    //
    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_GETKQ) {
        // Only use front_cache for 'get', not for 'gets'.
        //
        mcache *front_cache =
            (command[3] == ' ') ? &d->ptd->proxy->front_cache : NULL;

        return multiget_ascii_downstream(d, uc,
                                         a2b_multiget_start,
                                         a2b_multiget_skey,
                                         a2b_multiget_end,
                                         front_cache);
    }

    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_GETK) {
        d->upstream_suffix = "END\r\n";
        d->upstream_suffix_len = 0;
        d->upstream_retry = 0;
    }

    assert(uc->next == NULL);

    // TODO: Inefficient repeated scan_tokens.
    //
    int      cmd_len = 0;
    token_t  tokens[MAX_TOKENS];
    size_t   ntokens = scan_tokens(command, tokens, MAX_TOKENS, &cmd_len);
    char    *key     = tokens[KEY_TOKEN].value;
    int      key_len = tokens[KEY_TOKEN].length;

    if (ntokens <= 1) { // This was checked long ago, while parsing
        assert(false);  // the upstream conn.
        return false;
    }

    uint8_t *out_key    = NULL;
    uint16_t out_keylen = 0;
    uint8_t  out_extlen = 0;

    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_FLUSH) {
        protocol_binary_request_flush req;
        memset(&req, 0, sizeof(req));
        protocol_binary_request_header *preq =
            (protocol_binary_request_header *) &req;

        int size = a2b_fill_request(uc->cmd_curr,
                                    tokens, ntokens,
                                    uc->noreply, preq,
                                    &out_key,
                                    &out_keylen,
                                    &out_extlen);
        if (size > 0) {
            assert(out_key == NULL);
            assert(out_keylen == 0);

            if (settings.verbose > 2) {
                moxi_log_write("a2b broadcast flush_all\n");
            }

            if (out_extlen == 0) {
                preq->request.extlen   = out_extlen = 4;
                preq->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
            }

            return cproxy_broadcast_a2b_downstream(d, preq, size,
                                                   out_key,
                                                   out_keylen,
                                                   out_extlen, uc,
                                                   "OK\r\n");
        }

        if (settings.verbose > 2) {
            moxi_log_write("a2b broadcast flush_all no size\n");
        }

        return false;
    }

    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_STAT) {
        protocol_binary_request_stats req;
        memset(&req, 0, sizeof(req));
        protocol_binary_request_header *preq =
            (protocol_binary_request_header *) &req;

        int size = a2b_fill_request(uc->cmd_curr,
                                    tokens, ntokens,
                                    uc->noreply, preq,
                                    &out_key,
                                    &out_keylen,
                                    &out_extlen);
        if (size > 0) {
            assert(out_extlen == 0);
            assert(uc->noreply == false);

            if (settings.verbose > 2) {
                moxi_log_write("a2b broadcast %s\n", command);
            }

            if (strncmp(command + 5, " reset", 6) == 0) {
                return cproxy_broadcast_a2b_downstream(d, preq, size,
                                                       out_key,
                                                       out_keylen,
                                                       out_extlen, uc,
                                                       "RESET\r\n");
            }

            if (cproxy_broadcast_a2b_downstream(d, preq, size,
                                                out_key,
                                                out_keylen,
                                                out_extlen, uc,
                                                "END\r\n")) {
                d->merger = genhash_init(128, skeyhash_ops);
                return true;
            }
        }

        return false;
    }

    // Assuming we're already connected to downstream.
    // Handle all other simple commands.
    //
    bool self = false;
    int  vbucket = -1;

    conn *c = cproxy_find_downstream_conn_ex(d, key, key_len,
                                             &self, &vbucket);
    if (c != NULL) {
        if (self) {
            // TODO: This optimization could be done much earlier,
            // even before the upstream conn was assigned
            // to a downstream.
            //
            cproxy_optimize_to_self(d, uc, command);
            process_command(uc, command);
            return true;
        }

        if (cproxy_prep_conn_for_write(c)) {
            assert(c->state == conn_pause);
            assert(c->wbuf);
            assert(c->wsize >= a2b_size_max);

            protocol_binary_request_header *header =
                (protocol_binary_request_header *) c->wbuf;

            memset(header, 0, a2b_size_max);

            int size = a2b_fill_request(uc->cmd_curr,
                                        tokens, ntokens,
                                        uc->noreply,
                                        header,
                                        &out_key,
                                        &out_keylen,
                                        &out_extlen);
            if (size > 0) {
                assert(size <= a2b_size_max);
                assert(key     == (char *) out_key);
                assert(key_len == (int)    out_keylen);
                assert(header->request.bodylen == 0);

                if (vbucket >= 0) {
                    header->request.reserved = htons(vbucket);
                    header->request.opaque   = htonl(vbucket);
                }

                header->request.bodylen =
                    htonl(out_keylen + out_extlen);

                a2b_set_opaque(c, header, uc->noreply);

                add_iov(c, header, size);

                if (out_key != NULL &&
                    out_keylen > 0) {
                    add_iov(c, out_key, out_keylen);
                }

                if (settings.verbose > 2) {
                    moxi_log_write("forwarding a2b to %d, cmd %x, noreply %d, vbucket %d\n",
                                   c->sfd, header->request.opcode, uc->noreply, vbucket);

                    cproxy_dump_header(c->sfd, (char *) header);
                }

                conn_set_state(c, conn_mwrite);
                c->write_and_go = conn_new_cmd;

                if (update_event(c, EV_WRITE | EV_PERSIST)) {
                    d->downstream_used_start = 1;
                    d->downstream_used       = 1;

                    if (cproxy_dettach_if_noreply(d, uc) == false) {
                        cproxy_start_downstream_timeout(d, c);
                    } else {
                        c->write_and_go = conn_pause;

                        if (key != NULL &&
                            key_len > 0) {
                            mcache_delete(&d->ptd->proxy->front_cache,
                                          key, key_len);
                        }
                    }

                    return true;
                } else {
                    // TODO: Error handling.
                    //
                    if (settings.verbose > 1) {
                        moxi_log_write("ERROR: Couldn't a2b update write event\n");
                    }

                    if (d->upstream_suffix == NULL) {
                        d->upstream_suffix = "SERVER_ERROR a2b event oom\r\n";
                        d->upstream_suffix_len = 0;
                        d->upstream_retry = 0;
                    }
                }
            } else {
                // TODO: Error handling.
                //
                if (settings.verbose > 1) {
                    moxi_log_write("ERROR: Couldn't a2b fill request: %s (%x)\n",
                            command, uc->cmd_curr);
                }

                if (d->upstream_suffix == NULL) {
                    d->upstream_suffix = "CLIENT_ERROR a2b parse request\r\n";
                    d->upstream_suffix_len = 0;
                    d->upstream_retry = 0;
                }
            }

            d->ptd->stats.stats.err_oom++;
            cproxy_close_conn(c);
        } else {
            d->ptd->stats.stats.err_downstream_write_prep++;
            cproxy_close_conn(c);
        }
    }

    return false;
}

int a2b_multiget_start(conn *c, char *cmd, int cmd_len) {
    (void)c;
    (void)cmd;
    (void)cmd_len;
    return 0; // No-op.
}

/* An skey is a space prefixed key string.
 */
int a2b_multiget_skey(conn *c, char *skey, int skey_length, int vbucket, int key_index) {
    char *key     = skey + 1;
    int   key_len = skey_length - 1;

    item *it = item_alloc("b", 1, 0, 0, sizeof(protocol_binary_request_get));
    if (it != NULL) {
        if (add_conn_item(c, it)) {
            protocol_binary_request_getk *req =
                (protocol_binary_request_getk *) ITEM_data(it);

            memset(req, 0, sizeof(req->bytes));

            req->message.header.request.magic  = PROTOCOL_BINARY_REQ;
            req->message.header.request.opcode = PROTOCOL_BINARY_CMD_GETKQ;
            req->message.header.request.keylen = htons((uint16_t) key_len);
            req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
            req->message.header.request.bodylen  = htonl(key_len);
            req->message.header.request.opaque   = htonl(key_index);

            if (vbucket >= 0) {
                req->message.header.request.reserved = htons(vbucket);

                if (settings.verbose > 2) {
                    char key_buf[KEY_MAX_LENGTH + 10];
                    assert(key_len <= KEY_MAX_LENGTH);
                    memcpy(key_buf, key, key_len);
                    key_buf[key_len] = '\0';

                    moxi_log_write("<%d a2b_multiget_skey '%s' %d %d\n",
                            c->sfd, key_buf, vbucket, key_index);
                }
            }

            if (add_iov(c, ITEM_data(it), sizeof(req->bytes)) == 0 &&
                add_iov(c, key, key_len) == 0) {
                return 0; // Success.
            }

            return -1;
        }

        item_remove(it);
    }

    return -1;
}

int a2b_multiget_end(conn *c) {
    return add_iov(c, &req_noop.bytes, sizeof(req_noop.bytes));
}

/* Used for broadcast commands, like flush_all or stats.
 */
bool cproxy_broadcast_a2b_downstream(downstream *d,
                                     protocol_binary_request_header *req,
                                     int req_size,
                                     uint8_t *key,
                                     uint16_t keylen,
                                     uint8_t  extlen,
                                     conn *uc,
                                     char *suffix) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(d->downstream_used_start == 0);
    assert(d->downstream_used == 0);
    assert(req != NULL);
    assert(req_size >= (int) sizeof(req));
    assert(req->request.bodylen == 0);
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(uc->item == NULL);

    req->request.bodylen = htonl(keylen + extlen);

    int nwrite = 0;
    int nconns = mcs_server_count(&d->mst);

    for (int i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL) {
            if (cproxy_prep_conn_for_write(c)) {
                assert(c->state == conn_pause);
                assert(c->wbuf);
                assert(c->wsize >= req_size);

                memcpy(c->wbuf, req, req_size);

                add_iov(c, c->wbuf, req_size);

                if (key != NULL &&
                    keylen > 0) {
                    add_iov(c, key, keylen);
                }

                conn_set_state(c, conn_mwrite);
                c->write_and_go = conn_new_cmd;

                if (update_event(c, EV_WRITE | EV_PERSIST)) {
                    nwrite++;

                    if (uc->noreply) {
                        c->write_and_go = conn_pause;
                    }
                } else {
                    if (settings.verbose > 1) {
                        moxi_log_write("ERROR: Update cproxy write event failed\n");
                    }

                    d->ptd->stats.stats.err_oom++;
                    cproxy_close_conn(c);
                }
            } else {
                if (settings.verbose > 1) {
                    moxi_log_write("ERROR: a2b broadcast prep conn failed\n");
                }

                d->ptd->stats.stats.err_downstream_write_prep++;
                cproxy_close_conn(c);
            }
        }
    }

    if (settings.verbose > 2) {
        moxi_log_write("%d: a2b broadcast nwrite %d out of %d\n",
                uc->sfd, nwrite, nconns);
    }

    if (nwrite > 0) {
        d->downstream_used_start = nwrite;
        d->downstream_used       = nwrite;

        if (cproxy_dettach_if_noreply(d, uc) == false) {
            d->upstream_suffix = suffix;
            d->upstream_suffix_len = 0;
            d->upstream_retry = 0;

            cproxy_start_downstream_timeout(d, NULL);
        } else {
            // TODO: Handle flush_all's expiration parameter against
            // the front_cache.
            //
            if (req->request.opcode == PROTOCOL_BINARY_CMD_FLUSH ||
                req->request.opcode == PROTOCOL_BINARY_CMD_FLUSHQ) {
                mcache_flush_all(&d->ptd->proxy->front_cache, 0);
            }
        }

        return true;
    }

    return false;
}

/* Forward an upstream command that came with item data,
 * like set/add/replace/etc.
 */
bool cproxy_forward_a2b_item_downstream(downstream *d, short cmd,
                                        item *it, conn *uc) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->proxy != NULL);
    assert(d->downstream_conns != NULL);
    assert(it != NULL);
    assert(it->nbytes >= 2);
    assert(uc != NULL);
    assert(uc->next == NULL);
    assert(cmd > 0);

    // Assuming we're already connected to downstream.
    //
    bool self = false;
    int  vbucket = -1;

    conn *c = cproxy_find_downstream_conn_ex(d, ITEM_key(it), it->nkey,
                                             &self, &vbucket);
    if (c != NULL) {
        if (self) {
            cproxy_optimize_to_self(d, uc, uc->cmd_start);
            complete_nread_ascii(uc);
            return true;
        }

        if (cproxy_prep_conn_for_write(c)) {
            assert(c->state == conn_pause);

            uint8_t  extlen = (cmd == NREAD_APPEND ||
                               cmd == NREAD_PREPEND) ? 0 : 8;
            uint32_t hdrlen =
                sizeof(protocol_binary_request_header) +
                extlen;

            item *it_hdr = item_alloc("i", 1, 0, 0, hdrlen);
            if (it_hdr != NULL) {
                if (add_conn_item(c, it_hdr)) {
                    protocol_binary_request_header *req =
                        (protocol_binary_request_header *) ITEM_data(it_hdr);

                    memset(req, 0, hdrlen);

                    req->request.magic    = PROTOCOL_BINARY_REQ;
                    req->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
                    req->request.keylen   = htons((uint16_t) it->nkey);
                    req->request.extlen   = extlen;

                    if (vbucket >= 0) {
                        // We also put the vbucket id into the opaque,
                        // so we can have it later for not-my-vbucket
                        // error handling.
                        //
                        req->request.reserved = htons(vbucket);
                        req->request.opaque   = htonl(vbucket);
                    }

                    switch (cmd) {
                    case NREAD_SET:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_SETQ :
                            PROTOCOL_BINARY_CMD_SET;
                        break;
                    case NREAD_CAS: {
                        uint64_t cas = ITEM_get_cas(it);
                        req->request.cas = mc_swap64(cas);
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_SETQ :
                            PROTOCOL_BINARY_CMD_SET;
                        break;
                    }
                    case NREAD_ADD:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_ADDQ :
                            PROTOCOL_BINARY_CMD_ADD;
                        break;
                    case NREAD_REPLACE:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_REPLACEQ :
                            PROTOCOL_BINARY_CMD_REPLACE;
                        break;
                    case NREAD_APPEND:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_APPENDQ :
                            PROTOCOL_BINARY_CMD_APPEND;
                        break;
                    case NREAD_PREPEND:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_PREPENDQ :
                            PROTOCOL_BINARY_CMD_PREPEND;
                        break;
                    default:
                        assert(false); // TODO.
                        break;
                    }

                    a2b_set_opaque(c, req, uc->noreply);

                    if (cmd != NREAD_APPEND &&
                        cmd != NREAD_PREPEND) {
                        protocol_binary_request_set *req_set =
                            (protocol_binary_request_set *) req;

                        req_set->message.body.flags =
                            htonl(strtoul(ITEM_suffix(it), NULL, 10));

                        req_set->message.body.expiration =
                            htonl(it->exptime);
                    }

                    req->request.bodylen =
                        htonl(it->nkey + (it->nbytes - 2) + extlen);

                    if (add_iov(c, ITEM_data(it_hdr), hdrlen) == 0 &&
                        add_iov(c, ITEM_key(it),  it->nkey) == 0 &&
                        add_iov(c, ITEM_data(it), it->nbytes - 2) == 0) {
                        conn_set_state(c, conn_mwrite);
                        c->write_and_go = conn_new_cmd;

                        if (update_event(c, EV_WRITE | EV_PERSIST)) {
                            d->downstream_used_start = 1;
                            d->downstream_used       = 1;

                            if (cproxy_dettach_if_noreply(d, uc) == false) {
                                cproxy_start_downstream_timeout(d, c);

                                if (cmd == NREAD_SET &&
                                    cproxy_optimize_set_ascii(d, uc,
                                                              ITEM_key(it),
                                                              it->nkey)) {
                                    d->ptd->stats.stats.tot_optimize_sets++;
                                }
                            } else {
                                c->write_and_go = conn_pause;

                                // TODO: At this point, the item key string is
                                // not '\0' or space terminated, which is
                                // required by the mcache API.
                                // Be sure to config front_cache to be off
                                // for binary protocol downstreams.
                                //
                                // mcache_delete(&d->ptd->proxy->front_cache,
                                //               ITEM_key(it), it->nkey);
                            }

                            return true;
                        }
                    }
                }

                item_remove(it_hdr);
            }

            d->ptd->stats.stats.err_oom++;
            cproxy_close_conn(c);
        } else {
            d->ptd->stats.stats.err_downstream_write_prep++;
            cproxy_close_conn(c);
        }
    }

    return false;
}

void a2b_set_opaque(conn *c, protocol_binary_request_header *header, bool noreply) {
    if (noreply) {
        // Set a magic opaque value during quiet commands that tells us later
        // that we can ignore the downstream's error response messge,
        // since the upstream ascii client doesn't want it.
        //
        header->request.opaque = htonl(OPAQUE_IGNORE_REPLY);

        if (settings.verbose > 2) {
            moxi_log_write("%d: a2b_set_opaque OPAQUE_IGNORE_REPLY, cmdq: %x\n",
                    c->sfd, header->request.opcode);
        }
    }
}

bool a2b_not_my_vbucket(conn *uc, conn *c,
                        protocol_binary_response_header *header) {
    downstream *d = c->extra;
    assert(d != NULL);
    assert(d->ptd != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("<%d a2b_not_my_vbucket, "
                       "cmd: %x %d\n",
                       c->sfd, header->response.opcode, uc != NULL);
    }

    if (c->cmd != PROTOCOL_BINARY_CMD_GETK ||
        c->noreply == false) {
        // For non-multi-key GET commands, enqueue a retry after
        // informing the vbucket map.  This includes single-key GET's.
        //
        if (uc == NULL) {
            // If the client went away, though, don't retry.
            //
            conn_set_state(c, conn_pause);
            return true;
        }

        int vbucket = ntohl(header->response.opaque);
        int sindex = downstream_conn_index(d, c);

        if (settings.verbose > 2) {
            moxi_log_write("<%d a2b_not_my_vbucket, "
                           "cmd: %x not multi-key get, sindex %d, vbucket %d, retries %d\n",
                           c->sfd, header->response.opcode, sindex, vbucket, uc->cmd_retries);
        }

        mcs_server_invalid_vbucket(&d->mst, sindex, vbucket);

        // As long as the upstream is still open and we haven't
        // retried too many times already.
        //
        int max_retries = (mcs_server_count(&d->mst) * 2);

        if (uc->cmd_retries < max_retries) {
            uc->cmd_retries++;

            d->upstream_retry++;
            d->ptd->stats.stats.tot_retry_vbucket++;

            conn_set_state(c, conn_pause);
            return true;
        }

        if (settings.verbose > 2) {
            moxi_log_write("%d: a2b_not_my_vbucket, "
                           "cmd: %x skipping retry %d >= %d\n",
                           c->sfd, header->response.opcode, uc->cmd_retries,
                           max_retries);
        }

        return false;
    } else {
        // Handle ascii multi-GET commands by awaiting all NOOP's from
        // downstream servers, eating the NOOP's, and retrying with
        // the same multiget de-duplication map, which might be partially
        // filled in already.
        //
        if (uc == NULL) {
            // If the client went away, though, don't retry,
            // but keep looking for that NOOP.
            //
            conn_set_state(c, conn_new_cmd);
            return true;
        }

        assert(uc->cmd_start != NULL);
        assert(header->response.opaque != 0);

        int   key_index = ntohl(header->response.opaque);
        char *key       = uc->cmd_start + key_index;
        int   key_len   = skey_len(key);

        // The key is not NULL or space terminated.
        //
        char key_buf[KEY_MAX_LENGTH + 10];
        assert(key_len <= KEY_MAX_LENGTH);
        memcpy(key_buf, key, key_len);
        key_buf[key_len] = '\0';

        int vbucket = -1;
        int sindex = downstream_conn_index(d, c);

        mcs_key_hash(&d->mst, key_buf, key_len, &vbucket);

        if (settings.verbose > 2) {
            moxi_log_write("<%d a2b_not_my_vbucket, "
                           "cmd: %x get/getk '%s' %d retry %d, sindex %d, vbucket %d\n",
                           c->sfd, header->response.opcode, key_buf, key_len,
                           d->upstream_retry + 1, sindex, vbucket);
        }

        mcs_server_invalid_vbucket(&d->mst, sindex, vbucket);

        // Update the de-duplication map, removing the key, so that
        // we'll reattempt another request for the key during the
        // retry.
        //
        if (d->multiget != NULL) {
            multiget_entry *entry = genhash_find(d->multiget, key_buf);

            if (settings.verbose > 2) {
                moxi_log_write("<%d a2b_not_my_vbucket, "
                               "cmd: %x get/getk '%s' %d retry: %d, entry: %d, vbucket %d "
                               "deleting multiget entry\n",
                               c->sfd, header->response.opcode, key_buf, key_len,
                               d->upstream_retry + 1, entry != NULL, vbucket);
            }

            genhash_delete(d->multiget, key_buf);

            while (entry != NULL) {
                multiget_entry *curr = entry;
                entry = entry->next;
                free(curr);
            }
        }

        // Signal that we need to retry, where this counter is
        // later checked after all NOOP's from downstreams are
        // received.
        //
        d->upstream_retry++;
        d->ptd->stats.stats.tot_retry_vbucket++;

        conn_set_state(c, conn_new_cmd);
        return true;
    }
}
