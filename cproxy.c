/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <assert.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"

/** From libmemcached. */
memcached_return memcached_version(memcached_st *ptr);
memcached_return memcached_connect(memcached_server_st *ptr);
uint32_t memcached_generate_hash(memcached_st *ptr, const char *key, size_t key_length);

// TODO: Move into configurable settings one day.
//
#define DOWNSTREAM_MAX 10

typedef struct proxy      proxy;
typedef struct proxy_td   proxy_td;
typedef struct downstream downstream;

struct proxy {
    int   port;   // Immutable.
    char *config; // Immutable, alloc'ed by proxy.

    // Number of listening conn's acting as a proxy,
    // where ((proxy *) conn->extra == this).
    //
    int listening;

    proxy_td *thread_data;     // Immutable.
    int       thread_data_num; // Immutable.
};

struct proxy_td { // Per proxy, per worker-thread data struct.
    proxy *proxy; // Immutable parent pointer.

    conn *waiting_for_downstream_head; // Upstream conns paused, waiting for a free downstream.
    conn *waiting_for_downstream_tail;

    downstream *downstream_free; // Downstreams not servicing an upstream conn.
    int         downstream_num;  // Number of downstreams created (free + busy).
    int         downstream_max;  // Max number of downstreams created, for concurrency.
};

struct downstream {
    proxy_td      *ptd;                 // Immutable parent pointer.
    memcached_st   mst;                 // Immutable.
    downstream    *next;                // To track free list.
    conn         **downstream_conns;    // To downstream servers, wraps mst's file descriptors.
    conn          *upstream_conn;       // Current upstream client, when downstream assigned/reserved.
    char          *upstream_suffix;     // Suffix to write when no more replies (reply_expect == 0).
    item          *reply_item_head;     // To serialize multi-get/scatter-gather response.
    item          *reply_item_tail;
    int            reply_expect;        // Number of replies to expect, >1 during scatter/gather broadcast.
};

proxy       *cproxy_create(int proxy_port, char *proxy_sect, int nthreads);
int          cproxy_listen(proxy *p);
proxy_td    *cproxy_find_thread_data(proxy *p, pthread_t thread_id);
void         cproxy_init_upstream_conn(conn *c);
void         cproxy_init_downstream_conn(conn *c);
void         cproxy_on_close_upstream_conn(conn *c);
void         cproxy_on_close_downstream_conn(conn *c);

void         cproxy_add_downstream(proxy_td *ptd);
downstream  *cproxy_reserve_downstream(proxy_td *ptd);
void         cproxy_release_downstream(proxy_td *ptd, downstream *d);
void         cproxy_release_downstream_conn(downstream *d, conn *c);
downstream  *cproxy_create_downstream(char *proxy_sect);
int          cproxy_connect_downstream(downstream *d, LIBEVENT_THREAD *thread);
void         cproxy_wait_for_downstream(proxy_td *ptd, conn *c);
void         cproxy_assign_downstream(proxy_td *ptd);
bool         cproxy_forward_downstream(downstream *d);
void         cproxy_pause_upstream_for_downstream(proxy_td *ptd, conn *upstream);
conn        *cproxy_find_downstream_conn(downstream *d, char *key, int key_length);
int          cproxy_server_index(downstream *d, char *key, size_t key_length);

void cproxy_process_upstream_ascii(conn *c, char *line);
void cproxy_process_upstream_ascii_nread(conn *c);

void cproxy_process_downstream_ascii(conn *c, char *line);

size_t scan_tokens(char *command, token_t *tokens, const size_t max_tokens);

conn_funcs cproxy_upstream_funcs = {
    cproxy_init_upstream_conn,
    cproxy_on_close_upstream_conn,
    add_bytes_read,
    out_string,
    cproxy_process_upstream_ascii,
    dispatch_bin_command,
    reset_cmd_handler,
    cproxy_process_upstream_ascii_nread
};

conn_funcs cproxy_downstream_funcs = {
    cproxy_init_downstream_conn,
    cproxy_on_close_downstream_conn,
    add_bytes_read,
    out_string,
    cproxy_process_downstream_ascii,
    dispatch_bin_command,
    reset_cmd_handler,
    complete_nread
};

/**
 * cfg should look like "local_port=host:port,host:port;local_port=host:port"
 * like "11222=memcached1.foo.net:11211"  This means local port 11222
 * will be a proxy to downstream memcached server running at
 * host memcached1.foo.net on port 11211.
 */
int cproxy_init(const char *cfg, int nthreads) {
    if (cfg == NULL)
        return 0;

    char *buff;
    char *next;
    char *proxy_sect;
    char *proxy_port_str;
    int   proxy_port;

    buff = strdup(cfg);
    next = buff;
    while (next != NULL) {
        proxy_sect = strsep(&next, ";");

        proxy_port_str = strsep(&proxy_sect, "=");
        if (proxy_sect == NULL) {
            fprintf(stderr, "bad cproxy config, missing =\n");
            exit(EXIT_FAILURE);
        }
        proxy_port = atoi(proxy_port_str);
        if (proxy_port <= 0) {
            fprintf(stderr, "bad cproxy config, bad proxy port\n");
            exit(EXIT_FAILURE);
        }

        proxy *p = cproxy_create(proxy_port, proxy_sect, nthreads);
        if (p != NULL) {
            int n = cproxy_listen(p);
            if (n > 0) {
                if (settings.verbose > 1)
                    fprintf(stderr, "cproxy listening on %d conns\n", n);
            }
        } else {
            fprintf(stderr, "could not alloc proxy\n");
            exit(EXIT_FAILURE);
        }
    }
    free(buff);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_init done\n");

    return 0;
}

proxy *cproxy_create(int port, char *config, int nthreads) {
    assert(port > 0);
    assert(config != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_create on port %d, downstream %s\n",
                port, config);

    proxy *p = (proxy *) calloc(1, sizeof(proxy));
    if (p != NULL) {
        p->port   = port;
        p->config = strdup(config);
        p->listening = 0;

        p->thread_data_num = nthreads;
        p->thread_data = (proxy_td *) calloc(p->thread_data_num,
                                             sizeof(proxy_td));
        if (p->thread_data != NULL) {
            // We start at 1, because thread[0] is the main listen/accept
            // thread, and not a true worker thread.  Too lazy to save
            // the wasted thread[0] slot memory.
            //
            for (int i = 1; i < p->thread_data_num; i++) {
                proxy_td *ptd = &p->thread_data[i];
                ptd->proxy = p;
                ptd->waiting_for_downstream_head = NULL;
                ptd->waiting_for_downstream_tail = NULL;
                ptd->downstream_free = NULL;
                ptd->downstream_num  = 0;
                ptd->downstream_max  = DOWNSTREAM_MAX;
            }
            return p;
        }
        free(p->config);
    }
    free(p);

    return NULL;
}

int cproxy_listen(proxy *p) {
    assert(p != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy_listen on port %d, downstream %s\n",
                p->port, p->config);

    conn *listen_conn_orig = listen_conn;

    // Idempotent, remembers if it already created listening socket(s).
    //
    if (p->listening == 0 &&
        server_socket(p->port, proxy_upstream_ascii_prot) == 0) {
        assert(listen_conn != NULL);

        // The listen_conn global list is changed by server_socket(),
        // which adds a new listening conn on p->port for each bindable
        // host address.
        //
        // For example, after the call to server_socket(), there
        // might be two new listening conn's -- one for localhost,
        // another for 127.0.0.1.
        //
        conn *c = listen_conn;
        while (c != NULL &&
               c != listen_conn_orig) {
            if (settings.verbose > 1)
                fprintf(stderr, "<%d cproxy listening on port %d, downstream %s\n",
                        c->sfd, p->port, p->config);

            p->listening++;

            // TODO: Memory leak, need to clean up listen_conn->extra,
            //       or, perhaps listening conn's never close?
            //
            c->extra = p;
            c->funcs = &cproxy_upstream_funcs;
            c = c->next;
        }
    }

    return p->listening;
}

proxy_td *cproxy_find_thread_data(proxy *p, pthread_t thread_id) {
    int i = thread_index(thread_id);

    // 0 is the main listen thread, not a worker thread.
    assert(i > 0);
    assert(i < p->thread_data_num);

    if (i > 0 && i < p->thread_data_num)
        return &p->thread_data[i];

    return NULL;
}

void cproxy_init_upstream_conn(conn *c) {
    assert(c->extra != NULL);

    // We're called once per client/upstream conn early in its
    // lifecycle, so it's a good place to remember the proxy_td.
    //
    proxy *p = c->extra;
    if (p != NULL) {
        if (settings.verbose > 1)
            fprintf(stderr, "<%d cproxy_init_upstream_conn (%s) for %d, downstream %s\n",
                    c->sfd, state_text(c->state), p->port, p->config);

        proxy_td *ptd = cproxy_find_thread_data(p, pthread_self());
        if (ptd != NULL) {
            c->extra = ptd;
            return; // Success.
        }
    }

    // TODO: Error, so close the conn?
}

void cproxy_init_downstream_conn(conn *c) {
    assert(c->extra != NULL);

    downstream *d = c->extra;
    if (d != NULL) {
        if (settings.verbose > 1)
            fprintf(stderr, "<%d cproxy_init_downstream_conn (%s) to downstream %s\n",
                    c->sfd, state_text(c->state), d->ptd->proxy->config);
    }
}

void cproxy_on_close_upstream_conn(conn *c) {
    assert(c != NULL);
    assert(c->extra != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d cproxy_on_close_upstream_conn\n", c->sfd);

    c->extra = NULL;
}

void cproxy_on_close_downstream_conn(conn *c) {
    assert(c != NULL);
    assert(c->extra != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d cproxy_on_close_downstream_conn\n", c->sfd);

    c->extra = NULL;
}

void cproxy_add_downstream(proxy_td *ptd) {
    assert(ptd != NULL);

    if (ptd != NULL &&
        ptd->downstream_num < ptd->downstream_max) {
        downstream *d = cproxy_create_downstream(ptd->proxy->config);
        if (d != NULL) {
            d->ptd = ptd;
            ptd->downstream_num++;
            cproxy_release_downstream(ptd, d);
        }
    }
}

downstream *cproxy_reserve_downstream(proxy_td *ptd) {
    assert(ptd != NULL);

    downstream *d;

    d = ptd->downstream_free;
    if (d == NULL)
        cproxy_add_downstream(ptd);

    d = ptd->downstream_free;
    if (d != NULL) {
        ptd->downstream_free = d->next;
        d->next = NULL;
    }

    assert(d->upstream_conn == NULL);
    assert(d->upstream_suffix == NULL);
    assert(d->reply_item_head == NULL);
    assert(d->reply_item_tail == NULL);
    assert(d->reply_expect == 0);

    d->upstream_conn = NULL;
    d->upstream_suffix = NULL;
    d->reply_item_head = NULL;
    d->reply_item_tail = NULL;
    d->reply_expect = 0;

    return d;
}

void cproxy_release_downstream(proxy_td *ptd, downstream *d) {
    assert(ptd != NULL);
    assert(d != NULL);
    assert(d->next == NULL);

    d->upstream_conn = NULL;
    d->upstream_suffix = NULL;

    // Back onto the free/available downstream list.
    //
    d->next = ptd->downstream_free;
    ptd->downstream_free = d;

    // TODO: Should cleanup here rather than just assert?
    //
    assert(d->reply_item_head == NULL);
    assert(d->reply_item_tail == NULL);
    assert(d->reply_expect == 0);

    d->reply_item_head = NULL;
    d->reply_item_tail = NULL;
    d->reply_expect = 0;
}

downstream *cproxy_create_downstream(char *config) {
    downstream *d = (downstream *) calloc(1, sizeof(downstream));
    if (d != NULL) {
        if (memcached_create(&d->mst) != NULL) {
            memcached_server_st *mservers;

            mservers = memcached_servers_parse(config);
            if (mservers != NULL) {
                memcached_server_push(&d->mst, mservers);
                memcached_server_list_free(mservers);
                mservers = NULL;

                int nconns = memcached_server_count(&d->mst);

                d->downstream_conns = (conn **) calloc(nconns, sizeof(conn *));
                if (d->downstream_conns != NULL) {
                    return d;
                }
            }
            if (mservers != NULL)
                memcached_server_list_free(mservers);

            memcached_free(&d->mst);
        }
        free(d);
    }
    return NULL;
}

int cproxy_connect_downstream(downstream *d, LIBEVENT_THREAD *thread) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->ptd->downstream_free != d); // Should not be in free list if we're connecting.
    assert(d->next == NULL);
    assert(d->downstream_conns != NULL);
    assert(memcached_server_count(&d->mst) > 0);

    memcached_return rc;

    int s = 0; // Number connected.
    int n = memcached_server_count(&d->mst);

    for (int i = 0; i < n; i++) {
        if (d->downstream_conns[i] == NULL) {
            rc = memcached_connect(&d->mst.hosts[i]);
            if (rc == MEMCACHED_SUCCESS) {
                int fd = d->mst.hosts[i].fd;
                if (fd >= 0) {
                    d->downstream_conns[i] =
                        conn_new(fd, conn_pause, 0, DATA_BUFFER_SIZE,
                                 proxy_downstream_ascii_prot,
                                 thread->base, &cproxy_downstream_funcs, d);
                    d->downstream_conns[i]->thread = thread;
                }
            }
        }
        if (d->downstream_conns[i] != NULL)
            s++;
    }

    return s;
}

#define COMMAND_TOKEN    0
#define SUBCOMMAND_TOKEN 1
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
        fprintf(stderr, "<%d %s\n", c->sfd, line);

    /* For commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */
    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;

    if (add_msghdr(c) != 0) {
        c->funcs->conn_out_string(c, "SERVER_ERROR out of memory preparing response");
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

    } else if (ntokens >= 3 &&
               (strncmp(cmd, "gets", 4) == 0)) {

        c->funcs->conn_out_string(c, "ERROR");
        // process_get_command(c, tokens, ntokens, true);

    } else if (ntokens >= 2 &&
               (strncmp(cmd, "stats", 5) == 0)) {

        c->funcs->conn_out_string(c, "ERROR");
        // process_stat(c, tokens, ntokens);

    } else if (ntokens >= 2 && ntokens <= 4 &&
               (strncmp(cmd, "flush_all", 9) == 0)) {

        c->funcs->conn_out_string(c, "ERROR");

#ifdef SKIP_THIS
        time_t exptime = 0;
        set_current_time();

        set_noreply_maybe(c, tokens, ntokens);

        if(ntokens == (c->noreply ? 3 : 2)) {
            settings.oldest_live = current_time - 1;
            item_flush_expired();
            c->funcs->conn_out_string(c, "OK");
            return;
        }

        exptime = strtol(tokens[1].value, NULL, 10);
        if(errno == ERANGE) {
            c->funcs->conn_out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        /*
          If exptime is zero realtime() would return zero too, and
          realtime(exptime) - 1 would overflow to the max unsigned
          value.  So we process exptime == 0 the same way we do when
          no delay is given at all.
        */
        if (exptime > 0)
            settings.oldest_live = realtime(exptime) - 1;
        else /* exptime == 0 */
            settings.oldest_live = current_time - 1;
        item_flush_expired();
        c->funcs->conn_out_string(c, "OK");
        return;
#endif

    } else if (ntokens == 2 &&
               (strncmp(cmd, "version", 7) == 0)) {

        c->funcs->conn_out_string(c, "VERSION " VERSION);

    } else if (ntokens == 2 &&
               (strncmp(cmd, "quit", 4) == 0)) {

        conn_set_state(c, conn_closing);

    } else {
        c->funcs->conn_out_string(c, "ERROR");
    }
}

/* We get here after reading the value in set/add/replace
 * commands. The command has been stored in c->cmd, and
 * the item is ready in c->item.
 */
void cproxy_process_upstream_ascii_nread(conn *c) {
    assert(c != NULL);
    assert(c->item != NULL);
    assert(c->extra != NULL);

    item *it = c->item;

    // pthread_mutex_lock(&c->thread->stats.mutex);
    // c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    // pthread_mutex_unlock(&c->thread->stats.mutex);

    if (strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) == 0) {
        proxy_td *ptd = c->extra;
        if (ptd != NULL) {
            cproxy_pause_upstream_for_downstream(ptd, c);
        } else
            c->funcs->conn_out_string(c, "SERVER_ERROR expected proxy_td");
    } else
        c->funcs->conn_out_string(c, "CLIENT_ERROR bad data chunk");
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
        fprintf(stderr, "<%d %s\n", c->sfd, line);

    downstream *d = c->extra;

    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->next == NULL);

    conn *uc = d->upstream_conn;

    assert(uc != NULL);
    assert(uc->funcs != NULL);
    assert(IS_ASCII(uc->protocol));
    assert(IS_PROXY(uc->protocol));

    if (strncmp(line, "VALUE ", 6) == 0) {
        token_t tokens[MAX_TOKENS];
        size_t  ntokens = scan_tokens(line, tokens, MAX_TOKENS);

        if (ntokens > 1) {
        }
    } else {
        uc->funcs->conn_out_string(uc, line);

        if (!update_event(uc, EV_WRITE | EV_PERSIST)) {
            if (settings.verbose > 0)
                fprintf(stderr, "Couldn't update upstream write event\n");
            conn_set_state(uc, conn_closing);
        }

        cproxy_release_downstream_conn(d, c);
    }
}

/* Tokenize the command string by updating the token array
 * with pointers to start of each token and length.
 * Does not modify the input command string.
 *
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while (scan_tokens(command, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      command = tokens[ix].value;
 *  }
 */
size_t scan_tokens(char *command, token_t *tokens, const size_t max_tokens) {
    char *s, *e;
    size_t ntokens = 0;

    assert(command != NULL && tokens != NULL && max_tokens > 1);

    for (s = e = command; ntokens < max_tokens - 1; ++e) {
        if (*e == '\0' || *e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }
            if (*e == '\0')
                break; /* string end */
            s = e + 1;
        }
    }

    /* If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    tokens[ntokens].value = (*e == '\0' ? NULL : e);
    tokens[ntokens].length = 0;
    ntokens++;

    return ntokens;
}

conn *cproxy_find_downstream_conn(downstream *d, char *key, int key_length) {
    assert(d != NULL);
    assert(d->downstream_conns != NULL);
    assert(key != NULL);
    assert(key_length > 0);

    int s = cproxy_server_index(d, key, key_length);
    if (s >= 0 &&
        s < memcached_server_count(&d->mst)) {
        conn *c = d->downstream_conns[s];

        assert(c->state == conn_pause);

        c->msgcurr = 0; // TODO: Mem leak just by blowing these to 0?
        c->msgused = 0;
        c->iovused = 0;

        if (add_msghdr(c) == 0)
            return c;

        // TODO: Need separate error msg/code for add_msghdr failure.
    }

    return NULL;
}

int cproxy_server_index(downstream *d, char *key, size_t key_length) {
    assert(d != NULL);
    assert(key != NULL);
    assert(key_length > 0);

    // memcached_return rc;
    //
    // rc = memcached_validate_key_length(key_length, d->mst.flags & MEM_BINARY_PROTOCOL);
    // unlikely (rc != MEMCACHED_SUCCESS)
    //    return -1;

    if (memcached_server_count(&d->mst) <= 0)
        return -1;

    // if (memcached_key_test((char **) &key, &key_length, 1) == MEMCACHED_BAD_KEY_PROVIDED)
    //     return -1;

    return (int) memcached_generate_hash(&d->mst, key, key_length);
}

void cproxy_assign_downstream(proxy_td *ptd) {
    assert(ptd != NULL);

    // Key loop that tries to reserve any free downstream
    // resources to waiting upstream conns.
    //
    // Remember the wait list tail when we start, in case more
    // upstream conns tacked onto the wait list while we're
    // processing.  This helps avoid infinite loop where conn's
    // just keep on moving to the tail.
    //
    conn *tail = ptd->waiting_for_downstream_tail;
    int   stop = 0;

    while (ptd->waiting_for_downstream_head != NULL && !stop) {
        if (ptd->waiting_for_downstream_head == tail)
            stop = 1;

        downstream *d = cproxy_reserve_downstream(ptd);
        if (d == NULL)
            break; // If no downstreams are available, stop loop.

        assert(d->next == NULL);
        assert(d->upstream_conn == NULL);
        assert(d->upstream_suffix == NULL);
        assert(d->reply_expect == 0);
        assert(d->reply_item_head == NULL);
        assert(d->reply_item_tail == NULL);

        // We have a downstream reserved, so assign the first
        // waiting upstream conn to it.
        //
        d->upstream_conn = ptd->waiting_for_downstream_head;
        ptd->waiting_for_downstream_head = ptd->waiting_for_downstream_head->next;
        if (ptd->waiting_for_downstream_head == NULL)
            ptd->waiting_for_downstream_tail = NULL;
        d->upstream_conn->next = NULL;

        if (!cproxy_forward_downstream(d)) {
            // We reach here on error, so put upstream conn back on the wait
            // list to retry, and release the downstream.
            //
            // TOOD: Count this to eventually give up & error, rather than retry.
            //
            conn *uc = d->upstream_conn;

            assert(uc != NULL);

            cproxy_release_downstream(ptd, d);
            cproxy_wait_for_downstream(ptd, uc);
        }
    }
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
            // Simple, one-liner command.
            //
            assert(uc->item == NULL);

            char    *command = uc->rcurr;
            token_t  tokens[MAX_TOKENS];
            size_t   ntokens = scan_tokens(command, tokens, MAX_TOKENS);
            char    *key     = tokens[KEY_TOKEN].value;
            int      key_len = tokens[KEY_TOKEN].length;

            // TODO: Handle multi-get.
            //
            if (ntokens > 1) {
                conn *c = cproxy_find_downstream_conn(d, key, key_len);
                if (c != NULL) {
                    assert(IS_ASCII(c->protocol));
                    assert(IS_PROXY(c->protocol));

                    c->funcs->conn_out_string(c, command);

                    if (update_event(c, EV_WRITE | EV_PERSIST)) {
                        d->reply_expect = 1;
                        return true;
                    }

                    if (settings.verbose > 0)
                        fprintf(stderr, "Couldn't update cproxy write event\n");

                    conn_set_state(c, conn_closing);
                }
            }
        } else {
            // Command that came with item data, like set/add/replace/etc.
            //
            item *it = uc->item;

            assert(it != NULL);

            conn *c = cproxy_find_downstream_conn(d, ITEM_key(it), it->nkey);
            if (c != NULL) {
                assert(c->item == NULL);

                if (add_iov(c, "set ", 4) == 0 &&
                    add_iov(c, ITEM_key(it), it->nkey) == 0 &&
                    add_iov(c, " 0 ", 3) == 0 &&
                    add_iov(c, ITEM_suffix(it), it->nsuffix + it->nbytes) == 0) {
                    conn_set_state(c, conn_mwrite);

                    if (update_event(c, EV_WRITE | EV_PERSIST)) {
                        d->reply_expect = 1;
                        return true;
                    }

                    if (settings.verbose > 0)
                        fprintf(stderr, "Couldn't update cproxy write event\n");

                    conn_set_state(c, conn_closing);
                } else {
                    // TODO: Need better out-of-memory behavior.
                    //
                    if (settings.verbose > 0)
                        fprintf(stderr, "Couldn't alloc cproxy iov memory\n");
                }
            }
        }
    }

    return false;
}

void cproxy_wait_for_downstream(proxy_td *ptd, conn *c) {
    assert(c != NULL);
    assert(ptd != NULL);
    assert(!ptd->waiting_for_downstream_tail || !ptd->waiting_for_downstream_tail->next);

    // Add the conn to the wait list.
    //
    c->next = NULL;
    if (ptd->waiting_for_downstream_tail != NULL)
        ptd->waiting_for_downstream_tail->next = c;
    ptd->waiting_for_downstream_tail = c;
    if (ptd->waiting_for_downstream_head == NULL)
        ptd->waiting_for_downstream_head = c;
}

void cproxy_release_downstream_conn(downstream *d, conn *c) {
    assert(d != NULL);
    assert(d->ptd != NULL);
    assert(d->reply_expect > 0);
    assert(d->upstream_conn != NULL);
    assert(c != NULL);

    conn_set_state(c, conn_pause);

    d->reply_expect--;
    if (d->reply_expect <= 0) {
        if (d->upstream_suffix != NULL) {
            add_iov(d->upstream_conn, d->upstream_suffix, strlen(d->upstream_suffix));
            d->upstream_suffix = NULL; // Assuming static string, like "END\r\n", no free() needed.
        }

        cproxy_release_downstream(d->ptd, d);
        cproxy_assign_downstream(d->ptd);
    }
}

void cproxy_pause_upstream_for_downstream(proxy_td *ptd, conn *upstream) {
    assert(ptd != NULL);
    assert(upstream != NULL);

    conn_set_state(upstream, conn_pause);
    cproxy_wait_for_downstream(ptd, upstream);
    cproxy_assign_downstream(ptd);
}

