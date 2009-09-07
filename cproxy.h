/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef CPROXY_H
#define CPROXY_H

#include <libmemcached/memcached.h>
#include "genhash.h"
#include "work.h"
#include "matcher.h"

int cproxy_init(char *cfg_str,
                char *behavior_str,
                int nthreads,
                struct event_base *main_base);

#define IS_PROXY(x) (x == proxy_upstream_ascii_prot || \
                     x == proxy_upstream_binary_prot || \
                     x == proxy_downstream_ascii_prot || \
                     x == proxy_downstream_binary_prot)

#define CPROXY_NOT_CAS UINT64_MAX

// TODO: Millisecond capacity in 32-bit field not enough?
//
extern volatile uint32_t msec_current_time;

extern char cproxy_hostname[300]; // Immutable after init.

// -------------------------------

typedef struct {
    char *(*item_key)(void *it);
    int   (*item_key_len)(void *it);
    int   (*item_len)(void *it);
    void  (*item_add_ref)(void *it);
    void  (*item_dec_ref)(void *it);
    void *(*item_get_next)(void *it);
    void  (*item_set_next)(void *it, void *next);
    void *(*item_get_prev)(void *it);
    void  (*item_set_prev)(void *it, void *prev);
    uint32_t (*item_get_exptime)(void *it);
    void     (*item_set_exptime)(void *it, uint32_t exptime);
} mcache_funcs;

extern mcache_funcs mcache_item_funcs;
extern mcache_funcs mcache_key_stats_funcs;

typedef struct {
    mcache_funcs *funcs;

    pthread_mutex_t *lock; // NULL-able, for non-multithreaded.

    bool key_alloc;        // True if mcache must alloc key memory.

    genhash_t *map;       // NULL-able, keyed by string, value is item.

    uint32_t max;          // Maxiumum number of items to keep.

    void *lru_head;        // Most recently used.
    void *lru_tail;        // Least recently used.

    uint32_t oldest_live;  // In millisecs, relative to msec_current_time.

    // Statistics.
    //
    uint64_t tot_get_hits;
    uint64_t tot_get_expires;
    uint64_t tot_get_misses;
    uint64_t tot_get_bytes;
    uint64_t tot_adds;
    uint64_t tot_add_skips;
    uint64_t tot_add_fails;
    uint64_t tot_add_bytes;
    uint64_t tot_deletes;
    uint64_t tot_evictions;
} mcache;

typedef struct proxy               proxy;
typedef struct proxy_td            proxy_td;
typedef struct proxy_main          proxy_main;
typedef struct proxy_stats         proxy_stats;
typedef struct proxy_behavior      proxy_behavior;
typedef struct proxy_behavior_pool proxy_behavior_pool;
typedef struct downstream          downstream;
typedef struct key_stats           key_stats;

struct proxy_behavior {
    // IL means startup, system initialization level behavior.
    // ML means proxy/pool manager-level behavior (proxy_main).
    // PL means proxy/pool-level behavior.
    // SL means server-level behavior, although we inherit from proxy level.
    //
    uint32_t       cycle;               // IL: Clock resolution in millisecs.
    uint32_t       downstream_max;      // PL: Downstream concurrency.
    uint32_t       downstream_weight;   // SL: Server weight.
    uint32_t       downstream_retry;    // SL: How many times to retry a cmd.
    enum protocol  downstream_protocol; // SL: Favored downstream protocol.
    struct timeval downstream_timeout;  // SL: Fields of 0 mean no timeout.
    struct timeval wait_queue_timeout;  // PL: Fields of 0 mean no timeout.

    uint32_t front_cache_max;         // PL: Max # of front cachable items.
    uint32_t front_cache_lifespan;    // PL: In millisecs.
    char     front_cache_spec[300];   // PL: Matcher prefixes for front caching.
    char     front_cache_unspec[100]; // PL: Don't front cache prefixes.

    uint32_t key_stats_max;         // PL: Max # of key stats entries.
    uint32_t key_stats_lifespan;    // PL: In millisecs.
    char     key_stats_spec[300];   // PL: Matcher prefixes for key-level stats.
    char     key_stats_unspec[100]; // PL: Don't key stat prefixes.

    char optimize_set[400]; // PL: Matcher prefixes for SET optimization.

    char usr[250];    // SL.
    char pwd[900];    // SL.
    char host[250];   // SL.
    int  port;        // SL.
    char bucket[250]; // SL.

    // ML: Port for proxy_main to listen on.
    //
    int port_listen;
};

struct proxy_behavior_pool {
    proxy_behavior  base; // Proxy pool-level (PL) behavior.
    int             num;  // Number of server-level (SL) behaviors.
    proxy_behavior *arr;  // Array, size is num.
};

/* Structure used and owned by main listener thread to
 * track all the outstanding proxy objects.
 */
struct proxy_main {
    proxy_behavior behavior; // Default, main listener modifiable only.

    // Start of proxy list.  Only the main listener thread
    // should access or modify this field.
    //
    proxy *proxy_head;

    int nthreads; // Immutable.

    // Updated by main listener thread only,
    // so no extra locking needed.
    //
    uint64_t stat_configs;
    uint64_t stat_config_fails;
    uint64_t stat_proxy_starts;
    uint64_t stat_proxy_start_fails;
    uint64_t stat_proxy_existings;
    uint64_t stat_proxy_shutdowns;
};

/* Owned by main listener thread.
 */
struct proxy {
    int   port;   // Immutable.
    char *name;   // Mutable, covered by proxy_lock, for debugging, NULL-able.
    char *config; // Mutable, covered by proxy_lock, mem owned by proxy,
                  // might be NULL if the proxy is shutting down.

    // Mutable, covered by proxy_lock, incremented
    // whenever config changes.
    //
    uint32_t config_ver;

    // Mutable, covered by proxy_lock.
    //
    proxy_behavior_pool behavior_pool;

    // Any thread that accesses the mutable fields should
    // first acquire the proxy_lock.
    //
    pthread_mutex_t proxy_lock;

    // Number of listening conn's acting as a proxy,
    // where (((proxy *) conn->extra) == this).
    // Modified/accessed only by main listener thread.
    //
    uint64_t listening;
    uint64_t listening_failed; // When server_socket() failed.

    proxy *next; // Modified/accessed only by main listener thread.

    mcache  front_cache;
    matcher front_cache_matcher;
    matcher front_cache_unmatcher;

    matcher optimize_set_matcher;

    proxy_td *thread_data;     // Immutable.
    int       thread_data_num; // Immutable.
};

struct proxy_stats {
    // Naming convention is that num_xxx's go up and down,
    // while tot_xxx's and err_xxx's only increase.  Only
    // the tot_xxx's and err_xxx's can be reset to 0.
    //
    uint64_t num_upstream; // Current # of upstreams conns using this proxy.
    uint64_t tot_upstream; // Total # upstream conns that used this proxy.

    uint64_t num_downstream_conn;
    uint64_t tot_downstream_conn;
    uint64_t tot_downstream_released;
    uint64_t tot_downstream_reserved;
    uint64_t tot_downstream_freed;
    uint64_t tot_downstream_quit_server;
    uint64_t tot_downstream_max_reached;
    uint64_t tot_downstream_create_failed;
    uint64_t tot_downstream_connect;
    uint64_t tot_downstream_connect_failed;
    uint64_t tot_downstream_auth;
    uint64_t tot_downstream_auth_failed;
    uint64_t tot_downstream_bucket;
    uint64_t tot_downstream_bucket_failed;
    uint64_t tot_downstream_propagate_failed;
    uint64_t tot_downstream_close_on_upstream_close;
    uint64_t tot_downstream_timeout;
    uint64_t tot_wait_queue_timeout;
    uint64_t tot_assign_downstream;
    uint64_t tot_assign_upstream;
    uint64_t tot_assign_recursion;
    uint64_t tot_reset_upstream_avail;
    uint64_t tot_retry;
    uint64_t tot_multiget_keys;
    uint64_t tot_multiget_keys_dedupe;
    uint64_t tot_multiget_bytes_dedupe;
    uint64_t tot_optimize_sets;
    uint64_t tot_optimize_self;
    uint64_t err_oom;
    uint64_t err_upstream_write_prep;
    uint64_t err_downstream_write_prep;
};

typedef struct {
    uint64_t seen;        // Number of times a command was seen.
    uint64_t hits;        // Number of hits or successes.
    uint64_t misses;      // Number of misses or failures.
    uint64_t read_bytes;  // Total bytes read, incoming into proxy.
    uint64_t write_bytes; // Total bytes written, outgoing from proxy.
    uint64_t cas;         // Number that had or required cas-id.
} proxy_stats_cmd;

typedef enum {
    STATS_CMD_GET = 0, // For each "get" cmd, even if multikey get.
    STATS_CMD_GET_KEY, // For each key in a "get".
    STATS_CMD_SET,
    STATS_CMD_ADD,
    STATS_CMD_REPLACE,
    STATS_CMD_DELETE,
    STATS_CMD_APPEND,
    STATS_CMD_PREPEND,
    STATS_CMD_INCR,
    STATS_CMD_DECR,
    STATS_CMD_FLUSH_ALL,
    STATS_CMD_CAS,
    STATS_CMD_STATS,
    STATS_CMD_STATS_RESET,
    STATS_CMD_VERSION,
    STATS_CMD_VERBOSITY,
    STATS_CMD_QUIT,
    STATS_CMD_ERROR,
    STATS_CMD_last
} enum_stats_cmd;

typedef enum {
    STATS_CMD_TYPE_REGULAR = 0,
    STATS_CMD_TYPE_QUIET,
    STATS_CMD_TYPE_last
} enum_stats_cmd_type;

typedef struct {
    proxy_stats     stats;
    proxy_stats_cmd stats_cmd[STATS_CMD_TYPE_last][STATS_CMD_last];
} proxy_stats_td;

struct key_stats {
    char key[KEY_MAX_LENGTH + 1];
    int  refcount;
    uint32_t exptime;
    uint32_t added_at;
    key_stats *next;
    key_stats *prev;
    proxy_stats_cmd stats_cmd[STATS_CMD_TYPE_last][STATS_CMD_last];
};

/* We mirror memcached's threading model with a separate
 * proxy_td (td means "thread data") struct owned by each
 * worker thread.  The idea is to avoid extraneous locks.
 */
struct proxy_td { // Per proxy, per worker-thread data struct.
    proxy *proxy; // Immutable parent pointer.

    // Snapshot of proxy-level configuration to avoid locks.
    //
    char    *config;
    uint32_t config_ver;

    proxy_behavior_pool behavior_pool;

    // Upstream conns that are paused, waiting for
    // an available, released downstream.
    //
    conn *waiting_any_downstream_head;
    conn *waiting_any_downstream_tail;

    downstream *downstream_reserved; // Downstreams assigned to upstreams.
    downstream *downstream_released; // Downstreams unassigned to upstreams.
    uint64_t    downstream_tot;      // Total lifetime downstreams created.
    int         downstream_num;      // Number downstreams existing.
    int         downstream_max;      // Max downstream concurrency number.
    uint64_t    downstream_assigns;  // Track recursion.

    // A timeout for the wait_queue, so that we can emit error
    // on any upstream conn's that are waiting too long for
    // an available downstream.
    //
    // Timeout is in use when timeout_tv fields are non-zero.
    //
    struct timeval timeout_tv;
    struct event   timeout_event;

    mcache  key_stats;
    matcher key_stats_matcher;
    matcher key_stats_unmatcher;

    proxy_stats_td stats;
};

/* Owned by worker thread.
 */
struct downstream {
    // The following group of fields are immutable or read-only (RO),
    // except for config_ver, which gets updated if the downstream's
    // config/behaviors still matches the parent ptd's config/behaviors.
    //
    proxy_td       *ptd;           // RO: Parent pointer.
    char           *config;        // RO: Mem owned by downstream.
    uint32_t        config_ver;    // RW: Mutable, copy of proxy->config_ver.
    int             behaviors_num; // RO: Snapshot of ptd->behavior_pool.num.
    proxy_behavior *behaviors_arr; // RO: Snapshot of ptd->behavior_pool.arr.
    memcached_st    mst;           // RW: From libmemcached.

    downstream *next;         // To track reserved/free lists.

    // Immutable function pointer that determines how we propagate
    // an upstream request to a downstream.  Eg, ascii vs binary,
    // replicating or not, etc.
    //
    // TODO: Move propagate func ptr to a per-downstream-conn
    // level, so we can have non-uniform downstream conns.
    // For example, some downstream conn's are ascii, some binary.
    //
    bool (*propagate)(downstream *d);

    conn **downstream_conns;  // Wraps the fd's of mst with conns.
    int    downstream_used;   // Number of in-use downstream conns, might
                              // be >1 during scatter-gather commands.
    int    downstream_used_start;
    conn  *upstream_conn;     // Non-NULL when downstream is reserved.
    char  *upstream_suffix;   // Last bit to write when downstreams are done.

    genhash_t *multiget; // Keyed by string.
    genhash_t *merger;   // Keyed by string, for merging replies like STATS.

    // Timeout is in use when timeout_tv fields are non-zero.
    //
    struct timeval timeout_tv;
    struct event   timeout_event;
};

// Functions.
//
proxy *cproxy_create(char    *name,
                     int      port,
                     char    *config,
                     uint32_t config_ver,
                     proxy_behavior_pool *behavior_pool,
                     int nthreads);

int cproxy_listen(proxy *p);
int cproxy_listen_port(int port,
                       enum protocol protocol,
                       enum network_transport transport,
                       void       *conn_extra,
                       conn_funcs *conn_funcs);

proxy_td *cproxy_find_thread_data(proxy *p, pthread_t thread_id);
void      cproxy_init_upstream_conn(conn *c);
void      cproxy_init_downstream_conn(conn *c);
void      cproxy_on_close_upstream_conn(conn *c);
void      cproxy_on_close_downstream_conn(conn *c);
void      cproxy_on_pause_downstream_conn(conn *c);

void cproxy_add_downstream(proxy_td *ptd);
void cproxy_free_downstream(downstream *d);

downstream *cproxy_create_downstream(char *config,
                                     uint32_t config_ver,
                                     proxy_behavior_pool *behavior_pool);

downstream *cproxy_reserve_downstream(proxy_td *ptd);
bool        cproxy_release_downstream(downstream *d, bool force);
void        cproxy_release_downstream_conn(downstream *d, conn *c);
bool        cproxy_check_downstream_config(downstream *d);

int   cproxy_connect_downstream(downstream *d,
                                LIBEVENT_THREAD *thread);
conn *cproxy_connect_downstream_conn(downstream *d,
                                     LIBEVENT_THREAD *thread,
                                     memcached_server_st *msst,
                                     proxy_behavior *behavior);

void  cproxy_wait_any_downstream(proxy_td *ptd, conn *c);
void  cproxy_assign_downstream(proxy_td *ptd);

bool  cproxy_auth_downstream(memcached_server_st *server,
                             proxy_behavior *behavior);
bool  cproxy_bucket_downstream(memcached_server_st *server,
                               proxy_behavior *behavior);

void  cproxy_pause_upstream_for_downstream(proxy_td *ptd, conn *upstream);
conn *cproxy_find_downstream_conn(downstream *d, char *key, int key_length,
                                  bool *self);
int   cproxy_server_index(downstream *d, char *key, size_t key_length);
bool  cproxy_prep_conn_for_write(conn *c);
bool  cproxy_dettach_if_noreply(downstream *d, conn *uc);

void cproxy_reset_upstream(conn *uc);

void cproxy_process_upstream_ascii(conn *c, char *line);
void cproxy_process_upstream_ascii_nread(conn *c);

// ---------------------------------------------------------------
// a2a means ascii upstream, ascii downstream.
//
void cproxy_init_a2a(void);
void cproxy_process_a2a_downstream(conn *c, char *line);
void cproxy_process_a2a_downstream_nread(conn *c);

bool cproxy_forward_a2a_downstream(downstream *d);
bool cproxy_forward_a2a_multiget_downstream(downstream *d, conn *uc);
bool cproxy_forward_a2a_simple_downstream(downstream *d, char *command,
                                          conn *uc);
bool cproxy_forward_a2a_item_downstream(downstream *d, short cmd,
                                        item *it, conn *uc);
bool cproxy_broadcast_a2a_downstream(downstream *d, char *command,
                                     conn *uc, char *suffix);

// ---------------------------------------------------------------
// a2b means ascii upstream, binary downstream.
//
void cproxy_init_a2b(void);
void cproxy_process_a2b_downstream(conn *c);
void cproxy_process_a2b_downstream_nread(conn *c);

bool cproxy_forward_a2b_downstream(downstream *d);
bool cproxy_forward_a2b_multiget_downstream(downstream *d, conn *uc);
bool cproxy_forward_a2b_simple_downstream(downstream *d, char *command,
                                          conn *uc);
bool cproxy_forward_a2b_item_downstream(downstream *d, short cmd,
                                        item *it, conn *uc);
bool cproxy_broadcast_a2b_downstream(downstream *d,
                                     protocol_binary_request_header *req,
                                     int req_size,
                                     uint8_t *key,
                                     uint16_t keylen,
                                     uint8_t  extlen,
                                     conn *uc, char *suffix);

// ---------------------------------------------------------------

proxy_behavior cproxy_parse_behavior(char          *behavior_str,
                                     proxy_behavior behavior_default);

void cproxy_parse_behavior_key_val_str(char *key_val,
                                       proxy_behavior *behavior);

void cproxy_parse_behavior_key_val(char *key,
                                   char *val,
                                   proxy_behavior *behavior);

proxy_behavior *cproxy_copy_behaviors(int arr_size, proxy_behavior *arr);

bool cproxy_equal_behaviors(int x_size, proxy_behavior *x,
                            int y_size, proxy_behavior *y);
bool cproxy_equal_behavior(proxy_behavior *x,
                           proxy_behavior *y);

void cproxy_dump_behavior(proxy_behavior *b, char *prefix, int level);
void cproxy_dump_behavior_ex(proxy_behavior *b, char *prefix, int level,
                             void (*dump)(const void *dump_opaque,
                                          const char *prefix,
                                          const char *key,
                                          const char *buf),
                             const void *dump_opaque);
void cproxy_dump_behavior_stderr(const void *dump_opaque,
                                 const char *prefix,
                                 const char *key,
                                 const char *val);

// ---------------------------------------------------------------

void cproxy_upstream_ascii_item_response(item *it, conn *uc,
                                         int cas_emit);

struct timeval cproxy_get_downstream_timeout(downstream *d, conn *c);

bool cproxy_start_downstream_timeout(downstream *d, conn *c);
bool cproxy_start_wait_queue_timeout(proxy_td *ptd, conn *uc);

rel_time_t cproxy_realtime(const time_t exptime);

void cproxy_close_conn(conn *c);

void cproxy_reset_stats_td(proxy_stats_td *pstd);
void cproxy_reset_stats(proxy_stats *ps);
void cproxy_reset_stats_cmd(proxy_stats_cmd *sc);

// Multiget key de-duplication.
//
typedef struct multiget_entry multiget_entry;

struct multiget_entry {
    conn           *upstream_conn;
    uint32_t        opaque; // For binary protocol.
    uint64_t        hits;
    multiget_entry *next;
};

bool multiget_ascii_downstream(
    downstream *d, conn *uc,
    int (*emit_start)(conn *c, char *cmd, int cmd_len),
    int (*emit_skey)(conn *c, char *skey, int skey_len),
    int (*emit_end)(conn *c),
    mcache *front_cache);

void multiget_ascii_downstream_response(downstream *d, item *it);

void multiget_foreach_free(const void *key,
                           const void *value,
                           void *user_data);

void multiget_remove_upstream(const void *key,
                              const void *value,
                              void *user_data);

// Space or null terminated key funcs.
//
size_t skey_len(const char *key);
int    skey_hash(const void *v);
int    skey_equal(const void *v1, const void *v2);

extern struct hash_ops strhash_ops;
extern struct hash_ops skeyhash_ops;

void noop_free(void *v);

// Stats handling.
//
bool protocol_stats_merge_line(genhash_t *merger, char *line);

bool protocol_stats_merge_name_val(genhash_t *merger,
                                   char *prefix,
                                   int   prefix_len,
                                   char *name,
                                   int   name_len,
                                   char *val,
                                   int   val_len);

void protocol_stats_foreach_free(const void *key,
                                 const void *value,
                                 void *user_data);

void protocol_stats_foreach_write(const void *key,
                                  const void *value,
                                  void *user_data);

void cproxy_optimize_to_self(downstream *d, conn *uc,
                             char *command);

bool cproxy_optimize_set_ascii(downstream *d, conn *uc,
                               char *key, int key_len);

void cproxy_del_front_cache_key_ascii(downstream *d,
                                      char *command);

void cproxy_del_front_cache_key_ascii_response(downstream *d,
                                               char *response,
                                               char *command);

// Functions for the front cache.
//
void  mcache_init(mcache *m, bool multithreaded,
                  mcache_funcs *funcs, bool key_alloc);
void  mcache_start(mcache *m, uint32_t max);
bool  mcache_started(mcache *m);
void  mcache_stop(mcache *m);
void  mcache_reset_stats(mcache *m);
void *mcache_get(mcache *m, char *key, int key_len,
                 uint32_t curr_time);
void  mcache_set(mcache *m, void *it,
                 uint32_t exptime,
                 bool add_only,
                 bool mod_exptime_if_exists);
void  mcache_delete(mcache *m, char *key, int key_len);
void  mcache_flush_all(mcache *m, uint32_t msec_exp);

// Functions for key stats.
//
key_stats *find_key_stats(proxy_td *ptd, char *key, int key_len,
                          uint32_t msec_time);

void touch_key_stats(proxy_td *ptd, char *key, int key_len,
                     uint32_t msec_current_time,
                     enum_stats_cmd_type cmd_type,
                     enum_stats_cmd cmd,
                     int delta_seen,
                     int delta_hits,
                     int delta_misses,
                     int delta_read_bytes,
                     int delta_write_bytes);

void key_stats_add_ref(void *it);
void key_stats_dec_ref(void *it);

// TODO: The following generic items should be broken out into util file.
//
bool  add_conn_item(conn *c, item *it);
char *add_conn_suffix(conn *c);

size_t scan_tokens(char *command, token_t *tokens, const size_t max_tokens,
                   int *command_len);

char *nread_text(short x);

char *skipspace(char *s);
char *trailspace(char *s);
char *trimstr(char *s);
char *trimstrdup(char *s);
bool  wordeq(char *s, char *word);

#endif // CPROXY_H
