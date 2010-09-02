/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Thread management for memcached.
 */
#include "memcached.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include "log.h"

#define ITEMS_PER_ALLOC 64

extern struct hash_ops strhash_ops;
extern struct hash_ops skeyhash_ops;

/* An item in the connection queue. */
typedef struct conn_queue_item CQ_ITEM;
struct conn_queue_item {
    int               sfd;
    enum conn_states  init_state;
    int               event_flags;
    int               read_buffer_size;
    enum protocol     protocol;
    enum network_transport     transport;
    conn_funcs       *funcs;
    void             *extra;
    CQ_ITEM          *next;
};

/* A connection queue. */
typedef struct conn_queue CQ;
struct conn_queue {
    CQ_ITEM *head;
    CQ_ITEM *tail;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
};

/* Lock for cache operations (item_*, assoc_*) */
pthread_mutex_t cache_lock;

/* Connection lock around accepting new connections */
pthread_mutex_t conn_lock = PTHREAD_MUTEX_INITIALIZER;

/* Lock for global stats */
static pthread_mutex_t stats_lock;

/* Free list of CQ_ITEM structs */
static CQ_ITEM *cqi_freelist;
static pthread_mutex_t cqi_freelist_lock;

/*
 * Each libevent instance has a wakeup pipe, which other threads
 * can use to signal that they've put a new connection on its queue.
 */
static LIBEVENT_THREAD *threads;

/*
 * Number of threads that have finished setting themselves up.
 */
static int init_count = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;


static void thread_libevent_process(int fd, short which, void *arg);

/*
 * Initializes a connection queue.
 */
static void cq_init(CQ *cq) {
    pthread_mutex_init(&cq->lock, NULL);
    pthread_cond_init(&cq->cond, NULL);
    cq->head = NULL;
    cq->tail = NULL;
}

/*
 * Looks for an item on a connection queue, but doesn't block if there isn't
 * one.
 * Returns the item, or NULL if no item is available
 */
static CQ_ITEM *cq_pop(CQ *cq) {
    CQ_ITEM *cq_item;

    pthread_mutex_lock(&cq->lock);
    cq_item = cq->head;
    if (NULL != cq_item) {
        cq->head = cq_item->next;
        if (NULL == cq->head)
            cq->tail = NULL;
    }
    pthread_mutex_unlock(&cq->lock);

    return cq_item;
}

/*
 * Adds an item to a connection queue.
 */
static void cq_push(CQ *cq, CQ_ITEM *cq_item) {
    cq_item->next = NULL;

    pthread_mutex_lock(&cq->lock);
    if (NULL == cq->tail)
        cq->head = cq_item;
    else
        cq->tail->next = cq_item;
    cq->tail = cq_item;
    pthread_cond_signal(&cq->cond);
    pthread_mutex_unlock(&cq->lock);
}

/*
 * Returns a fresh connection queue item.
 */
static CQ_ITEM *cqi_new(void) {
    CQ_ITEM *cq_item = NULL;
    pthread_mutex_lock(&cqi_freelist_lock);
    if (cqi_freelist) {
        cq_item = cqi_freelist;
        cqi_freelist = cq_item->next;
    }
    pthread_mutex_unlock(&cqi_freelist_lock);

    if (NULL == cq_item) {
        int i;

        /* Allocate a bunch of items at once to reduce fragmentation */
        cq_item = malloc(sizeof(CQ_ITEM) * ITEMS_PER_ALLOC);
        if (NULL == cq_item)
            return NULL;

        /*
         * Link together all the new items except the first one
         * (which we'll return to the caller) for placement on
         * the freelist.
         */
        for (i = 2; i < ITEMS_PER_ALLOC; i++)
            cq_item[i - 1].next = &cq_item[i];

        pthread_mutex_lock(&cqi_freelist_lock);
        cq_item[ITEMS_PER_ALLOC - 1].next = cqi_freelist;
        cqi_freelist = &cq_item[1];
        pthread_mutex_unlock(&cqi_freelist_lock);
    }

    return cq_item;
}


/*
 * Frees a connection queue item (adds it to the freelist.)
 */
static void cqi_free(CQ_ITEM *cq_item) {
    pthread_mutex_lock(&cqi_freelist_lock);
    cq_item->next = cqi_freelist;
    cqi_freelist = cq_item;
    pthread_mutex_unlock(&cqi_freelist_lock);
}


/*
 * Creates a worker thread.
 */
static void create_worker(void *(*func)(void *), void *arg) {
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;

    pthread_attr_init(&attr);

    if ((ret = pthread_create(&thread, &attr, func, arg)) != 0) {
        moxi_log_write("Can't create thread: %s\n",
                strerror(ret));
        exit(1);
    }
}

/*
 * Sets whether or not we accept new connections.
 */
void accept_new_conns(const bool do_accept) {
    pthread_mutex_lock(&conn_lock);
    do_accept_new_conns(do_accept);
    pthread_mutex_unlock(&conn_lock);
}
/****************************** LIBEVENT THREADS *****************************/

/*
 * Set up a thread's information.
 */
static void setup_thread(LIBEVENT_THREAD *me) {
    if (! me->base) {
        me->base = event_init();
        if (! me->base) {
            moxi_log_write("Can't allocate event base\n");
            exit(1);
        }
    }

    /* Listen for notifications from other threads */
    event_set(&me->notify_event, me->notify_receive_fd,
              EV_READ | EV_PERSIST, thread_libevent_process, me);
    event_base_set(me->base, &me->notify_event);

    if (event_add(&me->notify_event, 0) == -1) {
        moxi_log_write("Can't monitor libevent notify pipe\n");
        exit(1);
    }

    me->new_conn_queue = malloc(sizeof(struct conn_queue));
    if (me->new_conn_queue == NULL) {
        perror("Failed to allocate memory for connection queue");
        exit(EXIT_FAILURE);
    }
    cq_init(me->new_conn_queue);

    // TODO: Merge new_conn_queue with work_queue.
    //
    me->work_queue = calloc(1, sizeof(work_queue));
    if (me->work_queue == NULL) {
        perror("Failed to allocate memory for work queue");
        exit(EXIT_FAILURE);
    }
    work_queue_init(me->work_queue, me->base);

    if (pthread_mutex_init(&me->stats.mutex, NULL) != 0) {
        perror("Failed to initialize mutex");
        exit(EXIT_FAILURE);
    }

    me->suffix_cache = cache_create("suffix", SUFFIX_SIZE, sizeof(char*),
                                    NULL, NULL);
    if (me->suffix_cache == NULL) {
        moxi_log_write("Failed to create suffix cache\n");
        exit(EXIT_FAILURE);
    }

    me->conn_hash = genhash_init(512, strhash_ops);
    if (me->conn_hash == NULL) {
        moxi_log_write("Failed to create connection hash\n");
        exit(EXIT_FAILURE);
    }
}


/*
 * Worker thread: main event loop
 */
static void *worker_libevent(void *arg) {
    LIBEVENT_THREAD *me = arg;

    /* Any per-thread setup can happen here; thread_init() will block until
     * all threads have finished initializing.
     */
    me->thread_id = pthread_self();
#ifndef WIN32
    if (settings.verbose > 1)
        moxi_log_write("worker_libevent thread_id %ld\n", (long)me->thread_id);
#endif

    pthread_mutex_lock(&init_lock);
    init_count++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);

    event_base_loop(me->base, 0);
    return NULL;
}


/*
 * Processes an incoming "handle a new connection" item. This is called when
 * input arrives on the libevent wakeup pipe.
 */
static void thread_libevent_process(int fd, short which, void *arg) {
    LIBEVENT_THREAD *me = arg;
    CQ_ITEM *cq_item;
    char buf[1];

    (void)which;

    if (read(fd, buf, 1) != 1)
        if (settings.verbose > 0)
            moxi_log_write("Can't read from libevent pipe\n");

    cq_item = cq_pop(me->new_conn_queue);

    if (NULL != cq_item) {
        conn *c = conn_new(cq_item->sfd, cq_item->init_state, cq_item->event_flags,
                           cq_item->read_buffer_size,
                           cq_item->transport,
                           me->base,
                           cq_item->funcs, cq_item->extra);
        if (c == NULL) {
            if (IS_UDP(cq_item->transport)) {
                moxi_log_write("Can't listen for events on UDP socket\n");
                exit(1);
            } else {
                if (settings.verbose > 0) {
                    moxi_log_write("Can't listen for events on fd %d\n",
                        cq_item->sfd);
                }
                close(cq_item->sfd);
            }
        } else {
            c->protocol = cq_item->protocol;
            c->thread = me;
        }
        cqi_free(cq_item);
    }
}

/* Which thread we assigned a connection to most recently. */
static int last_thread = 0;

/*
 * Dispatches a new connection to another thread. This is only ever called
 * from the main thread, either during initialization (for UDP) or because
 * of an incoming connection.
 */
void dispatch_conn_new(int sfd, enum conn_states init_state, int event_flags,
                       int read_buffer_size,
                       enum protocol prot,
                       enum network_transport transport,
                       conn_funcs *funcs, void *extra) {
    int tid = last_thread % (settings.num_threads - 1);

    /* Skip the dispatch thread (0) */
    tid++;

    last_thread = tid;

    dispatch_conn_new_to_thread(tid, sfd, init_state, event_flags,
                                read_buffer_size,
                                prot,
                                transport,
                                funcs, extra);
}

void dispatch_conn_new_to_thread(int tid, int sfd, enum conn_states init_state,
                                 int event_flags, int read_buffer_size,
                                 enum protocol prot,
                                 enum network_transport transport,
                                 conn_funcs *funcs, void *extra) {
    assert(tid > 0);
    assert(tid < settings.num_threads);

    LIBEVENT_THREAD *thread = threads + tid;

    CQ_ITEM *cq_item = cqi_new();

    cq_item->sfd = sfd;
    cq_item->init_state = init_state;
    cq_item->event_flags = event_flags;
    cq_item->read_buffer_size = read_buffer_size;
    cq_item->protocol = prot;
    cq_item->transport = transport;
    cq_item->funcs = funcs;
    cq_item->extra = extra;

    cq_push(thread->new_conn_queue, cq_item);

    MEMCACHED_CONN_DISPATCH(sfd, thread->thread_id);
    if (write(thread->notify_send_fd, "", 1) != 1) {
        perror("Writing to thread notify pipe");
    }
}

static bool compare_pthread_t(pthread_t a, pthread_t b) {
#ifdef WIN32
    return a.p == b.p && a.x == b.x;
#else
    return a == b;
#endif
}


/*
 * Returns true if this is the thread that listens for new TCP connections.
 */
int is_listen_thread() {
    return compare_pthread_t(pthread_self(), threads[0].thread_id);
}

int thread_index(pthread_t thread_id) {
    for (int i = 0; i < settings.num_threads; i++) {
        if (compare_pthread_t(threads[i].thread_id, thread_id)) {
            return i;
        }
    }
    return -1;
}

LIBEVENT_THREAD *thread_by_index(int i) {
    return &threads[i];
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
item *item_alloc(char *key, size_t nkey, int flags, rel_time_t exptime, int nbytes) {
#ifdef MOXI_ITEM_MALLOC
    // Skip past the lock, since we're using malloc.
    return do_item_alloc(key, nkey, flags, exptime, nbytes);
#else
    item *it;
    pthread_mutex_lock(&cache_lock);
    it = do_item_alloc(key, nkey, flags, exptime, nbytes);
    pthread_mutex_unlock(&cache_lock);
    return it;
#endif
}

/*
 * Returns an item if it hasn't been marked as expired,
 * lazy-expiring as needed.
 */
item *item_get(const char *key, const size_t nkey) {
    item *it;
    pthread_mutex_lock(&cache_lock);
    it = do_item_get(key, nkey);
    pthread_mutex_unlock(&cache_lock);
    return it;
}

/*
 * Links an item into the LRU and hashtable.
 */
int item_link(item *cq_item) {
    int ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_item_link(cq_item);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void item_remove(item *cq_item) {
#ifdef MOXI_ITEM_MALLOC
    // Skip past the lock, since we're using malloc.
    do_item_remove(cq_item);
#else
    pthread_mutex_lock(&cache_lock);
    do_item_remove(cq_item);
    pthread_mutex_unlock(&cache_lock);
#endif
}

/*
 * Replaces one item with another in the hashtable.
 * Unprotected by a mutex lock since the core server does not require
 * it to be thread-safe.
 */
int item_replace(item *old_it, item *new_it) {
    return do_item_replace(old_it, new_it);
}

/*
 * Unlinks an item from the LRU and hashtable.
 */
void item_unlink(item *cq_item) {
    pthread_mutex_lock(&cache_lock);
    do_item_unlink(cq_item);
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Moves an item to the back of the LRU queue.
 */
void item_update(item *cq_item) {
    pthread_mutex_lock(&cache_lock);
    do_item_update(cq_item);
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Does arithmetic on a numeric item value.
 */
enum delta_result_type add_delta(conn *c, item *cq_item, int incr,
                                 const int64_t delta, char *buf) {
    enum delta_result_type ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_add_delta(c, cq_item, incr, delta, buf);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
enum store_item_type store_item(item *cq_item, int comm, conn* c) {
    enum store_item_type ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_store_item(cq_item, comm, c);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Flushes expired items after a flush_all call
 */
void item_flush_expired() {
    pthread_mutex_lock(&cache_lock);
    do_item_flush_expired();
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Dumps part of the cache
 */
char *item_cachedump(unsigned int clsid, unsigned int limit, unsigned int *bytes) {
    char *ret;

    pthread_mutex_lock(&cache_lock);
    ret = do_item_cachedump(clsid, limit, bytes);
    pthread_mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Dumps statistics about slab classes
 */
void  item_stats(ADD_STAT add_stats, void *c) {
    pthread_mutex_lock(&cache_lock);
    do_item_stats(add_stats, c);
    pthread_mutex_unlock(&cache_lock);
}

/*
 * Dumps a list of objects of each size in 32-byte increments
 */
void  item_stats_sizes(ADD_STAT add_stats, void *c) {
    pthread_mutex_lock(&cache_lock);
    do_item_stats_sizes(add_stats, c);
    pthread_mutex_unlock(&cache_lock);
}

/******************************* GLOBAL STATS ******************************/

void STATS_LOCK() {
    pthread_mutex_lock(&stats_lock);
}

void STATS_UNLOCK() {
    pthread_mutex_unlock(&stats_lock);
}

void threadlocal_stats_reset(void) {
    int ii, sid;
    for (ii = 0; ii < settings.num_threads; ++ii) {
        pthread_mutex_lock(&threads[ii].stats.mutex);

        threads[ii].stats.get_cmds = 0;
        threads[ii].stats.get_misses = 0;
        threads[ii].stats.delete_misses = 0;
        threads[ii].stats.incr_misses = 0;
        threads[ii].stats.decr_misses = 0;
        threads[ii].stats.cas_misses = 0;
        threads[ii].stats.bytes_read = 0;
        threads[ii].stats.bytes_written = 0;
        threads[ii].stats.flush_cmds = 0;
        threads[ii].stats.conn_yields = 0;

        for(sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
            threads[ii].stats.slab_stats[sid].set_cmds = 0;
            threads[ii].stats.slab_stats[sid].get_hits = 0;
            threads[ii].stats.slab_stats[sid].delete_hits = 0;
            threads[ii].stats.slab_stats[sid].incr_hits = 0;
            threads[ii].stats.slab_stats[sid].decr_hits = 0;
            threads[ii].stats.slab_stats[sid].cas_hits = 0;
            threads[ii].stats.slab_stats[sid].cas_badval = 0;
        }

        pthread_mutex_unlock(&threads[ii].stats.mutex);
    }
}

void threadlocal_stats_aggregate(struct thread_stats *thread_stats) {
    int ii, sid;
    /* The struct contains a mutex, so I should probably not memset it.. */
    thread_stats->get_cmds = 0;
    thread_stats->get_misses = 0;
    thread_stats->delete_misses = 0;
    thread_stats->incr_misses = 0;
    thread_stats->decr_misses = 0;
    thread_stats->cas_misses = 0;
    thread_stats->bytes_written = 0;
    thread_stats->bytes_read = 0;
    thread_stats->flush_cmds = 0;
    thread_stats->conn_yields = 0;

    memset(thread_stats->slab_stats, 0,
           sizeof(struct slab_stats) * MAX_NUMBER_OF_SLAB_CLASSES);

    for (ii = 0; ii < settings.num_threads; ++ii) {
        pthread_mutex_lock(&threads[ii].stats.mutex);

        thread_stats->get_cmds += threads[ii].stats.get_cmds;
        thread_stats->get_misses += threads[ii].stats.get_misses;
        thread_stats->delete_misses += threads[ii].stats.delete_misses;
        thread_stats->decr_misses += threads[ii].stats.decr_misses;
        thread_stats->incr_misses += threads[ii].stats.incr_misses;
        thread_stats->cas_misses += threads[ii].stats.cas_misses;
        thread_stats->bytes_read += threads[ii].stats.bytes_read;
        thread_stats->bytes_written += threads[ii].stats.bytes_written;
        thread_stats->flush_cmds += threads[ii].stats.flush_cmds;
        thread_stats->conn_yields += threads[ii].stats.conn_yields;

        for (sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
            thread_stats->slab_stats[sid].set_cmds +=
                threads[ii].stats.slab_stats[sid].set_cmds;
            thread_stats->slab_stats[sid].get_hits +=
                threads[ii].stats.slab_stats[sid].get_hits;
            thread_stats->slab_stats[sid].delete_hits +=
                threads[ii].stats.slab_stats[sid].delete_hits;
            thread_stats->slab_stats[sid].decr_hits +=
                threads[ii].stats.slab_stats[sid].decr_hits;
            thread_stats->slab_stats[sid].incr_hits +=
                threads[ii].stats.slab_stats[sid].incr_hits;
            thread_stats->slab_stats[sid].cas_hits +=
                threads[ii].stats.slab_stats[sid].cas_hits;
            thread_stats->slab_stats[sid].cas_badval +=
                threads[ii].stats.slab_stats[sid].cas_badval;
        }

        pthread_mutex_unlock(&threads[ii].stats.mutex);
    }
}

void slab_stats_aggregate(struct thread_stats *thread_stats, struct slab_stats *out) {
    int sid;

    out->set_cmds = 0;
    out->get_hits = 0;
    out->delete_hits = 0;
    out->incr_hits = 0;
    out->decr_hits = 0;
    out->cas_hits = 0;
    out->cas_badval = 0;

    for (sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
        out->set_cmds += thread_stats->slab_stats[sid].set_cmds;
        out->get_hits += thread_stats->slab_stats[sid].get_hits;
        out->delete_hits += thread_stats->slab_stats[sid].delete_hits;
        out->decr_hits += thread_stats->slab_stats[sid].decr_hits;
        out->incr_hits += thread_stats->slab_stats[sid].incr_hits;
        out->cas_hits += thread_stats->slab_stats[sid].cas_hits;
        out->cas_badval += thread_stats->slab_stats[sid].cas_badval;
    }
}

/*
 * Initializes the thread subsystem, creating various worker threads.
 *
 * nthreads  Number of event handler threads to spawn
 * main_base Event base for main thread
 */
void thread_init(int nthreads, struct event_base *main_base) {
    int         i;
#ifdef WIN32
    struct sockaddr_in serv_addr;
    int sockfd;

    if ((sockfd = createLocalListSock(&serv_addr)) < 0)
        exit(1);
#endif

    pthread_mutex_init(&cache_lock, NULL);
    pthread_mutex_init(&stats_lock, NULL);

    pthread_mutex_init(&init_lock, NULL);
    pthread_cond_init(&init_cond, NULL);

    pthread_mutex_init(&cqi_freelist_lock, NULL);
    cqi_freelist = NULL;

    threads = calloc(nthreads, sizeof(LIBEVENT_THREAD));
    if (! threads) {
        perror("Can't allocate thread descriptors");
        exit(1);
    }

    threads[0].base = main_base;
    threads[0].thread_id = pthread_self();

    for (i = 0; i < nthreads; i++) {
        int fds[2];

#ifdef WIN32
        if (createLocalSocketPair(sockfd,fds,&serv_addr) == -1) {
            fprintf(stderr, "Can't create notify pipe: %s", strerror(errno));
            exit(1);
        }
#else
        if (pipe(fds)) {
            perror("Can't create notify pipe");
            exit(1);
        }
#endif

        threads[i].notify_receive_fd = fds[0];
        threads[i].notify_send_fd = fds[1];

        setup_thread(&threads[i]);
#ifdef WIN32
        if (i == (nthreads - 1)) {
            shutdown(sockfd, 2);
        }
#endif

    }

    /* Create threads after we've done all the libevent setup. */
    for (i = 1; i < nthreads; i++) {
        create_worker(worker_libevent, &threads[i]);
    }

    /* Wait for all the threads to set themselves up before returning. */
    pthread_mutex_lock(&init_lock);
    init_count++; /* main thread */
    while (init_count < nthreads) {
        pthread_cond_wait(&init_cond, &init_lock);
    }
    pthread_mutex_unlock(&init_lock);
}

