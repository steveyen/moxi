/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>
#include <event.h>
#include "work.h"
#include "log.h"

#undef WORK_DEBUG

/** A work queue is a mechanism to allow thread-to-thread
 *  communication in a libevent-based, multithreaded system.
 *
 *  One thread can send work to another thread.  The receiving thread
 *  should be libevent-based, with a processing loop handled by
 *  libevent.
 *
 *  Use work_queue_init() to initialize a work_queue structure,
 *  where the work_queue structure memory is owned by the caller.
 *
 *  Returns true on success.
 */
bool work_queue_init(work_queue *m, struct event_base *event_base) {
    assert(m != NULL);

    memset(m, 0, sizeof(work_queue));

    pthread_mutex_init(&m->work_lock, NULL);

    m->work_head = NULL;
    m->work_tail = NULL;

    m->num_items = 0;
    m->tot_sends = 0;
    m->tot_recvs = 0;

    m->event_base = event_base;
    assert(m->event_base != NULL);

    int fds[2] = {0};
#ifdef WIN32
    struct sockaddr_in serv_addr;
    int sockfd;

    if ((sockfd = createLocalListSock(&serv_addr)) < 0 ||
        createLocalSocketPair(sockfd,fds,&serv_addr) == -1)
    {
        fprintf(stderr, "Can't create notify pipe: %s", strerror(errno));
        return false;
    }
#else
    if (pipe(fds)) {
        perror("Can't create notify pipe");
        return false;
    }
#endif

    m->recv_fd = fds[0];
    m->send_fd = fds[1];

    event_set(&m->event, m->recv_fd,
              EV_READ | EV_PERSIST, work_recv, m);
    event_base_set(m->event_base, &m->event);

    if (event_add(&m->event, 0) == 0) {
#ifdef WORK_DEBUG
            moxi_log_write("work_queue_init %x %x %x %d %d %u %llu\n",
                    (int) pthread_self(),
                    (int) m,
                    (int) m->event_base,
                    m->send_fd,
                    m->recv_fd,
                    m->work_head != NULL,
                    m->tot_sends);
#endif

        return true;
    }

#ifdef WORK_DEBUG
    moxi_log_write("work_queue_init error\n");
#endif

    return false;
}

/** Use work_send() to place work on another thread's work queue.
 *  The receiving thread will invoke the given function with
 *  the given callback data.
 *
 *  Returns true on success.
 */
bool work_send(work_queue *m,
               void (*func)(void *data0, void *data1),
               void *data0, void *data1) {
    assert(m != NULL);
    assert(m->recv_fd >= 0);
    assert(m->send_fd >= 0);
    assert(m->event_base != NULL);
    assert(func != NULL);

    bool rv = false;

    // TODO: Add a free-list of work_items.
    //
    work_item *w = calloc(1, sizeof(work_item));
    if (w != NULL) {
        w->func  = func;
        w->data0 = data0;
        w->data1 = data1;
        w->next  = NULL;

        pthread_mutex_lock(&m->work_lock);

        if (m->work_tail != NULL)
            m->work_tail->next = w;
        m->work_tail = w;
        if (m->work_head == NULL)
            m->work_head = w;

        if (write(m->send_fd, "", 1) == 1) {
            m->num_items++;
            m->tot_sends++;

#ifdef WORK_DEBUG
            moxi_log_write("work_send %x %x %x %d %d %d %llu %llu\n",
                    (int) pthread_self(),
                    (int) m,
                    (int) m->event_base,
                    m->send_fd, m->recv_fd,
                    m->work_head != NULL,
                    m->num_items,
                    m->tot_sends);
#endif

            rv = true;
        }

        pthread_mutex_unlock(&m->work_lock);
    }

    return rv;
}

/** Called by libevent, on the receiving thread, when
 *  there is work for the receiving thread to handle.
 */
void work_recv(int fd, short which, void *arg) {
    (void)which;

    work_queue *m = arg;
    assert(m != NULL);
    assert(m->recv_fd == fd);
    assert(m->send_fd >= 0);
    assert(m->event_base != NULL);

    work_item *curr = NULL;
    work_item *next = NULL;

    char buf[1];

    // The lock area includes the read() for safety,
    // as the pipe acts like a cond variable.
    //
    pthread_mutex_lock(&m->work_lock);

    int readrv = read(fd, buf, 1);
    assert(readrv == 1);
    if (readrv != 1) {
#ifdef WORK_DEBUG
        // Perhaps libevent called us in incorrect way.
        //
        moxi_log_write("unexpected work_recv read value\n");
#endif
    }

    curr = m->work_head;
    m->work_head = NULL;
    m->work_tail = NULL;

#ifdef WORK_DEBUG
    moxi_log_write("work_recv %x %x %x %d %d %d %llu %llu %d\n",
            (int) pthread_self(),
            (int) m,
            (int) m->event_base,
            m->send_fd, m->recv_fd,
            curr != NULL,
            m->num_items,
            m->tot_sends,
            fd);
#endif

    pthread_mutex_unlock(&m->work_lock);

    uint64_t num_items = 0;

    while (curr != NULL) {
        next = curr->next;
        num_items++;
        curr->func(curr->data0, curr->data1);
        free(curr);
        curr = next;
    }

    if (num_items > 0) {
        pthread_mutex_lock(&m->work_lock);

        m->tot_recvs += num_items;
        m->num_items -= num_items;
        assert(m->num_items >= 0);

        pthread_mutex_unlock(&m->work_lock);
    }
}

// ------------------------------------

/** The "work_collect" abstraction helps to make scatter/gather easier
 *  when using work queue's.  The main caller uses work_collect_init()
 *  to initialize the work_collect tracking data structure.  The
 *  work_collect structure is then scattered across worker threads
 *  (such as by using work_send()).  The main thread then calls
 *  work_collect_wait() to wait for N responses.  A worker thread
 *  invokes work_collect_one() when it's finished with its assigned
 *  work and has one response to contribute.  When N responses have
 *  been counted, work_collect_wait() returns control back to the
 *  main caller.
 */
void work_collect_init(work_collect *c, int count, void *data) {
    assert(c);

    memset(c, 0, sizeof(work_collect));

    c->count = count;
    c->data  = data;

    pthread_mutex_init(&c->collect_lock, NULL);
    pthread_cond_init(&c->collect_cond, NULL);
}

void work_collect_wait(work_collect *c) {
    pthread_mutex_lock(&c->collect_lock);
    while (c->count != 0) { // Can't test for > 0, due to -1 on init race.
        pthread_cond_wait(&c->collect_cond, &c->collect_lock);
    }
    pthread_mutex_unlock(&c->collect_lock);
}

void work_collect_count(work_collect *c, int count) {
    pthread_mutex_lock(&c->collect_lock);
    c->count = count;
    if (c->count <= 0)
        pthread_cond_signal(&c->collect_cond);
    pthread_mutex_unlock(&c->collect_lock);
}

void work_collect_one(work_collect *c) {
    pthread_mutex_lock(&c->collect_lock);
    assert(c->count >= 1);
    c->count--;
    if (c->count <= 0)
        pthread_cond_signal(&c->collect_cond);
    pthread_mutex_unlock(&c->collect_lock);
}
