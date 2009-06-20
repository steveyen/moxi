/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef WORK_H
#define WORK_H

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <stdbool.h>
#include <stdint.h>
#include <event.h>

typedef struct work_item   work_item;
typedef struct work_queue  work_queue;
typedef struct work_collect work_collect;

struct work_item {
    void      (*func)(void *data0, void *data1);
    void       *data0;
    void       *data1;
    work_item  *next;
};

struct work_queue {
    int send_fd; // Pipe to notify thread.
    int recv_fd;

    work_item *work_head;
    work_item *work_tail;

    uint64_t num_items; // Current number of items in queue.
    uint64_t tot_sends;
    uint64_t tot_recvs;

    struct event_base *event_base;
    struct event       event;

    pthread_mutex_t work_lock;
};

struct work_collect {
    int count;

    void *data;

    pthread_mutex_t collect_lock;
    pthread_cond_t  collect_cond; // Signaled when count drops to 0.
};

bool work_queue_init(work_queue *m, struct event_base *base);

bool work_send(work_queue *m,
               void (*func)(void *data0, void *data1),
               void *data0, void *data1);

void work_recv(int fd, short which, void *arg);

void work_collect_init(work_collect *c, int count, void *data);
void work_collect_wait(work_collect *c);
void work_collect_count(work_collect *c, int count);
void work_collect_one(work_collect *c);

#endif // WORK_H
