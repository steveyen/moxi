/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef WORK_H
#define WORK_H

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <stdbool.h>
#include <stdint.h>
#include <event.h>

typedef struct work_item  work_item;
typedef struct work_queue work_queue;

struct work_item {
    void      (*func)(void *data0, void *data1);
    void       *data0;
    void       *data1;
    work_item  *next;
};

struct work_queue {
    int send_fd; // Pipe to notify thread.
    int recv_fd;

    struct event_base *event_base;
    struct event       event;

    work_item      *work_head;
    work_item      *work_tail;
    pthread_mutex_t work_lock;
};

bool work_queue_init(work_queue *m, struct event_base *base);

bool work_send(work_queue *m,
               void (*func)(void *data0, void *data1),
               void *data0, void *data1);

void work_recv(int fd, short which, void *arg);

#endif // WORK_H
