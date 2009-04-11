/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>
#include <event.h>
#include "work.h"

bool work_queue_init(work_queue *m, struct event_base *event_base) {
    assert(m != NULL);

    pthread_mutex_init(&m->work_lock, NULL);
    m->work_head = NULL;
    m->work_tail = NULL;

    m->event_base = event_base;
    assert(m->event_base != NULL);

    int fds[2];

    if (pipe(fds) == 0) {
        m->recv_fd = fds[0];
        m->send_fd = fds[1];

        event_set(&m->event, m->recv_fd,
                  EV_READ | EV_PERSIST, work_recv, m);
        event_base_set(m->event_base, &m->event);

        if (event_add(&m->event, 0) == 0) {
            return true;
        }
    }

    return false;
}

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

        if (write(m->send_fd, "", 1) == 1)
            rv = true;

        pthread_mutex_unlock(&m->work_lock);
    }

    return rv;
}

void work_recv(int fd, short which, void *arg) {
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

    read(fd, buf, 1);

    curr = m->work_head;
    m->work_head = NULL;
    m->work_tail = NULL;

    pthread_mutex_unlock(&m->work_lock);

    while (curr != NULL) {
        next = curr->next;
        curr->func(curr->data0, curr->data1);
        free(curr);
        curr = next;
    }
}

