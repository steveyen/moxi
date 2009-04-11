/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <pthread.h>
#include <assert.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "memagent.h"
#include "cproxy.h"
#include "work.h"

typedef struct proxy_stats_collect proxy_stats_collect;

struct proxy_stats_collect {
    proxy_main     *m;         // Immutable.
    int             nreqs;     // Immutable.
    pthread_mutex_t done_lock;
    pthread_cond_t  done_cond; // Signaled when nreqs drops to 0.
    proxy_stats     proxy_stats;
};

void cproxy_request_stats(void *data0, void *data1);
void cproxy_gather_stats(void *data0, void *data1);

void cproxy_request_stats(void *data0, void *data1) {
    proxy_main *m = data0;
    assert(m);
    proxy_stats_collect *c = data1;
    assert(c);
    assert(is_listen_thread());

    if (!is_listen_thread())
        return;

    int    nproxy = 0;
    proxy *p;

    for (p = m->proxy_head; p != NULL; p = p->next)
        nproxy++;

    proxy_stats_collect *pc = calloc(nproxy, sizeof(proxy_stats_collect));
    if (pc != NULL) {
        for (p = m->proxy_head; p != NULL; p = p->next) {
            // Starting at 1 because 0 is the main listen thread.
            //
            for (int i = 1; i < p->thread_data_num; i++) {
                proxy_td *ptd = &p->thread_data[i];
                if (ptd != NULL) {
                    if (false
                        /*work_send(ptd->work_queue, cproxy_gather_stats,
                          p, c) */) {
                        if (c) {
                        }
                    }
                }
            }
        }
    }

    // Need to wait until done so that number of proxies
    // doesn't change on us, such as during a reconfig.
    //
}

void cproxy_gather_stats(void *data0, void *data1) {
    proxy *p = data0;
    assert(p);
    proxy_stats_collect *c = data1;
    assert(c);
    assert(is_listen_thread() == false); // Expecting a worker thread.

    if (p && c) {
    }
}

void on_memagent_get_stats(void *userdata, void *opaque,
                           agent_add_stat add_stat) {
    proxy_main *m = userdata;
    assert(m != NULL);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread != NULL);

    proxy_stats_collect *c = calloc(1, sizeof(proxy_stats_collect));
    if (c != NULL) {
        c->m     = m;
        c->nreqs = 0;

        pthread_mutex_init(&c->done_lock, NULL);
        pthread_cond_init(&c->done_cond, NULL);

        if (work_send(mthread->work_queue, cproxy_request_stats,
                      m, c)) {
            // Wait for all the stats gathering to finish.
            //
            pthread_mutex_lock(&c->done_lock);
            // while (c->thread < c->nthreads) {
            //     pthread_cond_wait(&c->done_cond, &c->done_lock);
            // }
            pthread_mutex_unlock(&c->done_lock);

            char buf[100];

#define more_stat(spec, key, val) \
    sprintf(buf, spec, val);      \
    add_stat(opaque, key, buf);

            more_stat("%u", "nthreads",               m->nthreads);
            more_stat("%u", "default_downstream_max", m->default_downstream_max);
        }

        free(c);
    }

    add_stat(opaque, NULL, NULL);
}

