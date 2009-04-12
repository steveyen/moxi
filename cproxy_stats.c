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

static void add_proxy_stats(proxy_stats *agg, proxy_stats *x);

static void request_stats(void *data0, void *data1);
static void collect_stats(void *data0, void *data1);

/* This callback is invoked by memagent on a memagent thread
 * when it wants proxy stats.
 *
 * We use the work_queues to retrieve the info, so that normal
 * runtime has fewer locks, at the cost of scatter/gather
 * complexity.
 */
void on_memagent_get_stats(void *userdata, void *opaque,
                           agent_add_stat add_stat) {
    proxy_main *m = userdata;
    assert(m);
    assert(m->nthreads > 1);

    LIBEVENT_THREAD *mthread = thread_by_index(0);
    assert(mthread);
    assert(mthread->work_queue);

    work_collect *ca = calloc(m->nthreads, sizeof(work_collect));
    if (ca != NULL) {
        for (int i = 1; i < m->nthreads; i++)
            ca[i].count = -1; // Prevent init race.

        if (work_send(mthread->work_queue, request_stats, m, ca)) {
            // Wait for all the stats gathering to finish.
            //
            for (int i = 1; i < m->nthreads; i++)
                work_collect_wait(&ca[i]);

            char buf[100];

#define more_stat(spec, key, val) \
    sprintf(buf, spec, val);      \
    add_stat(opaque, key, buf);

            more_stat("%u", "nthreads",               m->nthreads);
            more_stat("%u", "default_downstream_max", m->default_downstream_max);
        }

        free(ca);
    }

    add_stat(opaque, NULL, NULL);
}

/* Must be invoked on the main listener thread.
 */
static void request_stats(void *data0, void *data1) {
    proxy_main *m = data0;
    assert(m);
    assert(m->nthreads > 1);

    work_collect *ca = data1;
    assert(ca);

    assert(is_listen_thread());

    int sent   = 0;
    int nproxy = 0;

    for (proxy *p = m->proxy_head; p != NULL; p = p->next)
        nproxy++;

    // Starting at 1 because 0 is the main listen thread.
    //
    for (int i = 1; i < m->nthreads; i++) {
        work_collect *c = &ca[i];

        work_collect_init(c, nproxy, calloc(1, sizeof(proxy_stats)));

        LIBEVENT_THREAD *t = thread_by_index(i);
        assert(t);
        assert(t->work_queue);

        for (proxy *p = m->proxy_head; p != NULL; p = p->next) {
            proxy_td *ptd = &p->thread_data[i];
            if (ptd != NULL &&
                work_send(t->work_queue, collect_stats, ptd, c)) {
                sent++;
            }
        }
    }

    // TODO: If sent is too small, then some proxies were disabled?
    //       Need to decrement count?

    // TODO: Might want to block here until children are done,
    //       so that concurrent reconfigs don't cause issues.
    //
    // No need to wait for the worker threads to finish,
    // as the last worker thread will signal the collect_cond.
}

static void collect_stats(void *data0, void *data1) {
    proxy_td *ptd = data0;
    assert(ptd);

    work_collect *c = data1;
    assert(c);

    assert(is_listen_thread() == false); // Expecting a worker thread.

    add_proxy_stats((proxy_stats *) c->data, &ptd->stats);

    work_collect_one(c);
}

static void add_proxy_stats(proxy_stats *agg, proxy_stats *x) {
    assert(agg);
    assert(x);

    agg->num_upstream += x->num_upstream;
    agg->tot_upstream += x->tot_upstream;
    agg->tot_downstream_released += x->tot_downstream_released;
    agg->tot_downstream_reserved += x->tot_downstream_reserved;
}
