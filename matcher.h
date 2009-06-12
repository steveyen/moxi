/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef MATCHER_H
#define MATCHER_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

typedef struct {
    // We have a simple implementation currently that
    // only supports simple string prefix matching, O(N^2).
    //
    pthread_mutex_t *lock;

    int patterns_max; // Size of patterns array, may be 0.
    int patterns_num; // Number of active patterns, <= patterns_max.
    char **patterns;  // May be NULL.
    int   *lengths;   // May be NULL, same size as patterns array.

    // Statistics.
    //
    uint64_t *hits;   // May be NULL, same size as patterns array.
    uint64_t  misses;
} matcher;

void     matcher_init(matcher *m, bool multithreaded);
void     matcher_start(matcher *m, char *spec);
bool     matcher_started(matcher *m);
void     matcher_stop(matcher *m);
matcher *matcher_clone(matcher *m, matcher *copy);
bool     matcher_check(matcher *m, char *str, int str_len,
                       bool default_when_unstarted);

#endif /* MATCHER_H */

