/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef MATCHER_H
#define MATCHER_H

#include <stdint.h>
#include <stdbool.h>

typedef struct {
    // We have a simple implementation currently that
    // only supports simple string prefix matching, O(N^2).
    //
    // Number of changes to this matcher, for fast clone comparison.
    unsigned int version;

    int patterns_max; // Size of patterns array, may be 0.
    int patterns_num; // Number of active patterns, <= patterns_max.
    char **patterns;  // May be NULL.
    int   *lengths;   // May be NULL, same size as patterns array.

    // Statistics.
    //
    uint64_t *hits;   // May be NULL, same size as patterns array.
    uint64_t  misses;

    int initted; // Last field.
} matcher;

void     matcher_init(matcher *m, char *spec);
bool     matcher_initted(matcher *m);
void     matcher_uninit(matcher *m);
matcher *matcher_clone(matcher *m, matcher *copy);
bool     matcher_check(matcher *m, char *str, int str_len);

#endif /* MATCHER_H */

