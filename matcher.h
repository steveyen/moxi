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

    // Statistics.
    //
    uint64_t *hits;   // Same size as patterns array, may be NULL.
    uint64_t  misses;

    int initted; // Last field.
} matcher;

void matcher_init(matcher *m);

matcher *matcher_clone(matcher *m, matcher *copy);

void matcher_add(matcher *m, char *pattern);

void matcher_remove(matcher *m, char *pattern);

void matcher_remove_all(matcher *m);

bool matcher_check(matcher *m, char *str, int str_len);

#endif /* MATCHER_H */

