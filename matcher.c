/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <assert.h>
#include "matcher.h"

#define MATCHER_MAGIC 0xa135b21a
#define IS_INITTED(m) (m->initted == MATCHER_MAGIC)

void matcher_init(matcher *m) {
    assert(m);
    assert(!IS_INITTED(m));
    memset(m, 0, sizeof(matcher));
    m->initted = MATCHER_MAGIC;
}

matcher *matcher_clone(matcher *m, matcher *copy) {
    assert(m);
    assert(IS_INITTED(m));
    if (!IS_INITTED(m)) return NULL;
    assert(m->patterns_num <= m->patterns_max);

    assert(copy);
    assert(!IS_INITTED(copy));
    matcher_init(copy);

    copy->version = m->version;
    copy->patterns_max = m->patterns_num; // Optimize copy's array size.
    copy->patterns_num = m->patterns_num;

    if (copy->patterns_max > 0) {
        copy->patterns = calloc(copy->patterns_max, sizeof(char *));
        copy->hits     = calloc(copy->patterns_max, sizeof(uint64_t));
        if (copy->patterns != NULL &&
            copy->hits != NULL) {
            for (int i = 0; i < copy->patterns_num; i++) {
                assert(m->patterns[i]);
                copy->patterns[i] = strdup(m->patterns[i]);
                if (copy->patterns[i] == NULL)
                    goto fail;
            }
            return copy;
        }
    } else
        return copy;

 fail:
    for (int i = 0; copy->patterns && i < copy->patterns_num; i++) {
        free(copy->patterns[i]);
    }
    free(copy->patterns);
    free(copy->hits);
    memset(copy, 0, sizeof(matcher));
    return NULL;
}

void matcher_add(matcher *m, char *pattern) {
    assert(m);
    assert(IS_INITTED(m));
    if (!IS_INITTED(m)) return;
    assert(m->patterns_num <= m->patterns_max);
    // TODO.
}

void matcher_remove(matcher *m, char *pattern) {
    assert(m);
    assert(IS_INITTED(m));
    if (!IS_INITTED(m)) return;
    assert(m->patterns_num <= m->patterns_max);
    // TODO.
}

void matcher_remove_all(matcher *m) {
    assert(m);
    assert(IS_INITTED(m));
    if (!IS_INITTED(m)) return;
    assert(m->patterns_num <= m->patterns_max);
    // TODO.
}

bool matcher_check(matcher *m, char *str, int str_len) {
    assert(m);
    if (!IS_INITTED(m)) return false;
    assert(m->patterns_num <= m->patterns_max);
    // TODO.
    return false;
}


