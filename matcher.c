/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <assert.h>
#include "matcher.h"

void matcher_add(matcher *m, char *pattern);

void matcher_init(matcher *m, bool multithreaded) {
    assert(m);

    memset(m, 0, sizeof(matcher));

    if (multithreaded) {
        m->lock = malloc(sizeof(pthread_mutex_t));
        if (m->lock != NULL) {
            pthread_mutex_init(m->lock, NULL);
        }
    } else {
        m->lock = NULL;
    }
}

void matcher_start(matcher *m, char *spec) {
    assert(m);

    if (m->lock) {
        pthread_mutex_lock(m->lock);
    }

    // The spec currently is a string of '|' separated prefixes.
    //
    if (spec != NULL &&
        strlen(spec) > 0) {
        char *copy = strdup(spec);
        if (copy != NULL) {
            char *next = copy;
            while (next != NULL) {
                char *patt = strsep(&next, "|");
                if (patt != NULL) {
                    matcher_add(m, patt);
                }
            }
            free(copy);
        }
    }

    if (m->lock) {
        pthread_mutex_unlock(m->lock);
    }
}

bool matcher_started(matcher *m) {
    assert(m);

    if (m->lock) {
        pthread_mutex_lock(m->lock);
    }

    bool rv = m->patterns != NULL && m->patterns_num > 0;

    if (m->lock) {
        pthread_mutex_unlock(m->lock);
    }

    return rv;
}

void matcher_stop(matcher *m) {
    assert(m);

    if (m->lock) {
        pthread_mutex_lock(m->lock);
    }

    if (m->patterns != NULL) {
        for (int i = 0; i < m->patterns_num; i++) {
            free(m->patterns[i]);
        }
    }

    m->patterns_max = 0;
    m->patterns_num = 0;

    free(m->patterns);
    m->patterns = NULL;

    free(m->lengths);
    m->lengths = NULL;

    free(m->hits);
    m->hits = NULL;

    m->misses = 0;

    if (m->lock) {
        pthread_mutex_unlock(m->lock);
    }
}

matcher *matcher_clone(matcher *m, matcher *copy) {
    assert(m);

    if (m->lock) {
        pthread_mutex_lock(m->lock);
    }

    assert(m->patterns_num <= m->patterns_max);

    assert(copy);
    matcher_init(copy, m->lock != NULL);

    copy->patterns_max = m->patterns_num; // Optimize copy's array size.
    copy->patterns_num = m->patterns_num;

    if (copy->patterns_max > 0) {
        copy->patterns = calloc(copy->patterns_max, sizeof(char *));
        copy->lengths  = calloc(copy->patterns_max, sizeof(int));
        copy->hits     = calloc(copy->patterns_max, sizeof(uint64_t));
        if (copy->patterns != NULL &&
            copy->lengths != NULL &&
            copy->hits != NULL) {
            for (int i = 0; i < copy->patterns_num; i++) {
                assert(m->patterns[i]);
                copy->patterns[i] = strdup(m->patterns[i]);
                if (copy->patterns[i] == NULL) {
                    goto fail;
                }

                copy->lengths[i] = m->lengths[i];

                // Note we don't copy statistics.
            }

            if (m->lock)
                pthread_mutex_unlock(m->lock);

            return copy;
        }
    }

 fail:
    if (m->lock) {
        pthread_mutex_unlock(m->lock);
    }

    matcher_stop(copy);

    return NULL;
}

/** Assuming caller has m->lock already.
 */
void matcher_add(matcher *m, char *pattern) {
    assert(m);
    assert(m->patterns_num <= m->patterns_max);
    assert(pattern);

    int length = strlen(pattern);
    if (length <= 0) {
        return;
    }

    if (m->patterns_num >= m->patterns_max) {
        int    nmax = (m->patterns_num * 2) + 4; // 4 is slop when 0.
        char **npatterns = realloc(m->patterns, nmax * sizeof(char *));
        int   *nlengths  = realloc(m->lengths,  nmax * sizeof(int));
        uint64_t *nhits  = realloc(m->hits,     nmax * sizeof(uint64_t));
        if (npatterns != NULL &&
            nlengths != NULL &&
            nhits != NULL) {
            m->patterns_max = nmax;
            m->patterns     = npatterns;
            m->lengths      = nlengths;
            m->hits         = nhits;
        } else {
            free(npatterns);
            free(nlengths);
            free(nhits);

            return; // Failed to alloc.
        }
    }

    assert(m->patterns_num < m->patterns_max);

    m->patterns[m->patterns_num] = strdup(pattern);
    if (m->patterns[m->patterns_num] != NULL) {
        m->lengths[m->patterns_num] = strlen(pattern);
        m->patterns_num++;
    }
}

bool matcher_check(matcher *m, char *str, int str_len,
                   bool default_when_unstarted) {
    assert(m);

    bool found = false;

    if (m->lock) {
        pthread_mutex_lock(m->lock);
    }

    if (m->patterns != NULL &&
        m->patterns_num > 0) {
        assert(m->patterns_num <= m->patterns_max);

        for (int i = 0; i < m->patterns_num; i++) {
            assert(m->patterns);
            assert(m->lengths);
            assert(m->hits);

            int n = m->lengths[i];
            if (n <= str_len) {
                if (strncmp(str, m->patterns[i], n) == 0) {
                    m->hits[i]++;
                    found = true;
                }
            }
        }

        if (!found) {
            m->misses++;
        }
    } else {
        found = default_when_unstarted;
    }

    if (m->lock) {
        pthread_mutex_unlock(m->lock);
    }

    return found;
}

