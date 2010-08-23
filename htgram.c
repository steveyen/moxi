/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "htgram.h"

struct htgram_bin_st {
    int64_t start;
    int64_t width;
    uint64_t count;
};

struct htgram_st {
    int64_t bin_start;
    int64_t bin_start_width;
    double  bin_width_growth;
    size_t  num_bins;

    struct htgram_bin_st *bins;

    uint64_t lt_count; // For data points < the bins.
    uint64_t gt_count; // For data points > the bins.

    // For data points > the bins, there may be another
    // histogram instead of using gt_count.
    //
    HTGRAM_HANDLE next;
};

HTGRAM_HANDLE htgram_mk(int64_t bin_start,
                        int64_t bin_start_width,
                        double  bin_width_growth,
                        size_t  num_bins,
                        HTGRAM_HANDLE next) {
    struct htgram_st *h = calloc(sizeof(struct htgram_st), 1);
    if (h == NULL) {
        return NULL;
    }

    h->bin_start = bin_start;
    h->bin_start_width = bin_start_width;
    h->bin_width_growth = bin_width_growth;
    h->num_bins = num_bins;
    h->next = next;

    if (num_bins > 0) {
        h->bins = calloc(sizeof(struct htgram_bin_st), num_bins);
        if (h->bins == NULL) {
            free(h);
            return NULL;
        }

        int64_t r = bin_start;
        int64_t w = bin_start_width;
        for (size_t i = 0; i < num_bins; i++) {
            h->bins[i].start = r;
            h->bins[i].width = w;
            r = r + w;
            w = w * bin_width_growth;
        }
    }

    return h;
}

void htgram_destroy(HTGRAM_HANDLE h) {
    if (h->bins != NULL) {
        free(h->bins);
    }
    if (h->next != NULL) {
        htgram_destroy(h->next);
    }
    free(h);
}

int64_t htgram_get_bin_start(HTGRAM_HANDLE h) {
    return h->bin_start;
}

int64_t htgram_get_bin_start_width(HTGRAM_HANDLE h) {
    return h->bin_start_width;
}

double htgram_get_bin_width_growth(HTGRAM_HANDLE h) {
    return h->bin_width_growth;
}

size_t htgram_get_num_bins(HTGRAM_HANDLE h) {
    return h->num_bins;
}

void htgram_incr(HTGRAM_HANDLE h, int64_t data_point, uint64_t count) {
    if (data_point < h->bin_start) {
        h->lt_count += count;
        return;
    }

    size_t i = 0;

    if (h->bin_width_growth == 1.0) {
        i = data_point / h->bin_start_width;
    }

    while (i < h->num_bins) {
        if (data_point < (h->bins[i].start +
                          h->bins[i].width)) {
            h->bins[i].count += count;
            return;
        }

        i++;
    }

    if (h->next != NULL) {
        htgram_incr(h->next, data_point, count);
        return;
    }

    h->gt_count += count;
}

bool htgram_get_bin_data(HTGRAM_HANDLE h, int bin_index,
                         int64_t *out_bin_start,
                         int64_t *out_bin_width,
                         uint64_t *out_bin_count) {
    if (bin_index < 0) {
        *out_bin_count = h->lt_count;
        return false;
    }

    if (bin_index >= (int) h->num_bins) {
        if (h->next != NULL) {
            return htgram_get_bin_data(h->next, bin_index - h->num_bins,
                                       out_bin_start,
                                       out_bin_width,
                                       out_bin_count);
        }

        *out_bin_count = h->gt_count;
        return false;
    }

    *out_bin_start = h->bins[bin_index].start;
    *out_bin_width = h->bins[bin_index].width;
    *out_bin_count = h->bins[bin_index].count;

    return true;
}

void htgram_reset(HTGRAM_HANDLE h) {
    for (size_t i = 0; i < h->num_bins; i++) {
        h->bins[i].count = 0;
    }

    h->lt_count = 0;
    h->gt_count = 0;

    if (h->next != NULL) {
        htgram_reset(h->next);
    }
}

void htgram_add(HTGRAM_HANDLE agg, HTGRAM_HANDLE x) {
    int64_t  astart;
    int64_t  awidth;
    uint64_t acount;

    int64_t  xstart;
    int64_t  xwidth;
    uint64_t xcount;

    int i = 0;

    while (htgram_get_bin_data(agg, i, &astart, &awidth, &acount) &&
           htgram_get_bin_data(x,   i, &xstart, &xwidth, &xcount)) {
        assert(astart == xstart);
        assert(awidth == xwidth);

        htgram_incr(agg, astart, xcount);

        i++;
    }
}
