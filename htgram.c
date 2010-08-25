/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <limits.h>

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

static void emit_bar(int64_t start, int64_t width, uint64_t count,
                     uint64_t max_count, uint64_t tot_count, uint64_t run_count,
                     char *buf, int buf_len,
                     int plus_spaces, int equal_spaces, int space_spaces);

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

void htgram_dump(HTGRAM_HANDLE h,
                 HTGRAM_DUMP_CALLBACK dump_callback, void *dump_callback_data) {
    if (h == NULL) {
        return;
    }

    int64_t  max_start = 0;
    int64_t  max_width = 0;
    uint64_t max_count = 0;
    int      end_num_bins = 0; // Max non-zero bin.
    int      beg_bin = INT_MAX;
    uint64_t tot_count = 0;
    uint64_t run_count; // Cummulative count.

    int64_t  start;
    int64_t  width;
    uint64_t count;
    int      num_bins = 0;

    while (htgram_get_bin_data(h, num_bins, &start, &width, &count)) {
        num_bins++;

        if (count > 0) {
            if (max_start < start) {
                max_start = start;
            }
            if (max_width < width) {
                max_width = width;
            }
            if (max_count < count) {
                max_count = count;
            }
            if (end_num_bins < num_bins) {
                end_num_bins = num_bins;
            }
            if (beg_bin > num_bins - 1) {
                beg_bin = num_bins - 1;
            }
            tot_count = tot_count + count;
        }
    }

    // Columns in a row look like "START+WIDTH=COUNT PCT% BAR_GRAPH_LINE"
    //
    int  max_plus = 0;  // Width of the 'plus' column ('+').
    int  max_equal = 0; // Width of the 'equal' column ('=').
    int  max_space = 0; // Width of the 'space' column (' ').

    char buf[2000];

    run_count = 0;
    for (int i = beg_bin; i < end_num_bins + 1 && i < num_bins; i++) {
        if (htgram_get_bin_data(h, i, &start, &width, &count)) {
            emit_bar(start, width, count, max_count, tot_count, run_count,
                     buf, sizeof(buf) - 1, 0, 0, 0);

            char *s0 = strchr(buf, '+');
            assert(s0 != NULL);
            if (max_plus < s0 - buf) {
                max_plus = s0 - buf;
            }
            char *s1 = strchr(s0, '=');
            assert(s1 != NULL);
            if (max_equal < s1 - s0) {
                max_equal = s1 - s0;
            }
            char *s2 = strchr(s1, '%');
            assert(s2 != NULL);
            if (max_space < s2 - s1) {
                max_space = s2 - s1;
            }

            run_count += count;
        }
    }

    run_count = 0;
    for (int i = beg_bin; i < end_num_bins + 1 && i < num_bins; i++) {
        if (htgram_get_bin_data(h, i, &start, &width, &count)) {
            emit_bar(start, width, count, max_count, tot_count, run_count,
                     buf, sizeof(buf) - 1, 0, 0, 0);

            char *s0 = strchr(buf, '+');
            char *s1 = strchr(s0, '=');
            char *s2 = strchr(s1, '%');

            emit_bar(start, width, count, max_count, tot_count, run_count,
                     buf, sizeof(buf) - 1,
                     max_plus - (s0 - buf),
                     max_equal - (s1 - s0),
                     max_space - (s2 - s1));

            dump_callback(h, buf, dump_callback_data);

            run_count += count;
        }
    }
}

static void fill_spaces(char *buf, int buf_len, int num_spaces) {
    int i;
    for (i = 0; i < num_spaces && i < buf_len - 1; i++) {
        buf[i] = ' ';
    }
    buf[i] = '\0';
}

static void emit_bar(int64_t start, int64_t width, uint64_t count,
                     uint64_t max_count, uint64_t tot_count, uint64_t run_count,
                     char *buf, int buf_len,
                     int plus_spaces, int equal_spaces, int space_spaces) {
    char bar_buf[25];
    char plus_buf[40];
    char equal_buf[40];
    char space_buf[40];

    size_t j = 0;

    if (count > 0) {
        uint64_t bar = (((sizeof(bar_buf) - 1) * count) / max_count);
        while (j < bar && j < sizeof(bar_buf) - 1) {
            bar_buf[j] = '*';
            j++;
        }
    }
    bar_buf[j] = '\0';

    fill_spaces(plus_buf, sizeof(plus_buf), plus_spaces);
    fill_spaces(equal_buf, sizeof(equal_buf), equal_spaces);
    fill_spaces(space_buf, sizeof(space_buf), space_spaces);

    snprintf(buf, buf_len - 1, "%s%lld+%lld%s=%llu %s%.2f%% %s",
             plus_buf, start, width, equal_buf, count, space_buf,
             100.0 * ((float) (count + run_count) / (float) tot_count), bar_buf);
}
