/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <htgram.h>

static void testSimple(void) {
    HTGRAM_HANDLE h0;

    int64_t start;
    int64_t width;
    uint64_t count;

    h0 = htgram_mk(0, 5, 1.0, 3, NULL);
    assert(h0 != NULL);
    assert(htgram_get_bin_start(h0) == 0);
    assert(htgram_get_bin_start_width(h0) == 5);
    assert(htgram_get_bin_width_growth(h0) == 1.0);
    assert(htgram_get_num_bins(h0) == 3);

    start = width = count = 123;
    assert(htgram_get_bin_data(h0, -1, &start, &width, &count) == false);
    assert(start == 123);
    assert(width == 123);
    assert(count == 0);

    int i;
    for (i = 0; i < (int) htgram_get_num_bins(h0); i++) {
        start = width = count = 123;
        assert(htgram_get_bin_data(h0, i, &start, &width, &count) == true);
        assert(start == i * 5);
        assert(width == 5);
        assert(count == 0);

        htgram_incr(h0, (i * 5) + 0, 1);
        htgram_incr(h0, (i * 5) + 1, 2);
        htgram_incr(h0, (i * 5) + 2, 3);
        htgram_incr(h0, (i * 5) + 5, 0);

        start = width = count = 123;
        assert(htgram_get_bin_data(h0, i, &start, &width, &count) == true);
        assert(start == i * 5);
        assert(width == 5);
        assert(count == 6);
    }

    start = width = count = 123;
    assert(htgram_get_bin_data(h0, i, &start, &width, &count) == false);
    assert(start == 123);
    assert(width == 123);
    assert(count == 0);

    htgram_reset(h0);

    start = width = count = 123;
    assert(htgram_get_bin_data(h0, -1, &start, &width, &count) == false);
    assert(start == 123);
    assert(width == 123);
    assert(count == 0);

    for (i = 0; i < (int) htgram_get_num_bins(h0); i++) {
        start = width = count = 123;
        assert(htgram_get_bin_data(h0, i, &start, &width, &count) == true);
        assert(start == i * 5);
        assert(width == 5);
        assert(count == 0);
    }

    start = width = count = 123;
    assert(htgram_get_bin_data(h0, i, &start, &width, &count) == false);
    assert(start == 123);
    assert(width == 123);
    assert(count == 0);

    htgram_incr(h0, -10000, 111);
    htgram_incr(h0, 10000, 222);

    start = width = count = 123;
    assert(htgram_get_bin_data(h0, -1, &start, &width, &count) == false);
    assert(start == 123);
    assert(width == 123);
    assert(count == 111);

    start = width = count = 123;
    assert(htgram_get_bin_data(h0, i, &start, &width, &count) == false);
    assert(start == 123);
    assert(width == 123);
    assert(count == 222);

    htgram_destroy(h0);
}

static void testChained(void) {
    HTGRAM_HANDLE h0, h1;

    int64_t start;
    int64_t width;
    uint64_t count;

    // Have 200 bins from [0 to 2000), with bin widths of 10.
    // Have 36 bins from 2000 onwards, with bin width growing at 1.5, chained.
    h1 = htgram_mk(2000, 10, 1.5, 36, NULL);
    h0 = htgram_mk(0, 10, 1.0, 200, h1);

    int i;
    for (i = 0; i < (int) (htgram_get_num_bins(h0) + htgram_get_num_bins(h1)); i++) {
        assert(htgram_get_bin_data(h0, i, &start, &width, &count) == true);
        // printf("%d %d %d %d\n", i, start, width, count);
    }

    htgram_incr(h0, 28000000, 111);

    assert(htgram_get_bin_data(h0, 200 + 36 - 1, &start, &width, &count) == true);
    assert(start == 27692301);
    assert(width == 13845150);
    assert(count == 111);

    htgram_reset(h0);

    assert(htgram_get_bin_data(h0, 200 + 36 - 1, &start, &width, &count) == true);
    assert(start == 27692301);
    assert(width == 13845150);
    assert(count == 0);
}

int main(void) {
    testSimple();
    testChained();

    return 0;
}


