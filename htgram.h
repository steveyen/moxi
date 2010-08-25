/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*! \mainpage htgram
 *
 * \section intro_sec Introduction
 *
 * htgram is a short histogram implementation
 *
 * \section docs_sec API Documentation
 *
 * Jump right into <a href="modules.html">the modules docs</a> to get started.
 */

/**
 * Histogram Utility Library.
 *
 * \defgroup CD Creation and Destruction
 * \defgroup Data Collecting and retrieving stats
 */

#ifndef HTGRAM_H
#define HTGRAM_H 1

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <math.h>

#define HTGRAM_PUBLIC_API

#ifdef __cplusplus
extern "C" {
#endif

    struct htgram_st;

    /**
     * Opaque histogram representation.
     */
    typedef struct htgram_st *HTGRAM_HANDLE;

    /**
     * \addtogroup CD
     *  @{
     */

    /**
     * Create an instance of htgram.
     *
     * htgram_mk(0, 5, 1.0, 3, NULL) means have 3 bins, each of width
     * 5, such as [0, 5), [5, 10), [10, 15).
     *
     * htgram_mk(0, 5, 2.0, 3, NULL) means have 3 bins, but the bin
     * widths should double, such as [0, 5), [5, 15), [15, 35).
     *
     * @param bin_start the first bin starts at this range
     *                  value, inclusive.
     * @param bin_start_width  width of the first bin.
     * @param bin_width_growth grow each bin width by this factor.
     * @param num_bins number of bins.
     * @param next optional chained HTGRAM_HANDLE, which is useful
     *             to change bin_width_growth factors; may be NULL.
     */
    HTGRAM_PUBLIC_API
    HTGRAM_HANDLE htgram_mk(int64_t bin_start,
                            int64_t bin_start_width,
                            double  bin_width_growth,
                            size_t  num_bins,
                            HTGRAM_HANDLE next);

    /**
     * Destroy a htgram.
     *
     * @param h the htgram handle
     */
    HTGRAM_PUBLIC_API
    void htgram_destroy(HTGRAM_HANDLE h);

    /**
     * @}
     */

    /**
     * \addtogroup Data
     * @{
     */

    /**
     * Get the range start value for the first bin.
     */
    HTGRAM_PUBLIC_API
    int64_t htgram_get_bin_start(HTGRAM_HANDLE h);

    /**
     * Get the width of the first bin.
     */
    HTGRAM_PUBLIC_API
    int64_t htgram_get_bin_start_width(HTGRAM_HANDLE h);

    /**
     * Get the growth factor for bin widths.
     */
    HTGRAM_PUBLIC_API
    double htgram_get_bin_width_growth(HTGRAM_HANDLE h);

    /**
     * Get the total number of bins.
     */
    HTGRAM_PUBLIC_API
    size_t htgram_get_num_bins(HTGRAM_HANDLE h);

    /**
     * Add a data_point to the histogram, where the correct bin will
     * be incremented by count.  For example, htgram_incr(h, request_latency, 1);
     */
    HTGRAM_PUBLIC_API
    void htgram_incr(HTGRAM_HANDLE h, int64_t data_point, uint64_t count);

    /**
     * Retrieves collected data (out_bin_count) for a bin, given a
     * bin_index.  The first bin has bin_index 0.  Returns false if
     * there's no bin at the given bin_index.
     */
    HTGRAM_PUBLIC_API
    bool htgram_get_bin_data(HTGRAM_HANDLE h, int bin_index,
                             int64_t *out_bin_start,
                             int64_t *out_bin_width,
                             uint64_t *out_bin_count);

    /**
     * Reset all bin counts to zero.
     */
    HTGRAM_PUBLIC_API
    void htgram_reset(HTGRAM_HANDLE h);

    /**
     * Add the values from histogram x into histogram agg (aggregate).
     */
    HTGRAM_PUBLIC_API
    void htgram_add(HTGRAM_HANDLE agg, HTGRAM_HANDLE x);

    /**
     * Call signature of callback from htgram_dump().
     */
    typedef void (*HTGRAM_DUMP_CALLBACK)(HTGRAM_HANDLE h, const char *dump_line, void *cbdata);

    /**
     * Invoke the HTGRAM_DUMP_CALLBACK function with pretty histogram lines.
     */
    HTGRAM_PUBLIC_API
    void htgram_dump(HTGRAM_HANDLE h,
                     HTGRAM_DUMP_CALLBACK dump_callback,
                     void *dump_callback_data);

    /**
     * @}
     */

#ifdef __cplusplus
}
#endif

#endif
