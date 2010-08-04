/*
 * Wrappers around strtoull/strtoll that are safer and easier to
 * use.  For tests and assumptions, see internal_tests.c.
 *
 * str   a NULL-terminated base decimal 10 unsigned integer
 * out   out parameter, if conversion succeeded
 *
 * returns true if conversion succeeded.
 */
bool safe_strtoull(const char *str, uint64_t *out);
bool safe_strtoll(const char *str, int64_t *out);
bool safe_strtoul(const char *str, uint32_t *out);
bool safe_strtol(const char *str, int32_t *out);

/* This was stolen from the glibc docs.

   Why they'd write documentation to show how to do this vs. just
   provide the function is unclear.
*/
int timeval_subtract(struct timeval *result,
                     struct timeval *x, struct timeval *y);

/**
 * Convert a timeval to a simple double.
 *
 * This is generally useful for deltas.
 */
double timeval_to_double(struct timeval tv);

struct moxi_stats {
    double min;
    double max;
    double avg;
    double stddev;
    double ninetyfifth;
};

/**
 * Compute some statistics over a sequence.
 *
 * @param out accumulated stats for input values
 * @param vals input values (note: these will be reordered)
 * @param num_values the number of values to be processed
 */
void compute_stats(struct moxi_stats *out, double *vals, int num_vals);

/* should be fixed in libconflate instead */
#ifdef __gcc_attribute__
#undef __gcc_attribute__
#endif

#ifdef __GCC
# define __gcc_attribute__ __attribute__
#else
# define __gcc_attribute__(x)
#endif

/**
 * Vararg variant of perror that makes for more useful error messages
 * when reporting with parameters.
 *
 * @param fmt a printf format
 */
void vperror(const char *fmt, ...)
    __gcc_attribute__ ((format (printf, 1, 2)));

#undef __gcc_attribute__

uint64_t ntohll(uint64_t value);
uint64_t htonll(uint64_t value);
