#include "config.h"
#include <stdio.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <stdarg.h>

#include "memcached.h"

bool safe_strtoull(const char *str, uint64_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    unsigned long long ull = strtoull(str, &endptr, 10);
    if (errno == ERANGE)
        return false;
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long long) ull < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }
        *out = ull;
        return true;
    }
    return false;
}

bool safe_strtoll(const char *str, int64_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    long long ll = strtoll(str, &endptr, 10);
    if (errno == ERANGE)
        return false;
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = ll;
        return true;
    }
    return false;
}

bool safe_strtoul(const char *str, uint32_t *out) {
    char *endptr = NULL;
    unsigned long l = 0;
    assert(out);
    assert(str);
    *out = 0;
    errno = 0;

    l = strtoul(str, &endptr, 10);
    if (errno == ERANGE) {
        return false;
    }

    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long) l < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }
        *out = l;
        return true;
    }

    return false;
}

bool safe_strtol(const char *str, int32_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    long l = strtol(str, &endptr, 10);
    if (errno == ERANGE)
        return false;
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l;
        return true;
    }
    return false;
}

int timeval_subtract(struct timeval *result,
                            struct timeval *x, struct timeval *y)
{
    /* Perform the carry for the later subtraction by updating y. */
    if (x->tv_usec < y->tv_usec) {
        int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
        y->tv_usec -= 1000000 * nsec;
        y->tv_sec += nsec;
    }
    if (x->tv_usec - y->tv_usec > 1000000) {
        int nsec = (x->tv_usec - y->tv_usec) / 1000000;
        y->tv_usec += 1000000 * nsec;
        y->tv_sec -= nsec;
    }

    /* Compute the time remaining to wait.
       tv_usec is certainly positive. */
    result->tv_sec = x->tv_sec - y->tv_sec;
    result->tv_usec = x->tv_usec - y->tv_usec;

    /* Return 1 if result is negative. */
    return x->tv_sec < y->tv_sec;
}

double timeval_to_double(struct timeval tv)
{
    return (double)tv.tv_sec + ((double)tv.tv_usec / 1000000);
}

static int cmp_doubles(const void *pa, const void *pb)
{
    double a = *(double*)pa;
    double b = *(double*)pb;
    return a == b ? 0 : (a < b ? -1 : 1);
}

void compute_stats(struct moxi_stats *out, double *vals, int num_vals)
{
    assert(out);
    assert(vals);
    assert(num_vals > 0);

    out->min = vals[0];
    out->max = vals[0];

    double sum = 0;

    // min, max and sum
    for (int i = 0; i < num_vals; i++) {
        sum += vals[i];
        out->min = fmin(out->min, vals[i]);
        out->max = fmax(out->max, vals[i]);
    }

    // avg
    out->avg = sum / (double)num_vals;

    // stddev
    sum = 0;
    for (int i = 0; i < num_vals; i++) {
        sum += pow(vals[i] - out->avg, 2);
    }

    out->stddev = sqrt(sum / num_vals);

    // 95th %ile
    qsort(vals, num_vals, sizeof(double), cmp_doubles);
    out->ninetyfifth = vals[(int)((float)num_vals * 0.95)];
}

void vperror(const char *fmt, ...) {
    int old_errno = errno;
    char buf[80];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    errno = old_errno;

    perror(buf);
}

// The following are from libmemcached/byteorder.c

/* Byte swap a 64-bit number. */
#ifndef swap64
static inline uint64_t swap64(uint64_t in)
{
# ifndef BYTEORDER_BIG_ENDIAN
  /* Little endian, flip the bytes around until someone makes a faster/better
   * way to do this. */
  uint64_t rv= 0;
  uint8_t x= 0;
  for(x= 0; x < 8; x++)
  {
    rv= (rv << 8) | (in & 0xff);
    in >>= 8;
  }
  return rv;
# else
  /* big-endian machines don't need byte swapping */
  return in;
# endif
}
#endif

uint64_t ntohll(uint64_t value)
{
  return swap64(value);
}

uint64_t htonll(uint64_t value)
{
  return swap64(value);
}
