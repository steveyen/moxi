/* libmemcached/libmemcached_config.h.  Generated from libmemcached_config.h.in by configure.  */
/* libmemcached/libmemcached_config.h.in.  Generated from configure.ac by autoheader.  */

/* Enable big endian byteorder */
/* #undef BYTEORDER_BIG_ENDIAN */

/* Enable little endian byteorder */
#define BYTEORDER_LITTLE_ENDIAN 1

/* Enables DEBUG Support */
/* #undef HAVE_DEBUG */

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Enables DTRACE Support */
/* #undef HAVE_DTRACE */

/* Enables hsieh hashing support */
/* #undef HAVE_HSIEH_HASH */

/* Have ntohll */
/* #undef HAVE_HTONLL */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Enables libmemcachedutil Support */
#define HAVE_LIBMEMCACHEDUTIL 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have a working SO_RCVTIMEO */
#define HAVE_RCVTIMEO 1

/* Define to 1 if you have a working SO_SNDTIMEO */
#define HAVE_SNDTIMEO 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Name of the memcached binary used in make test */
#define MEMCACHED_BINARY "memcached"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "http://tangent.org/552/libmemcached.html"

/* Define to the full name of this package. */
#define PACKAGE_NAME "libmemcached"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "libmemcached 0.30"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "libmemcached"

/* Define to the version of this package. */
#define PACKAGE_VERSION "0.30"

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
#define TIME_WITH_SYS_TIME 1

/* Define to 1 if on MINIX. */
/* #undef _MINIX */

/* Define to 2 if the system does not provide POSIX.1 features except with
   this defined. */
/* #undef _POSIX_1_SOURCE */

/* Define to 1 if you need to in order for `stat' and other things to work. */
/* #undef _POSIX_SOURCE */

/* Define to 500 only on HP-UX. */
/* #undef _XOPEN_SOURCE */

/* Enable extensions on AIX 3, Interix.  */
#ifndef _ALL_SOURCE
# define _ALL_SOURCE 1
#endif
/* Enable GNU extensions on systems that have them.  */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif
/* Enable threading extensions on Solaris.  */
#ifndef _POSIX_PTHREAD_SEMANTICS
# define _POSIX_PTHREAD_SEMANTICS 1
#endif
/* Enable extensions on HP NonStop.  */
#ifndef _TANDEM_SOURCE
# define _TANDEM_SOURCE 1
#endif
/* Enable general extensions on Solaris.  */
#ifndef __EXTENSIONS__
# define __EXTENSIONS__ 1
#endif


/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* Define to `unsigned int' if <sys/types.h> does not define. */
/* #undef size_t */
