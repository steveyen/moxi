dnl ---------------------------------------------------------------------------
dnl Macro: PROTOCOL_BINARY_TEST
dnl ---------------------------------------------------------------------------
save_CFLAGS="$CFLAGS"
CFLAGS="$CFLAGS -I${srcdir}"
AC_RUN_IFELSE([
   AC_LANG_PROGRAM([
      #include "libmemcached/memcached/protocol_binary.h"
   ], [
      protocol_binary_request_set request;
      if (sizeof(request) != sizeof(request.bytes)) {
         return 1;
      }
   ])
],, AC_MSG_ERROR([Unsupported struct padding done by compiler.]))
CFLAGS="$save_CFLAGS"

dnl ---------------------------------------------------------------------------
dnl End Macro: PROTOCOL_BINARY_TEST
dnl ---------------------------------------------------------------------------
