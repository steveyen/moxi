dnl ---------------------------------------------------------------------------
dnl Macro: PROTOCOL_BINARY_TEST
dnl ---------------------------------------------------------------------------
save_CPPFLAGS="$CPPFLAGS"
CPPFLAGS="$CPPFLAGS -I${srcdir} -I${srcdir}/libmemcached-0.30"
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
CPPFLAGS="$save_CPPFLAGS"

dnl ---------------------------------------------------------------------------
dnl End Macro: PROTOCOL_BINARY_TEST
dnl ---------------------------------------------------------------------------
