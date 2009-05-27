dnl ---------------------------------------------------------------------------
dnl Macro: DTRACE_TEST
dnl ---------------------------------------------------------------------------
AC_ARG_ENABLE(dtrace,
    [  --enable-dtrace      Build with support for the DTRACE.],
    [ 
    AC_PATH_PROG([DTRACE], [dtrace], "no", [/usr/sbin:$PATH])
    if test "x$DTRACE" != "xno"; then
      AC_DEFINE([HAVE_DTRACE], [1], [Enables DTRACE Support])
      DTRACE_HEADER=dtrace_probes.h

      # DTrace on MacOSX does not use -G option
      $DTRACE -G -o conftest.$$ -s libmemcached/libmemcached_probes.d 2>/dev/zero
      if test $? -eq 0
      then
        DTRACE_OBJ=libmemcached_probes.lo
        rm conftest.$$
      fi

      ENABLE_DTRACE="yes"
      AC_SUBST(HAVE_DTRACE)
    else
      AC_MSG_ERROR([Need dtrace binary and OS support.])
    fi
    ],
    [
      ENABLE_DTRACE="no" 
    ]
    )

AC_SUBST(DTRACEFLAGS)
AC_SUBST(DTRACE_HEADER)
AC_SUBST(DTRACE_OBJ)
AM_CONDITIONAL([HAVE_DTRACE], [ test "$ENABLE_DTRACE" = "yes" ])
dnl ---------------------------------------------------------------------------
dnl End Macro: DTRACE_TEST
dnl ---------------------------------------------------------------------------
