AC_PREREQ(2.52)
m4_include([m4/version.m4])
AC_INIT(moxi, VERSION_NUMBER, moxi@googlegroups.com)
AC_CONFIG_SRCDIR([memcached.c])
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])
AC_DISABLE_STATIC

PANDORA_CANONICAL_TARGET(less-warnings, warnings-always-on, force-gcc42)

AC_PROG_CC_C99
AC_PROG_INSTALL

PANDORA_REQUIRE_LIBEVENT
PANDORA_REQUIRE_PTHREAD
PANDORA_REQUIRE_LIBMEMCACHED
PANDORA_REQUIRE_LIBHASHKIT
PANDORA_HAVE_LIBCONFLATE
AS_IF([test "x${ac_cv_libconflate}" = "xyes"], [
      AC_DEFINE([HAVE_CONFLATE_H], 1,
          [Defined if building with libconflate XMPP management])])
PANDORA_HAVE_LIBVBUCKET

AC_SUBST(PROFILER_FLAGS)
AC_SUBST(PROFILER_LDFLAGS)
AC_HEADER_TIME
AC_HEADER_STDBOOL
AC_SUBST(NO_STRICT_ALIASING)

AH_TOP([
#ifndef CONFIG_H
#define CONFIG_H
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#ifdef _SYS_FEATURE_TESTS_H
#error "You should include config.h as your first include file"
#endif
])

AH_BOTTOM([
#include "config_static.h"
#endif
])

AC_ARG_ENABLE(moxi-vbucket,
  [AS_HELP_STRING([--enable-moxi-vbucket], [use libvbucket instead of libmemcached @<:@default=off@:>@])],
  [ac_cv_enable_moxi_vbucket="$enableval"],
  [ac_cv_enable_moxi_vbucket="no"])

AS_IF(test "x$ac_cv_enable_moxi_vbucket" = "xyes",
      AC_DEFINE([MOXI_USE_VBUCKET], 1, [Defined moxi to use libvbucket]))
AM_CONDITIONAL(MOXI_USE_VBUCKET, test "x$ac_cv_enable_moxi_vbucket" = "xyes")

AC_ARG_ENABLE(moxi-malloc,
  [AS_HELP_STRING([--enable-moxi-malloc], [use malloc for items instead of slabber])])

if test "x$enable_moxi_malloc" != "xno"; then
  AC_DEFINE([MOXI_ITEM_MALLOC], 1, [Defined moxi to use malloc instead of slabber])
fi

dnl ----------------------------------------------------------------------------

AC_SEARCH_LIBS(socket, socket)
AC_SEARCH_LIBS(gethostbyname, nsl)
AC_SEARCH_LIBS(umem_cache_create, umem)

AC_ARG_WITH([check],
        [AS_HELP_STRING([--with-check=yes], [look for check tool for unit tests @<:@default=yes@:>@])],
        [ac_cv_with_check="$withval"], [ac_cv_with_check=yes])

AS_IF(test "x$ac_cv_with_check" = "xyes",
      [ PKG_CHECK_MODULES([CHECK], [check >= 0.9.4]) ],
      [ AC_MSG_NOTICE([skipping check unit-test dependency])])

AC_C_CONST

dnl From licq: Copyright (c) 2000 Dirk Mueller
dnl Check if the type socklen_t is defined anywhere
AC_DEFUN([AC_C_SOCKLEN_T],
[AC_CACHE_CHECK(for socklen_t, ac_cv_c_socklen_t,
[
  AC_TRY_COMPILE([
#ifdef WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
#else
    #include <sys/types.h>
    #include <sys/socket.h>
#endif
  ],[
    socklen_t foo;
  ],[
    ac_cv_c_socklen_t=yes
  ],[
    ac_cv_c_socklen_t=no
  ])
])
if test $ac_cv_c_socklen_t = no; then
  AC_DEFINE(socklen_t, int, [define to int if socklen_t not available])
fi
])

AC_C_SOCKLEN_T

AC_DEFUN([AC_C_SIGPIPE],
[AC_CACHE_CHECK(for SIGPIPE, ac_cv_c_sigpipe,
[
  AC_TRY_COMPILE([
    #include <signal.h>
  ],[
    void (*disp)(int);
    signal(SIGPIPE, disp);
  ],[
    ac_cv_c_sigpipe=yes
  ],[
    ac_cv_c_sigpipe=no
  ])
])
AS_IF(test $ac_cv_c_sigpipe = yes,
  AC_DEFINE(HAVE_SIGPIPE, int, [define to 1 if you have SIGPIPE]))
])
AC_C_SIGPIPE

dnl Check if we're a little-endian or a big-endian system, needed by hash code
AC_DEFUN([AC_C_ENDIAN],
[AC_CACHE_CHECK(for endianness, ac_cv_c_endian,
[
  AC_RUN_IFELSE(
    [AC_LANG_PROGRAM([], [dnl
        long val = 1;
        char *c = (char *) &val;
        exit(*c == 1);
    ])
  ],[
    ac_cv_c_endian=big
  ],[
    ac_cv_c_endian=little
  ])
])
if test $ac_cv_c_endian = big; then
  AC_DEFINE(ENDIAN_BIG, 1, [machine is bigendian])
fi
if test $ac_cv_c_endian = little; then
  AC_DEFINE(ENDIAN_LITTLE, 1, [machine is littleendian])
fi
])

if test $host == $build; then
  AC_C_ENDIAN
else
  AS_CASE([$host_cpu],
    [i?86], [AC_MSG_RESULT([guessed machine as little endian])
             AC_DEFINE(ENDIAN_LITTLE, 1, [machine is littleendian])],
    [amd64], [AC_MSG_RESULT([guessed machine as little endian])
              AC_DEFINE(ENDIAN_LITTLE, 1, [machine is littleendian])],
    [AC_ERROR([don't know endian of this machine])])
fi

AC_CHECK_FUNCS(mlockall)
AC_CHECK_FUNCS(getpagesizes)
AC_CHECK_FUNCS(memcntl)
AC_CHECK_FUNCS(sigignore)

AC_DEFUN([AC_C_ALIGNMENT],
[AC_CACHE_CHECK(for alignment, ac_cv_c_alignment,
[
  AC_RUN_IFELSE(
    [AC_LANG_PROGRAM([
#include <stdlib.h>
#include <inttypes.h>
    ], [
       char *buf = malloc(32);
       uint64_t *ptr = (uint64_t*)(buf+2);
       *ptr = 0x1;
       return 0;
    ])
  ],[
    ac_cv_c_alignment=none
  ],[
    ac_cv_c_alignment=need
  ])
])
if test $ac_cv_c_alignment = need; then
  AC_DEFINE(NEED_ALIGN, 1, [Machine need alignment])
fi
])

if test $host == $build; then
  AC_C_ALIGNMENT
else
  AC_MSG_RESULT([guessed machine not requiring alignment])
  dnl  AC_DEFINE(NEED_ALIGN, 1, [Machine need alignment])
fi

dnl Check for the requirements for running moxi with less privileges
dnl than the default privilege set. On Solaris we need setppriv and priv.h
dnl If you want to add support for other platforms you should check for
dnl your requirements, define HAVE_DROP_PRIVILEGES, and make sure you add
dnl the source file containing the implementation into moxi_SOURCE
dnl in Makefile.am
AC_CHECK_FUNCS(setppriv, [
   AC_CHECK_HEADER(priv.h, [
      AC_DEFINE([HAVE_DROP_PRIVILEGES], 1,
         [Define this if you have an implementation of drop_privileges()])
      build_solaris_privs=yes
   ], [])
],[])

AM_CONDITIONAL([BUILD_SOLARIS_PRIVS],[test "$build_solaris_privs" = "yes"])

AC_CHECK_HEADER(umem.h, [
   AC_DEFINE([HAVE_UMEM_H], 1,
         [Define this if you have umem.h])
   build_cache=no
], [build_cache=yes])

AM_CONDITIONAL([BUILD_CACHE], [test "x$build_cache" = "xyes"])

AC_ARG_ENABLE(docs,
  [AS_HELP_STRING([--disable-docs],[Disable documentation generation])])

AC_PATH_PROG([XML2RFC], [xml2rfc], "no")
AC_PATH_PROG([XSLTPROC], [xsltproc], "no")

AM_CONDITIONAL([BUILD_SPECIFICATIONS],
               [test "x$enable_docs" != "xno" -a "x$XML2RFC" != "xno" -a "x$XSLTPROC" != "xno"])


AC_CHECK_HEADERS_ONCE(sys/socket.h netdb.h arpa/inet.h pwd.h sys/mman.h netinet/tcp.h sysexits.h sys/uio.h sys/resource.h sys/un.h netinet/in.h winsock2.h syslog.h)
AS_IF(test "x$ac_cv_header_winsock2_h" = "xyes",
      [AM_LDFLAGS="$AM_LDFLAGS -lws2_32" ])

# moxi generates warnings on all platforms, so disable warning == error
AM_CFLAGS="$AM_CFLAGS $NO_WERROR"
AM_CXXFLAGS="$AM_CXXFLAGS $NO_WERROR"

AC_CHECK_FUNCS_ONCE(getrlimit getpwnam)

AM_CONDITIONAL([BUILD_TESTAPPS], [test "x$ac_cv_header_winsock2_h" = "xno"])
AM_CONDITIONAL([BUILD_DAEMON], [test "x$ac_cv_header_winsock2_h" = "xno"])

AC_ARG_VAR([CONFLATE_DB_PATH], [Directory for libconflate config database (default $prefix/var/lib/]AC_PACKAGE_NAME[)])

AS_IF(test "x$CONFLATE_DB_PATH" = "x",
      CONFLATE_DB_PATH="$localstatedir/lib/$PACKAGE")

LIBS="$LIBS -lm"

AC_CONFIG_FILES(Makefile doc/Makefile debian/changelog scripts/rpm/moxi.spec)
AC_OUTPUT
