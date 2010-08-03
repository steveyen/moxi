dnl  Copyright (C) 2010 NorthScale
dnl This file is free software; NorthScale
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

AC_DEFUN([_PANDORA_SEARCH_LIBCONFLATE],[
  AC_REQUIRE([AC_LIB_PREFIX])

  dnl --------------------------------------------------------------------
  dnl  Check for libconflate
  dnl --------------------------------------------------------------------

  AC_ARG_ENABLE([libconflate],
    [AS_HELP_STRING([--disable-libconflate],
      [Build with libconflate support @<:@default=on@:>@])],
    [ac_enable_libconflate="$enableval"],
    [ac_enable_libconflate="yes"])

  AS_IF([test "x$ac_enable_libconflate" = "xyes"],[
    AC_LIB_HAVE_LINKFLAGS(conflate,,[
      #include <libconflate/conflate.h>
    ],[
      conflate_config_t config;
    ])
  ],[
    ac_cv_libconflate="no"
  ])

  AM_CONDITIONAL(HAVE_LIBCONFLATE, [test "x${ac_cv_libconflate}" = "xyes"])
])

AC_DEFUN([PANDORA_HAVE_LIBCONFLATE],[
  AC_REQUIRE([_PANDORA_SEARCH_LIBCONFLATE])
])

AC_DEFUN([PANDORA_REQUIRE_LIBCONFLATE],[
  AC_REQUIRE([PANDORA_HAVE_LIBCONFLATE])
  AS_IF([test x$ac_cv_libconflate = xno],
      AC_MSG_ERROR([libconflate is required for ${PACKAGE}]))
])
