BUILD_UTILLIB=yes

AC_ARG_ENABLE(utils,
    [  --enable-utils          Build libmemcachedutils [[default=yes]]],
    [
      if test "x$enableval" = "xno"; then
        BUILD_UTILLIB="no"
      fi
    ]
    )

if test "x$BUILD_UTILLIB" = "xyes"; then
  AC_SEARCH_LIBS(pthread_create, pthread)
  if test "x$ac_cv_search_pthread_create" = "xno"; then
    AC_MSG_ERROR([Sorry you need POSIX thread library to build libmemcachedutil.])
  fi
  AC_DEFINE([HAVE_LIBMEMCACHEDUTIL], [1], [Enables libmemcachedutil Support])
fi

AM_CONDITIONAL([BUILD_LIBMEMCACHEDUTIL],[test "x$BUILD_UTILLIB" = "xyes"])
