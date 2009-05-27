AC_ARG_WITH(memcached,
[[  --with-memcached[=memcached binary]
                          Memcached binary to use for make test]],
[
  if test -n "$withval"
  then
    MEMC_BINARY="$withval"
  fi

  if test x$withval == xyes
  then
    MEMC_BINARY=memcached
  fi

  # just ignore the user if --without-memcached is passed.. it is
  # only used by make test
  if test x$withval == xno
  then
    MEMC_BINARY=memcached
  fi
],
[
   AC_PATH_PROG([MEMC_BINARY], [memcached], "no", [$PATH])
])

if test x$MEMC_BINARY == "xno"
then
  AC_MSG_ERROR(["could not find memcached binary"])
fi

AC_DEFINE_UNQUOTED([MEMCACHED_BINARY], "$MEMC_BINARY", 
            [Name of the memcached binary used in make test])
