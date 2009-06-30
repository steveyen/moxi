dnl ---------------------------------------------------------------------------
dnl Macro: HSIEH_HASH
dnl ---------------------------------------------------------------------------
AC_ARG_ENABLE(hsieh_hash,
    [  --enable-hsieh_hash     build with support for hsieh hashing.],
    [
      if test "x$enableval" != "xno"; then
          ENABLE_HSIEH="true"
          AC_DEFINE([HAVE_HSIEH_HASH], [1], [Enables hsieh hashing support])
      else
          ENABLE_HSIEH="false"
      fi
    ],
    [
      ENABLE_HSIEH="false"
    ]
)

AM_CONDITIONAL([INCLUDE_HSIEH_SRC], [test "x$ENABLE_HSIEH" = "xtrue"])
dnl ---------------------------------------------------------------------------
dnl End Macro: HSIEH_HASH
dnl ---------------------------------------------------------------------------
