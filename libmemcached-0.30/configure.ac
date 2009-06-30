AC_INIT([libmemcached],[0.30],[http://tangent.org/552/libmemcached.html])
AC_CONFIG_SRCDIR([clients/memcat.c])
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER(libmemcached/libmemcached_config.h)

# Setting CFLAGS here prevents AC_CANONICAL_TARGET from injecting them
SAVE_CFLAGS=${CFLAGS}
SAVE_CXXFLAGS=${CXXFLAGS}
CFLAGS=
CXXFLAGS=

AC_CANONICAL_TARGET

CFLAGS=${SAVE_CFLAGS}
CXXFLAGS=${SAVE_CXXFLAGS}

#shared library versioning
MEMCACHED_LIBRARY_VERSION=2:0:0
#                         | | |
#                  +------+ | +---+
#                  |        |     |
#                 current:revision:age
#                  |        |     |
#                  |        |     +- increment if interfaces have been added
#                  |        |        set to zero if interfaces have been removed or changed
#                  |        +- increment if source code has changed
#                  |           set to zero if current is incremented
#                  +- increment if interfaces have been added, removed or changed
AC_SUBST(MEMCACHED_LIBRARY_VERSION)
MEMCACHEDUTIL_LIBRARY_VERSION=0:0:0
AC_SUBST(MEMCACHEDUTIL_LIBRARY_VERSION)

AM_INIT_AUTOMAKE(nostdinc no-define -Wall -Werror)
AC_USE_SYSTEM_EXTENSIONS

AC_PROG_CC
AC_PROG_CC_C99
AC_PROG_CXX
AC_PROG_LIBTOOL
AM_SANITY_CHECK

AC_CHECK_DECL([__SUNPRO_C], [SUNCC="yes"], [SUNCC="no"])


# libmemcached versioning when linked with GNU ld.
if test "$lt_cv_prog_gnu_ld" = "yes"
then
    LD_VERSION_SCRIPT="-Wl,--version-script=\$(top_srcdir)/libmemcached/libmemcached.ver"
    LD_UTIL_VERSION_SCRIPT="-Wl,--version-script=\$(top_srcdir)/libmemcachedutil/libmemcachedutil.ver"
fi
AC_SUBST(LD_VERSION_SCRIPT)
AC_SUBST(LD_UTIL_VERSION_SCRIPT)

AC_C_CONST
AC_HEADER_TIME
AC_TYPE_SIZE_T
AC_SEARCH_LIBS(getopt_long, gnugetopt)
AC_SEARCH_LIBS(socket, socket)
AC_SEARCH_LIBS(gethostbyname, nsl)

save_LIBS="$LIBS"
LIBS="$LIBS -lm"
AC_LINK_IFELSE(
  [AC_LANG_PROGRAM(
    [[
#include <stdlib.h>
    ]],[[
      float f= floorf((float) 1.0);
    ]],
    [LIBM="-lm"],[LIBM=""])])

AC_SUBST(LIBM)
LIBS="$save_LIBS"
AC_SEARCH_LIBS(floorf, m)


sinclude(m4/pod2man.m4)
sinclude(m4/debug.m4)
sinclude(m4/dtrace.m4)
sinclude(m4/byteorder.m4)
sinclude(m4/64bit.m4)
sinclude(m4/protocol_binary.m4)
sinclude(m4/memcached.m4)
sinclude(m4/setsockopt.m4)
sinclude(m4/hsieh.m4)
sinclude(m4/util.m4)

dnl This is likely subverted by vpath builds. How do we find the original
dnl source dir in the configure step of a vpath build?
if test -d ".hg"
then
  building_from_hg=yes
else
  building_from_hg=no
fi


# We only support GCC and Sun's forte at the moment
if test "$GCC" = "yes"
then
  if test "$ENABLE_DEBUG" = "yes"
  then
    CFLAGS="-O0 -DHAVE_DEBUG $CFLAGS"
    CXXFLAGS="-O0 -DHAVE_DEBUG $CXXFLAGS"
  else
    CFLAGS="-O3 $CFLAGS"
    CXXFLAGS="-O3 $CXXFLAGS"
  fi
  if test "$building_from_hg" = "yes"
  then
    CFLAGS="-Werror $CFLAGS"
    CXXFLAGS="-Werror $CXXFLAGS"
  fi

  CFLAGS="-W -std=iso9899:1999 -Wall -Wextra -Wno-strict-aliasing -pedantic -Wundef -Wstrict-prototypes -Wmissing-prototypes -Wmissing-declarations -Wredundant-decls -ggdb3 $CFLAGS"
  CXXFLAGS="-W -Wall -Wextra -Wno-strict-aliasing -pedantic -Wundef -Woverloaded-virtual  -Wnon-virtual-dtor -Wctor-dtor-privacy -Wold-style-cast -Weffc++ -Wconversion -Wmissing-declarations -Wredundant-decls -ggdb3 $CXXFLAGS"


fi

if test "$SUNCC" = "yes"
then

  if test "$ENABLE_DEBUG" = "yes"
  then
    CFLAGS="-xO0 -DHAVE_DEBUG $CFLAGS"
    CXXFLAGS="-xO0 -DHAVE_DEBUG $CXXFLAGS"
  else
    CFLAGS="-xO4 -xlibmil -xdepend $CFLAGS"
    CXXFLAGS="-xO4 -xlibmil -xdepend $CXXFLAGS"
  fi
  if test "$building_from_hg" = "yes"
  then
    CFLAGS="-errwarn $CFLAGS"
    CXXFLAGS="-errwarn $CXXFLAGS"
  fi
  CFLAGS="-Xa -xstrconst -mt -errfmt=error -errshort=tags ${CFLAGS}"
  CXXFLAGS="+w +w2 -xwe -mt ${CXXFLAGS}"
fi


AM_CPPFLAGS="-I\$(top_srcdir) -I\$(top_builddir) ${CPPFLAGS}"
AM_CFLAGS="${CFLAGS}"
AM_CXXFLAGS="${CXXFLAGS}"

AC_SUBST([AM_CPPFLAGS])
AC_SUBST([AM_CFLAGS])
AC_SUBST([AM_CXXFLAGS])

dnl We've collected the flags in AM_*FLAGS now, so blank these.
CFLAGS=""
CXXFLAGS=""
CPPFLAGS=""

AC_CONFIG_FILES([
  Makefile
  clients/Makefile
  tests/Makefile
  docs/Makefile
  libmemcached/Makefile
  libmemcachedutil/Makefile
  support/Makefile
  support/libmemcached.pc
  support/libmemcached.spec
  support/libmemcached-fc.spec
  ])
AC_OUTPUT
