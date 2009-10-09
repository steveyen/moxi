#!/bin/sh
#

set -e

# Get the initial version.
sh version.sh

# check for sane defaults on some platforms
UNAME=`uname -s`
if [ -d /opt/local/share/aclocal ] && [ $UNAME = "Darwin" ] ; then
ACLOCALFLAGS=" -I /opt/local/share/aclocal"
fi

echo "libtoolize..."
if [ $UNAME = "Darwin" ]; then
glibtoolize --automake
else
libtoolize --automake
fi

echo "aclocal..."
ACLOCAL=`which aclocal-1.10 || which aclocal-1.9 || which aclocal19 || which aclocal-1.7 || which aclocal17 || which aclocal-1.5 || which aclocal15`
${ACLOCAL:-aclocal} $ACLOCALFLAGS || exit 1

echo "autoheader..."
AUTOHEADER=${AUTOHEADER:-autoheader}
$AUTOHEADER || exit 1

echo "automake..."
AUTOMAKE=`which automake-1.10 || which automake-1.9 || which automake-1.7`
$AUTOMAKE --foreign --add-missing || automake --foreign --add-missing || exit 1

echo "autoconf..."
AUTOCONF=${AUTOCONF:-autoconf}
$AUTOCONF || exit 1

# this is important so that this file is not rebuilt during builds
touch libmemcached-0.30/libmemcached/libmemcached_config.h.in

if [ "x$MOXI_DEVELOPMENT" != xyes ]; then
  if [ -d libconflate ] && [ -d .git ] && \
     ( ! [ -d libconflate/libstrophe/.git ] || \
       ! [ -f libconflate/autogen.sh ] ); then
    echo "The libconflate submodule or it's submodules seem to be absent."
    echo "Fetching submodules recursively."
    git submodule init && git submodule update &&
  (cd libconflate && git submodule init && git submodule update) &&
  (cd libconflate/libstrophe && git submodule init && git submodule update)
  fi

  # if this project is based on git, not just make-dist tarball,
  # clean submodules recursively on autogen
  for i in $(find . -name .git \
             | awk 'BEGIN{getline} \
             {print (substr($0, 0, index($0, ".git")-1 ))}');
  do
    currdir=$(pwd)
    cd $i && git reset --hard > /dev/null 2>&1 && \
       git clean -d -f -x > /dev/null 2>&1 && \
       git submodule update > /dev/null 2>&1
    cd $currdir
  done
fi

if (test -f libconflate/autogen.sh) && ! (test -f libconflate/configure); then
    echo "libconflate..."
    (cd libconflate && ./autogen.sh)
fi
