#!/bin/sh
#

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

if [ -d libconflate ] && [ -d .git ] && ! [ -f libconflate/autogen.sh ]; then
  echo "libconflate submodule seem to be absent. Will fetch it now."
  git submodule update -i && (cd libconflate && git submodule update -i) && (cd libconflate/libstrophe && git submodule update -i)
fi

if (test -f libconflate/autogen.sh) && ! (test -f libconflate/configure); then
    echo "libconflate..."
    (cd libconflate && ./autogen.sh)
fi
