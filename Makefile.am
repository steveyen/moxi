AUTOMAKE_OPTIONS = foreign

bin_PROGRAMS = moxi
##pkginclude_HEADERS = protocol_binary.h
noinst_PROGRAMS = moxi-debug sizes testapp

## INCLUDES is apparently not previously set, if you set it
##  change this
INCLUDES = $(GLIB2_CFLAGS)
LIBS += $(GLIB2_LIBS)
LIBS += -lgthread-2.0

BUILT_SOURCES= setuplibmemcached

testapp_SOURCES = testapp.c util.c util.h

moxi_SOURCES = memcached.c memcached.h \
                    hash.c hash.h \
                    slabs.c slabs.h \
                    items.c items.h \
                    assoc.c assoc.h \
                    thread.c daemon.c \
                    stats.c stats.h \
                    util.c util.h \
                    trace.h cache.h \
                    work.c work.h \
                    cproxy.c cproxy.h \
                    cproxy_config.c \
                    cproxy_protocol.c \
                    cproxy_protocol_a2a.c \
                    cproxy_protocol_a2b.c \
                    cproxy_multiget.c \
                    cproxy_stats.c \
                    cproxy_front.c \
                    matcher.c matcher.h

TESTS = check_util check_moxi check_moxi_agent check_work

check_PROGRAMS = check_util check_moxi check_moxi_agent check_work

check_util_SOURCES = check_util.c util.c util.h
check_util_CFLAGS = @CHECK_CFLAGS@
check_util_LDADD = @CHECK_LIBS@

if BUILD_CACHE
moxi_SOURCES += cache.c
testapp_SOURCES += cache.c
endif

if BUILD_WITH_CONFLATE
moxi_SOURCES += agent.h agent_config.c agent_ping.c agent_stats.c
endif

if BUILD_SOLARIS_PRIVS
moxi_SOURCES += solaris_priv.c
endif

moxi_debug_SOURCES = $(moxi_SOURCES)
moxi_debug_CPPFLAGS = -I$(top_srcdir)/libmemcached-0.30 \
                      -I$(top_srcdir)/../libstrophe
moxi_CPPFLAGS = -DNDEBUG $(moxi_debug_CPPFLAGS)
moxi_debug_LDADD = @PROFILER_LDFLAGS@ \
                   libmemcached-0.30/libmemcached/libmemcached.la
moxi_debug_CFLAGS = @PROFILER_FLAGS@

moxi_LDADD = libmemcached-0.30/libmemcached/libmemcached.la
moxi_DEPENDENCIES =
moxi_debug_DEPENDENCIES =
CLEANFILES=

if BUILD_DTRACE
BUILT_SOURCES += memcached_dtrace.h
CLEANFILES += memcached_dtrace.h
endif

if DTRACE_INSTRUMENT_OBJ
moxi_LDADD += memcached_dtrace.o
moxi_DEPENDENCIES += memcached_dtrace.o
moxi_debug_LDADD += memcached_debug_dtrace.o
moxi_debug_DEPENDENCIES += memcached_debug_dtrace.o
CLEANFILES += memcached_dtrace.o memcached_debug_dtrace.o
endif

memcached_dtrace.h: memcached_dtrace.d
	${DTRACE} -h -s memcached_dtrace.d 
	sed -e 's,void \*,const void \*,g' memcached_dtrace.h | \
            sed -e 's,char \*,const char \*,g' | tr '\t' ' ' > mmc_dtrace.tmp
	mv mmc_dtrace.tmp memcached_dtrace.h

memcached_dtrace.o: $(moxi_OBJECTS)
	$(DTRACE) $(DTRACEFLAGS) -G -o memcached_dtrace.o -s ${srcdir}/memcached_dtrace.d $(moxi_OBJECTS)

memcached_debug_dtrace.o: $(moxi_debug_OBJECTS)
	$(DTRACE) $(DTRACEFLAGS) -G -o memcached_debug_dtrace.o -s ${srcdir}/memcached_dtrace.d $(moxi_debug_OBJECTS)


SUBDIRS = doc libmemcached-0.30/libmemcached
DIST_DIRS = scripts
EXTRA_DIST = doc scripts TODO t memcached.spec memcached_dtrace.d version.m4

MOSTLYCLEANFILES = *.gcov *.gcno *.gcda *.tcov

memcached-debug: moxi-debug
	cp moxi-debug memcached-debug

test: memcached-debug sizes testapp
	$(srcdir)/sizes
	$(srcdir)/testapp
	prove $(srcdir)/t
	@if test `basename $(PROFILER)` = "gcov"; then \
	  for file in moxi_debug-*.gc??; do \
	    mv -f $$file `echo $$file | sed 's/moxi_debug-//'`; \
	  done && \
	  for file in *.gcda; do \
	    srcfile=`echo $$file | sed 's/.gcda/.c/'`; \
	    if test -n "`echo $(moxi_debug_SOURCES) | grep $$srcfile`"; then \
	      echo `$(PROFILER) $$srcfile` | sed 's/'$$srcfile':.*//'; \
	    fi \
	  done \
	elif test `basename $(PROFILER)` = "tcov"; then \
	  files=`grep SRCFILE moxi-debug.profile/tcovd | sed 's/SRCFILE://' | sort | uniq` && \
	  $(PROFILER) -x moxi-debug.profile $$files 2>&1; \
	  for file in *.tcov; do \
	    srcfile=`echo $$file | sed 's/.tcov//'`; \
	    if test -n "`echo $(moxi_debug_SOURCES) | grep $$srcfile`"; then \
	      echo $$srcfile : `grep 'Percent of the file executed' $$file`; \
	    fi \
	  done \
	else :; fi

setuplibmemcached:
	cd libmemcached-0.30; ./configure --without-memcached

docs:
	(cat Doxyfile ; echo "PROJECT_NUMBER=`cat version.num`") | doxygen -

dist-hook:
	rm -f $(distdir)/*/*~ $(distdir)/t/lib/*~ $(distdir)/*~

check_work_SOURCES = check_work.c $(moxi_debug_SOURCES)
check_work_CFLAGS = @CHECK_CFLAGS@ $(moxi_debug_CPPFLAGS) -DMAIN_CHECK
check_work_LDADD = @CHECK_LIBS@ $(moxi_debug_LDADD)

check_moxi_SOURCES = check_moxi.c $(moxi_debug_SOURCES)
check_moxi_CFLAGS = @CHECK_CFLAGS@ $(moxi_debug_CPPFLAGS) -DMAIN_CHECK
check_moxi_LDADD = @CHECK_LIBS@ $(moxi_debug_LDADD)

check_moxi_agent_SOURCES = check_moxi_agent.c $(moxi_debug_SOURCES)
check_moxi_agent_CFLAGS = @CHECK_CFLAGS@ $(moxi_debug_CPPFLAGS) -DMAIN_CHECK
check_moxi_agent_LDADD = @CHECK_LIBS@ $(moxi_debug_LDADD)

