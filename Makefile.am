AUTOMAKE_OPTIONS = foreign

bin_PROGRAMS = moxi
noinst_PROGRAMS = moxi-debug sizes testapp timedrun

## INCLUDES is apparently not previously set, if you set it
##  change this
INCLUDES = $(CONFLATE_CFLAGS)
LIBS += $(CONFLATE_LIBS)

BUILT_SOURCES =

testapp_SOURCES = testapp.c util.c util.h

moxi_SOURCES = memcached.c memcached.h \
					genhash.c genhash.h genhash_int.h \
                    hash.c hash.h \
                    slabs.c slabs.h \
                    items.c items.h \
                    assoc.c assoc.h \
                    thread.c daemon.c \
                    stats.c stats.h \
                    util.c util.h \
                    trace.h cache.h \
                    work.c work.h \
                    redirects.h \
                    cproxy.c cproxy.h \
                    cproxy_config.c \
                    cproxy_protocol.c \
                    cproxy_protocol_a2a.c \
                    cproxy_protocol_a2b.c \
                    cproxy_multiget.c \
                    cproxy_stats.c \
                    cproxy_front.c \
                    matcher.c matcher.h \
                    mcs.c mcs.h \
                    cJSON.c cJSON.h

timedrun_SOURCES = timedrun.c

TESTS = check_util check_moxi check_work
if BUILD_WITH_CONFLATE
TESTS += check_moxi_agent
endif

check_PROGRAMS = check_util \
                 check_moxi \
                 check_work

if BUILD_WITH_CONFLATE
check_PROGRAMS += check_moxi_agent
endif


test_moxi:
	./t/moxi_all.pl

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
moxi_debug_CPPFLAGS = -I$(top_srcdir)/libmemcached-0.30 -DCONFLATE_DB_PATH=\"$(CONFLATE_DB_PATH)\"
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

SUBDIRS = doc libmemcached-0.30/libmemcached $(MAYBE_LIBCONFLATE)
DIST_SUBDIRS = doc libmemcached-0.30/libmemcached $(MAYBE_LIBCONFLATE)
DIST_DIRS = scripts
EXTRA_DIST = doc scripts TODO t memcached.spec memcached_dtrace.d version.m4 libmemcached-0.30 protocol_binary.h patches debian

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

docs:
	(cat Doxyfile ; echo "PROJECT_NUMBER=`cat version.num`") | doxygen -

dist-hook:
	@if [ "x$(MAYBE_LIBCONFLATE)" = x ]; then \
	  echo "WARNING! WARNING! WARNING!: libconflate WILL NOT be bundled in distribution archive!"; \
	  echo "WARNING! WARNING! WARNING!: you should do 'make dist' after configuring with bundled libconflate (and libstrophe too)!!!!!"; \
	fi
	rm -rf $(distdir)/libmemcached-0.30/libmemcached/{.deps,.libs,*.lo,*.o,Makefile,*.la,libmemcached_config.h,stamp-h[0-9]}
	rm -f $(distdir)/*/*~ $(distdir)/t/lib/*~ $(distdir)/*~

install-data-hook:
	$(mkdir_p) $(DESTDIR)$(CONFLATE_DB_PATH)

check_work_SOURCES = check_work.c $(moxi_debug_SOURCES)
check_work_CFLAGS = @CHECK_CFLAGS@ $(moxi_debug_CPPFLAGS) -DMAIN_CHECK
check_work_LDADD = @CHECK_LIBS@ $(moxi_debug_LDADD)

check_moxi_SOURCES = check_moxi.c $(moxi_debug_SOURCES)
check_moxi_CFLAGS = @CHECK_CFLAGS@ $(moxi_debug_CPPFLAGS) -DMAIN_CHECK
check_moxi_LDADD = @CHECK_LIBS@ $(moxi_debug_LDADD)

if BUILD_WITH_CONFLATE
check_moxi_agent_SOURCES = check_moxi_agent.c redirects.c $(moxi_debug_SOURCES)
check_moxi_agent_CFLAGS = @CHECK_CFLAGS@ $(moxi_debug_CPPFLAGS) -DMAIN_CHECK -DREDIRECTS_FOR_MOCKS
check_moxi_agent_LDADD = @CHECK_LIBS@ $(moxi_debug_LDADD)
endif

check-syntax:
	$(COMPILE) $(moxi_debug_CPPFLAGS) -o /dev/null -S ${CHK_SOURCES}

fc_rpm_inner:
	-rm $(rpmtop)/SRPMS/moxi-$(VERSION)*.rpm
	-rm $(rpmtop)/RPMS/*/moxi-$(VERSION)*.rpm
	cp moxi-$(VERSION).tar.gz $(rpmtop)/SOURCES/
	rpmbuild -ba scripts/rpm/moxi.spec
	cp $(rpmtop)/SRPMS/moxi-$(VERSION)*.rpm .
	cp $(rpmtop)/RPMS/*/moxi{,-debuginfo}-$(VERSION)*.rpm .

fc_rpm: dist
	rpmtop=`rpm --eval "%{_topdir}"` $(MAKE) fc_rpm_inner
