AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4 --force

bin_PROGRAMS = moxi
noinst_PROGRAMS =
if BUILD_TESTAPPS
noinst_PROGRAMS += sizes testapp timedrun
endif

BUILT_SOURCES =

testapp_SOURCES = testapp.c util.c util.h

moxi_SOURCES = memcached.c memcached.h \
					genhash.c genhash.h genhash_int.h \
                    hash.c hash.h \
                    slabs.c slabs.h \
                    items.c items.h \
                    assoc.c assoc.h \
                    thread.c \
                    stats.c stats.h \
                    util.c util.h \
                    trace.h cache.h \
                    work.c work.h \
                    redirects.h \
                    cproxy.c cproxy.h \
                    cproxy_config.c \
                    cproxy_protocol_a.c \
                    cproxy_protocol_a2a.c \
                    cproxy_protocol_a2b.c \
                    cproxy_protocol_b.c \
                    cproxy_protocol_b2b.c \
                    cproxy_multiget.c \
                    cproxy_stats.c \
                    cproxy_front.c \
                    matcher.c matcher.h \
                    murmur_hash.c \
                    mcs.c mcs.h \
                    stdin_check.c stdin_check.h \
                    log.c log.h \
                    cJSON.c cJSON.h

if BUILD_DAEMON
moxi_SOURCES += daemon.c
endif

timedrun_SOURCES = timedrun.c

TESTS = check_util check_moxi check_work
if HAVE_LIBCONFLATE
TESTS += check_moxi_agent
endif

check_PROGRAMS = check_util \
                 check_moxi \
                 check_work

if HAVE_LIBCONFLATE
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

if HAVE_LIBCONFLATE
moxi_SOURCES += agent.h agent_config.c agent_ping.c agent_stats.c
endif

if BUILD_SOLARIS_PRIVS
moxi_SOURCES += solaris_priv.c
endif

moxi_CPPFLAGS = -DNDEBUG -DCONFLATE_DB_PATH=\"$(CONFLATE_DB_PATH)\"
moxi_LDADD =
moxi_LDFLAGS = $(LTLIBEVENT) $(LTLIBCONFLATE) $(LTLIBHASHKIT)
moxi_DEPENDENCIES =

if MOXI_USE_VBUCKET
moxi_LDFLAGS += $(LTLIBVBUCKET)
else
moxi_LDFLAGS += $(LTLIBMEMCACHED)
endif

CLEANFILES=

SUBDIRS = doc $(MAYBE_LIBCONFLATE)
DIST_SUBDIRS = doc $(MAYBE_LIBCONFLATE)
DIST_DIRS = scripts
EXTRA_DIST = doc scripts TODO t m4/version.m4 protocol_binary.h patches debian

MOSTLYCLEANFILES = *.gcov *.gcno *.gcda *.tcov

memcached-debug: moxi
	cp moxi memcached-debug

test: memcached-debug sizes testapp
	$(srcdir)/sizes
	$(srcdir)/testapp
	./t/moxi_all.pl

test_gcov: test
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

check_work_SOURCES = check_work.c $(moxi_SOURCES)
check_work_CFLAGS = @CHECK_CFLAGS@ $(moxi_CPPFLAGS) -DMAIN_CHECK
check_work_LDADD = @CHECK_LIBS@ $(moxi_LDADD)

check_moxi_SOURCES = check_moxi.c $(moxi_SOURCES)
check_moxi_CFLAGS = @CHECK_CFLAGS@ $(moxi_CPPFLAGS) -DMAIN_CHECK
check_moxi_LDADD = @CHECK_LIBS@ $(moxi_LDADD)

if HAVE_LIBCONFLATE
check_moxi_agent_SOURCES = check_moxi_agent.c redirects.c $(moxi_SOURCES)
check_moxi_agent_CFLAGS = @CHECK_CFLAGS@ $(moxi_CPPFLAGS) -DMAIN_CHECK -DREDIRECTS_FOR_MOCKS
check_moxi_agent_LDADD = @CHECK_LIBS@ $(moxi_LDADD)
endif

check-syntax:
	$(COMPILE) $(moxi_CPPFLAGS) -o /dev/null -S ${CHK_SOURCES}

fc_rpm_inner:
	-rm $(rpmtop)/SRPMS/moxi-$(VERSION)*.rpm
	-rm $(rpmtop)/RPMS/*/moxi-$(VERSION)*.rpm
	cp moxi-$(VERSION).tar.gz $(rpmtop)/SOURCES/
	rpmbuild -ba scripts/rpm/moxi.spec
	cp $(rpmtop)/SRPMS/moxi-$(VERSION)*.rpm .
	cp $(rpmtop)/RPMS/*/moxi{,-debuginfo}-$(VERSION)*.rpm .

fc_rpm: dist
	rpmtop=`rpm --eval "%{_topdir}"` $(MAKE) fc_rpm_inner

TMP_DIR=./tmp
TMP_VER=$(TMP_DIR)/version_num.tmp

version:
	test -d $(TMP_DIR) || mkdir $(TMP_DIR)
	git describe | sed s/-/_/g > $(TMP_VER)

bdist: version
	rm -f ./moxi_*.tar.gz
	rm -rf $(TMP_DIR)/moxi
	mkdir $(TMP_DIR)/moxi
	cp moxi $(TMP_DIR)/moxi
	(cd $(TMP_DIR); tar cf - moxi) | gzip -9 > moxi_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	echo created moxi_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
