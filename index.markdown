---
layout: default
title: NorthScale Labs - memcached moxi
---

# moxi

## memcached + integrated proxy + more

moxi is a memcached proxy with several features which can help keep
the memcached contract whole in complicated environments.  It also
brings several optimizations to memcached deployments, without
requiring any changes to the application software using memcached.

See [this summary][summarypdf] for a quick, highlevel overview.

### Obtaining

Version 0.9.6 is the latest release of moxi, and it became available
on the 16th of August, 2009.  We plan for packages of this release in
both .deb and .rpm to be available soon.  Right now, the release is
available via github as listed below.  See the [changelog][changelog]
for some clues on what has changed.

### The basics

* Open-source, same BSD license as memcached.
* Written in C.
* Based on the latest memcached server code for its high-performance
  connection and protocol handling.
* Uses libmemcached for its rich hashing features (consistent hashing,
  etc.).
* Uses libevent for both upstream (client) and downstream connection
  handling.

### Protocol support

* memcached text protocol (for upstream and downstream communication)
* memcached binary protocol (for downstream communication)
* later: binary support for upstream communication and mixing and
  matching protocols (e.g. some downstream text, some binary)

### Optimizations

* Multi-get Escalation and Protocol Pipelining - de-duplicates
  requests for popular keys base on ideas from Dustin Sallings'
  spymemcached java client library for memcached.  See
  [code.google.com/p/spymemcached/wiki/Optimizations][spyopt].
* Front cache - an L1 cache, to avoid network hops.
* Fire & forget `set` - optionally returned STORED from `set` requests
  before completely processing the request so that the application can
  proceed.  * Timeouts - set maximum time for operations. e.g. "no
  downstream memcached operation should take more than *X*ms.

### Statistics

Statistics are tracked independently on each thread, optimized for the
99% case of being a good proxy. A once-in-awhile request for proxy
statistics will perform an internal scatter-gather inside moxi to
aggregate statistics.

### Configuration

* Based on [libconflate][lconfl] reconfiguration to allow dynamic
  server pool reconfigurations.
* Assuming you run moxi on your web/app server boxes and you're using
  [libconflate][lconfl], your development, staging and production
  configurations gets easy: always point your memcached clients to
  "localhost:11211" (or whatever port you run moxi on).  </li> </ul>

### Features inherited from memcached

moxi is built "on the shoulders of giants", that is, memcached, so
there's lots to inherit...

* Multi-platform
* Multi-threaded
* Daemonizable
* Run as another user
* Bindable to IP interface, default is INADDR_ANY
* PID file
* etc

### Presentations

A subset of these slides were presented at OSCON 2009 as a Lightning
Talk

<div style="width:425px; text-align:left;" id="__ss_1747476">
<a style="font:14px Helvetica,Arial,Sans-serif; display:block;margin:12px 0 3px 0;text-decoration:underline;"
href="http://www.slideshare.net/northscale/moxi-memcached-proxy"
title="moxi - memcached + integrated proxy + more">memcached + integrated
proxy + more</a><object style="margin:0px" width="425" height="355">
<param name="movie" value="http://static.slidesharecdn.com/swf/ssplayer2.swf?doc=moxiabout-090721035815-phpapp02&stripped_title=moxi-memcached-proxy" /><param name="allowFullScreen" value="true"/><param name="allowScriptAccess" value="always"/><embed src="http://static.slidesharecdn.com/swf/ssplayer2.swf?doc=moxiabout-090721035815-phpapp02&stripped_title=moxi-memcached-proxy" type="application/x-shockwave-flash" allowscriptaccess="always" allowfullscreen="true" width="425" height="355"></embed></object>
<div style="font-size:11px;font-family:tahoma,arial;height:26px;padding-top:2px;">View more <a style="text-decoration:underline;" href="http://www.slideshare.net/">documents</a> from <a style="text-decoration:underline;" href="http://www.slideshare.net/northscale">NorthScale</a>.</div>
</div>

### Obtaining the Source

You can clone the project with [git][git] by running:

    $ git clone git://github.com/northscale/moxi

There are several dependencies to be noted if you're building from
source.  They are either bundled or should generally be available in
most OS distributions.

* [libmemcached][libmemcached] (specific version, statically linked
  and bundled)
* [libevent][libevent], a portable event notification layout for
  socket/file descriptor events on UNIX-like systems.
* [check][check], a C unit test framework (needed only for testing,
  we're fans of test driven design)

For more information on depedencies and building, see the project
[wiki][wiki] If you have questions, please join our [discussion
group][group] to ask!

### Installation

Packages will shortly be available for the most common platforms, and
that is the preferred method of installation.  Otherwise, the source
is automake/autconf based and should build on most UNIX and GNU/Linux
systems.

### Test suites

moxi aims to pass all the relevant memcached server compatibility
tests.

moxi has many new test cases to exercise proxy-only features and
topologies.

    $ make test
    $ make check
    $ make test_moxi

### License

[New BSD][license]

### What's next?  Want to help?

We'd love to hear your ideas, feedback and patches/improvements.  Join
us on the [moxi discussion group][group] if you want to collaborate.

Or, hit that fork button on
[http://github.com/northscale/moxi/tree/master][moxigithub].

### Contact

[feedback@northscale.com][feedback]

Alternatively, you may post comments below using disqus.

[summarypdf]: moxi_information_7.17.09.pdf
[changelog]: changelog.html
[spyopt]: http://code.google.com/p/spymemcached/wiki/Optimizations
[group]: http://groups.google.com/group/moxi
[libevent]: http://monkey.org/~provos/libevent/
[lconfl]: http://labs.northscale.com/libconflate/
[check]: http://check.sourceforge.net/
[libmemcached]: http://tangent.org/552/libmemcached.html
[wiki]: http://wiki.github.com/northscale/moxi
[git]: http://git-scm.com/
[moxigithub]: http://github.com/northscale/moxi/tree/master
[feedback]: feedback@northscale.com
[license]: http://www.opensource.org/licenses/bsd-license.php
