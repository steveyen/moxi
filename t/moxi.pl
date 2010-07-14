#!/usr/bin/perl

use strict;

# Invoke like this to test ascii protocol, with all the topologies.
#
# ./t/moxi.pl simple ascii && ./t/moxi.pl chain ascii &&
# ./t/moxi.pl fanout ascii && ./t/moxi.pl fanoutin ascii
#
# Invoke like this to test binary protocol.
#
# ./t/moxi.pl simple binary && ./t/moxi.pl fanout binary
#
# See: ./t/moxi_all.pl
#
# TODO: We can't pass chain and fanoutin topologies with binary protocol
#       until moxi supports upstream binary protocol.
#
my $topology_name = $ARGV[0] || 'simple';
my $protocol_name = $ARGV[1] || 'ascii';
my $hashlib_name  = $ARGV[2] || 'libvbucket';

print "moxi.pl: " . $topology_name . " " . $protocol_name . " " . $hashlib_name . "\n";

my @good_tests = qw(
  binary-get.t
  bogus-commands.t
  cas.t
  expirations.t
  flags.t
  incrdecr.t
  line-lengths.t
  maxconns.t
  multiversioning.t
  slab-reassign.t
  whitespace.t
);

my @skip_tests = qw(
  # TODO: SKIP: The noreply test fails because moxi uses
  # a pool of multiple concurrent downstream
  # connections rather than respecting the
  # serial expectations of the noreply test.
  #
  noreply.t

  # TODO: SKIP: The getset test fails because the failed set
  # of a huge >1MB value or failed item_alloc() does
  # not currently delete the item from the downstream
  # memcached server.
  #
  getset.t

  # TODO: Currently don't know why flush-all tests are failing against
  # the latest moxi.
  #
  flush-all.t

  # The following tests are not expected to succeed,
  # because they're testing command line stuff,
  # or the stats are wrong.
  #
  00-startup.t
  64bit.t
  binary.t
  daemonize.t
  dash-M.t
  issue_14.t
  issue_22.t
  issue_29.t
  lru.t
  stats-detail.t
  stats.t
  udp.t
  unixsocket.t
);

my %is_good_test;
my %is_skip_test;

for (map("./t/$_", @good_tests)) {
  $is_good_test{$_} = "ascii binary simple chain fanout fanoutin"
}
for (map("./t/$_", @skip_tests)) {
  $is_skip_test{$_} = "ascii binary simple chain fanout fanoutin"
}

# Skipping incrdecr.t for binary due to issue 48
# on code.google.com/p/memcached.
#
$is_good_test{"./t/incrdecr.t"} = "ascii simple chain fanout fanoutin";
$is_skip_test{"./t/incrdecr.t"} = "binary simple chain fanout fanoutin";

# Skipping cas.t for fanout/fanoutin due to multi-"gets" race condition.
# The test expects "gets x y" to return results in order "x y", but
# when moxi does a fanout, the order is non-deterministic.
#
$is_good_test{"./t/cas.t"} = "ascii binary simple chain";
$is_skip_test{"./t/cas.t"} = "ascii binary fanout fanoutin";

my $file;

foreach $file (<./t/*.t>) {
  if ($is_good_test{$file} =~ /$protocol_name/ &&
      $is_good_test{$file} =~ /$topology_name/) {
    print $file . "\n";
    my $result = system("./t/moxi_one.pl $file $topology_name $protocol_name $hashlib_name");
    if ($result != 0) {
      print "fail moxi_one.pl $file $topology_name $protocol_name $hashlib_name test";
      exit $result;
    }
  } elsif ($is_skip_test{$file} =~ /$protocol_name/ &&
           $is_skip_test{$file} =~ /$topology_name/) {
    print "skipping test: $file\n";
  } else {
    print "unknown test: $file\n";
  }
}


