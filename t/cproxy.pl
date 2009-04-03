#!/usr/bin/perl

use strict;

my $topology_name = $ARGV[1] || 'simple';

my @good_tests = qw(
binary-get.t
bogus-commands.t
cas.t
expirations.t
flags.t
flush-all.t
incrdecr.t
line-lengths.t
maxconns.t
multiversioning.t
slab-reassign.t
);

my @skip_tests = qw(
noreply.t
getset.t

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
whitespace.t
);

my %is_good_test;
my %is_skip_test;

for (map("./t/$_", @good_tests)) { $is_good_test{$_} = 1 }
for (map("./t/$_", @skip_tests)) { $is_skip_test{$_} = 1 }

my $file;

foreach $file (<./t/*.t>) {
  if ($is_good_test{$file}) {
    print $file . "\n";
    my $result = `./t/cproxy_one.pl $file $topology_name`;
    while ($result =~ m/^fail /g) {
      print "$&\n";
    }
  } elsif ($is_skip_test{$file}) {
    print "skipping test: $file\n";
  } else {
    print "unknown test: $file\n";
  }
}


