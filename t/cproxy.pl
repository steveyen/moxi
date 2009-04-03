#!/usr/bin/perl

use strict;

my $file;

foreach $file (<./t/*.t>) {
  if ($file !~ /cproxy/) {
    print $file . "\n";
    my $result = `./t/cproxy_one.pl $file`;
    print $result . "\n";
    while ($result =~ m/^fail /g) {
      print "$&\n";
    }
  }
}


