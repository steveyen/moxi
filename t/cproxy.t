#!/usr/bin/perl

use strict;

my $file;

foreach $file (<./t/*.t>) {
  if ($file !~ /cproxy/) {
    print $file . "\n";
    my $result = `./t/cproxy.pl $file`;
    while ($result =~ m/^fail /g) {
      print "$&\n";
    }
  }
}


