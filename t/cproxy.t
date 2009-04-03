#!/usr/bin/perl

my $prefix = <<'PREFIX';

use strict;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

*new_memcached_orig = *new_memcached;

sub new_memcached_proxy {
  my ($args, $passed_port) = @_;
  my $portA = $passed_port || free_port();
  my $portB = free_port();
  $args .= " -W $portA=localhost:$portB -p $portB";
  return new_memcached_orig($args, $portA);
}

*new_memcached = *new_memcached_proxy;

PREFIX

my $test_name = $ARGV[0] || 'flags';

if ($test_name eq "cproxy") {
  print("fail cannot test against self\n");
} else {
  eval($prefix . `cat ./t/$test_name.t`);
}

