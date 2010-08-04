#!/usr/bin/perl

my $test_name     = $ARGV[0] || './t/flags.t';
my $topology_name = $ARGV[1] || 'simple';
my $protocol_name = $ARGV[2] || 'ascii';
my $hashlib_name  = $ARGV[3] || 'libvbucket';

print "moxi_one.pl: " . $test_name . " " . $topology_name . " " . $protocol_name . " " . $hashlib_name . "\n";

my $prefix = <<'PREFIX';

use strict;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

*new_memcached_orig = *new_memcached;

use IO::Socket::INET;
use IO::Socket::UNIX;
use Exporter 'import';
use Carp qw(croak);
use vars qw(@EXPORT);

use Cwd;
my $builddir = getcwd;

sub new_memcached_proxy {
    my ($args, $passed_port) = @_;
    my $port = $passed_port || free_port();

    TOPOLOGY

    print("new moxi $args\n");

    my $udpport = free_port("udp");
    if (supports_udp()) {
        $args .= " -U $udpport";
    }
    if ($< == 0) {
        $args .= " -u root";
    }
    my $childpid = fork();

    my $exe = "$builddir/moxi";
    croak("moxi binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
    croak("moxi binary not executable\n") unless -x _;

    unless ($childpid) {
        setpgrp();
        exec "$exe $args";
        exit; # never gets here.
    }
    setpgrp $childpid, $childpid;

    # unix domain sockets
    if ($args =~ /-s (\S+)/) {
        sleep 1;
	my $filename = $1;
	my $conn = IO::Socket::UNIX->new(Peer => $filename) ||
	    croak("Failed to connect to unix domain socket: $! '$filename'");

	return Memcached::Handle->new(pid  => -$childpid,
				      conn => $conn,
				      domainsocket => $filename,
				      port => $port);
    }

    # try to connect / find open port, only if we're not using unix domain
    # sockets

    for (1..20) {
	my $conn = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$port");
	if ($conn) {
	    return Memcached::Handle->new(pid  => -$childpid,
					  conn => $conn,
					  udpport => $udpport,
					  port => $port);
	}
	select undef, undef, undef, 0.10;
    }
    croak("Failed to startup/connect to moxi server.");
}

sub supports_udp {
    my $output = `$builddir/moxi -h`;
    return 0 if $output =~ /^memcached 1\.1\./;
    return 1;
}

*new_memcached = *new_memcached_proxy;

PREFIX

# ------------------------------------------------------

my $simple_topology_libmemcached = <<'SIMPLE_TOPOLOGY';
    my $portA = free_port();
    my $portC = $portA;
    my $topology =
      " -z \"".
        "$port=".
          "localhost:$portA\"".
      " -p $portC";
    $args .= $topology;
SIMPLE_TOPOLOGY

my $simple_topology_libvbucket = <<'SIMPLE_TOPOLOGY';
    my $portA = free_port();
    my $portC = $portA;
    my $topology =
      " -z \'".
       "$port={\"hashAlgorithm\": \"CRC\",\"numReplicas\": 0,".
              "\"serverList\":[\"127.0.0.1:$portA\"],".
              "\"vBucketMap\":[[0]]}\"\'".
      " -p $portC";
    $args .= $topology;
SIMPLE_TOPOLOGY

# ------------------------------------------------------

my $chain_topology_libmemcached = <<'CHAIN_TOPOLOGY';
    my $portA0 = free_port();
    my $portA1 = free_port();
    my $portA2 = free_port();
    my $portA3 = free_port();
    my $portA4 = free_port();
    my $portC = $portA4;
    my $topology =
      " -z \"".
        "$port=".
          "localhost:$portA0;".
        "$portA0=".
          "localhost:$portA1;".
        "$portA1=".
          "localhost:$portA2;".
        "$portA2=".
          "localhost:$portA3;".
        "$portA3=".
          "localhost:$portA4\"".
      " -p $portC";
    $args .= $topology;
CHAIN_TOPOLOGY

my $chain_topology_libvbucket = <<'CHAIN_TOPOLOGY';
    my $portA0 = free_port();
    my $portA1 = free_port();
    my $portA2 = free_port();
    my $portA3 = free_port();
    my $portA4 = free_port();
    my $portC = $portA4;
    my $topology =
      " -z \'".
       "$port={\"hashAlgorithm\": \"CRC\",\"numReplicas\": 0,".
              "\"serverList\":[\"127.0.0.1:$portA0\"],".
              "\"vBucketMap\":[[0]]};".
       "$portA0={\"hashAlgorithm\": \"CRC\",\"numReplicas\": 0,".
              "\"serverList\":[\"127.0.0.1:$portA1\"],".
              "\"vBucketMap\":[[0]]};".
       "$portA1={\"hashAlgorithm\": \"CRC\",\"numReplicas\": 0,".
              "\"serverList\":[\"127.0.0.1:$portA2\"],".
              "\"vBucketMap\":[[0]]};".
       "$portA2={\"hashAlgorithm\": \"CRC\",\"numReplicas\": 0,".
              "\"serverList\":[\"127.0.0.1:$portA3\"],".
              "\"vBucketMap\":[[0]]};".
       "$portA3={\"hashAlgorithm\": \"CRC\",\"numReplicas\": 0,".
              "\"serverList\":[\"127.0.0.1:$portA4\"],".
              "\"vBucketMap\":[[0]]}\'".
      " -p $portC";
    $args .= $topology;
CHAIN_TOPOLOGY

# ------------------------------------------------------

my $fanout_topology_libmemcached = <<'FANOUT_TOPOLOGY';
    my $portA = free_port();
    my $portC = $portA;
    my $topology =
      " -z \"".
        "$port=".
          "localhost:$portA,".
          "localhost:$portA,".
          "localhost:$portA,".
          "localhost:$portA\"".
      " -p $portC";
    $args .= $topology;
FANOUT_TOPOLOGY

my $fanout_topology_libvbucket = <<'FANOUT_TOPOLOGY';
    my $portA = free_port();
    my $portC = $portA;
    my $topology =
      " -z \'".
       "$port={\"hashAlgorithm\": \"CRC\",\"numReplicas\": 0,".
              "\"serverList\":[\"127.0.0.1:$portA\",\"127.0.0.1:$portA\",\"127.0.0.1:$portA\",\"127.0.0.1:$portA\"],".
              "\"vBucketMap\":[[0],[1],[2],[3]]}\'".
      " -p $portC";
    $args .= $topology;
FANOUT_TOPOLOGY

# ------------------------------------------------------

my $fanoutin_topology_libmemcached = <<'FANOUTIN_TOPOLOGY';
    my $portA = free_port();
    my $portB = free_port();
    my $portC = $portB;
    my $topology =
      " -z \"".
        "$port=".
          "localhost:$portA,".
          "localhost:$portA,".
          "localhost:$portA,".
          "localhost:$portA;".
        "$portA=".
          "localhost:$portB\"".
      " -p $portC";
    $args .= $topology;
FANOUTIN_TOPOLOGY

my $fanoutin_topology_libvbucket = <<'FANOUTIN_TOPOLOGY';
    my $portA = free_port();
    my $portB = free_port();
    my $portC = $portB;
    my $topology =
      " -z \'".
       "$port={\"hashAlgorithm\": \"CRC\",\"numReplicas\": 0,".
              "\"serverList\":[\"127.0.0.1:$portA\",\"127.0.0.1:$portA\",\"127.0.0.1:$portA\",\"127.0.0.1:$portA\"],".
              "\"vBucketMap\":[[0],[1],[2],[3]]};".
       "$portA={\"hashAlgorithm\": \"CRC\",\"numReplicas\": 0,".
              "\"serverList\":[\"127.0.0.1:$portB\"],".
              "\"vBucketMap\":[[0]]}\'".
      " -p $portC";
    $args .= $topology;
FANOUTIN_TOPOLOGY

# ------------------------------------------------------

my %topology_map_libmemcached = (
    'simple' => $simple_topology_libmemcached,
    'chain' => $chain_topology_libmemcached,
    'fanout' => $fanout_topology_libmemcached,
    'fanoutin' => $fanoutin_topology_libmemcached
);

my %topology_map_libvbucket = (
    'simple' => $simple_topology_libvbucket,
    'chain' => $chain_topology_libvbucket,
    'fanout' => $fanout_topology_libvbucket,
    'fanoutin' => $fanoutin_topology_libvbucket,
);

# ------------------------------------------------------

my $topology = $topology_map_libvbucket{$topology_name};

if ($hashlib_name eq 'libmemcached') {
    $topology = $topology_map_libmemcached{$topology_name};
}

# ------------------------------------------------------

$topology .= "\$args .= \" -Z downstream_protocol=$protocol_name,downstream_max=1\";";

# Tack on ./t/ directory prefix if needed.
if ($test_name !~ /^\.\/t/) {
  $test_name = "./t/$test_name";
}

# Tack on .t filename suffix if needed.
if ($test_name !~ /\.t$/) {
  $test_name = "$test_name.t";
}

$prefix =~ s/TOPOLOGY/{$topology }/g;

eval($prefix . `cat $test_name`);

print($@);
