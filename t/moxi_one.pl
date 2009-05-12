#!/usr/bin/perl

my $test_name     = $ARGV[0] || './t/flags.t';
my $topology_name = $ARGV[1] || 'simple';
my $protocol_name = $ARGV[2] || 'ascii';

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

    my $exe = "$builddir/moxi-debug";
    croak("moxi binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
    croak("moxi binary not executable\n") unless -x _;

    unless ($childpid) {
        exec "$exe $args";
        exit; # never gets here.
    }

    # unix domain sockets
    if ($args =~ /-s (\S+)/) {
        sleep 1;
	my $filename = $1;
	my $conn = IO::Socket::UNIX->new(Peer => $filename) ||
	    croak("Failed to connect to unix domain socket: $! '$filename'");

	return Memcached::Handle->new(pid  => $childpid,
				      conn => $conn,
				      domainsocket => $filename,
				      port => $port);
    }

    # try to connect / find open port, only if we're not using unix domain
    # sockets

    for (1..20) {
	my $conn = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$port");
	if ($conn) {
	    return Memcached::Handle->new(pid  => $childpid,
					  conn => $conn,
					  udpport => $udpport,
					  port => $port);
	}
	select undef, undef, undef, 0.10;
    }
    croak("Failed to startup/connect to moxi server.");
}

sub supports_udp {
    my $output = `$builddir/moxi-debug -h`;
    return 0 if $output =~ /^memcached 1\.1\./;
    return 1;
}

*new_memcached = *new_memcached_proxy;

PREFIX

my $simple_topology = <<'SIMPLE_TOPOLOGY';
    my $portA = free_port();
    my $portC = $portA;
    my $topology =
      " -z \"".
        "$port=".
          "localhost:$portA\"".
      " -p $portC";
    $args .= $topology;
SIMPLE_TOPOLOGY

my $chain_topology = <<'CHAIN_TOPOLOGY';
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

my $fanout_topology = <<'FANOUT_TOPOLOGY';
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

my $fanoutin_topology = <<'FANOUTIN_TOPOLOGY';
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

my %topology_map = (
    'simple' => $simple_topology,
    'chain' => $chain_topology,
    'fanout' => $fanout_topology,
    'fanoutin' => $fanoutin_topology
);

my $topology = $topology_map{$topology_name};

$topology .= "\$args .= \" -Z downstream_prot=$protocol_name\";";

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




