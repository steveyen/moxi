#!/usr/bin/perl

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
    my $port  = $passed_port || free_port();
    my $portB = free_port();
    my $portC = $portB;

    $args .= " -W $port=localhost:$portB -p $portC";

    print("new_memcached_proxy $args\n");

    my $udpport = free_port("udp");
    if (supports_udp()) {
        $args .= " -U $udpport";
    }
    if ($< == 0) {
        $args .= " -u root";
    }
    my $childpid = fork();

    my $exe = "$builddir/memcached-debug";
    croak("memcached binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
    croak("memcached binary not executable\n") unless -x _;

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
    croak("Failed to startup/connect to memcached server.");
}

sub supports_udp {
    my $output = `$builddir/memcached-debug -h`;
    return 0 if $output =~ /^memcached 1\.1\./;
    return 1;
}

*new_memcached = *new_memcached_proxy;

PREFIX

my $test_name = $ARGV[0] || 'flags';

# Tack on ./t/ directory prefix if needed.
if ($test_name !~ /^\.\/t/) {
  $test_name = "./t/$test_name";
}

# Tack on .t filename suffix if needed.
if ($test_name !~ /\.t$/) {
  $test_name = "$test_name.t";
}

if ($test_name =~ /cproxy/) {
  print("fail cannot test against self\n");
} else {
  eval($prefix . `cat $test_name`);
  print($@);
}

