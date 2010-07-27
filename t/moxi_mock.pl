#!/usr/bin/perl

# This program starts moxi and then runs the python mock server tests.
#
#  ./t/moxi_mock.pl [upstream_protocol] [downstream_protocol] [test_name]
#
# Parameters are optional, so these examples work, and default to ascii...
#
#  ./t/moxi_mock.pl
#  ./t/moxi_mock.pl ascii
#  ./t/moxi_mock.pl ascii ascii TestProxyAscii.testBasicQuit
#
my $upstream_protocol   = $ARGV[0] || 'ascii';
my $downstream_protocol = $ARGV[1] || 'ascii';
my $test_name           = $ARGV[2] || '';

print "moxi_mock.pl: " . $upstream_protocol . " " . $downstream_protocol . " " . $test_name + "\n";

my $exe = "./moxi-debug";

croak("moxi binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
croak("moxi binary not executable\n") unless -x _;

# Fork moxi-debug for moxi-specific testing.
#
my $childargs =
      " -z ./t/moxi_mock.cfg".
      " -p 0 -U 0 -v -t 1 -Z \"downstream_max=1,downstream_protocol=" . $downstream_protocol . "\"";
if ($< == 0) {
   $childargs .= " -u root";
}
my $childpid = fork();

unless ($childpid) {
    setpgrp();
    exec "$exe $childargs";
    exit; # never gets here.
}
setpgrp($childpid, $childpid);

my $u = substr($upstream_protocol, 0, 1); # This is 'a' or 'b'.
my $d = substr($downstream_protocol, 0, 1); # This is 'a' or 'b'.

my $result = system("python ./t/moxi_mock_" . $u . "2" . $d . ".py " . $test_name);

kill 2, -$childpid;

exit $result;
