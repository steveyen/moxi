#!/usr/bin/perl

# This program starts moxi and then runs the python mock server tests.
#
#  ./t/moxi_mock.pl
#  ./t/moxi_mock.pl ascii
#  ./t/moxi_mock.pl ascii TestProxyAscii.testBasicQuit
#
my $protocol_name = $ARGV[0] || 'ascii';
my $test_name     = $ARGV[1] || '';

print "moxi_mock.pl: " . $protocol_name . "\n";

my $exe = "./moxi-debug";

croak("moxi binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
croak("moxi binary not executable\n") unless -x _;

# Fork moxi-debug for moxi-specific testing.
#
my $childargs =
      " -z ./t/moxi_mock.cfg".
      " -p 0 -U 0 -v -t 1 -Z \"downstream_max=1,downstream_protocol=" . $protocol_name . "\"";
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

my $result = system("python ./t/moxi_mock_a2" . $protocol_name . ".py " . $test_name);

kill 2, -$childpid;

exit $result;
