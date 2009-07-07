#!/usr/bin/perl

# Run this main test driver program from the project's
# top directory, which has t as a subdirectory.
#
my $exe = "./moxi-debug";

croak("moxi binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
croak("moxi binary not executable\n") unless -x _;

sub go {
  my ($topology, $protocol) = @_;
  print "testing $topology $protocol\n";
  if (system("./t/moxi.pl $topology $protocol") != 0) {
    print("failed $topology $protocol test\n");
    exit;
  }
}

# Ascii protocol compatibility tests.
#
go('simple',   'ascii');
go('chain',    'ascii');
go('fanout',   'ascii');
go('fanoutin', 'ascii');

# Binary protocol compatibility tests.
#
go('simple', 'binary');
go('fanout', 'binary');

# Fork moxi-debug for moxi-specific testing.
#
my $childargs = "-z 11333=localhost:11311 -p 0 -U 0 -v -t 1 -Z downstream_max=1";
my $childpid  = fork();

unless ($childpid) {
    exec "$exe $childargs";
    exit; # never gets here.
}

system("python ./t/moxi_mock.py");

kill 2, $childpid;

