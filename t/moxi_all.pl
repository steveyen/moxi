#!/usr/bin/perl

sub go {
  my ($topology, $protocol) = @_;
  print "testing $topology $protocol\n";
  print system("./t/moxi.pl $topology $protocol");
}

go('simple',   'ascii');
go('chain',    'ascii');
go('fanout',   'ascii');
go('fanoutin', 'ascii');

go('simple', 'binary');
go('fanout', 'binary');

