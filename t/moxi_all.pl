#!/usr/bin/perl

print `./t/moxi.pl simple   ascii`;
print `./t/moxi.pl chain    ascii`;
print `./t/moxi.pl fanout   ascii`;
print `./t/moxi.pl fanoutin ascii`;

print `./t/moxi.pl simple binary`;
print `./t/moxi.pl fanout binary`;
