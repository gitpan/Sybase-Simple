# $Id: Makefile.PL,v 1.1 1999/11/25 00:20:23 mpeppler Exp $
#
# $Log: Makefile.PL,v $
# Revision 1.1  1999/11/25 00:20:23  mpeppler
# Initial revision
#
# Revision 1.2  1998/10/16 18:23:26  mpeppler
# Added check for Sybase::CTlib existence.
#
#

eval {
    require Sybase::CTlib;
};
die "I can't find Sybase::CTlib.\nYou must install sybperl 2.xx before being able to use this module\n" if $@;

use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'	=> 'Sybase::Simple',
    'VERSION_FROM' => 'Simple.pm', # finds $VERSION
);
