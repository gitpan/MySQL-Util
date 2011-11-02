use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'MySQL::Util',
    AUTHOR              => q{John Gravatt <gravattj@cpan.org>},
    VERSION_FROM        => 'lib/MySQL/Util.pm',
    ABSTRACT_FROM       => 'lib/MySQL/Util.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
        'DBD::mysql' => 0,
        'Moose' => 1.24,
        'namespace::autoclean' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'MySQL-Util-*' },
);
