use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'MySQL::Util',
    AUTHOR        => q{John Gravatt <gravattj@cpan.org>},
    VERSION_FROM  => 'lib/MySQL/Util.pm',
    ABSTRACT_FROM => 'lib/MySQL/Util.pm',
    ( $ExtUtils::MakeMaker::VERSION >= 6.3002
        ? ( 'LICENSE' => 'perl' )
        : () ),
    PL_FILES  => {},
    PREREQ_PM => {
        'Test::More'           => 0,
        'DBD::mysql'           => 0,
        'Moose'                => 0,
        'namespace::autoclean' => 0,
        'Data::Compare'        => 0,
        'String::Util'         => 0,
        'autodie'              => 0,
        'Modern::Perl'         => 0,
        'DBIx::DataFactory'    => 0,
        'List::MoreUtils'      => 0,
        'SQL::Beautify'        => 0,
        'Smart::Args'          => 0,
        'Symbol::Util'         => 0,
        'Config::General'      => 0,
    },
    EXE_FILES => [
        "bin/myutil_create_data.pl",
        "bin/myutil_dump_table_constraints.pl",
        "bin/myutil_dump_table_fks.pl",
        "bin/myutil_dump_tables.pl"
    ],
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES    => 'MySQL-Util-*' },
);
