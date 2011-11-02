package TableCache;
use Moose;
use namespace::autoclean;

#
# the main purpose of this class is to capture cache hits
# to ensure the cache is working properly
#

has 'data' => (
    is => 'rw',
    required => 1,
    default  => sub { [] }
);

before 'data' => sub {
    my $self = shift;
    if ( !scalar(@_) ) 
        { $self->hits( $self->hits() + 1 ) }    # cache hit
};

has 'hits' => (
    is       => 'rw',
    isa      => 'Int',
    required => 0,
    default  => 0
);

package MySQL::Util;
use Moose;
use namespace::autoclean;
use DBI;
use Carp;

=head1 NAME

MySQL::Util - Utility functions for working with MySQL.

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

=for text

 my $util = MySQL::Util->new( dsn  => $ENV{DBI_DSN}, 
                              user => $ENV{DBI_USER} );

 my $aref = $util->describe_table('mytable');
 print "table: mytable\n";
 foreach my $href (@$aref) {
 	print "\t", $href->{FIELD}, "\n";
 }

 my $href = $util->get_ak_constraints('mytable');
 my $href = $util->get_ak_indexes('mytable');
 my $href = $util->get_constraints('mytable');
 ...

=cut 

#
# public variables
#

has 'dsn' => (
    is       => 'ro',
    isa      => 'Str',
    required => 1
);

has 'user' => (
    is       => 'ro',
    isa      => 'Str',
    required => 1
);

has 'pass' => (
    is       => 'ro',
    required => 0,
    default  => undef
);

#
# private variables
#

has '_dbh' => (
    is       => 'ro',
    writer   => '_set_dbh',
    init_arg => undef,        # By setting the init_arg to undef, we make it
         # impossible to set this attribute when creating a new object.
);

has '_index_cache' => (
    is       => 'rw',
    isa      => 'HashRef[TableCache]',
    init_arg => undef,
    default  => sub { {} }
);

has '_constraint_cache' => (
    is       => 'rw',
    isa      => 'HashRef[TableCache]',
    init_arg => undef,
    default  => sub { {} }
);

has '_depth_cache' => (
	is 		 => 'rw',
	isa		 => 'HashRef',
	init_arg => undef,
	default  => sub { {} }
);

has '_describe_cache' => (
	is 		 => 'rw',
	isa		 => 'HashRef',
	init_arg => undef,
	default  => sub { {} }
);

#
# this gets automatically invoked after the constructor
#
sub BUILD {
    my $self = shift;

    my $dbh = DBI->connect(
        $self->dsn,
        $self->user,
        $self->pass,
        {
            RaiseError       => 1,
            FetchHashKeyName => 'NAME_uc'
        }
    );

    $self->_set_dbh($dbh);
}

#######################################################################

=head1 METHODS

All methods croak in the event of failure unless otherwise noted.

=over 

=item new(dsn => $dsn, user => $user, [pass => $pass])

constructor

=cut


=item describe_table($table);

Returns an arrayref of column info for a given table. 

The structure of the returned data is:

$arrayref->[ { col1 }, { col2 } ]

Hash elements for each column:

	DEFAULT
	EXTRA
	FIELD
	KEY
	NULL
	TYPE
           
See MySQL documentation for more info on "describe <table>".
 
=cut

sub describe_table
{
	my $self  = shift;
	my $table = shift;	
	
    my $cache = '_describe_cache';

    if ( defined( $self->$cache->{$table} ) ) {
        return $self->$cache->{$table}->data;
    }

    my $sql = qq{
    	describe $table
	};

    my $dbh = $self->_dbh;
    my $sth = $dbh->prepare($sql);
    $sth->execute;

    my @cols;
    while ( my $row = $sth->fetchrow_hashref ) {
        push( @cols, { %$row } );
    }

    $self->$cache->{$table} = TableCache->new( data => \@cols );
    return \@cols;
}



=item get_ak_constraints($table)

Returns a hashref of the alternate key constraints for a given table.  Returns
an empty hashref if none were found.  The primary key is excluded from the
returned data.  

The structure of the returned data is:

$hashref->{constraint_name}->[ { col1 }, { col2 } ]

See "get_constraints" for a list of the hash elements in each column.

=cut

sub get_ak_constraints {
    my $self  = shift;
    my $table = shift;

    my $href = {};
    my $cons = $self->get_constraints($table);

    foreach my $con ( keys(%$cons) ) {
        if ( $cons->{$con}->[0]->{CONSTRAINT_TYPE} eq 'UNIQUE' ) {
            $href->{$con} = $cons->{$con};
        }
    }

    return $href;
}

=item get_ak_indexes($table)

Returns a hashref of the alternate key indexes for a given table.  Returns
an empty hashref if none were found.

The structure of the returned data is:

$href->{index_name}->[ { col1 }, { col2 } ]

See get_indexes for a list of hash elements in each column.
	
=cut

sub get_ak_indexes {
    my $self  = shift;
    my $table = shift;

    my $href = {};
    my $indexes = $self->get_indexes($table);

    foreach my $index ( keys(%$indexes) ) {
        if ( $indexes->{$index}->[0]->{NON_UNIQUE} == 0 ) {
            $href->{$index} = $indexes->{$index};
        }
    }

    return $href;
}

=item get_constraints($table)

Returns a hashref of the constraints for a given table.  Returns
an empty hashref if none were found.

The structure of the returned data is:

$hashref->{constraint_name}->[ { col1 }, { col2 } ]

Hash elements for each column:

	CONSTRAINT_TYPE
	COLUMN_NAME
	ORDINAL_POSITION
	POSITION_IN_UNIQUE_CONSTRAINT
	REFERENCED_TABLE_NAME
	REFERENCED_COLUMN_NAME
		
=cut

sub get_constraints {
    my $self  = shift;
    my $table = shift;

    my $cache = '_constraint_cache';

    if ( defined( $self->$cache->{$table} ) ) {
        return $self->$cache->{$table}->data;
    }

	croak "table '$table' does not exist: " if !$self->table_exists($table);
	
    my $sql = qq{
		select kcu.constraint_name, tc.constraint_type, column_name, 
		  ordinal_position, position_in_unique_constraint,
		  referenced_table_name, referenced_column_name 
	    from information_schema.table_constraints tc, 
	      information_schema.key_column_usage kcu 
		where tc.table_name = '$table'
		  and tc.table_name = kcu.table_name 
		  and tc.constraint_name = kcu.constraint_name 
		  and tc.constraint_schema = schema() 
		  and kcu.constraint_schema = tc.constraint_schema 
		order by constraint_name, ordinal_position
	};

    my $dbh = $self->_dbh;
    my $sth = $dbh->prepare($sql);
    $sth->execute;

    my $href = {};
    while ( my $row = $sth->fetchrow_hashref ) {
        my $name = $row->{CONSTRAINT_NAME};
        delete( $row->{CONSTRAINT_NAME} );

        if ( !defined( $href->{$name} ) ) { $href->{$name} = [] }

        push( @{ $href->{$name} }, {%$row} );
    }

    $self->$cache->{$table} = TableCache->new( data => $href );
    return $href;
}


=item get_depth($table)

Returns the table depth within the data model hierarchy.  The depth is 
zero based. 

For example:

=for text

 -----------       -----------
 | table A |------<| table B |
 -----------       -----------

=cut

=item 

Table A has a depth of 0 and table B has a depth of 1.  In other
words, table B is one level down in the model hierarchy.

If a table has multiple parents, the parent with the highest depth wins.

=cut

sub get_depth 
{
	my $self  = shift;
	my $table = shift;

	my $cache = '_depth_cache';
		
	if (defined($self->{$cache}->{$table})) 
		{ return $self->{$cache}->{$table}	; }
		
	my $dbh = $self->_dbh;
	
	my $fk_cons = $self->get_fk_constraints($table);	

	my $depth = 0;	
	
	foreach my $fk_name (keys (%$fk_cons)) {
		my $parent_table = $fk_cons->{$fk_name}->[0]->{REFERENCED_TABLE_NAME};
		
		if ($parent_table eq $table)
			{ next }  # self referencing table
		
		my $parent_depth = $self->get_depth($parent_table);	
		if ($parent_depth >= $depth)
			{ $depth = $parent_depth + 1 }
	}

	$self->{$cache}->{$table} = $depth;
	
	return $depth;
}


=item get_fk_constraints($table)

Returns a hashref of the foreign key constraints for a given table.  Returns
an empty hashref if none were found.

The structure of the returned data is:

$hashref->{constraint_name}->[ { col1 }, { col2 } ]

See "get_constraints" for a list of the hash elements in each column.

=cut

sub get_fk_constraints {
    my $self  = shift;
    my $table = shift;

    my $href = {};
    my $cons = $self->get_constraints($table);

    foreach my $con ( keys(%$cons) ) {
        if ( $cons->{$con}->[0]->{CONSTRAINT_TYPE} eq 'FOREIGN KEY' ) {
            $href->{$con} = $cons->{$con};
        }
    }

    return $href;
}

=item get_fk_indexes($table)

Returns a hashref of the foreign key indexes for a given table.  Returns
an empty hashref if none were found.  In order to qualify as a fk index, 
it must have a corresponding fk constraint.  

The structure of the returned data is:

$hashref->{index_name}->[ { col1 }, { col2 } ]

See "get_indexes" for a list of the hash elements in each column.

=cut

sub get_fk_indexes {
    my $self  = shift;
    my $table = shift;

    my $href = {};
    my $cons    = $self->get_fk_constraints($table);
    my $indexes = $self->get_indexes($table);

    foreach my $con_name ( keys(%$cons) ) {
        my @con_cols = @{ $cons->{$con_name} };

        foreach my $index_name ( keys(%$indexes) ) {
            my @index_cols = @{ $indexes->{$index_name} };

            if ( scalar(@con_cols) == scalar(@index_cols) ) {

                my $match = 1;
                for ( my $i = 0 ; $i < scalar(@con_cols) ; $i++ ) {
                    if ( $index_cols[$i]->{COLUMN_NAME} ne
                        $con_cols[$i]->{COLUMN_NAME} )
                    {
                        $match = 0;
                        last;
                    }
                }

                if ($match) {
                    $href->{$index_name} = $indexes->{$index_name};
                    last;
                }
            }
        }
    }

    return $href;
}

sub _get_indexes_arrayref {
    my $self  = shift;
    my $table = shift;

    my $cache = '_index_cache';

    if ( defined( $self->$cache->{$table} ) ) {
        return $self->$cache->{$table}->data;
    }

    my $dbh = $self->_dbh;
    my $sth = $dbh->prepare("show indexes in $table");
    $sth->execute;

    my $aref = [];
    while ( my $href = $sth->fetchrow_hashref ) {
        push( @$aref, {%$href} );
    }

    $self->$cache->{$table} = TableCache->new( data => $aref );
    return $aref;
}

=item get_indexes($table)

Returns a hashref of the indexes for a given table.  Returns
an empty hashref if none were found.

The structure of the returned data is:

$href->{index_name}->[ { col1 }, { col2 } ]

Hash elements for each column:

	CARDINALITY
	COLLATION
	COLUMN_NAME
	COMMENT
	INDEX_TYPE
	KEY_NAME
	NON_UNIQUE
	NULL
	PACKED
	SEQ_IN_INDEX
	SUB_PART
	TABLE
	
=cut

sub get_indexes {
    my $self  = shift;
    my $table = shift;

    my %h = ();
    my $indexes = $self->_get_indexes_arrayref($table);

    foreach my $index (@$indexes) {
        my $key_name = $index->{KEY_NAME};
        my $seq      = $index->{SEQ_IN_INDEX};

        if ( !exists( $h{$key_name} ) ) { $h{$key_name} = [] }

        $h{$key_name}->[ $seq - 1 ] = $index;
    }

    return \%h;
}

=item get_max_depth()

Returns the max table depth for all tables in the database.

See "get_depth" for additional info.

=cut

sub get_max_depth
{
	my $self = shift;
	
	my $dbh = $self->_dbh;
	
	my $tables = $self->get_tables();	
	
	my $max = 0;
	foreach my $table (@$tables) {
		my $depth = $self->get_depth($table);
		if ($depth > $max)
			{ $max = $depth }
	}
	
	return $max;
}

=item get_other_constraints($table)

Returns a hashref of the constraints that are not pk, ak, or fk  
for a given table.  Returns an empty hashref if none were found.

The structure of the returned data is:

$hashref->{constraint_name}->[ { col1 }, { col2 } ]

See "get_constraints" for a list of the hash elements in each column.

=cut

sub get_other_constraints {
    my $self  = shift;
    my $table = shift;

    my $fk = $self->get_fk_constraints($table);
    my $ak = $self->get_ak_constraints($table);

    my $href = {};
    my $cons = $self->get_constraints($table);

    foreach my $con_name ( keys(%$cons) ) {
        my $type = $cons->{$con_name}->[0]->{CONSTRAINT_TYPE};

        next if $type eq 'PRIMARY KEY';
        next if $type eq 'FOREIGN KEY';
        next if $type eq 'UNIQUE';

        $href->{$con_name} = $cons->{$con_name};
    }

    return $href;
}

=item get_other_indexes($table)

Returns a hashref of the indexes that are not pk, ak, or fk  
for a given table.  Returns an empty hashref if none were found.

The structure of the returned data is:

$hashref->{index_name}->[ { col1 }, { col2 } ]

See "get_indexes" for a list of the hash elements in each column.

=cut

sub get_other_indexes {
    my $self  = shift;
    my $table = shift;

    my $ak = $self->get_ak_indexes($table);
    my $fk = $self->get_fk_indexes($table);

    my $href = {};
    my $indexes = $self->get_indexes($table);

    foreach my $name ( keys %$indexes ) {
        next if $name eq 'PRIMARY';
        next if defined( $ak->{$name} );
        next if defined( $fk->{$name} );

        $href->{$name} = $indexes->{$name};
    }

    return $href;
}

=item get_pk_constraint($table)

Returns an arrayref of the primary key constraint for a given table.  Returns
an empty arrayref if none were found.

The structure of the returned data is:

$aref->[ { col1 }, { col2 }, ... ]

See "get_constraints" for a list of hash elements in each column.

=cut

sub get_pk_constraint {
    my $self  = shift;
    my $table = shift;

    my $cons = $self->get_constraints($table);

    foreach my $con_name ( keys(%$cons) ) {
        if ( $cons->{$con_name}->[0]->{CONSTRAINT_TYPE} eq 'PRIMARY KEY' ) {
            return $cons->{$con_name};
        }
    }

    return [];
}

=item get_pk_index($table)

Returns an arrayref of the primary key index for a given table. Returns
an empty arrayref if none were found.

The structure of the returned data is:

$aref->[ { col1 }, { col2 }, ... ]

See "get_indexes" for a list of the hash elements in each column.

=cut

sub get_pk_index {
    my $self  = shift;
    my $table = shift;

    my $href = $self->get_indexes($table);

    foreach my $name ( keys(%$href) ) {
        if ( $name eq 'PRIMARY' )    # mysql forces this naming convention
        {
            return $href->{$name};
        }
    }

    return [];
}


=item get_tables( )

Returns an arrayref of tables in the current database.  Returns undef
if no tables were found.

=cut

sub get_tables {
	my $self = shift;
	
    my $dbh = $self->_dbh;
    
    my $tables = undef;
    my $sth = $dbh->prepare("show tables");
    $sth->execute;
   
   	while(my ($table) = $sth->fetchrow_array) {
   		push(@$tables, $table);	
   	} 

   	return $tables;
}


=item table_exists($table)

Returns true if table exists.  Otherwise returns false.

=cut

sub table_exists
{
	my $self  = shift;
	my $table = shift;		
	
    my $dbh = $self->_dbh;
	my $sth = $dbh->prepare("show tables like '$table'");
	$sth->execute;

	my $cnt = 0;	
	while($sth->fetchrow_array) {
		$cnt++;	
	}	
	
	return $cnt;
}


=item use_dbh($dbname)

Used for switching database context.  Returns true on success.

=back

=cut

sub use_dbh {
    my $self   = shift;
    my $dbname = shift;

    $self->_dbh->do("use $dbname");
    return 1;
}


=head1 AUTHOR

John Gravatt, C<< <gravattj at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-mysql-util at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=MySQL-Util>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc MySQL::Util


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=MySQL-Util>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/MySQL-Util>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/MySQL-Util>

=item * Search CPAN

L<http://search.cpan.org/dist/MySQL-Util/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2011 John Gravatt.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

__PACKAGE__->meta->make_immutable; # moose stuff

1;
