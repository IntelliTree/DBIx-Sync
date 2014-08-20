package DBIx::Sync::ColumnConfig;
use Moose;
use RapidApp::Debug 'DEBUG';

# handle to the database
has dbh           => ( is => 'ro', isa => 'Maybe[DBI::db]', required => 1 );

# Table to search.  Should be an array of (Catalog,Schema,Table)
has tableName     => ( is => 'ro', isa => 'ArrayRef', auto_deref => 1, required => 1 );

# table name to use in queries.  Defaults to "$Catalog.$Schema.$Table", with undef elements omitted.
has tableNameStr  => ( is => 'ro', isa => 'Str', lazy_build =>1 );

# Columns which should be part of the serialize process (other columns will be completely ignored)
# This will be rearranged so that it starts with the primary key.
has _desiredColumns => ( is => 'ro', isa => 'ArrayRef', init_arg => 'columns' );
has columns       => ( is => 'ro', isa => 'ArrayRef', auto_deref => 1, lazy_build => 1, init_arg => undef );
sub columnCount { my $self= shift; $self->columns; scalar @{$self->{columns}} }
sub columnAt    { my ($self, $idx)= @_; $self->columns; $self->{columns}->[$idx]; }

has pkColumns     => ( is => 'ro', isa => 'ArrayRef', auto_deref => 1, lazy_build => 1 );
has pkColumnCount => ( is => 'ro', isa => 'Int', lazy_build => 1, required => 1 );

has nonPkColumns  => ( is => 'ro', isa => 'ArrayRef', auto_deref => 1, lazy_build => 1 );

has columnInfo    => ( is => 'ro', isa => 'ArrayRef', lazy_build => 1 );
has allColumnInfo => ( is => 'ro', isa => 'HashRef', lazy_build => 1 );

sub columnConfig {
	$_[0] # return self
}

sub BUILD {
	my $self= shift;
	if ($self->dbh) {
		unless ($self->dbh->{RaiseError}) {
			warn "Turning on RaiseError in dbh";
			$self->dbh->{RaiseError}= 1;
		}
	}
}

# Save everything that is needed to pass to the constructor to get tback the
# same column config without needing the db handle
sub toHash {
	my $self= shift;
	return {
		tableName     => [ $self->tableName ],
		pkColumns     => [ $self->pkColumns ],
		nonPkColumns  => [ $self->nonPkColumns ],
		allColumnInfo => $self->allColumnInfo
	};
}

sub _build_tableNameStr {
	my $self= shift;
	return join '.', grep { defined $_ } $self->tableName;
}

sub _build_allColumnInfo {
	my $self= shift;
	my $infoSth= $self->dbh->column_info( $self->tableName, '%' );
	my $ret= {};
	while (my $info= $infoSth->fetchrow_hashref) {
		$ret->{$info->{COLUMN_NAME}}= $info;
	}
	return $ret;
}

sub _build_pkColumns {
	my $self= shift;
	# found a bug where ->primary_key generates wrong columns! so use primary_key_info
	my $keyStmt= $self->dbh->primary_key_info($self->tableName);
	my $cols= $keyStmt->fetchall_arrayref({});
	my $x= [ map { $_->{COLUMN_NAME} } sort { $a->{KEY_SEQ} <=> $b->{KEY_SEQ} } @$cols ];
	DEBUG(columnconfig => "Loaded primary key: ", $x, " from table ", $self->tableNameStr, @$cols);
	return $x;
}

sub _build_pkColumnCount {
	my $self= shift;
	$self->pkColumns;
	return scalar @{$self->{pkColumns}};
}

sub _build_nonPkColumns {
	my $self= shift;
	my %isPk= map { $_ => 1 } $self->pkColumns;
	my $desired= $self->_desiredColumns;
	if (!$desired) {
		# default to all columns
		my @allInfo= values %{ $self->allColumnInfo };
		my @orderedInfo= sort { $a->{ORDINAL_POSITION} <=> $b->{ORDINAL_POSITION} } @allInfo;
		$desired= [ map { $_->{COLUMN_NAME} } @orderedInfo ];
	}
	return [ grep { !$isPk{$_} } @$desired ];
}

# note that attempts to set 'columns' in the constructor go into the private field '_desiredColumns'
sub _build_columns {
	my $self= shift;
	return [ $self->pkColumns, $self->nonPkColumns ];
}

sub _build_columnInfo {
	my $self= shift;
	my $allCols= $self->allColumnInfo;
	return [ map { $allCols->{$_} || die "Column does not exist: $_" } $self->columns ];
}

__PACKAGE__->meta->make_immutable;
1;