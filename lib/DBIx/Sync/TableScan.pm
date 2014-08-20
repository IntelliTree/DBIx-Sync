package DBIx::Sync::TableScan;
use Moose;
use Params::Validate ':all';
use DBIx::Sync::ColumnConfig;

has dbh          => ( is => 'ro', lazy => 1, default => sub { (shift)->columnConfig->dbh }, required => 1 );
has columnConfig => ( is => 'ro', isa => 'DBIx::Sync::ColumnConfig', required => 1,
	handles => { map { $_ => $_ } qw(
		tableName columns columnCount columnAt pkColumns pkColumnCount nonPkColumns columnInfo allColumnInfo
	)}
);

# We customize this one to be correctly quoted for the DBD type.
has tableNameStr  => ( is => 'ro', lazy_build => 1 );

has readCount    => ( is => 'rw', isa => 'Int', default => 0 );
has rowCount     => ( is => 'ro', lazy_build => 1 );

has _activeScanStmt => ( is => 'ro', lazy_build => 1 );

sub _build__activeScanStmt { (shift)->createScanStmt }

sub _build_rowCount {
	my $self= shift;
	my ($count)= $self->dbh->selectrow_array("SELECT COUNT(*) FROM ".$self->tableNameStr);
	return $count;
}

sub queryRowCount {
	my $self= shift;
	return $self->rowCount($self->_build_rowCount());
}

sub createScanStmt {
	my $self= shift;
	
	my @orderCols= $self->pkColumns;
	
	# handle case of no PK
	@orderCols= $self->columns unless scalar @orderCols;
	
	my $sql=
		"SELECT ".join(',', map { $self->quoteIdent($_) } $self->pkColumns, $self->nonPkColumns)
		." FROM ".$self->tableNameStr;
	
	scalar(@orderCols)
		and $sql .= " ORDER BY ".join(',', map { $self->quoteIdent($_) } @orderCols);

	my $sth= $self->dbh->prepare($sql);
	$sth->execute();
	$self->readCount(0);
	return $sth;
}

sub readRow {
	my $self= shift;
	++$self->{readCount};
	my @vals= $self->_activeScanStmt->fetchrow_array;
	return @vals? \@vals : undef;
}

sub reset {
	my $self= shift;
	$self->_clear_activeScanStmt;
	$self->readCount(0);
}

sub quoteIdent {
	my $self= shift;
	$self->dbh->quote_identifier((ref($_[0]) eq 'ARRAY')? @{ $_[0] } : @_);
}

sub _build_tableNameStr {
	my $self= shift;
	$self->quoteIdent($self->tableName);
}


__PACKAGE__->meta->make_immutable;
1;