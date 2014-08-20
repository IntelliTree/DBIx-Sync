package DBIx::Sync::TableDiffApply;
use Moose;

use Params::Validate ':all';
use RapidApp::Debug 'DEBUG';
use DBIx::Sync::ColumnConfig;

has dbh          => ( is => 'ro', lazy => 1, default => sub { (shift)->columnConfig->dbh }, required => 1 );
has columnConfig => ( is => 'ro', isa => 'DBIx::Sync::ColumnConfig', required => 1,
	handles => { map { $_ => $_ } qw(
		tableName columns columnCount columnAt pkColumns pkColumnCount nonPkColumns columnInfo allColumnInfo
	)}
);

has insertStmt    => ( is => 'ro', lazy_build => 1 );
has deleteStmt    => ( is => 'ro', lazy_build => 1 );

# We customize this one to be correctly quoted for the DBD type.
has tableNameStr  => ( is => 'ro', lazy_build => 1 );

has _updateStmtCache    => ( is => 'rw', isa => 'HashRef', default => sub {{}} );

sub BUILD {
	my $self= shift;
	unless ($self->dbh->{RaiseError}) {
		warn "Turning on RaiseError in dbh";
		$self->dbh->{RaiseError}= 1;
	}
}

sub applyDiffItem {
	my ($self, $change)= @_;
	my $flag= $change->[0];
	
	# row deleted in source
	if ($flag eq '-') {
		my $row= $change->[1];
		my @pkVal= @$row[0..$self->pkColumnCount-1];
		$self->delete(\@pkVal);
	}
	
	# row added to source
	elsif ($flag eq '+') {
		my $row= $change->[1];
		$self->insert($row);
	}
	
	# row changed in source
	elsif ($flag eq ':') {
		my ($flag, $pkVal, $old, $new)= @$change;
		my %namedKeys;
		while (my ($k, $v)= each %$new) {
			$namedKeys{$self->columnAt($k)}= $v;
		}
		$self->update($pkVal, \%namedKeys);
	}
	else {
		die "BUG: Unhandled diff type '$flag'";
	}
}

sub insert {
	my ($self, $row)= @_;
	scalar(@$row) eq $self->columnCount
		or die "Wrong number of elements for insert statement: ".scalar(@$row)." != ".$self->columnCount;
	$self->insertStmt->execute(@$row);
}

sub delete {
	my ($self, $pkVal)= @_;
	$self->pkColumnCount > 0
		or die "Cannot run deletions on a table without a primary key.  Use deleteAll.";
	scalar(@$pkVal) eq $self->pkColumnCount
		or die "Wrong number of key parameters for delete statement: ".scalar(@$pkVal)." != ".$self->pkColumnCount;
	$self->deleteStmt->execute(@$pkVal);
}

sub deleteAll {
	my ($self)= @_;
	$self->dbh->prepare('DELETE FROM '.$self->tableNameStr)->execute();
}

sub update {
	my ($self, $pkVal, $changeHash)= @_;
	$self->pkColumnCount > 0
		or die "Cannot run updates on a table without a primary key";
	scalar(@$pkVal) eq $self->pkColumnCount
		or die "Wrong number of primary key values";
	
	my @cols= sort keys %$changeHash;
	my @values= map { $changeHash->{$_} } @cols;
	$self->getUpdateStmt(\@cols)->execute(@values, @$pkVal);
}

sub quoteIdent {
	my $self= shift;
	$self->dbh->quote_identifier((ref($_[0]) eq 'ARRAY')? @{ $_[0] } : @_);
}

sub _build_tableNameStr {
	my $self= shift;
	$self->quoteIdent($self->tableName);
}

sub getUpdateStmt {
	my ($self, $columns)= @_;
	my $cacheKey= join("\n", @$columns);
	return ($self->_updateStmtCache->{$cacheKey} ||= $self->dbh->prepare(
		'UPDATE '.$self->tableNameStr.
		' SET '.join(', ', map { $self->quoteIdent($_).' = ?' } @$columns).
		' WHERE '.join(' AND ', map { $self->quoteIdent($_).' = ?' } $self->pkColumns)
	));
}

sub _build_insertStmt {
	my ($self)= @_;
	return $self->dbh->prepare(
		'INSERT INTO '.$self->tableNameStr.
		' ('.join(',', map { $self->quoteIdent($_) } $self->columns).')'.
		' VALUES ('.join(',', map { '?' } $self->columns).')'
	);
}

sub _build_deleteStmt {
	my ($self)= @_;
	return $self->dbh->prepare(
		'DELETE FROM '.$self->tableNameStr.
		' WHERE '.join(' AND ', map { $self->quoteIdent($_).' = ?' } $self->pkColumns)
	);
}

__PACKAGE__->meta->make_immutable;
1;
