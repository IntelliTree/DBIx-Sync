package DBIx::Sync::TableDiff;
use Moose;

use Params::Validate ':all';
use RapidApp::Debug 'DEBUG';
use DBIx::Sync::ColumnConfig;

# The columns which should be compared to see if a row has changed.
# If not defined, "->nonPkColumns" will be used instead.
# You could optimize the diff process by making this refer to an updated_timestamp or something.
# You might also want to specify this if you have a large blob field that you don't want to compare
#   byte-by-byte when another column(s) would tell you whether it changed.
has relevantColumns => ( is => 'ro', isa => 'ArrayRef', auto_deref => 1, default => sub {[]} );

has oldTable       => ( is => 'ro', required => 1 );
has newTable       => ( is => 'ro', required => 1,
	handles => { qw(
		columns columns
		columnCount columnCount
		pkColumns pkColumns
		pkColumnCount pkColumnCount
		nonPkColumns nonPkColumns
		columnInfo columnInfo
		allColumnInfo allColumnInfo
	)}
);

has rowCompareProc => ( is => 'ro', isa => 'CodeRef', lazy_build => 1 );
has matchCount     => ( is => 'rw', isa => 'Int', default => 0 );
has diffCount      => ( is => 'rw', isa => 'Int', default => 0 );
has insCount       => ( is => 'rw', isa => 'Int', default => 0 );
has delCount       => ( is => 'rw', isa => 'Int', default => 0 );

sub BUILD {
	my $self= shift;
	my %cols= map { $_ => 1 } $self->newTable->columns;
	my %pkCols= map { $_ => 1 } $self->newTable->pkColumns;
	for ($self->relevantColumns) {
		$pkCols{$_} and die "relevantColumns may not contain a primary key column ($_)";
		$cols{$_} or die "relevantColumns must be included in columns ($_)";
	}
	$self->checkSourceCompatibility;
}

sub checkSourceCompatibility {
	my $self= shift;
	my @oldPk= $self->oldTable->pkColumns;
	my @newPk= $self->newTable->pkColumns;
	_arrayEquals(\@oldPk, \@newPk) or die DEBUG(tablediff => \@oldPk, \@newPk) && "Primary keys are not compatible (use DEBUG_TABLEDIFF=1 for details)";
	my @oldCols= $self->oldTable->columns;
	my @newCols= $self->newTable->columns;
	_arrayEquals(\@oldCols, \@newCols) or die DEBUG(tablediff => \@oldCols, \@newCols) && "Column lists are not compatible (use DEBUG_TABLEDIFF=1 for details)";
	1;
}

sub _arrayEquals {
	my ($a, $b)= @_;
	scalar(@$a) eq scalar(@$b) or return 0;
	no warnings 'uninitialized';
	for (my $i= $#$a; $i >= 0; $i--) {
		return 0 unless $a->[$i] eq $b->[$i];
	}
	return 1;
}

# Pseudocode of the compare proc:
#   For each key column:
#     If row1 < row2, return -1;
#     If row1 > row2, return 1;
#   If relevantColumns
#     Identical= (row1->[x] eq row2->[x]) && ... for each relevantColumns
#   return '=' if Identical
#   For each column (not pk),
#     if the column values don't match,
#       add a diff entry
#   If no differences, return '=';
#   return (':', old, new)
sub _build_rowCompareProc {
	my $self= shift;
	my @cols= $self->columns;
	my @pkCols= $self->pkColumns;
	if (!scalar @pkCols) {
		# In the event we're scanning a table with no PK, things will actually still work
		# as long as there are only insertions and equality.  We need to compare all columns
		# as if they were a primary key, or else insertions look like changes.
		@pkCols= @cols;
	}
	my @checkCols= $self->relevantColumns;
	
	my %numericColType= ( NUMBER => 1, FLOAT => 1, DOUBLE => 1 );

	my $i= 0;
	my %colIdx= map { $_ => $i++ } @cols;
	
	my @stmts;
	for $i (0..$#pkCols) {
		my $colInfo= $self->allColumnInfo->{$pkCols[$i]};
		# TODO: XXX: We need to add date-time support
		my $op= $numericColType{uc $colInfo->{TYPE_NAME}}? '<=>' : 'cmp';
		push @stmts, '($cmp= $a->['.$i.'] '.$op.' $b->['.$i.']) and return $cmp;';
	}
	
	if (@checkCols) {
		my @checkColIdx= map { $colIdx{$_} } @checkCols;
		push @stmts, 'return "=" if '.join(' && ', map { '$a->['.$_.'] eq $b->['.$_.']' } @checkColIdx).';';
	}
	
	my @diffChecks;
	for (my $i= scalar(@pkCols); $i< scalar(@cols); $i++) {
		push @diffChecks, 'if ($a->['.$i.'] ne $b->['.$i.']) { $old{'.$i.'}= $a->['.$i.']; $new{'.$i.'}= $b->['.$i.']; }';
	}
	
	my $code= 'sub {
		my ($a, $b)= @_;
		my $cmp;
		no warnings "uninitialized";
		'.join("\n		",@stmts).'
		my (%old, %new);
		'.join("\n      ",@diffChecks).'
		return "=" unless %new;
		return (":", \%old, \%new );
	}';
	
	DEBUG(tablediff => 'compareProc =', $code);
	local $@;
	return eval $code or die $@;
}

sub nextChange {
	my ($self)= @_;
	
	# read the next live-table row, if don't have one from before
	my $new= delete $self->{_curNewRow};
	unless (defined $new or $self->{_newEof}) {
		$new= $self->newTable->readRow
			or $self->{_newEof}= 1;
	}
	
	# read the next historical row, if don't have one from before
	my $old= delete $self->{_curOldRow};
	unless (defined $old or $self->{_oldEof}) {
		$old= $self->oldTable->readRow
			or $self->{_oldEof}= 1;
	}
	
	# compare the rows, if we have one of each
	my @pkIdxs= 0 .. ($self->pkColumnCount-1);
	if (defined $old && defined $new) {
		# we use a dynamically generated compare function, which should be pretty fast
		my ($cmp, $oldValHash, $newValHash)= $self->rowCompareProc->($old, $new);
		DEBUG(tablediff_verbose => 'rowCompare(', $old, ',', $new, ,')', ' = (', $cmp, $oldValHash, $newValHash, ')');
		
		if ($cmp eq '=') {
			# identical.  Try again with new rows.
			$self->{matchCount}++;
			goto &nextChange;
		}
		
		if ($cmp eq ':') {
			$self->{diffCount}++;
			return [':', [ @$new[@pkIdxs] ], $oldValHash, $newValHash ];
		}
		
		if ($cmp > 0) {
			$self->{_curOldRow}= $old; # save for later
			$self->{insCount}++;
			return ['+', $new];
		}
		if ($cmp < 0) {
			$self->{_curNewRow}= $new; # save for later
			$self->{delCount}++;
			return ['-', $old];
		}
		
		die "Unhandled return value from compareProc: '$cmp'";
	}
	
	# return the leftovers as "deleted" or "new" rows
	if (defined $old) {
		$self->{delCount}++;
		return ['-', $old];
	}
	if (defined $new) {
		$self->{insCount}++;
		return ['+', $new];
	}
	
	# EOF on both sources.  nothing left to do.
	return;
}

sub reset {
	my $self= shift;
	$self->oldTable->reset();
	$self->newTable->reset();
	$self->matchCount(0);
	$self->diffCount(0);
	$self->insCount(0);
	$self->delCount(0);
}

__PACKAGE__->meta->make_immutable;
1;