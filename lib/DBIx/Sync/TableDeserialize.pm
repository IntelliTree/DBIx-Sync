package DBIx::Sync::TableDeserialize;
use Moose;
use RapidApp::StructuredIO::Reader;

has in            => ( is => 'ro', isa => 'RapidApp::StructuredIO::Reader', required => 1 );

has columnConfig  => ( is => 'ro', isa => 'DBIx::Sync::ColumnConfig', lazy_build => 1,
	handles => { map { $_ => $_ } qw(
		tableName columns columnCount pkColumns
		pkColumnCount nonPkColumns columnInfo allColumnInfo
	)}
);

sub _build_columnConfig {
	DBIx::Sync::ColumnConfig->new((shift)->streamHeader);
}

sub streamHeader {
	my $self= shift;
	return ($self->{streamHeader} ||= $self->_readHeader);
}

sub readHeader { (shift)->streamHeader }

sub readRow {
	my $self= shift;
	$self->streamHeader; # ensure header was read
	return $self->in->read;
}

sub _readHeader {
	my $self= shift;
	return $self->in->read || die "Expected header before EOF";
}

__PACKAGE__->meta->make_immutable;
1;