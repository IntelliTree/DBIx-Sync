package DBIx::Sync::TableSerialize;
use Moose;
use Params::Validate ':all';
use RapidApp::StructuredIO::Writer;

use DBIx::Sync::TableScan;

# handle to the database
has source       => ( is => 'ro', required => 1 );

# Filehandle to write the serialization.  If not specified, the serialization is silently discarded.
has out          => ( is => 'ro', isa => 'RapidApp::StructuredIO::Writer' );

has onProgress       => ( is => 'rw', isa => 'Maybe[CodeRef]' );
has progressInterval => ( is => 'rw', isa => 'Int', default => 0 );
has rowsProcessed    => ( is => 'rw', isa => 'Int', default => 0, init_arg => undef );
has _lastProgress    => ( is => 'rw', isa => 'Int', default => 0, init_arg => undef );

sub writeTableDescription {
	my $self= shift;
	$self->out->write( $self->source->columnConfig->toHash );
}

sub writeRow {
	my ($self, $row)= @_;
	$self->out->write($row);
}

sub run {
	my $self= shift;
	my %p= validate(@_, { onProgress => 0, progressInterval => 0 });
	
	local $self->{onProgress}= $p{onProgress} if defined $p{onProgress};
	local $self->{progressInterval}= $p{progressInterval} if defined $p{progressInterval};
	local $self->{rowsProcessed}= 0;
	
	my $count= $self->onProgress? $self->source->rowCount : 0;
	
	$self->writeTableDescription;
	while (my $row= $self->source->readRow) {
		$self->writeRow($row);
		my $procd= ++$self->{rowsProcessed};
		if ($self->onProgress) {
			if ($procd - $self->{_lastProgress} > $self->{progressInterval}) {
				$self->_lastProgress($procd);
				$self->onProgress->(processed => $procd, total => $count, row => $row);
			}
		}
	}
	if ($self->onProgress) {
		$self->onProgress->(processed => $self->rowsProcessed, total => $self->rowsProcessed, row => undef);
	}
	return $self->rowsProcessed > 0? $self->rowsProcessed : '0E0';
}

__PACKAGE__->meta->make_immutable;
1;