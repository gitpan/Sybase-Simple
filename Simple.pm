#
# $Id: Simple.pm,v 1.2 1999/11/25 00:41:55 mpeppler Exp $
#
# Copyright (c) 1998-1999   Michael Peppler
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file,
#   with the exception that it cannot be placed on a CD-ROM or similar media
#   for commercial distribution without the prior approval of the author.
#

package Sybase::Simple;

use strict;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

require Exporter;
require AutoLoader;

use Carp;
use Sybase::CTlib;

@ISA = qw(Exporter AutoLoader Sybase::CTlib);
@EXPORT = @Sybase::CTlib::EXPORT;
$VERSION = '0.50';

my %CallBacks;

Sybase::CTlib::ct_callback(CS_SERVERMSG_CB, \&srv_cb);
Sybase::CTlib::ct_callback(CS_CLIENTMSG_CB, \&msg_cb);

sub ct_callback($$) {
    my $type = shift;
    my $sub  = shift;

    if($sub && ref($sub) ne 'CODE') {
	carp "ct_callback() called without a subroutine reference";
	return $CallBacks{$type};
    }

    my $old = $CallBacks{$type};
    $CallBacks{$type} = $sub;

    $old;
}

sub srv_cb {
    my($dbh, $number, $severity, $state, $line, $server, $proc, $msg)
	= @_;

    # Don't print informational or status messages
    if($severity > 10)
    {
	local $^W = 0;
	$dbh->{SIMPLE}->{ERROR} = $number;
	$dbh->{SIMPLE}->{ERROR_TEXT} = 
	    sprintf("%d %d %d %d %s %s %s",
		    $number, $severity, $state, $line, $server, $proc, $msg);
    } 

    if($CallBacks{Sybase::CTlib::CS_SERVERMSG_CB()}) {
	&{$CallBacks{Sybase::CTlib::CS_SERVERMSG_CB()}}(@_);
    } else {
	if($severity > 10) {
	    print STDERR "$dbh->{SIMPLE}->{ERROR_TEXT}\n";
	} elsif($number == 0) {
	    print STDERR "$msg\n";
	}
    }

    CS_SUCCEED;
}

sub msg_cb
{
    my($layer, $origin, $severity, $number, $msg, $osmsg, $dbh) = @_;

    if($CallBacks{Sybase::CTlib::CS_CLIENTMSG_CB()}) {
	&{$CallBacks{Sybase::CTlib::CS_CLIENTMSG_CB()}}(@_);
    } else {
	my $string = sprintf("OC: %d %d %s", $number, $severity, $msg);
	if(defined($osmsg)) {
	    $string .= " OS: $osmsg";
	}
	
	if($dbh) {
	    $dbh->{SIMPLE}->{ERROR} = $number;
	    $dbh->{SIMPLE}->{ERROR_TEXT} = $string;
	}
	print STDERR $string, "\n";
    }

    CS_SUCCEED;
}



sub new {
    my ($package, $user, $pwd, $server, $appname, $hash) = @_;

    my %simple = (ERROR => 0, ERROR_TEXT => '', SQL => '', CONFIG => {});

    $hash->{SIMPLE} = {%simple};

    my $dbh = $package->SUPER::new($user, $pwd, $server, $appname, $hash);

    $dbh;
}

sub config {
    my $self = shift;
    my %vals = @_;

    foreach my $k (keys(%vals)) {
	$self->{SIMPLE}->{CONFIG}->{$k} = $vals{$k};
    }
}

sub lastErr {
    my $self = shift;

    $self->{SIMPLE}->{ERROR};
}

sub lastErrText {
    my $self = shift;

    $self->{SIMPLE}->{ERROR_TEXT};
}

sub cleanError {
    my $self = shift;

    if(defined($self->{SIMPLE}->{ERROR})) {
	$self->{SIMPLE}->{ERROR} = 0;
	$self->{SIMPLE}->{ERROR_TEXT} = '';
    }
    $self->{SIMPLE}->{SQL} = '';
}

sub Scalar {
    my $self = shift;
    my $sql  = shift;

    $self->cleanError;

    $self->{SIMPLE}->{SQL} = $sql;

    my $restype;
    my @data;
    $self->ct_execute($sql) == CS_SUCCEED || return undef;
    while($self->ct_results($restype) == CS_SUCCEED) {
	next unless $self->ct_fetchable($restype);
	@data = $self->ct_fetch;
	# we're only interested in the first row of the first result set
	$self->ct_cancel(CS_CANCEL_ALL);
    }

    $data[0];
}

sub HashRow {
    my $self = shift;
    my $sql  = shift;

    $self->cleanError;

    $self->{SIMPLE}->{SQL} = $sql;

    my $restype;
    my %data;
    my $seen;
    $self->ct_execute($sql) == CS_SUCCEED || return undef;
    while($self->ct_results($restype) == CS_SUCCEED) {
	next unless $self->ct_fetchable($restype);
	if($restype == CS_STATUS_RESULT) {
	    # This means that we've executed a stored proc, and the first
	    # result is the status result. This *probably* means that
	    # the proc didn't return any rows
	    while(my $d = $self->ct_fetch(0,1)) {
		;
	    }
	    next;
	}
	while(my $d = $self->ct_fetch(1, 1)) {
	    if(!$seen) {
		# fetch one row as a hash
		%data = %$d;
		$seen = 1;
	    }
	}
	# we're only interested in the first row of the first result set
	# we can't use ct_cancel() here because the a stored proc
	# might call a raiserror *after( the first SELECT, and we
	# still want the error handlers to catch the raiserror!
#	$self->ct_cancel(CS_CANCEL_ALL);
    }

    \%data;
}

sub ArrayOfHash {
    my $self = shift;
    my $sql  = shift;

    $self->cleanError;

    $self->{SIMPLE}->{SQL} = $sql;

    my $restype;
    my %data;
    my $ret = [];
    $self->ct_execute($sql) == CS_SUCCEED || return undef;
    while($self->ct_results($restype) == CS_SUCCEED) {
	next unless $self->ct_fetchable($restype);
	if($restype == CS_STATUS_RESULT) {
	    # We don't want to include the status result (the return xxx)
	    # from a stored procedure in the array of hashes that we return.
	    while(my $d = $self->ct_fetch(0,1)) {
	    }
	    next;
	}
	# fetch one row as a hash
	while(%data = $self->ct_fetch(CS_TRUE)) {
	    # push the results onto an array
	    push(@$ret, {%data});
	}
    }

    $ret;
}

sub HashOfScalar {
    my $self = shift;
    my $sql  = shift;
    my $key  = shift;
    my $val  = shift;

    $self->cleanError;

    $self->{SIMPLE}->{SQL} = $sql;

    my $restype;
    my %row;
    my $ret = {};
    $self->ct_execute($sql) == CS_SUCCEED || return undef;
    while($self->ct_results($restype) == CS_SUCCEED) {
	next unless $self->ct_fetchable($restype);
	# in this case we want to make sure that only "normal" rows are placed
	# in the result hash. Result rows of stored procedure output params,
	# or status results are ignored.
	if($restype == CS_ROW_RESULT) {
	    while(%row = $self->ct_fetch(CS_TRUE)) {
		if(!defined($row{$key})) {
		    # having a NULL key value is a problem - just like having a
		    # null primary key!
		    warn("Got a NULL value for $key in $sql - this is not supported");
		    next;
		}

		# store the value in the $val column in the hash at the index
		# position represented by the $key column
		$ret->{$row{$key}} = $row{$val};
	    }
	} else {
	    # ignore non-row results
	    while(my $d = $self->ct_fetch(0, 1)) {
		;
	    }
	}
    }

    $ret;
}

sub HashOfHash {
    my $self = shift;
    my $sql  = shift;
    my $key  = shift;

    $self->cleanError;

    $self->{SIMPLE}->{SQL} = $sql;

    my $restype;
    my %row;
    my $ret = {};
    $self->ct_execute($sql) == CS_SUCCEED || return undef;
    while($self->ct_results($restype) == CS_SUCCEED) {
	next unless $self->ct_fetchable($restype);
	# in this case we want to make sure that only "normal" rows are placed
	# in the result hash. Result rows of stored procedure output params,
	# or status results are ignored.
	if($restype == CS_ROW_RESULT) {
	    while(%row = $self->ct_fetch(CS_TRUE)) {
		if(!defined($row{$key})) {
		    # having a NULL key value is a problem - just like having a
		    # null primary key!
		    warn("Got a NULL value for $key in $sql - this is not supported");
		    next;
		}
		# store the entire row (via reference to the hash) in the hash 
		# at the index position represented by the $key column
		$ret->{$row{$key}} = {%row};
	    }
	} else {
	    # ignore non-row results
	    while(my $d = $self->ct_fetch(0, CS_TRUE)) {
		;
	    }
	}
    }

    $ret;
}

sub HashOfHashOfHash {
    my $self = shift;
    my $sql  = shift;
    my $key1 = shift;
    my $key2 = shift;

    $self->cleanError;

    $self->{SIMPLE}->{SQL} = $sql;

    my $restype;
    my %row;
    my $ret = {};
    $self->ct_execute($sql) == CS_SUCCEED || return undef;
    while($self->ct_results($restype) == CS_SUCCEED) {
	next unless $self->ct_fetchable($restype);
	# in this case we want to make sure that only "normal" rows are placed
	# in the result hash. Result rows of stored procedure output params,
	# or status results are ignored.
	if($restype == CS_ROW_RESULT) {
	    while(%row = $self->ct_fetch(CS_TRUE)) {
		if(!defined($row{$key1})) {
		    # having a NULL key value is a problem - just like having a
		    # null primary key!
		    warn("Got a NULL value for $key1 in $sql - this is not supported");
		    next;
		}
		if(!defined($row{$key2})) {
		    # having a NULL key value is a problem - just like having a
		    # null primary key!
		    warn("Got a NULL value for $key2 in $sql - this is not supported");
		    next;
		}
		# store the entire row (via reference to the hash) in the hash 
		# at the index position represented by the $key column
		$ret->{$row{$key1}}->{$row{$key2}} = {%row};
	    }
	} else {
	    # ignore non-row results
	    while(my $d = $self->ct_fetch(0, CS_TRUE)) {
		;
	    }
	}
    }

    $ret;
}

# Exec some SQL, and ignore any returned rows. Usefull for insert/delete/update
# or stored procs that perform those kinds of operations.
# If the AbortOnError config parameter is non-0 then the batch is 
# aborted on the first error.
# If the DeadlockRetry config parameter is non-0, then retry the batch if
# there is a deadlock error
sub ExecSql {
    my $self         = shift;
    my $sql          = shift;

    $self->cleanError;

    $self->{SIMPLE}->{SQL} = $sql;

    my $restype;
    my $row;
    my $err = 0;
    my $ret;
    my $status = 0;

    DEADLOCK_RETRY:;
    $self->ct_execute($sql) == CS_SUCCEED || return 0;
    while(($ret = $self->ct_results($restype)) == CS_SUCCEED) {
	if($restype == CS_CMD_FAIL) {
	    if($self->{SIMPLE}->{CONFIG}->{DeadlockRetry} &&
	      $self->{SIMPLE}->{ERROR} == 1205) {
		$self->ct_cancel(CS_CANCEL_ALL);
		$err = 0;
		$status = 0;
		goto DEADLOCK_RETRY;
	    }
	    if($self->{SIMPLE}->{CONFIG}->{AbortOnError}) {
		$self->ct_cancel(CS_CANCEL_ALL);
		return 0;
	    }
	    ++$err;
	}
	next unless $self->ct_fetchable($restype);

	if($restype == CS_STATUS_RESULT) {
	    ($status) = $self->ct_fetch;
	    if($status && $self->{SIMPLE}->{CONFIG}->{AbortOnError}) {
		$self->ct_cancel(CS_CANCEL_ALL);
		return 0;
	    }
	    while($row = $self->ct_fetch(0, 1)) {
		;
	    }
	} else {
	    # Oops - normally there shouldn't be any fetchable rows in the
	    # sql we execute - warn the user if the -w switch is set
	    carp "Found rows when executing '$sql'!" if $^W != 0;
	    while($row = $self->ct_fetch(0, 1)) {
		;
	    }
	}
    }
    if($ret == CS_FAIL) {
	$self->ct_cancel(CS_CANCEL_ALL);
	++$err;
    }

    $err++ if $status;

    $err == 0;			# return TRUE if no errors were found
}

    
sub HashIter {
    my $self = shift;
    my $sql  = shift;

    $self->cleanError;

    $self->{SIMPLE}->{SQL} = $sql;

    my $iter = {Handle => $self}; # $iter is a reference to a hash, where
				# one element is the database handle

    $self->ct_execute($sql) == CS_SUCCEED || return undef;

    my $restype;
    my $ret;
    while(($ret = $self->ct_results($restype)) == CS_SUCCEED) {
	# if we've got a fetchable result set we break out of this loop
	last if $self->ct_fetchable($restype);
    }
    return undef if($ret != CS_SUCCEED); # no fetchable rows in the query!

    $iter->{LastResType} = $restype; # remeber what the last ct_results()
				# $restype was

    # "bless" the $iter variable into the Sybase::Simple::HashIter package
    bless($iter, "Sybase::Simple::HashIter");
}

package Sybase::Simple::HashIter;

use Sybase::CTlib;		# import CS_* symbols into this namespace
sub next {
    my $self = shift;

    my %data;
    my $restype;

 loop: {
	%data = $self->{Handle}->ct_fetch(CS_TRUE);
	if(!defined(%data) || keys(%data) == 0) {
	    # no more data in this result set - so check if there is another
	    # one...
	    while($self->{Handle}->ct_results($restype) == CS_SUCCEED) {
		if($self->{Handle}->ct_fetchable($restype)) {
		    # yep - there's fetchable data
		    $self->{LastResType} = $restype;
		    redo loop;	# jump to the 'loop' lable above
		}
	    }
	    return undef;	# no more data - ct_results() returned
				# something other than CS_SUCCEED
	}
    }
    
    \%data;
}

sub DESTROY {
    my $self = shift;

    $self->{Handle}->ct_cancel(CS_CANCEL_ALL);
}

package Sybase::Simple;
# Autoload methods go after =cut, and are processed by the autosplit program.

1;
__END__
# Below is the stub of documentation for your module. You better edit it!

=head1 NAME

Sybase::Simple - Utility module for Sybase::CTlib

=head1 SYNOPSIS

  use Sybase::Simple;

  $dbh = new Sybase::Simple $user, $pwd, $server;

  $date = $dbh->Scalar("select getdate()");

=head1 DESCRIPTION

Sybase::Simple is a module built on top of Sybase::CTlib, and which provides some
simplified access methods to get at the database's data.

The following methods are defined:

=over 4

=item $dbh = new Sybase::Simple $user [, $pwd [, $server [, $appname [, \%attr]]]]

Open a new connection to the Sybase server $server, using $user and $pwd
for authentication. Optionally set the application name (as shown in 
sysprocesses) to $appname. The optional %attr hash can be used to add
attributes to the $dbh hash.

See the sybperl(3) man page for details.

=item $dbh->config(key => value [, key => value ...])

The behavior of Sybase::Simple can be modified by setting configuration values.
Currently two config values are supported:

=over 4

=item AbortOnError

If true, ExecSql() will abort and return 0 on the first failed command in
the batch that it executes.

Default: false

=item DeadlockRetry

If true, ExecSql() will retry the B<entire> batch if a deadlock error (error
number 1205) is detected.

Default: false

=back

=item $data = $dbh->Scalar($sql)

Execute the SQL in $sql, and take the first column of the first row and 
return it as a scalar value. Typical use might be

     $val = $dbh->Scalar("select max(foo) from bar");

=item $data = $dbh->HashRow($sql)

Execute the SQL in $sql, and return the first row, in hash format:

     $data = $dbh->HashRow("select * from sysusers where uid = 0");
     if($data->{name} ne 'public') {
          print "Your sysusers table is strange!\n";
     }

=item $data = $dbh->ArrayOfHash($sql)

Execute the SQL in $sql, and return an array of all the rows, each row
begin stored in hash format. Similar to the Sybase::CTlib ct_sql() subroutine.

=item $data = $dbh->HashOfScalar($sql, $key, $val)

Execute $sql, and return a hash where the key is based on the column $key
in the result set, and the value is the $val column:

     $data = $dbh->HashOfScalar("select uid, name from sysusers", 'uid', 'name');
     if($data->{0} ne 'public') {
          print "Your sysusers table is strange!\n";
     }

Rows where the $key column is NULL are ignored. No checking is made on the
uniqueness of the $key column - if multiple rows have the same value in the 
$key column then the last row retrieved will be stored.

=item $data = $dbh->HashOfHash($sql, $key)

Same as HashOfScalar(), except that the entire row is stored as a hash.

=item $iter = $dbh->HashIter($sql);

Executes $sql, and returns a Sybase::Simple::HashIter object. This can then be used 
to retrieve one row at a time. This is really usefull for queries where the
number of rows returned can be large.

    $iter = $dbh->HashIter($sql);
    while($data = $iter->next) {
        # do something with $data
    }

=item $status = $dbh->ExecSql($sql)

Executes $sql and ignores any rows that the statement may return. This routine
is usefull for executing insert/update/delete statements, or stored procedures
that perform those types of operation.

If $abortOnError is non-0 then B<ExecSql> will abort on the first failed 
statement.

If verbose warnings are turned on (ie if the B<-w> switch is passed to perl)
then a warning is issued if rows are returned when executing $sql. In any
case those rows are ignored.

Returns 0 for any failure, non-0 otherwise.

=back

=head2 Error Handling

This module adds a some error handling above what is normally found in
Sybase::CTlib.

In particular you can check $dbh->lastErr and $dbh->lastErrText to see
the last error associated with this database connection. There is also
some optional deadlock retry logic in the ExecSql() call. This logic can
certainly be extended.


=head1 AUTHOR

Michael Peppler, mpeppler@peppler.org

=head1 COPYRIGHT

Copyright (c) 1998   Michael Peppler

You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file,
with the exception that it cannot be placed on a CD-ROM or similar media
for commercial distribution without the prior approval of the author.

=head1 SEE ALSO

perl(1), sybperl(3)

=cut
