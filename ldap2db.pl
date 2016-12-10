#!/usr/local/bin/perl -w
#

use strict;
use DBI;
use Net::LDAP;
use Getopt::Std;
use Data::Dumper;
use DateTime;
use DateTime::Format::Oracle;

use Email::Sender::Simple qw(sendmail);
use Email::Simple;
use Email::Simple::Creator;
use Email::Sender::Transport::SMTP;

our %config;

$|=1;

sub insert_entries;
sub truncate_table;
sub my_print;
sub my_printf;
sub get_unique_key;
sub skip_value;
sub skip_next_time;
sub add_to_skipping;
sub individual_db_stmt;
sub increment_entry_count;

# suppress qw warnings (http://stackoverflow.com/questions/19573977/disable-warning-about-literal-commas-in-qw-list)
$SIG{__WARN__} = sub {
    return 
        if $_[0] =~ m{ separate words with commas };
    return CORE::warn @_;
};

my %opts;
getopts('ndc:o:', \%opts);

exists $opts{c} || print_usage();
exists $opts{o} || print_usage();

die "email_from is required in config for notification\n"
  if (exists $config{"notify_if_failure"} && !exists $config{"email_from"});

my $out;
if (exists $opts{o}) {
    open ($out, ">", $opts{o}) || die "unable to open $opts{o} for writing";
} else {
    print "you must specify an output file to print insert statements with -o\n";
}

my_print $out, "\nstarting at ", `date`;

my_print $out, "-n used, no changes will be made.\n"
  if (exists $opts{n});

require $opts{c};

my_print $out, "skip_if_attrs_empty set in config, entries with empty ldap values will be skipped\n"
  if (exists $config{skip_if_attrs_empty} && $config{skip_if_attrs_empty} =~ /yes/i);

my_print $out, "\n";

$ENV{ORACLE_BASE} = $config{oracle_base};
$ENV{ORACLE_HOME} = $config{oracle_home};

my $dbh = DBI->connect($config{connect_str}, $config{user}, $config{pass}) or
  die $DBI::errstr;

$dbh->{AutoCommit} = 0;

if ($config{truncate_table} =~ /yes/i) {
    if (ref $config{truncate_stmt} eq "ARRAY") {
	for my $stmt (@{$config{truncate_stmt}}) {
	    truncate_table($stmt);
	}
    } else {
	truncate_table($config{truncate_stmt});
    }
}

for my $procedure (@{$config{pre_stored_procedures}}) {
    my_print $out, "calling pre stored procedure $procedure\n";
    if (!($dbh->do("call $procedure"))) {
	my_printf $out,("Error executing stored procedure: MySQL error %d (SQLSTATE %s)\n %s\n",
	       $dbh->err,$dbh->state,$dbh->errstr); 
    }
}

my $ldap=Net::LDAP->new($config{ldap_host});
my $bind_rslt = $ldap->bind($config{ldap_binddn}, password => $config{ldap_pass});
$bind_rslt->code && die "unable to bind as $config{ldap_binddn}";

# TODO: verify length of arrays match
# TODO: if (ref $config{ldap_filter} eq "ARRAY") {
if (ref $config{ldap_attrs} eq "ARRAY") {
    die "insert_stmt must be an array if ldap_attrs is an array"
      if (ref $config{insert_stmt} ne "ARRAY");
}

if (ref $config{insert_stmt} eq "ARRAY") {
    die "ldap_attrs must be an array if insert_stmt is an array"
      if (ref $config{ldap_attrs} ne "ARRAY");

    if (ref $config{insert_stmt} eq "ARRAY") {
	my $i=0;
	#	for my $insert_stmt (@{$config{insert_stmt}}) {
	for (@{$config{insert_stmt}}) {
	    insert_entries($config{insert_stmt}->[$i], $config{ldap_filter}->[$i], $out, @{$config{ldap_attrs}->[$i]});
	    $i++;
	}
    }
} else {
    insert_entries($config{insert_stmt}, $out, @{$config{ldap_attrs}});
}

for my $procedure (@{$config{post_stored_procedures}}) {
    my_print $out, "calling post stored procedure $procedure\n";
    if (!($dbh->do("call $procedure"))) {
	my_printf $out,("Error executing stored procedure: MySQL error %d (SQLSTATE %s)\n %s\n",
	       $dbh->err,$dbh->state,$dbh->errstr); 
    }
}
if (exists $config{print_counts}) {
    print "\n";
    if (exists $config{print_counts}->{ldap_filter}) {
	for my $f (@{$config{print_counts}->{ldap_filter}}) {
	    print "$f: ";

	    my $rslt2 = $ldap->search(base=>$config{ldap_base}, filter=>$f);
	    $rslt2->code && die "problem searching: ", $rslt2->error;

	    print $rslt2->entries . " entries\n";
	}
    }
    
}

$dbh->commit;
$dbh->disconnect;

alert_on_skipped_entries();

my_print $out, "\nfinished at ", `date`;

close ($out);


sub truncate_table {
    my $stmt = shift;

    my_print $out, "truncating table: $stmt\n";
    my $sth;
    $sth = $dbh->prepare($stmt)
      or die "problem preparing truncate: " . $sth->errstr;
    $sth->execute() or die "problem truncating: " . $sth->errstr
      if (!exists $opts{n})
}


sub insert_entries {
    my $try_count=0;
    while (1) {
	if ($try_count>9) {
	    print "\nexceeded 10 tries, exiting\n";
	    exit;
	}
	last unless _insert_entries(@_);

	$try_count++;
    }
}



sub _insert_entries {
    my ($insert_stmt, $ldap_filter, $out, @in_ldap_attrs) = @_;

    my @attr_keys;
    my %gen_attrs;
    my @ldap_attrs;

    my (%in_ldap, %in_db);

    $insert_stmt =~ /insert\s+into\s+([^\(]+)\(([^\)]+)\)/i;
    my $insert_into = $1;
    my @db_cols = split /\s*,\s*/, $2;
    for (@db_cols) {
    	s/^\s*//;
    	s/\s*$//;
    }

    # work through @in_ldap_attrs and break off config directives
    # final attr names end up in @ldap_attrs
    my $count2 = 0;
    for (@in_ldap_attrs) {
	if (/^singlekey:/i) {
	    s/^singlekey://i;
	    push @attr_keys, $_;
	    push @ldap_attrs, $_;
	} elsif (/^gen:/i) {
	    s/^gen://i;
	    $gen_attrs{$db_cols[$count2]} = $_;
	    push @ldap_attrs, $_;
	} else {
	    push @ldap_attrs, $_;
	}
	$count2++
    }


    my @sql_types;
    if (exists $config{only_insert_diffs} && $config{only_insert_diffs} =~ /yes/i) {
	my $sql = "SELECT " . join (', ', @db_cols) . " FROM " . $insert_into;

	print "searching db $sql\n";

	my $sth = $dbh->prepare($sql);
	$sth->execute();

	# http://docstore.mik.ua/orelly/linux/dbi/ch06_01.htm#FOOTNOTE-61
	# see 'TYPE' in footnote comment at bottom of this script.

	@sql_types = @{$sth->{TYPE}};
	    
	my $i=0;
	while (my @row = $sth->fetchrow_array) {
	    for (@row) {
		$_ = ""
		  if (!defined ($_));
	    }

	    my $index = join ' ', @row;
	    # die "/$index/ already exists in \%in_db, this shouldn't happen"
	    #   if (exists $in_db{$index});
	    
#	    print "inserting into in_db /$index/\n";
	    $in_db{$index} = \@row;
	}

	# doesn't work, not sure why
	#	    $sql = "SELECT NLS_DATE_FORMAT FROM NLS_SESSION_PARAMETERS";
	#	    $sth = $dbh->prepare($sql);
	#	    $sth-> execute();
	# while (my @row = $sth->fetchrow_array) {
	# 	print join (' ', @row);
	# }
	# exit;
    }
    

    my_print $out, "\nsearching ldap: $ldap_filter\n";
    my $rslt = $ldap->search(base=>$config{ldap_base}, filter=>$ldap_filter, attrs => [@ldap_attrs]);
    $rslt->code && die "problem searching: ", $rslt->error;

    # put ldap values in @values
#    my $count=0;
    for my $entry ($rslt->entries) {
	my $next = 0;
	my @values;
	my $longest_attr_list=0;
	my $i=0;

	for my $attr (@ldap_attrs) {
	    my @attr_values = $entry->get_value($attr);

	    if (exists($gen_attrs{$db_cols[$i]})) {
		my $sub_name = "gen_".$db_cols[$i];
		my $subref = \&$sub_name;
		@attr_values = $subref->($gen_attrs{$db_cols[$i]}, $out, @attr_values);
	    }

	    # check for a normalize section in the config.
	    # identify the attr with one or more regexes and run a
	    # user-defined sub on it
	    #
	    # this should probably be replaced by gen: functionality
	    if (exists $config{normalize}) {
		for my $n (@{$config{normalize}}) {
		    if (exists $n->{regex}) {
			for my $r (@{$n->{regex}}) {
			    if ($attr =~ /$r/i) {
				my $j = 0;
				for (@attr_values) {
				    if (exists $n->{sub}) {
					$attr_values[$j] = $n->{sub}->($attr, $attr_values[$j], $out);
				    }
				}
			    }
			}
		    }
		}
	    }

	    # TODO save value to skip next time and return here?
	    die "multiple values for singlekey attr $attr in " . $entry->dn()
	      if (($#attr_values > 0) && grep /$attr/i, @attr_keys);

	    if ( ($#attr_values < 0) &&
		 exists $config{skip_if_attrs_empty} && $config{skip_if_attrs_empty} =~ /yes/i) {
		$next = 1;
	    } else {
		if ($#attr_values > -1 && $attr_values[0] !~ /^\s*$/) {
		    push @{$values[$i]}, @attr_values;
		} else {
		    $values[$i] = "";
		}
		$longest_attr_list = $#attr_values 
		  if ($#attr_values > $longest_attr_list);
	    }
	    $i++;
	}

	next
	  if ($next);

	my $j=0;
	my $k=0;
	while ($k <= $longest_attr_list) {
	    my @insert_values;

	    my $j=0;
	    for (@values) {
		if (grep /$ldap_attrs[$j]/i, @attr_keys) {
		    # my_print $out, $values[$j][0], " "
		    #   if (exists $opts{d});
		    push @insert_values, $values[$j][0]; 
		} elsif (ref $values[$j] ne "ARRAY") {
 		    # my_print $out, "<empty> "
		    #   if (exists $opts{d});
		    push @insert_values, ""; 
		} else {
		    # my_print $out, $values[$j][$k], " "
		    #   if (exists $opts{d});
		    push @insert_values, $values[$j][$k];
		}
		$j++
	    }
	    print $out "\n";


	    if (exists $config{only_insert_diffs} && $config{only_insert_diffs} =~ /yes/i) {
		# we were asked to diff so things get a little more complicated
		# convert and save values in %in_ldap for comparison later

		# http://alvinalexander.com/java/edu/pj/jdbc/recipes/ResultSet-ColumnType.shtml
		#		$ENV{'NLS_DATE_FORMAT'} = 'YYYY-MM-DD HH24:MI:SS';
		$ENV{'NLS_DATE_FORMAT'} = 'YY-MON-DD';

		if ($#insert_values != $#sql_types) {
		    print "count of types does not match the count of values:\n";
		    print Dumper @insert_values, "\n", Dumper @sql_types, "\n";
		    die;
		}

		my $i = 0;
		for (@insert_values) {
		    if ($sql_types[$i] == 8) {
			# sql double, convert to a number
			$insert_values[$i] += 0;
		    } elsif ($sql_types[$i] == 12) {
			# varchar, do nothing
		    } elsif ($sql_types[$i] == 93) {
			# timestamp
			my $dt;
			eval {
			    $dt = DateTime::Format::LDAP->parse_datetime($insert_values[$i]);
			};
			if ( $@ ) {
			    $insert_values[$i] = "";
			} else {
			    $insert_values[$i] = DateTime::Format::Oracle->format_datetime($dt);
			    $insert_values[$i] = uc  $insert_values[$i];
			}
		    } else {
			die "unknown sql type $sql_types[$i].  This is because it needs to be added here.";
		    }
		    $i++;
		}

		my $index = join ' ', @insert_values;
		die "/$index/ already exists in \%in_ldap, this shouldn't happen"
		  if (exists $in_ldap{$index});

#		print "inserting into in_ldap /$index/\n";
		$in_ldap{$index} = \@insert_values;
	    } else {
		# We weren't asked to diff so just do a straight insert of the data
		individual_db_stmt($insert_stmt, \@ldap_attrs, $out, @insert_values);
	    }

	    $k++;
	}

	# my_print $out, "\n*****\n"
	#   if (exists $opts{d});
    }

    if (!exists $config{only_insert_diffs} && $config{only_insert_diffs} !~ /yes/i) {
	my_print $out, entry_count(), " entries inserted.\n\n";
    }

    print "comparing...\n";

    for my $k (sort keys %in_ldap) {
	if (!exists ($in_db{$k})) {
	    print "inserting into db: ", $k, "\n";

	    individual_db_stmt($insert_stmt, \@ldap_attrs, $out, @{$in_ldap{$k}})
	}
    }

    for my $k (sort keys %in_db) {
	if (!exists ($in_ldap{$k})) {
	    print "removing from db: ", $k, "\n"
	      if (exists $opts{d});
	    print $out "removing from db: ", $k, "\n";

	    my $delete_stmt = "DELETE FROM " . $insert_into . " WHERE ";

	    my $i = 0;
	    for (@db_cols) {
		$delete_stmt .= $db_cols[$i] . "='" . $in_db{$k}[$i] . "'";
		$delete_stmt .= " AND "
		  if ($i < $#db_cols);
		$i++;
	    }
	    individual_db_stmt($delete_stmt, \@ldap_attrs, $out, @{$in_ldap{$k}});
	}
    }


    
    return 0;
}


sub delete_stmt {
    
}


sub individual_db_stmt {
    my ($stmt, $ldap_attrs, $out, @values) = @_;

    print "statment: $stmt\n"
      if (exists $opts{d});
    print $out "statment: $stmt\n";

    my $values_to_print = join ', ', @values, "\n";
    $values_to_print =~ s/,\s*$//;

    unless ($stmt =~ /^\s*delete/i) {
	print "values ", $values_to_print, "\n"
	  if (exists $opts{d});
	print $out "values ", $values_to_print, "\n";
    }
    if (skip_value($ldap_attrs, \@values)) {
	my_print $out, "skipping ",  $values_to_print, "\n\n";
	add_to_skipping($values_to_print);
    } else {
	my $sth;
	unless ($sth = $dbh->prepare($stmt)) {
	    my_print $out, "problem preparing insert: " . $sth->errstr;
	    die;
	}
	if (!exists $opts{n}) {
	    my $uk = get_unique_key($ldap_attrs, \@values);	    

	    my $rc;
	    if ($stmt =~ /^\s*delete/i) {
		$rc = $sth->execute();
		print "rc: $rc\n";
	    } else {
		$rc = $sth->execute(@values)
	    };
#	      unless ($sth->execute(@values)) {
	      unless ($rc) {		  
		my_print $out, "problem executing statement: ", $sth->errstr, "\n";
		my_print $out, "pushing ", $uk, " onto entries_to_skip and restarting import\n";
		skip_next_time($uk);
		return 1;
	    }
	}
#	$count++;
	increment_entry_count();
    }
}
    

    

sub get_unique_key {
    my ($ldap_attrs, $insert_values) = @_;

    if (exists $config{"unique_key"}) {
	# get the number of the unique key and save the value for the next run
	my $i=0;
	for my $a (@$ldap_attrs) {
	    if (lc $a eq lc $config{unique_key}) {
		return @$insert_values[$i];
	    }
	    $i++;
	}
    }
    
    # concat the values in @$insert_values as a "unique" key
    return join (' ', @$insert_values);
}



{
    my @entries_to_skip;

    sub skip_value {
	my ($ldap_attrs, $insert_values) = @_;

	my $uk = get_unique_key($ldap_attrs, $insert_values);

	return 1
	  if (grep /$uk/i, @entries_to_skip);

	return 0;
    }


    sub skip_next_time {
	my $uk = shift;

	push @entries_to_skip, $uk;
    }

}


sub my_print {
    my $out = shift;

    print @_;
    print $out @_;
}

sub my_printf {
    my $out = shift;

    printf @_;
    printf $out @_;
}

{
    my @skipping;
    
    sub add_to_skipping {
	my $skipping = shift;
	push @skipping, $skipping;
    }

    sub alert_on_skipped_entries {
	return unless (exists $config{"notify_if_failure"});
	
	if (@skipping) {
	    my_print $out, "notifying these addresses of skipped entries: ", $config{"notify_if_failure"}, "\n";
	    
	    my $message_body;

    	    for my $s (@skipping) {
	     	$message_body .= $s . "\n";
	    }

	    my $email = Email::Simple->create (
                header => [
                    To      => $config{"notify_if_failure"},
                    From    => $config{"email_from"},
                    Subject => "ldap2db skipped entries",
                ],
                body => $message_body
					     );
	    sendmail($email);
	}
    }
}


{
    my $c=0;

    sub increment_entry_count {
	$c++;
    }

    sub entry_count {
	return $c;
    }
    
}


sub print_usage {
    print "usage: $0 [-n] [-d] -c config.cf -o output_file\n\n";
    exit;
}





  # footnote
  #
  # TYPE
  #     The TYPE attribute contains a reference to an array
  #     of integer values representing the international
  #     standard values for the respective datatypes. The
  #     array of integers has a length equal to the number
  #     of columns selected within the original statement,
  #     and can be referenced in a similar way to the NAME
  #     attribute example shown earlier.
  #
  #     The standard values for common types are:
  #
  #         SQL_CHAR             1
  #         SQL_NUMERIC          2
  #         SQL_DECIMAL          3
  #         SQL_INTEGER          4
  #         SQL_SMALLINT         5
  #         SQL_FLOAT            6
  #         SQL_REAL             7
  #         SQL_DOUBLE           8
  #         SQL_DATE             9
  #         SQL_TIME            10
  #         SQL_TIMESTAMP       11
  #         SQL_VARCHAR         12
  #         SQL_LONGVARCHAR     -1
  #         SQL_BINARY          -2
  #         SQL_VARBINARY       -3
  #         SQL_LONGVARBINARY   -4
  #         SQL_BIGINT          -5
  #         SQL_TINYINT         -6
  #         SQL_BIT             -7
  #         SQL_WCHAR           -8
  #         SQL_WVARCHAR        -9
  #         SQL_WLONGVARCHAR   -10
