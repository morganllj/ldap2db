#!/usr/bin/perl -w
#

use strict;
use DBI;
use Net::LDAP;
use Getopt::Std;
use Data::Dumper;

our %config;

$|=1;

sub insert_entries;
sub truncate_table;
sub my_print;
sub my_printf;

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

my $out;
if (exists $opts{o}) {
    open ($out, ">", $opts{o}) || die "unable to open $opts{o} for writing";
} else {
    print "you must specify an output file to print insert statements with -o\n";
}

my_print $out, "\nstarting at ", `date`;

my_print $out, "-n used, ldap will not be modifed.\n"
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
	for my $insert_stmt (@{$config{insert_stmt}}) {
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

$dbh->commit;
$dbh->disconnect;

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
    my ($insert_stmt, $ldap_filter, $out, @in_ldap_attrs) = @_;

    my @attr_keys;
    my %gen_keys;
    my @ldap_attrs;

    $insert_stmt =~ /insert\s*into\s*[^\(]+\(([^\)]+)\)/i;

    my @db_cols = split /\s*,\s*/, $1;
    for (@db_cols) {
	s/^\s*//;
	s/\s*$//;
    }

    my $count2 = 0;
    for (@in_ldap_attrs) {
	if (/^singlekey:/i) {
	    s/^singlekey://i;
	    push @attr_keys, $_;
	    push @ldap_attrs, $_;
	} elsif (/^gen:/i) {
	    s/^gen://i;
	    @{$gen_keys{$db_cols[$count2]}} = split /\s*,\s*/;
	    push @ldap_attrs, split /\s*,\s*/;
	} else {
	    push @ldap_attrs, $_;
	}
	$count2++
    }

    my_print $out, "\nsearching ldap: $ldap_filter\n";
    my $rslt = $ldap->search(base=>$config{ldap_base}, filter=>$ldap_filter, attrs => [@ldap_attrs]);
    $rslt->code && die "problem searching: ", $rslt->error;

    my $count=0;
    for my $entry ($rslt->entries) {
	my $next = 0;
	my @values;

	my $longest_attr_list=0;

	my $i=0;
	for my $attr (@ldap_attrs) {
	    my @attr_values = $entry->get_value($attr);

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


	    die "multiple values for singlekey attr $attr in " . $entry->dn()
	      if (($#attr_values > 0) && grep /$attr/i, @attr_keys);

	    if ( ($#attr_values < 0 )&&
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
		    my_print $out, $values[$j][0], " "
		      if (exists $opts{d});
		    push @insert_values, $values[$j][0]; 
		} elsif (ref $values[$j] ne "ARRAY") {
		    my_print $out, "<empty> "
		      if (exists $opts{d});
		    push @insert_values, ""; 
		} else {
		    my_print $out, $values[$j][$k], " "
		      if (exists $opts{d});
		    push @insert_values, $values[$j][$k];
		}
		$j++
	    }
	    print $out "\n";

	    print $out "inserting into table: $insert_stmt\n";

	    my $values_to_print = join ', ', @insert_values, "\n";
	    $values_to_print =~ s/,\s*$//;
	    #my_print $out, "values ", $values_to_print, "\n";
	    print $out "values ", $values_to_print, "\n";


	    my $insert_sth;
	    $insert_sth = $dbh->prepare($insert_stmt)
	      or die "problem preparing insert: " . $insert_sth->errstr;

	    $insert_sth->execute(@insert_values) or die "problem executing statement: " . $insert_sth->errstr
	      if (!exists $opts{n});

	    $k++;
	}

	$count++;

	my_print $out, "\n*****\n"
	  if (exists $opts{d});

     }
     my_print $out, "$count entries inserted.\n\n";
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


sub print_usage {
    print "usage: $0 [-n] [-d] -c config.cf -o output_file\n\n";
    exit;
}


