#!/usr/bin/perl -w
#

use strict;
use DBI;
use Net::LDAP;
use Getopt::Std;
use Data::Dumper;

our %config;

sub insert_entries;
sub truncate_table;

my %opts;
getopts('ndc:', \%opts);

exists $opts{c} || print_usage();

print "\nstarting at ", `date`;

print "-n used, ldap will not be modifed.\n"
  if (exists $opts{n});

require $opts{c};

print "skip_if_attrs_empty set in config, entries with empty ldap values will be skipped\n"
  if (exists $config{skip_if_attrs_empty} && $config{skip_if_attrs_empty} =~ /yes/i);

print "\n";

$ENV{ORACLE_BASE} = $config{oracle_base};
$ENV{ORACLE_HOME} = $config{oracle_home};

my $dbh = DBI->connect($config{connect_str}, $config{user}, $config{pass}) or
  die $DBI::errstr;

if ($config{truncate_table} =~ /yes/i) {

    if (ref $config{truncate_stmt} eq "ARRAY") {
	for my $stmt (@{$config{truncate_stmt}}) {
	    truncate_table($stmt);
	}

    } else {
	truncate_table($config{truncate_stmt});
    }


    # print "truncating table: $config{truncate_stmt}\n";
    # my $sth;
    # $sth = $dbh->prepare($config{truncate_stmt})
    #   or die "problem preparing truncate: " . $sth->errstr;
    # $sth->execute() or die "problem truncating: " . $sth->errstr
    #   if (!exists $opts{n})
}

my $ldap=Net::LDAP->new($config{ldap_host});
my $bind_rslt = $ldap->bind($config{ldap_binddn}, password => $config{ldap_pass});
$bind_rslt->code && die "unable to bind as $config{ldap_binddn}";



# #TODO: verify length of arrays match

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
	    insert_entries($config{insert_stmt}->[$i], @{$config{ldap_attrs}->[$i]});
	    $i++;
	}
    }
} else {
    insert_entries($config{insert_stmt}, @{$config{ldap_attrs}});
}



# #$dbh->commit;
$dbh->disconnect;



print "\nfinished at ", `date`;

sub truncate_table {
    my $stmt = shift;

    print "truncating table: $stmt\n";
    my $sth;
    $sth = $dbh->prepare($stmt)
      or die "problem preparing truncate: " . $sth->errstr;
    $sth->execute() or die "problem truncating: " . $sth->errstr
      if (!exists $opts{n})
}


sub insert_entries {
    my ($insert_stmt, @ldap_attrs) = @_;

    my @attr_keys;

    for (@ldap_attrs) {
	if (/singlekey:/i) {
	    s/singlekey://i;
	    push @attr_keys, $_;
	}
    }

    print "\nsearching ldap: $config{ldap_filter}\n";
    my $rslt = $ldap->search(base=>$config{ldap_base}, filter=>$config{ldap_filter}, attrs => [@ldap_attrs]);
    $rslt->code && die "problem searching: ", $rslt->error;

    my $count=0;
    for my $entry ($rslt->entries) {
	my $next = 0;
	my @values;

	my $longest_attr_list=0;

	my $i=0;
	for my $attr (@ldap_attrs) {
	    my @attr_values = $entry->get_value($attr);

	    die "multiple values for singlekey attr $attr in " . $entry->dn()
	      if (($#attr_values > 0) && grep /$attr/i, @attr_keys);

	    if ( ($#attr_values < 0 )&&
		 exists $config{skip_if_attrs_empty} && $config{skip_if_attrs_empty} =~ /yes/i) {
#	    $next = 1
#	      if (($entry->get_value($attr))[0] =~ /^\s*$/ && 
#		  exists $config{skip_if_attrs_empty} && $config{skip_if_attrs_empty} =~ /yes/i);
		$next = 1;
	    } else {
#		print "pushing ", Dumper @attr_values;
#		push @values, @attr_values;
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

#	print "values:\n", Dumper @values;
#	print "entering for..\n";

	# my $j=0;
	# for (@values) {
	#     print "inserting into table: $insert_stmt\n";
	    
	#     if (grep /$ldap_attrs[$j]/i, @attr_keys) {
	# 	my $k=0;
	# 	while ($k <= $longest_attr_list) {
	# 	    print $values[$j][0], " ";
	# 	    $k++;
	# 	}
	#     } elsif (ref $values[$j] ne "ARRAY") {
	# 	print "empty";
	#     } else {
	# 	my $k=0;
	# 	while ($k <= $longest_attr_list) {
	# 	    print $values[$j][$k], " ";
	# 	    $k++;
	# 	}
	#     }
	#     print "\n";
	#     $j++;
	# }



	my $j=0;
	my $k=0;
	while ($k <= $longest_attr_list) {

	    my @insert_values;

	    my $j=0;
	    for (@values) {
		if (grep /$ldap_attrs[$j]/i, @attr_keys) {
		    print $values[$j][0], " "
		      if (exists $opts{d});
		    push @insert_values, $values[$j][0]; 
		} elsif (ref $values[$j] ne "ARRAY") {
		    print "<empty> "
		      if (exists $opts{d});
		    push @insert_values, ""; 
		} else {
		    print $values[$j][$k], " "
		      if (exists $opts{d});
		    push @insert_values, $values[$j][$k];
		}
		$j++
	    }
	    print "\n";

	    print "inserting into table: $insert_stmt\n";


	    my $values_to_print = join ', ', @insert_values, "\n";
	    $values_to_print =~ s/,\s*$//;
	    print "values ", $values_to_print, "\n";

	    my $insert_sth;
	    $insert_sth = $dbh->prepare($insert_stmt)
	      or die "problem preparing insert: " . $insert_sth->errstr;

	    $insert_sth->execute(@insert_values) or die "problem executing statement: " . $insert_sth->errstr
	      if (!exists $opts{n});

	    $k++;
	}


	# 	# print "executing insert with values ", join ' ', @values, "\n"
	# 	#   if (exists $opts{d});
	# 	print "executing insert with values ", join ' ', @insert_values, "\n"
	# 	  if (exists $opts{d});
		
	# 	$insert_sth->execute(@values) or die "problem executing statement: " . $insert_sth->errstr
	# 	  if (!exists $opts{n});
		
	$count++;

	print "\n*****\n"
	  if (exists $opts{d});

     }
#     print "$count entries inserted.\n";


}


sub print_usage {
    print "usage: $0 [-n] -c config.cf\n\n";
    exit;
}


