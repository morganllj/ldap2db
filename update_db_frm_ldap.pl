#!/usr/bin/perl -w
#

use strict;
use DBI;
use Net::LDAP;
use Getopt::Std;

our %config;

my %opts;
getopts('nc:', \%opts);

exists $opts{c} || print_usage();

print "-n used, ldap will not be modifed.\n"
  if (exists $opts{n});

require $opts{c};

$ENV{ORACLE_BASE} = $config{oracle_base};
$ENV{ORACLE_HOME} = $config{oracle_home};

my $dbh = DBI->connect($config{connect_str}, $config{user}, $config{pass}) or
  die $DBI::errstr;

if ($config{truncate_table} =~ /yes/i) {
    my $sth = $dbh->prepare($config{truncate_stmt});
    $sth->execute();
}

my $ldap=Net::LDAP->new($config{ldap_host});
my $bind_rslt = $ldap->bind($config{ldap_binddn}, password => $config{ldap_pass});
$bind_rslt->code && die "unable to bind as $config{ldap_binddn}";

my $rslt = $ldap->search(base=>$config{ldap_base}, filter=>$config{ldap_filter}, attrs => [@{$config{ldap_attrs}}]);
$rslt->code && die "problem searching: ", $rslt->error;

my $insert_sth = $dbh->prepare($config{insert_stmt});

for my $entry ($rslt->entries) {
    # my $email = ($entry->get_value('sdpprivateemail'))[0];
    # my $sidn = ($entry->get_value('sdpsidn'))[0];
    my $next = 0;
    my @values;
    for my $attr (@{$config{ldap_attrs}}) {
	push @values, ($entry->get_value($attr))[0];
	$next = 1
	  if (($entry->get_value($attr))[0] =~ /^\s*$/ && $config{skip_if_attrs_empty} =~ /yes/i);
    }
    next
      if ($next);
    print join (' ', @values), "\n";
    
    $insert_sth->execute(@values);
}

#$dbh->commit;
$dbh->disconnect;


    sub print_usage {
	print "usage: $0 [-n] -c config.cf\n\n";
	exit;
    }
	
