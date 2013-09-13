#!/usr/bin/perl -w
#

use strict;
use DBI;
use Net::LDAP;
use Getopt::Std;

our %config;

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
    print "truncating table: $config{truncate_stmt}\n";
    my $sth;
    $sth = $dbh->prepare($config{truncate_stmt})
      or die "problem preparing truncate: " . $sth->errstr;
    $sth->execute() or die "problem truncating: " . $sth->errstr
      if (!exists $opts{n})
}

my $ldap=Net::LDAP->new($config{ldap_host});
my $bind_rslt = $ldap->bind($config{ldap_binddn}, password => $config{ldap_pass});
$bind_rslt->code && die "unable to bind as $config{ldap_binddn}";

print "searching ldap: $config{ldap_filter}\n";
my $rslt = $ldap->search(base=>$config{ldap_base}, filter=>$config{ldap_filter}, attrs => [@{$config{ldap_attrs}}]);
$rslt->code && die "problem searching: ", $rslt->error;

print "inserting into table: $config{insert_stmt}\n";

my $insert_sth;
$insert_sth = $dbh->prepare($config{insert_stmt})
  or die "problem preparing insert: " . $insert_sth->errstr;

my $count=0;
for my $entry ($rslt->entries) {
    my $next = 0;
    my @values;

    for my $attr (@{$config{ldap_attrs}}) {
	push @values, ($entry->get_value($attr))[0];
	$next = 1
	  if (($entry->get_value($attr))[0] =~ /^\s*$/ && 
	      exists $config{skip_if_attrs_empty} && $config{skip_if_attrs_empty} =~ /yes/i);
    }
    next
      if ($next);

    print "executing insert with values ", join ' ', @values, "\n"
      if (exists $opts{d});
    
    $insert_sth->execute(@values) or die "problem executing statement: " . $insert_sth->errstr
      if (!exists $opts{n});
    
    $count++;
}

#$dbh->commit;
$dbh->disconnect;

print "$count entries inserted.\n";

print "\nfinished at ", `date`;


    sub print_usage {
	print "usage: $0 [-n] -c config.cf\n\n";
	exit;
    }
	
