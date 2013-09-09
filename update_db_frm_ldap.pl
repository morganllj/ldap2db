#!/usr/bin/perl -w
#

use strict;
use DBI;
use Net::LDAP;

our %config;

$ENV{ORACLE_BASE} = "/home/oracle";
$ENV{ORACLE_HOME} = "/home/oracle/app/oracle/product/11.2.0.1/client_1";


my $dbh = DBI->connect("dbi:Oracle:host=10.0.103.14;port=1567;sid=PROD1", "SLDAP", "pass") or
  die $DBI::errstr;

my $sth = $dbh->prepare("TRUNCATE TABLE LDAP_STUDENTS");
$sth->execute();
exit;

my $ldap=Net::LDAP->new('ldaps://sgldap.philasd.net');
my $bind_rslt = $ldap->bind("cn=directory manager", password => "CiDdti");
$bind_rslt->code && die "unable to bind";

my $rslt = $ldap->search(base=>"dc=philasd,dc=org", filter=>"(&(objectClass=sdpStudent)(sdpPrivateEmail=*)(!(memberOf=cn=sdpExcludes,ou=groups,dc=philasd,dc=org)))", attrs => ['sdpPrivateEmail', 'sdpSIDN']);
$rslt->code && die "problem searching: ", $rslt->error;

my $insert_sth = $dbh->prepare("INSERT INTO LDAP_STUDENTS (SIDN, EMAIL_ACCOUNT_ID) VALUES (?, ?)");

for my $entry ($rslt->entries) {
    my $email = ($entry->get_value('sdpprivateemail'))[0];
    my $sidn = ($entry->get_value('sdpsidn'))[0];
    next 
      if ($email =~ /^\s*$/);
    print "/$sidn/ /$email/\n";
    $insert_sth->execute($sidn, $email);
}

#$dbh->commit;
$dbh->disconnect;

