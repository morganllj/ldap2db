#!/bin/sh
#
# Morgan Jones (morgan@morganjones.org)
# $Id$

base_path=`echo $0 | awk -F/ '{for (i=1;i<NF;i++){printf $i "/"}}' | sed 's/\/$//'`
cmd_base=ldap2db
log_path=${base_path}/log
log=${log_path}/${cmd_base}_`date +%y%m%d%H%M%S`

p=`cd $base_path && pwd`;

cmd="${base_path}/${cmd_base}.pl -c ${base_path}/`basename ${p}`.cf -o $log $*"
echo $cmd
$cmd

gzip ${log}

