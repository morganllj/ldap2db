#!/bin/sh
#
# Morgan Jones (morgan@morganjones.org)
# $Id$

cmd_base=update_db_frm_ldap

base_path=`echo $0 | awk -F/ '{for (i=1;i<NF;i++){printf $i "/"}}' | sed 's/\/$//'`
log_path=${base_path}/log
log=${log_path}/${cmd_base}_`date +%y%m%d.%H:%M:%S`

cmd="${base_path}/${cmd_base}.pl $*"
echo "** output logged to ${log}"
echo
echo $cmd
$cmd 2>&1 | tee $log

