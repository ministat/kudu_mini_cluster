#!/bin/bash
basedir=$(dirname $0)
. ${basedir}/env.sh

CLUSTER_ROOT=`readlink -f $basedir`

function usage() {
cat << EOF
  $0: <options>
  -h: help
  -m <1>:         specify the master index
  -t <1|2|3>:     specify the tserver index
  -a:             list all tables statistics
  -s:             start the server
  -u:             get the status of the cluser
  -d:             stop the server
  -c:             clean the local content
  -l:             list all tables
  -r <table>:     remove one table
EOF
}


function cluster_status() {
  ${kudu_bin} cluster ksck $MASTERS
}

function start_master() {
  local index=$1
  local pidfile=${CLUSTER_ROOT}/master${index}/run.pid
  local rpc_port="7$((index-1))51"
  local web_port="8$((index-1))51"
  local log_dir=${CLUSTER_ROOT}/master${index}/log
  local wal_dir=${CLUSTER_ROOT}/master${index}/wal
  local data_dir=${CLUSTER_ROOT}/master${index}/data
  local daemon=${CLUSTER_ROOT}/master${index}/daemon.out
  local infofile=${CLUSTER_ROOT}/master${index}/info.json
  if [ ! -d $log_dir ];then
     mkdir -p $log_dir
  fi
  if [ ! -d $wal_dir ];then
     mkdir -p $wal_dir
  fi
  if [ ! -d $data_dir ];then
     mkdir -p $data_dir
  fi
  exec ${kudu_master} -server_dump_info_path=$infofile -rpc_bind_addresses=0.0.0.0:${rpc_port} -log_dir=${log_dir} -fs_wal_dir=${wal_dir} -fs_data_dirs=${data_dir} -webserver_doc_root=${kudu_www_dir} -webserver_port=${web_port}  > ${daemon} 2>&1 &
  local pid=$!
  local retval=$?
  echo $pid > $pidfile
  if [ $retval -eq 0 ];
  then
     echo "Started kudu-master"
  else
     echo "Fail to start kudu-master"
  fi
}

function stop_master() {
  local index=$1
  local pidfile=${CLUSTER_ROOT}/master${index}/run.pid
  if [ -e $pidfile ];then
    kill -9 `cat $pidfile`
  fi
}

function start_tserver() {
  local index=$1
  local pidfile=${CLUSTER_ROOT}/tserver${index}/run.pid
  local rpc_port="7$((index-1))50"
  local web_port="8$((index-1))50"
  local log_dir=${CLUSTER_ROOT}/tserver${index}/log
  local wal_dir=${CLUSTER_ROOT}/tserver${index}/wal
  local data_dir=${CLUSTER_ROOT}/tserver${index}/data
  local daemon=${CLUSTER_ROOT}/tserver${index}/daemon.out
  local infofile=${CLUSTER_ROOT}/tserver${index}/info.json
  if [ ! -d $log_dir ];then
     mkdir -p $log_dir
  fi
  if [ ! -d $wal_dir ];then
     mkdir -p $wal_dir
  fi
  if [ ! -d $data_dir ];then
     mkdir -p $data_dir
  fi
  exec ${kudu_tserver} -server_dump_info_path=$infofile -rpc_bind_addresses=0.0.0.0:${rpc_port} -log_dir=${log_dir} -fs_wal_dir=${wal_dir} -fs_data_dirs=${data_dir} -webserver_doc_root=${kudu_www_dir} -webserver_port=${web_port} -superuser_acl=* -tserver_master_addrs=${MASTERS} >${daemon} 2>&1 &
  local pid=$!
  local retval=$?
  echo $pid > $pidfile
  if [ $retval -eq 0 ];
  then
     echo "Started kudu-tserver"
  else
     echo "Fail to start kudu-tserver"
  fi
}

function stop_tserver() {
  local index=$1
  local pidfile=${CLUSTER_ROOT}/tserver${index}/run.pid
  if [ -e $pidfile ];then
    kill -9 `cat $pidfile`
  fi
}

function clean_folder() {
  local prefix=$1
  local index=$2
  local log_dir=${CLUSTER_ROOT}/${prefix}${index}/log
  local wal_dir=${CLUSTER_ROOT}/${prefix}${index}/wal
  local data_dir=${CLUSTER_ROOT}/${prefix}${index}/data
  rm -rf $log_dir/* $wal_dir/* $data_dir/*
}

function clean_master() {
  local index=$1
  clean_folder "master" $index
}

function clean_tserver() {
  local index=$1
  clean_folder "tserver" $index
}

function list_all_tables() {
  $kudu_bin table list $MASTERS
}

function remove_table() {
  local table=$1
  $kudu_bin table delete $MASTERS $table
}

function all_table_stat() {
  local t
  for t in $($kudu_bin table list $MASTERS)
  do
     echo "====$t===="
     $kudu_bin table statistics $MASTERS $t
  done
}

function main() {
  if [ "$master_index" == "" ] && [ "$tserver_index" == "" ];then
     if [ "$start" != "" ] || [ "$stop" != "" ];then
       echo "Must specify either master or tserver index"
       usage
       exit 1
     fi
  fi
  if [ "$master_index" != "" ];then
     if [ "$start" == "1" ];then
         start_master $master_index
     fi
     if [ "$stop" == "1" ];then
         stop_master $master_index
     fi
     if [ "$clean" == "1" ];then
	 clean_master $master_index
     fi
  fi
  if [ "$tserver_index" != "" ];then
     if [ "$start" == "1" ];then
         start_tserver $tserver_index
     fi
     if [ "$stop" == "1" ];then
         stop_tserver $tserver_index
     fi
     if [ "$clean" == "1" ];then
	 clean_tserver $tserver_index
     fi
  fi
  if [ "$status" == "1" ];then
     cluster_status
  fi
  if [ "$list" == "1" ];then
     list_all_tables
  fi
  if [ "$remove_table" != "" ];then
     remove_table $remove_table
  fi
  if [ "$stat" == "1" ];then
     all_table_stat
  fi
}

master_index=""
tserver_index=""
start=""
stop=""
status=""
clean=""
list=""
remove_table=""
stat=""

while getopts ":m:t:r:ahsudcl" opt
do
  case $opt in
    a) stat=1
      ;;
    m)
      master_index=$OPTARG
      ;;
    t)
      tserver_index=$OPTARG
      ;;
    r)
      remove_table=$OPTARG
      ;;
    h) usage
      ;;
    s) start=1
      ;;
    d) stop=1
      ;;
    u) status=1
      ;;
    c) clean=1
      ;;
    l) list=1
      ;;
    *)
      echo "unrecognized parameters $opt"
      usage
      exit 1
  esac
done

main
