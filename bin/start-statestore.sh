#!/usr/bin/env bash

this=${BASH_SOURCE-$0}
bin=`dirname $this`
bin=`cd $bin;pwd -P`
. $bin/impala-config.sh

remote_cmd="cd $IMPALA_HOME;$bin/impala-daemon.sh stop impalaserver"

echo "#$remote_cmd#"
#start statestore


#start backends


for backend in `cat "$bin/backends"`; do
  if ${HBASE_SLAVE_PARALLEL:-true}; then 
    ssh  $backend "$remote_cmd" \
      2>&1 | sed "s/^/$backend: /" &
  else # run each command serially 
    ssh  $backend $"${remote_cmd// /\\ }" \
      2>&1 | sed "s/^/$backend: /"
  fi
  if [ "$HBASE_SLAVE_SLEEP" != "" ]; then
    sleep $HBASE_SLAVE_SLEEP
  fi
done




wait
