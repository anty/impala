#!/usr/bin/env bash

this=${BASH_SOURCE-$0}
bin=`dirname $this`
bin=`cd $bin;pwd -P`

. $bin/impala-config.sh
. $bin/impala-args.sh
#get arguments

COMMAND=$1
shift

if [ "x$IMPALA_BUILD_TYPE" = "x" ];then

   echo "built type must be sepcified in impala-args.sh"
   exit 1
fi

nohup $IMPALA_HOME/be/build/$IMPALA_BUILD_TYPE/statestore/statestored >statestore-$USER.log 2>&1 &
