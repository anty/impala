#!/usr/bin/env bash

this=${BASH_SOURCE-$0}
bin=`dirname $this`
bin=`cd $bin;pwd -P`
. $bin/impala-config.sh

#"$bin"/impala-daemon.sh  stop statestore
"$bin"/impala-daemons.sh stop impalaserver



