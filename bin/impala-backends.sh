usage="Usage: impala-daemons --config <impala-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/impala-config.sh

# If the backends file is specified in the command line,
# then it takes precedence over the definition in 
# impala-env.sh. Save it here.
HOSTLIST=$IMPALA_BACKENDS

if [ "$HOSTLIST" = "" ]; then
  if [ "$IMPALA_BACKENDS" = "" ]; then
    export HOSTLIST="${bin}/backends"
  else
    export HOSTLIST="${IMPALA_BACKENDS}"
  fi
fi

for backend in `cat "$HOSTLIST"`; do
  if ${IMPALA_SLAVE_PARALLEL:-true}; then 
    ssh $IMPALA_SSH_OPTS $backend $"${@// /\\ }" \
      2>&1 | sed "s/^/$backend: /" &
  else # run each command serially 
    ssh $IMPALA_SSH_OPTS $backend $"${@// /\\ }" \
      2>&1 | sed "s/^/$backend: /"
  fi
  if [ "$IMPALA_SLAVE_SLEEP" != "" ]; then
    sleep $IMPALA_SLAVE_SLEEP
  fi
done

wait
