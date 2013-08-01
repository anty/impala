IMPALA_STATESTORE_HOST=nd0-rack0-cloud
IMPALA_STATESTORE_PORT=24000
IMPALA_BACKEND_PORT=22000
IMPALA_LOG_DIR=/var/log/impala
IMPALA_MEM_LIMIT=5G

export IMPALA_BUILD_TYPE=release

export IMPALA_STATESTORE_ARGS=${IMPALA_STATESTORE_ARGS:- \
-log_dir=${IMPALA_LOG_DIR} \
-state_store_port=${IMPALA_STATESTORE_PORT} \
-mem_limit=2G \
}


export IMPALA_SERVER_ARGS=${IMPALA_SERVER_ARGS:-\
-log_dir=${IMPALA_LOG_DIR} \
-state_store_port=${IMPALA_STATESTORE_PORT} \
-state_store_host=${IMPALA_STATESTORE_HOST} \
-use_statestore \
-be_port=${IMPALA_BACKEND_PORT} \
-mem_limit=$IMPALA_MEM_LIMIT \
}
