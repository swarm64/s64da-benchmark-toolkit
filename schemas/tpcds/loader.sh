#!/bin/bash

cd "$(dirname "$(readlink -f "$0")")"
cd ../../

source ./scripts/functions.sh

check_and_set_python

${PYTHON} prepare_benchmark \
    --dsn=postgresql://postgres@${DB_HOST}:${DB_PORT}/${DB} \
    --scale-factor=${SCALE_FACTOR} \
    --schema=${SCHEMA} \
    --chunks=${CHUNKS} \
    --max-jobs=${MAX_JOBS} \
    --check-diskspace-of-directory=${DISK_SPACE_CHECK_DIR} \
    --benchmark=tpcds
