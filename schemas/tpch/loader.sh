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
    --num-partitions=${NUM_PARTITIONS} \
    --check-diskspace-of-directory=${DISK_SPACE_CHECK_DIR} \
    --s64da-license-path=${S64DA_LICENSE_PATH} \
    --benchmark=tpch
