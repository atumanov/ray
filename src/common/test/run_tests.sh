#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$ROOT_DIR/.."
  # This assumes the default port is not already being used.
  ./thirdparty/redis-3.2.3/src/redis-server &
  REDIS_SERVER_PID=$!
  sleep 1s
  ./build/common_tests
  ./build/db_tests
  ./build/io_tests
  ./build/task_tests
  ./build/redis_tests
  ./build/task_table_tests
  ./build/object_table_tests
  kill $REDIS_SERVER_PID
popd
