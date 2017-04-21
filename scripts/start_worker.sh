#!/bin/sh

if [ $# -lt 2 ]; then
  echo "received only $# command line arguments..."
  echo "USAGE: $0 redisip:redisport redis_shard_pylist"
  exit 1
fi

RAY_HOME=$HOME/ray
REDIS_ADDRESS=$1
REDIS_SHARDS=$2
NUM_CPUS=64
NUM_GPUS=1
NUM_WORKERS=${NUM_CPUS}

$RAY_HOME/scripts/start_ray.sh       \
    --redis-address ${REDIS_ADDRESS} \
    --redis-shards ${REDIS_SHARDS}   \
    --num-cpus ${NUM_CPUS}           \
    --num-gpus ${NUM_GPUS}           \
    --num-workers ${NUM_WORKERS}

