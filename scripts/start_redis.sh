#!/bin/sh

REDIS_PORT_START=$1
RAY_HOME=/data/atumanov/ray

for i in $(seq 0 9); do
  redis_port=`expr $REDIS_PORT_START + $i` 
  echo "starting redis on port $redis_port"
  ${RAY_HOME}/python/ray/core/src/common/thirdparty/redis/src/redis-server \
    --port ${redis_port} \
    --loadmodule ${RAY_HOME}/python/ray/core/src/common/redis_module/libray_redis_module.so > /dev/null 2> /dev/null &
  sleep 1
  redis-cli -p ${redis_port} config set protected-mode no
done
