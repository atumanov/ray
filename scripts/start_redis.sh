#!/bin/sh

RAY_HOME=$HOME/ray
${RAY_HOME}/python/ray/core/src/common/thirdparty/redis/src/redis-server \
    --port 6379 \
    --loadmodule ${RAY_HOME}/python/ray/core/src/common/redis_module/libray_redis_module.so > /dev/null 2> /dev/null &
sleep 1
redis-cli -p 6379 config set protected-mode no
