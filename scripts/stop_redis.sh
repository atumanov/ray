#!/bin/sh
# Find the PID of the Redis process and kill it.
kill $(ps aux | grep redis-server | awk '{ print $2 }') 2> /dev/null

