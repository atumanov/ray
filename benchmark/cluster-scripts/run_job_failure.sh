NUM_RAYLETS=$1
NUM_REDIS_SHARDS=${2:-1}
NUM_REDUCERS=${3:-$NUM_RAYLETS}
EXPERIMENT_TIME=${4:-60}
GCS_DELAY_MS=${5:--1}
USE_REDIS=${6:-0}
USE_JSON=${7:-""}

THROUGHPUT=150000

LINEAGE_POLICY=1
MAX_LINEAGE_SIZE=1

HEAD_IP=$(head -n 1 workers.txt)

OUTPUT_FILENAME="$NUM_RAYLETS-nodes-$NUM_REDIS_SHARDS-shards-$NUM_REDUCERS-reducers-100-timeslice-$GCS_DELAY_MS-gcs-$EXPERIMENT_TIME-s"

if [ $# -gt 7 ] || [ $# -eq 0 ]
then
    echo "Usage: ./run_jobs.sh <num raylets> <num shards> <num reducers> <experiment time> <gcs delay>"
    exit
else
    echo "Logging output to $OUTPUT_FILENAME"
fi


./stop_cluster.sh
./start_cluster.sh $NUM_RAYLETS $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS $NUM_REDIS_SHARDS

sleep 5

echo "Starting job..."

DUMP_ARG=""
if [ $EXPERIMENT_TIME -le 60 ]
then
    DUMP_ARG="--dump dump.json"
fi

REDIS_ADDRESS=""
if [ $USE_REDIS -eq 1 ]
then
    echo "Starting redis for YSB results at $HEAD_IP:6380..."
    /home/ubuntu/redis-4.0.11/src/redis-cli -p 6380 shutdown
    REDIS_UP=PONG
    while [ ! -z $REDIS_UP ]; do
        REDIS_UP=$(/home/ubuntu/redis-4.0.11/src/redis-cli -p 6380 ping)
    done

    /home/ubuntu/redis-4.0.11/src/redis-server --port 6380 &
    REDIS_UP=""
    while [ -z $REDIS_UP ]; do
        REDIS_UP=$(/home/ubuntu/redis-4.0.11/src/redis-cli -p 6380 ping)
    done
    /home/ubuntu/redis-4.0.11/src/redis-cli -p 6380 CONFIG SET protected-mode no
    REDIS_ADDRESS="--reduce-redis-address $HEAD_IP:6380"
    echo "...Redis up"
fi

JSON_ARG=""
if [ ! -z $USE_JSON ]
then
    JSON_ARG="--use-json"
fi

# Pick a node that isn't the reducer for now.
WORKER_RAYLETS=$(( $NUM_RAYLETS - 1 ))
DEAD_NODE=$(tail -n $NUM_RAYLETS workers.txt | tail -n $(( $RANDOM % $WORKER_RAYLETS )) | head -n 1)
echo "dead node is $DEAD_NODE"


python ~/ray/benchmark/stream/ysb_stream_bench.py --redis-address $HEAD_IP --num-nodes $NUM_RAYLETS --num-parsers 2 --target-throughput $THROUGHPUT --num-reducers $NUM_REDUCERS --exp-time $EXPERIMENT_TIME --num-reducers-per-node 4 $DUMP_ARG $REDIS_ADDRESS --output-filename $OUTPUT_FILENAME --actor-checkpointing $JSON_ARG --node-failure $DEAD_NODE