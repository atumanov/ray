HEAD_IP=$1
WORKER_INDEX=$2
NUM_RAYLETS=$3
LINEAGE_POLICY=$4
MAX_LINEAGE_SIZE=$5
GCS_DELAY_MS=$6


export PATH=/home/ubuntu/anaconda3/bin/:$PATH

ulimit -c unlimited
ulimit -n 65536
ulimit -a

SLEEP_TIME=$(( $RANDOM % 5 ))
sleep $SLEEP_TIME

ray start --num-workers 5 --use-raylet --redis-address=$HEAD_IP:6379 --resources='{"Node'$WORKER_INDEX'": 100}' --gcs-delay-ms $GCS_DELAY_MS --lineage-cache-policy=$LINEAGE_POLICY --max-lineage-size=$MAX_LINEAGE_SIZE --plasma-directory=/mnt/hugepages --huge-pages --num-cpus 4
