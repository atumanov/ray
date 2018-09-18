#parallel-ssh -t 0 -i -P -h workers.txt -O "StrictHostKeyChecking=no" -O "IdentityFile=~/devenv-key.pem" -I < stop_worker.sh
parallel-ssh -t 0 -i -P -h workers.txt -O "StrictHostKeyChecking=no" -I < stop_worker.sh
