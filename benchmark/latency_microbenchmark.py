from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import datetime
import time
import argparse
import numpy as np

BATCH_SIZE = 1000

class A(object):
    def __init__(self, node_resource):
        print("Actor A start...")
        self.b = ray.remote(resources={node_resource: 1})(B).remote()
        ray.get(self.b.ready.remote())

    def ready(self):
        time.sleep(1)
        return True

    def f(self, target_throughput, experiment_time):
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        start = time.time()
        start2 = time.time()
        i = 0
        batch_size = BATCH_SIZE
        if target_throughput < batch_size:
            batch_size = int(target_throughput)
        while True:
            self.b.f.remote(time.time())
            if i % batch_size == 0 and i > 0:
                end = time.time()
                sleep_time = (batch_size / target_throughput) - (end - start2)
                if sleep_time > 0.00003:
                    time.sleep(sleep_time)
                start2 = time.time()
                if end - start >= experiment_time:
                    break
            i += 1

        return ray.get(self.b.get_sum.remote())

class B(object):
    def __init__(self):
        print("Actor B start...")
        self.latencies = []
        #import yep
        #yep.start('actorB.prof')

    def ready(self):
        time.sleep(1)
        return True

    def f(self, submit_time):
        latency = time.time() - submit_time
        self.latencies.append(latency)
        if len(self.latencies) % BATCH_SIZE == 0:
            print(latency)


    def get_sum(self):
        #import yep
        #yep.stop()
        return self.latencies


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--target-throughput', type=int, default=3000)
    parser.add_argument('--experiment-time', type=int, default=10)
    parser.add_argument('--num-workers', type=int, default=1)
    parser.add_argument('--num-raylets', type=int, default=1)
    parser.add_argument('--num-shards', type=int, default=None)
    parser.add_argument('--gcs', action='store_true')
    parser.add_argument('--policy', type=int, default=0)
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--max-lineage-size', type=str)
    args = parser.parse_args()

    gcs_delay_ms = -1
    if args.gcs:
        gcs_delay_ms = 0

    if args.redis_address is None:
        ray.worker._init(start_ray_local=True, use_raylet=True, num_local_schedulers=args.num_raylets * 2,
                         resources=[
                            {
                                "Node{}".format(i): args.num_workers,
                            } for i in range(args.num_raylets * 2)],
                         gcs_delay_ms=gcs_delay_ms,
                         num_redis_shards=args.num_shards,
                         lineage_cache_policy=args.policy,
                         num_workers=args.num_workers,
                         max_lineage_size=args.max_lineage_size)
    else:
        ray.init(redis_address=args.redis_address + ":6379", use_raylet=True)

    actors = []
    for node in range(args.num_raylets):
        for _ in range(args.num_workers):
            actor_cls = ray.remote(resources={
                "Node{}".format(node * 2): 1,
                })(A)
            actors.append(actor_cls.remote("Node{}".format(node * 2 + 1)))
    total_time = ray.get([actor.ready.remote() for actor in actors])
    latencies = ray.get([actor.f.remote(
        args.target_throughput,
        args.experiment_time) for actor in actors])
    latencies = [latency[len(latency) // 4 : 3 * len(latency) // 4] for latency in latencies]
    for latency in latencies:
        print("DONE, average latency:", sum(latency) / len(latency))
        for point in latency:
            print(point)
