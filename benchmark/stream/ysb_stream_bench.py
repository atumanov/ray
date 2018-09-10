from __future__ import print_function
import argparse
from collections import defaultdict
from collections import Iterable
import time
import logging
import numpy as np
import ujson
import uuid
import sys

import ray
import ray.cloudpickle as pickle


from conf import *
from generate import *

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

BATCH = True
USE_OBJECTS = True
USE_MULTIPLE_PROJECTOR_OUTPUT = False

@ray.remote
def warmup_objectstore():
    return
    x = np.ones(10 ** 8)
    for _ in range(100):
        ray.put(x)

@ray.remote
def warmup(*args):
    return

def ray_warmup(reducers, node_resources):
    """
    Warmup Ray
    """
    warmups = []
    for node_resource in node_resources:
        warmups.append(warmup_objectstore._submit(
            args=[],
            resources={node_resource: 1}))
    ray.wait(warmups, num_returns=len(warmups))

    num_rounds = 50
    for i in range(num_rounds):
        with ray.profiling.profile("reduce_round", worker=ray.worker.global_worker):
            start = time.time()
            args = [[time.time()] for _ in range(len(node_resources))]
            if BATCH:
                for _ in range(5):
                    batch = []
                    for j, node_resource in enumerate(node_resources):
                        task = reduce_warmup._submit(
                                        args=[args[j]],
                                        resources={node_resource: 1},
                                        batch=True)
                        batch.append(task)
                    args = ray.worker.global_worker.submit_batch(batch)
                batch = [reducer.foo.remote_batch(*args) for reducer in reducers]
                ray.worker.global_worker.submit_batch(batch)
            else:
                for _ in range(5):
                    batch = []
                    for j, node_resource in enumerate(node_resources):
                        return_value = reduce_warmup._submit(
                                        args=[args[j]],
                                        resources={node_resource: 1})
                        batch.append(return_value)
                    args = batch
                [reducer.foo.remote(*args) for reducer in reducers]

        took = time.time() - start
        if took > 0.1:
            print("Behind by", took - 0.1)
        else:
            print("Ahead by", 0.1 - took)
            time.sleep(0.1 - took)
        if i % 10 == 0:
            print("finished warmup round", i, "out of", num_rounds)

    gen_deps = init_generator._submit(
            args=[AD_TO_CAMPAIGN_MAP, time_slice_num_events],
            resources={
                "Node0": 1,
                })
    ray.wait(gen_deps, num_returns=len(gen_deps))
    warmups = []
    for node_resource in reversed(node_resources):
        warmups.append(warmup._submit(
            args=gen_deps,
            resources={node_resource: 1}))
    ray.wait(warmups, num_returns=len(warmups))
    return gen_deps


@ray.remote
def reduce_warmup(timestamp, *args):
    timestamp = timestamp[0]
    time.sleep(0.05)
    if USE_OBJECTS:
        return [timestamp for _ in range(1000)]
    else:
        return [timestamp]

def submit_tasks():
    generated = []
    for i in range(num_nodes):
        for _ in range(num_generators_per_node):
            generated.append(generate._submit(
                args=[num_generator_out] + list(gen_deps) + [time_slice_num_events],
                num_return_vals=num_generator_out, 
                resources={node_resources[i]: 1},
                batch=BATCH))
    if BATCH:
        generated = ray.worker.global_worker.submit_batch(generated)
    generated = flatten(generated)

    parsed, start_idx = [], 0
    for i in range(num_nodes):
        for _ in range(num_parsers_per_node):
            parsed.append(parse_json._submit(
                args=[num_parser_out] + generated[start_idx : start_idx + num_parser_in],
                num_return_vals=num_parser_out,
                resources={node_resources[i] : 1},
                batch=BATCH))
            start_idx += num_parser_in
    if BATCH:
        parsed = ray.worker.global_worker.submit_batch(parsed)
    parsed = flatten(parsed)

    filtered, start_idx = [], 0
    for i in range(num_nodes):
        for _ in range(num_filters_per_node):
            filtered.append(filter._submit(
                args=[num_filter_out] + parsed[start_idx : start_idx + num_filter_in],
                num_return_vals=num_filter_out,
                resources={node_resources[i] : 1},
                batch=BATCH))
            start_idx += num_filter_in
    if BATCH:
        filtered = ray.worker.global_worker.submit_batch(filtered)
    filtered = flatten(filtered)

    shuffled, start_idx = [], 0
    if USE_MULTIPLE_PROJECTOR_OUTPUT:
        num_return_vals = num_projector_out
    else:
        num_return_vals = 1
    for i in range(num_nodes):
        for _ in range(num_projectors_per_node):
            batches = filtered[start_idx : start_idx + num_projector_in]
            shuffled.append(project_shuffle._submit(
                args=[num_projector_out] + batches,
                num_return_vals=num_return_vals,
                resources={node_resources[i] : 1},
                batch=BATCH))
            start_idx += num_projector_in
    if BATCH:
        shuffled = ray.worker.global_worker.submit_batch(shuffled)

    if USE_MULTIPLE_PROJECTOR_OUTPUT:
        shuffled = np.array(shuffled)

        if num_reducers == 1:
            # Handle return type difference for num_ret_vals=1
            shuffled = np.reshape(shuffled, (len(shuffled), 1))

        if BATCH:
            batch = [reducer.reduce.remote_batch(*shuffled[:,i]) for i, reducer in enumerate(reducers)]
            ray.worker.global_worker.submit_batch(batch)
        else:
            [reducer.reduce.remote(*shuffled[:,i]) for i, reducer in enumerate(reducers)]
    else:
        if BATCH:
            batch = [reducer.reduce.remote_batch(*shuffled) for reducer in reducers]
            ray.worker.global_worker.submit_batch(batch)
        else:
            [reducer.reduce.remote(*shuffled) for reducer in reducers]


def compute_stats():
    i, total = 0, 0
    missed_total, miss_time_total = 0, 0
    counts = ray.get([reducer.count.remote() for reducer in reducers])
    for count in counts:
        print("Counted:", count)

    all_seen = ray.get([reducer.seen.remote() for reducer in reducers])
    for seen in all_seen:
        reducer_total = 0
        i += 1
        for key in seen:
            if key[1] < end_time:
                reducer_total += seen[key]
            else:
                missed_total += seen[key]
                miss_time_total += (end_time - key[1])
		#print("Missed by:", )
        total += reducer_total
        print("Seen by reducer", i, ": ", reducer_total)

    thput = 3 * total/exp_time
    miss_thput = 3 * missed_total/exp_time
    avg_miss_lat = miss_time_total / missed_total if missed_total != 0 else 0
    print("System Throughput: ", thput, "events/s")
    print("Missed: ", miss_thput, "events/s by", avg_miss_lat, "on average.")

def compute_checkpoint_overhead():
    checkpoints = ray.get([reducer.__ray_save__.remote() for reducer in reducers])
    start = time.time()
    pickles = [ray.cloudpickle.dumps(checkpoint) for checkpoint in checkpoints]
    end = time.time()
    print("Checkpoint pickle time", end - start)
    print("Checkpoint size(bytes):", [len(pickle) for pickle in pickles])

def write_latencies():
    f = open("lat.txt", "w+")
    lat_total, count = 0, 0
    mid_time = (end_time - start_time) / 2
    for reducer in reducers:
        lats = ray.get(reducer.get_latencies.remote())
        for key in lats:
            if key[1] < end_time:
                f.write(str(lats[key]) + "\n")
                lat_total += lats[key]
                count += 1
    print("Average latency: ", lat_total/count)
    f.close()

def write_timeseries():
    f = open("timeseries.txt", "w+")
    total_lat = defaultdict(int)
    total_count = defaultdict(int)
    for reducer in reducers:
        lats = ray.get(reducer.get_latencies.remote())
        for key in lats:
            if key[1] < end_time:
                total_lat[key[1]] += lats[key]
                total_count[key[1]] += 1
    for key in total_lat:
        f.write(str(key) + " " + str(total_lat[key]/total_count[key]) + "\n")
    f.close()

def get_node_names(num_nodes):
    node_names = set()
    while len(node_names) < num_nodes:
        hosts = [ping.remote() for _ in range(num_nodes * 100)]
        hosts, incomplete = ray.wait(hosts, timeout=30000) # timeout after 10s
        [node_names.add(ray.get(host_id)) for host_id in hosts]
        print(len(hosts), len(node_names))
        print("Nodes:", node_names)
        if incomplete:
            print("Timed-out after getting: ", len(hosts), "and missing", len(incomplete))
    return list(node_names)

def read_node_names(num_nodes):
    with open('/home/ubuntu/ray/benchmarks/stream/conf/priv-hosts-all') as f:
        lines = f.readlines()
    names = [l.strip() for l in lines][:num_nodes]
    if len(names) < num_nodes:
        raise IOError("File contains less than the requested number of nodes")
    return names

def ping_node(node_name):
    name = ray.get(ping._submit(args=[0.1], resources={node_name:1}))
    if name != node_name:
        print("Tried to ping", node_name, "but got", name)
    else:
        print("Pinged", node_name)
    return name

FIELDS = [
    u'user_id',
    u'page_id',
    u'ad_id',
    u'ad_type',
    u'event_type',
    u'event_time',
    u'ip_address',
    ]
USER_ID    = 0
PAGE_ID    = 1
AD_ID      = 2
AD_TYPE    = 3
EVENT_TYPE = 4
EVENT_TIME = 5
IP_ADDRESS = 6

########## helper functions #########

def generate_id():
    return str(uuid.uuid4()).encode('ascii')

# Take the ceiling of a timestamp to the end of the window.
def ts_to_window(timestamp):
    return ((float(timestamp) // WINDOW_SIZE_SEC) + 1) * WINDOW_SIZE_SEC

def flatten(x):
    if isinstance(x, Iterable):
        return [a for i in x for a in flatten(i)]
    else:
        return [x]

def init_actor(node_index, node_resources, actor_cls, checkpoint, args=None):
    if args is None:
        args = []
    args.append(node_index)

    if checkpoint:
        actor = ray.remote(
		num_cpus=0,
                checkpoint_interval=int(WINDOW_SIZE_SEC / BATCH_SIZE_SEC),
                resources={ node_resources[node_index]: 1, })(actor_cls).remote(*args)
    else:
        actor = ray.remote(
		num_cpus=0,
                resources={ node_resources[node_index]: 1, })(actor_cls).remote(*args)
    ray.get(actor.clear.remote())
    # Take a checkpoint once every window.
    actor.node_index = node_index
    return actor

############### Tasks ###############

@ray.remote
def ping(time_to_sleep=0.01):
    time.sleep(time_to_sleep)
    return socket.gethostname()


def to_list(json_object):
    return [json_object[field] for field in FIELDS]

@ray.remote
def parse_json(num_ret_vals, *batches):
    """
    Parse batch of JSON events
    """
    parsed = np.array([to_list(ujson.loads(e.tobytes().decode('ascii'))) for batch in batches for e in batch])
    return parsed if num_ret_vals == 1 else tuple(np.array_split(parsed, num_ret_vals))


@ray.remote
def filter(num_ret_vals, *batches):
    """
    Filter events for view events
    """
    filtered_batches = []
    for batch in batches:
        filtered_batches.append(batch[batch[:, EVENT_TYPE] == u"view"])

    if len(batches) > 1:
        filtered = np.concatenate(filtered_batches)
    else:
        filtered = filtered_batches[0]
    return filtered if num_ret_vals == 1 else tuple(np.array_split(filtered, num_ret_vals))


@ray.remote
def project_shuffle(num_ret_vals, *batches):
    """
    Project: e -> (campaign_id, window)
    Count by: (campaign_id, window)
    Shuffles by hash(campaign_id)
    """
    shuffled = [defaultdict(int) for _ in range(num_ret_vals)]
    for batch in batches:
        window = ts_to_window(batch[0][EVENT_TIME])
        for e in batch:
            cid = AD_TO_CAMPAIGN_MAP[e[AD_ID].encode('ascii')]
            shuffled[hash(cid) % num_ret_vals][(cid, window)] += 1
    # Deserializing tuples is a lot faster than defaultdict.
    shuffled = [tuple(partition.items()) for partition in shuffled]
    if USE_MULTIPLE_PROJECTOR_OUTPUT:
        return shuffled[0] if len(shuffled) == 1 else tuple(shuffled)
    else:
        return shuffled


class Reducer(object):

    def __init__(self, reduce_index):
        """
        Constructor
        """
        self.reduce_index = reduce_index
        # (campaign_id, window) --> count
        self.clear()

    def __ray_save__(self):
        checkpoint = pickle.dumps({
                "seen": list(self.seen.items()),
                "latencies": list(self.latencies.items()),
                "calls": self.calls,
                "count": self.count,
                "reduce_index": self.reduce_index,
                })
        self.checkpoints.append(checkpoint)
        self.seen.clear()
        self.latencies.clear()
        return self.checkpoints

    def __ray_restore__(self, checkpoints):
        self.__init__()
        self.checkpoints = checkpoints
        if len(self.checkpoints) > 0:
            checkpoint = pickle.loads(self.checkpoints[-1])
            self.calls = checkpoint["calls"]
            self.count = checkpoint["count"]
            self.reduce_index = checkpoint["reduce_index"]

    def seen(self):
        if not self.checkpoints:
            return self.seen
        else:
            seen = defaultdict(int)
            for checkpoint in self.checkpoints:
                checkpoint_seen = pickle.loads(checkpoint)["seen"]
                for key, count in checkpoint_seen:
                    seen[tuple(key)] += count
            return seen

    def get_latencies(self):
        if not self.checkpoints:
            return self.latencies
        else:
            latencies = defaultdict(int)
            for checkpoint in self.checkpoints:
                checkpoint_latencies = pickle.loads(checkpoint)["latencies"]
                for key, latency in checkpoint_latencies:
                    latencies[tuple(key)] = latency
            return latencies

    def count(self):
        return self.count

    def clear(self):
        """
        Clear all data structures.
        """
        self.seen = defaultdict(int)
        self.latencies = defaultdict(int)
        self.calls = 0
        self.count = 0
        self.checkpoints = []

    def reduce(self, *partitions):
        """
        Reduce by key
        Increment and store in dictionary
        """
        if not USE_MULTIPLE_PROJECTOR_OUTPUT:
            partitions = [partition[self.reduce_index] for partition in partitions]
        self.calls += 1
        for partition in partitions:
            for key, count in partition:
                self.seen[key] += count
                self.count += count
                latency = time.time() - key[1]
                self.latencies[key] = max(self.latencies[key], latency)
        if DEBUG and self.calls % 10 == 0:
            for key, latency in self.latencies:
                if latency > 0:
                    print(latency)

    def foo(self, *timestamps):
        """
        Reduce by key
        Increment and store in dictionary
        """
        timestamp = min([lst[0] for lst in timestamps])
        print("foo latency:", time.time() - timestamp - 0.05 * 5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump', type=str, default='dump.json')
    parser.add_argument('--exp-time', type=int, default=30)
    parser.add_argument('--no-hugepages', action='store_true')
    parser.add_argument('--actor-checkpointing', action='store_true')
    parser.add_argument('--num-projectors', type=int, default=1)
    parser.add_argument('--num-filters', type=int, default=1)
    parser.add_argument('--num-generators', type=int, default=1)
    parser.add_argument('--num-nodes', type=int, required=True)
    parser.add_argument('--num-parsers', type=int, default=1)
    parser.add_argument('--num-reducers', type=int, default=1)
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--target-throughput', type=int, default=1e5)
    parser.add_argument('--test-throughput', action='store_true')
    parser.add_argument('--time-slice-ms', type=int, default=100)
    parser.add_argument('--warmup-time', type=int, default=10)
    args = parser.parse_args()

    checkpoint = args.actor_checkpointing
    exp_time = args.exp_time
    warmup_time = args.warmup_time
    num_nodes = args.num_nodes
    time_slice_num_events = int(args.target_throughput * BATCH_SIZE_SEC)

    num_generators_per_node = args.num_generators
    num_parsers_per_node = args.num_parsers 
    num_filters_per_node = args.num_filters
    num_projectors_per_node = args.num_projectors 

    num_generators = args.num_generators * num_nodes
    num_parsers = args.num_parsers * num_nodes
    num_filters = args.num_filters * num_nodes
    num_projectors = args.num_projectors * num_nodes
    num_reducers = args.num_reducers

    num_generator_out = max(1, num_parsers // num_generators)
    num_parser_in = max(1, num_generators // num_parsers)
    num_parser_out = max(1, num_filters // num_parsers)
    num_filter_in = max(1, num_parsers // num_filters)
    num_filter_out = max(1, num_projectors  // num_filters)
    num_projector_in = max(1, num_filters_per_node * num_filter_out // num_projectors_per_node)
    num_projector_out = num_reducers

    print("Per node setup: ")
    print("[", num_generators_per_node, "generators ] -->", num_generator_out)
    print(num_parser_in, "--> [", num_parsers_per_node, "parsers ] -->", num_parser_out  )
    print(num_filter_in, "--> [", num_filters_per_node, "filters ] -->", num_filter_out  )
    print(num_projector_in, "--> [", num_projectors_per_node, "projectors ] -->", num_projector_out)
    print(num_projectors, "--> [", num_reducers, "reducers]")

    node_resources = ["Node{}".format(i) for i in range(num_nodes)]

    if args.redis_address is None:
        huge_pages = not args.no_hugepages
        plasma_directory = "/mnt/hugepages" if huge_pages else None
        resources = [dict([(node_resource, num_generators + num_projectors + 
                            num_parsers + num_filters + num_reducers)]) 
                    for node_resource in node_resources]
        ray.worker._init(
            start_ray_local=True,
            redirect_output=True,
            use_raylet=True,
            resources=resources,
            num_local_schedulers=num_nodes,
            huge_pages=huge_pages,
            plasma_directory=plasma_directory)
    else:
        ray.init(redis_address="{}:6379".format(args.redis_address), use_raylet=True)
    time.sleep(2)

    print("Warming up...")
    #ray_warmup(node_resources)

    print("Initializing generators...")
    print("Initializing reducers...")
    reducers = [init_actor(i, node_resources, Reducer, checkpoint) for i in range(num_reducers)]
    time.sleep(1)
    # Make sure the reducers are initialized.
    ray.get([reducer.clear.remote() for reducer in reducers])
    print("...finished initializing reducers:", len(reducers))

    print("Placing dependencies on nodes...")
    gen_deps = ray_warmup(reducers, node_resources)

    if exp_time > 0:
        try:
            time_to_sleep = BATCH_SIZE_SEC
            time.sleep(10) # TODO non-deterministic, fix

            print("Warming up...")
            start_time = time.time()
            end_time = start_time + warmup_time

            for i in range(10):
                round_start = time.time()
                submit_tasks()
                time.sleep(time_to_sleep)
                ray.wait([reducer.clear.remote() for reducer in reducers],
                         num_returns = len(reducers))
                print("finished round", i, "in", time.time() - round_start)

            time.sleep(10) # TODO non-deterministic, fix

            print("Measuring...")
            start_time = time.time()
            end_time = start_time + exp_time

            while time.time() < end_time:
                loop_start = time.time()
                submit_tasks()
                loop_end = time.time()
                time_to_sleep = BATCH_SIZE_SEC - (loop_end - loop_start)
                if time_to_sleep > 0:
                    time.sleep(time_to_sleep)
                else:
                    print("WARNING: behind by", time_to_sleep * -1)

            end_time = time.time()

            print("Finished in: ", (end_time - start_time), "s")

            print("Dumping...")
            ray.global_state.chrome_tracing_dump(filename=args.dump)

            print("Computing throughput...")
            compute_stats()

            print("Writing latencies...")
            write_latencies()

            print("Computing checkpoint overhead...")
            compute_checkpoint_overhead()

            print("Writing timeseries...")
            write_timeseries()

        except KeyboardInterrupt:
            print("Dumping current state...")
            ray.global_state.chrome_tracing_dump(filename=args.dump)
            exit()
