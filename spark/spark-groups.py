from pyspark import SparkContext
import logging
from itertools import combinations_with_replacement
import time

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="GroupedShuffleTrafficTest")

q = 1000  # Predefined max reducer capacity for elements
d = 5000  # our total multitude of data
group_size = int(q * 0.5)  # use 0.5 or 0.05 for half or 10% use of q
payload = 1  # Configurable payload

# Step 1: Predefine groups
groups = [list(range(i, i + group_size)) for i in range(1, d + 1, group_size)]

# Step 2: Create group pairs (with self-pairs included)
# Result: [ ((1, group1_100), (2, group101_200)), ((1, group1_100)), (3, group201_300)), ... ]
group_pairs = list(combinations_with_replacement(enumerate(groups, 1), 2))


start_time = time.perf_counter()
# Step 3: Parallelize group pairs (one per partition)
group_pair_rdd = sc.parallelize(group_pairs, len(groups))

# print(len(groups), f'length of group pairs {len(group_pairs)}')


# Step 4: Define logic to emit all (i, j) → [i, j] from two groups
data_exchanged = sc.accumulator(0)


def emit_pairwise_records(group_pair):
    (id1, g1), (id2, g2) = group_pair
    l1 = len(g1)
    l2 = len(g2)
    outputs = []
    if id1 != id2:
        data_exchanged.add(l1 + l2)  # two groups were shuffled
    # else:
    # data_exchanged.add(l1) # one group was shuffled, and matched with itself e.g. (1,1)
    reducer_id = id1
    outputs.append((reducer_id % 30, (g1, g2)))
    # for i in g1:
    #     for j in g2:
    #         # outputs.append((i, j), payload)
    #         yield (i, j), payload
    return outputs


# Step 5: Apply the mapping logic
result_rdd = group_pair_rdd.flatMap(emit_pairwise_records)


def f(splitIndex, iterator):
    yield splitIndex


grouped_by_reducer = result_rdd.groupByKey().mapPartitionsWithIndex(f).max()

print(grouped_by_reducer + 1)

# result_rdd.foreach(lambda x: x)
# grouped_by_reducer.foreach(lambda x: x)#.collect()
# result_rdd.collect()
print("DATA_EXCHANGED", data_exchanged.value)
print(f"time taken: {time.perf_counter() - start_time}")
sc.stop()
