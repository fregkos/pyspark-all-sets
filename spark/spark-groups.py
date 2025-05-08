from pyspark import SparkContext
import logging
from itertools import combinations_with_replacement
from pprint import pprint
import sys

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="GroupedShuffleTrafficTest")


n = 3000
group_size = 100
payload = b"a" * (1024 * 1024)
payload = len(payload)  # 1048576

# Step 1: Predefine groups
groups = [list(range(i, i + group_size)) for i in range(1, n + 1, group_size)]

# Step 2: Create group pairs (with self-pairs included)
# Result: [ ((1, group1_100), (2, group101_200)), ((1, group1_100)), (3, group201_300)), ... ]
group_pairs = list(combinations_with_replacement(enumerate(groups, 1), 2))


# Step 3: Parallelize group pairs (one per partition)
group_pair_rdd = sc.parallelize(group_pairs, len(groups))


# Step 4: Define logic to emit all (i, j) → [i, j] from two groups
data_exchanged = sc.accumulator(0)


def emit_pairwise_records(group_pair):
    (id1, g1), (id2, g2) = group_pair
    l1 = len(g1)
    l2 = len(g2)
    
    data_exchanged.add(l1 + l2)
    for i in g1:
        for j in g2:
            yield ((i, j), 1)

# Step 5: Apply the mapping logic
result_rdd = group_pair_rdd.flatMap(emit_pairwise_records).groupByKey().mapValues(list)

# (Optional) Inspect or save result
result_rdd.take(1000)

print("DATA_EXCHANGED", data_exchanged.value)
sc.stop()
