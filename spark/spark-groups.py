from pyspark import SparkContext
import logging
from itertools import combinations_with_replacement

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="GroupedShuffleTrafficTest")

q = 1000  # Predefined max reducer capacity for elements
d = 5000  # our total multitude of data
group_size = int(q * 0.1)
payload = 1  # Configurable payload

# Step 1: Predefine groups
groups = [list(range(i, i + group_size)) for i in range(1, d + 1, group_size)]

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
            yield ((i, j), payload)


# Step 5: Apply the mapping logic
result_rdd = group_pair_rdd.flatMap(emit_pairwise_records)

result_rdd.foreach(lambda x: x)
print("DATA_EXCHANGED", data_exchanged.value)
sc.stop()
