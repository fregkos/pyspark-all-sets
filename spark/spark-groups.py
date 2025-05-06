from pyspark import SparkContext
import logging
from itertools import combinations_with_replacement
from pprint import pprint
import sys

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="GroupedShuffleTrafficTest")


n = 3000
group_size = 100

# Step 1: Predefine groups
groups = [list(range(i, i + group_size)) for i in range(1, n + 1, group_size)]
# print(f"{groups=}")
# pprint(groups)

# Step 2: Create group pairs (with self-pairs included)
# Result: [ ((1, group1_100), (2, group101_200)), ((1, group1_100)), (3, group201_300)), ... ]
group_pairs = list(combinations_with_replacement(enumerate(groups, 1), 2))


# Step 3: Parallelize group pairs (one per partition)
group_pair_rdd = sc.parallelize(group_pairs, len(groups))
# group_pair_rdd = sc.parallelize(group_pairs, len(groups)).glom()
# pprint(group_pair_rdd.collect(), width=120)
# pprint(len(group_pair_rdd.collect()), width=120)
# sc.stop()

# Step 4: Define logic to emit all (i, j) → [i, j] from two groups
def emit_pairwise_records(group_pair):
    (id1, g1), (id2, g2) = group_pair
    # output = []
    for i in g1:
        for j in g2:
            # output.append(((i, j), [i, j]))
            yield ((i, j), "a" * 1024 ** 2)

            # yield ((i, j), [i, j])


# Step 5: Apply the mapping logic
result_rdd = group_pair_rdd.flatMap(emit_pairwise_records).groupByKey().mapValues(list)
# print(result_rdd.collect(), width=120)
# pprint(len(result_rdd.collect()), width=120)

# (Optional) Inspect or save result
print(result_rdd.take(10))
# result_rdd.collect()

sc.stop()
