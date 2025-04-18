from pyspark import SparkContext
import logging
from itertools import combinations_with_replacement

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="GroupedShuffleTrafficTest")

n = 1_000
group_size = 100

# Step 1: Predefine groups
groups = [list(range(i, i+group_size)) for i in range(1,n+1, group_size)]

# Step 2: Create group pairs (with self-pairs included)
# Result: [ ((1, group1), (1, group1)), ((1, group1), (2, group2)), ... ]
group_pairs = list(combinations_with_replacement(enumerate(groups, 1), 2))

# Step 3: Parallelize group pairs (one per partition)
group_pair_rdd = sc.parallelize(group_pairs)

# Step 4: Define logic to emit all (i, j) → [i, j] from two groups
def emit_pairwise_records(group_pair):
    (id1, g1), (id2, g2) = group_pair
    output = []
    for i in g1:
        for j in g2:
            output.append(((i,j), [i, j]))
    return output

# Step 5: Apply the mapping logic
result_rdd = group_pair_rdd.flatMap(emit_pairwise_records)


# (Optional) Inspect or save result
print(result_rdd.take(10))

sc.stop()
