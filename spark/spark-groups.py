from pyspark import SparkContext
import logging
from itertools import combinations_with_replacement
from pprint import pprint
import sys

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="GroupedShuffleTrafficTest")

n = 1000
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
    output = []
    for i in g1:
        for j in g2:
            output.append(((i, j), [i, j]))
    return output

def emit_dummy_payload(pairwise_record):
    (id1, g1), (id2, g2), record = pairwise_record

    # Calculate the size of the current payload in bytes
    current_size = sys.getsizeof(record)
    
    # Target payload size in bytes (1 MB)
    target_size = 1024 * 1024 * 300
    
    # Calculate how much more space we need to fill
    additional_space_needed = target_size - current_size
    
    # Append dummy data until the desired size is reached
    while additional_space_needed > 0:
        record.append(0)  # Use a dummy value, e.g., 0
        current_size = sys.getsizeof(record)
        additional_space_needed -= (sys.getsizeof(record) - current_size)
    
    # (id1, g1), (id2, g2) = pairwise_record
    output = []
    for i in g1:
        for j in g2:
            output.append(((i, j), [i, j], record))

    return output


# Step 5: Apply the mapping logic
# result_rdd = group_pair_rdd.flatMap(emit_pairwise_records)
result_rdd = group_pair_rdd.flatMap(emit_dummy_payload)


# (Optional) Inspect or save result
# print(result_rdd.take(10))

sc.stop()
