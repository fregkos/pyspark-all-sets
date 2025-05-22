from pyspark import SparkContext
import logging
from itertools import combinations_with_replacement
from pprint import pprint
from pyspark.accumulators import AccumulatorParam

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="GroupedShuffleTrafficTest")

d = 5000 # our total multitude of data
q = 1000 # our max reducer capacity for elements

# we have established the value of `p` to be
p = d // q  # p is 5 in this case, a prime number
# such that p^2 divides d AND q=d/p :)

g = p**2  # number of groups
items_per_group = d // p**2  # the number of items per group
# since we need many of the p*p groups to exist in the reducers, we shall
# split them in the following manner
# (
#     [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15], etc],
#     [[], [], []],
#     [[], [], []],
# )

# Step 1: Predefine groups
groups = []
for group_counter in range(d // q):  # for each supergroup
    supergroup = []
    for subgroup_counter in range(q // items_per_group):  # for each subgroup
        subgroup = []
        for quantum in range(items_per_group):  # for each quantum of the subgroup
            subgroup.append(
                quantum + subgroup_counter * items_per_group + group_counter * q
            )  # plus some counter
        supergroup.append(subgroup)
    groups.append(supergroup)

# groups = [list(range(i, i + group_size)) for i in range(1, d + 1, group_size)]

# Step 2: Create group pairs (with self-pairs included)
group_pairs = list(combinations_with_replacement(enumerate(groups, 1), 2))
# ------- this step creates combinations of the SUPER groups
# ------- meaning, the [[],[],[],[]], not the individual inner groups [], [],
# ------- neither the quanta


# Step 3: Parallelize group pairs (one per partition)
group_pair_rdd = sc.parallelize(group_pairs, len(groups))


class SetAccumulator(AccumulatorParam):
    def zero(self, value):
        return set()

    def addInPlace(self, v1, v2):
        v1.update(v2)
        return v1


# Step 4: Define logic to emit all (i, j) → [i, j] from two groups
data_exchanged = sc.accumulator(0)
sc.setLogLevel("INFO")
records = sc.accumulator(set(), SetAccumulator())

def emit_pairwise_records(group_pair):
    (id1, supergroup1), (id2, supergroup2) = group_pair
    # records = []

    data_exchanged.add(len(supergroup1)*len(supergroup1[0]) * 2)
    # For each subgroup in first supergroup
    for subgroup1 in supergroup1:
        # For each subgroup in second supergroup
        for subgroup2 in supergroup2:
            # Compare elements across subgroups
            for i in subgroup1:
                for j in subgroup2:
                    records.add([(i,j)])
            #         if i != j:
            #             # records.append(((i, j), i))
            #             data_exchanged.add(1)
                    yield ((i, j), 1)


# Step 5: Apply the mapping logic
result_rdd = group_pair_rdd.flatMap(emit_pairwise_records)# .groupByKey().mapValues(list)
result_rdd.foreach(lambda x: x)
print("DATA_EXCHANGED", data_exchanged.value)
# pprint(records.value)
sc.stop()
