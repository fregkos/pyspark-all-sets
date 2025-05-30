from pyspark import SparkContext
import logging
from itertools import combinations_with_replacement
from pyspark.accumulators import AccumulatorParam
import time
import numpy as np

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="OptimalGroupedShuffling")

q = 1000  # Predefined max reducer capacity for elements
d = 5000  # our total multitude of data

# we have established the value of `p` to be
p = d // q  # p is 5 in this case, a prime number
# such that p^2 divides d AND q=d/p :)

g = p**2  # number of groups, 25
items_per_group = d // p**2  # the number of items per group, 200
# since we need many of the p*p groups to exist in the reducers, we shall
# split them in the following manner
# (
#     [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15], etc],
#     [[], [], []],
#     [[], [], []],
# )

# Data preparation
data = np.arange(0, d).reshape(p,p,items_per_group) # shape: (5,5,200)


start_time = time.perf_counter()
# Create the RDD in the following manner ((i,j), array_200) 
# where each element of the array_200 has a {k,v}: {index, 1}
rdd = sc.parallelize([((i,j), [(int(x),1) for x in data[i][j]] ) for i in range(p) for j in range(p)])

data_exchanged = sc.accumulator(0)
def assign_to_reducers(record):
    (i,j), element_pairs = record
    outputs = []

    # Count the data exchange: p+1 teams of len(pair of elements) each
    data_exchanged.add((p+1)*len(element_pairs))

    for k in range(p): # team first p teams k-indexed, where 0 <= k < p 
        reducer_id = (k, (i+k*j)%p)
        for kv in element_pairs:
            outputs.append((reducer_id, kv))
            # data_exchanged.add(1) # normally, this would be put here

    # deal with team p now!
    reducer_id = (p,j)
    for kv in element_pairs:
        outputs.append((reducer_id, kv))
        # data_exchanged.add(1) # and here
    return outputs

# map each gruop to its assigned reducers
rdd_mapped = rdd.flatMap(assign_to_reducers)

# group by reducer ID to shuffle data (simulation)
grouped_by_reducer = rdd_mapped.groupByKey()


# collect  data
result = grouped_by_reducer.collect()

print("DATA_EXCHANGED", data_exchanged.value)
print(f"time taken: {time.perf_counter() - start_time}")

for reducer_id, values in result:
    print(f"Reducer {reducer_id} received {len(values)} items")

sc.stop()
