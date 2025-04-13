from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext("local", "AllPairsMapReduce")

# Step 1: Create RDD
ids = list(range(1, 21))  # [1, 2 , ..., 20]
rdd = sc.parallelize(ids)

broadcast_ids = sc.broadcast(ids)


def emit_pairs(id1):
    return [((id1, id2), hash((id1, id2))) for id2 in broadcast_ids.value if id2 >= id1]


pairs_rdd = rdd.flatMap(emit_pairs)

even_pairs = pairs_rdd.filter(lambda kv: kv[1] % 2 == 0)  # keep only even nums

print("Even hash pairs")
for pair in even_pairs.map(lambda kv: kv[0]).collect():
    print(pair)

# result_ids = even_pairs.flatMap(lambda kv: [kv[0][0], kv[0][1]]).distinct().sorted()

# print(result_ids.collect())
