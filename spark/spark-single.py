from pyspark import SparkContext
import logging

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="ShuffleTrafficTest")

# Test
n = 1_000
rdd = sc.parallelize(range(1,n+1))

def emit_pairs(partition_index, iterator):
    data = list(iterator)
    result = []
    for i in data:
        for j in range(1,n+1):
            if i != j:
                result.append(((i,j), i))
    return result 

pairs_rdd = rdd.mapPartitionsWithIndex(emit_pairs)

pair_lists_rdd = pairs_rdd.map(lambda kv: (kv[0], [kv[1]]))

reduced_rdd = pair_lists_rdd.reduceByKey(lambda a,b: a + b)

final_rdd = reduced_rdd.map(lambda kv: (kv[0], sorted(set(kv[1] + [kv[0][1]]))))

print(final_rdd.take(10))

# Test case 1: [key, (value)]
"""mapped1 = rdd.map(lambda x: (x % 100, x))
reduced1 = mapped1.groupByKey().mapValues(sum)

results = reduced1.collect()
logging.info("Count of reduced1: %s", len(results))
logging.info(f"Results:  {str(results)}")

# Test case 2: [key, (v1, v2, v3)]
mapped2 = rdd.map(lambda x: (x % 100, (x, x*2, x*3)))
reduced2 = mapped2.groupByKey().mapValues(lambda vals: sum(a+b+c for (a,b,c) in vals))
# reduced2.count()
results = reduced2.collect()
logging.info("Count of reduced2: %s", len(results))
logging.info(f"Results: {str(results)}")
"""
sc.stop()
