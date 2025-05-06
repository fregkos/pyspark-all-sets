import os
import shutil
from pyspark import SparkContext
import logging
import sys

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="ShuffleTrafficTest")

# Test
n = 1_000
rdd = sc.parallelize(range(1, n + 1), n)


def emit_pairs(data: iter):
    result = []
    for i in data:
        for j in range(1, n + 1):
            if i != j:
                key = tuple(sorted((i, j)))
                result.append((key, i))
    return result


def emit_dummy_payload(data: iter):
    for i in data:
        for j in range(1, n + 1):
            if i != j:
                key = tuple(sorted((i, j)))
                # result.append((key, "dummy payload of size 100 MB".encode('utf-8') * 125000))``
                # yield (key, "dummy payload of size 100 MB".encode('utf-8') * 1250)
                yield (key, "a" * 1024 ** 2)

pairs_rdd = rdd.mapPartitions(emit_dummy_payload).groupByKey().mapValues(list)

# print(pairs_rdd.take(5))


# remove the whole directory first
if os.path.exists("pairs_output"):
    shutil.rmtree("pairs_output")

pairs_rdd.saveAsTextFile("pairs_output")


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
