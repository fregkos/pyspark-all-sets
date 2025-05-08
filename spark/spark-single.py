import os
import shutil
from pyspark import SparkContext
import logging
import sys

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="ShuffleTrafficTest")

n = 3000
rdd = sc.parallelize(range(1, n + 1), n)

# n = 3000
# data_exchanged = 18_868_814_298_000

data_exchanged = sc.accumulator(0)

payload = b"a" * (1024 * 1024)
payload = len(payload)  # 1048576

def emit_pairs(data: iter):
    result = []
    data_exchanged.add(1) # one for i, one for j
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
                data_exchanged.add(1)  # one for i, one for j
                # is this valid? should it be 1 maybe ?
                key = tuple(sorted((i, j)))
                yield (key, 1)


pairs_rdd = rdd.mapPartitions(emit_dummy_payload).groupByKey().mapValues(list)

pairs_rdd.take(1000)
print("DATA_EXCHANGED", data_exchanged.value)


sc.stop()
# single: 8997000 iterations
# groups:   93000 iterations
