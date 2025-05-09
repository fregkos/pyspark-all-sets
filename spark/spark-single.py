import os
import shutil
from pyspark import SparkContext
import logging
import sys

sc = SparkContext(appName="ShuffleTrafficTest")

n = 3000
rdd = sc.parallelize(range(1, n + 1), n)

data_exchanged = sc.accumulator(0)

def emit_dummy_payload(data: iter):
    for i in data:
        for j in range(1, n + 1):
            data_exchanged.add(1)
            if i != j:
                key = tuple(sorted((i, j)))
                yield (key, 1)


pairs_rdd = rdd.mapPartitions(emit_dummy_payload).groupByKey().mapValues(list)

pairs_rdd.take(10) # has no effect on result, 10 or 1000
print("DATA_EXCHANGED", data_exchanged.value)


sc.stop()
# single: 9000000 iterations
# groups:   93000 iterations
