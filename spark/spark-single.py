from pyspark import SparkContext
import logging
import time


logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="ShuffleTrafficTest")

d = 5000  # our total multitude of data


start_time = time.perf_counter()
rdd = sc.parallelize(range(1, d + 1), d)
payload = 1  # Configurable payload

data_exchanged = sc.accumulator(0)


def emit_pairwise_records(data):
    for i in data:
        for j in range(1, d + 1):
            if i != j:
                data_exchanged.add(1)
                key = tuple(sorted((i, j)))
                yield key, payload


result_rdd = rdd.mapPartitions(emit_pairwise_records)
grouped_by_reducer = result_rdd.groupByKey()
grouped_by_reducer.foreach(lambda x: x)
print("DATA_EXCHANGED", data_exchanged.value)
print(f"time taken: {time.perf_counter() - start_time}")
sc.stop()
# single: 9000000 iterations
# groups:   93000 iterations
