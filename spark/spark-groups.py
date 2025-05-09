from pyspark import SparkContext
import logging
from itertools import combinations_with_replacement
from pprint import pprint
import sys
import time
import requests 

logging.basicConfig(level=logging.INFO)
sc = SparkContext(appName="GroupedShuffleTrafficTest")


n = 3000
group_size = 100
payload = b"a" * (1024 * 1024)
payload = len(payload)  # 1048576

# Step 1: Predefine groups
groups = [list(range(i, i + group_size)) for i in range(1, n + 1, group_size)]

# Step 2: Create group pairs (with self-pairs included)
# Result: [ ((1, group1_100), (2, group101_200)), ((1, group1_100)), (3, group201_300)), ... ]
group_pairs = list(combinations_with_replacement(enumerate(groups, 1), 2))


# Step 3: Parallelize group pairs (one per partition)
group_pair_rdd = sc.parallelize(group_pairs, len(groups))


# Step 4: Define logic to emit all (i, j) → [i, j] from two groups
data_exchanged = sc.accumulator(0)
sc.setLogLevel("INFO")


def emit_pairwise_records(group_pair):
    (id1, g1), (id2, g2) = group_pair
    l1 = len(g1)
    l2 = len(g2)

    data_exchanged.add(l1 + l2)
    for i in g1:
        for j in g2:
            if i != j:
                yield ((i, j), payload)


# Step 5: Apply the mapping logic
result_rdd = group_pair_rdd.flatMap(emit_pairwise_records).groupByKey().mapValues(list)

result_rdd.take(10)  # has no effect on result, 10 or 1000
print("DATA_EXCHANGED", data_exchanged.value)

# Add this one line to get shuffle metrics from the Spark UI API
print(
    "SHUFFLE_DATA",
    requests.get("http://localhost:4040/api/v1/applications").json()[0]["id"],
)
try:
    app_id = requests.get("http://localhost:4040/api/v1/applications").json()[0][
        "id"
    ]
    stages = requests.get(
        f"http://localhost:4040/api/v1/applications/{app_id}/stages"
    ).json()
    total_shuffle_read = sum(
        stage.get("shuffleReadMetrics", {}).get("totalBytesRead", 0)
        for stage in stages
    )
    total_shuffle_write = sum(
        stage.get("shuffleWriteMetrics", {}).get("bytesWritten", 0)
        for stage in stages
    )
    print(
        f"Total Shuffle Read: {total_shuffle_read/(1024*1024):.2f} MB, Write: {total_shuffle_write/(1024*1024):.2f} MB"
    )
except:
    print('Passed exception')
inp = input()
while inp != "q":
    inp = input()
sc.stop()
