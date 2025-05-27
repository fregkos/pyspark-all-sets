#!/bin/bash
docker exec -it spark-master pip install numpy
docker exec -it spark-master spark-submit   --master spark://spark-master:7077   /opt/spark-app/spark-optimal-groups.py