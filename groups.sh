docker exec -it spark-master pip install requests
docker exec -it spark-master spark-submit   --master spark://spark-master:7077   /opt/spark-app/spark-groups.py