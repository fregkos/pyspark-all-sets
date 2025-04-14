# pyspark-all-sets

## Description

This project demonstrates how to use PySpark to perform operations on a CSV dataset. It includes reading the dataset, renaming and aliasing the DataFrames, performing a cross join, and adding a dummy test to ensure the column names remain identical.

## Requirements

- Python 3.x
- Docker Engine

## Run the Project

### Generate dataset

```bash
python src/generate_dataset.py <number>
```

### Run the Spark job through Docker

```bash
docker run --rm -v "$(pwd)":/opt/spark/work-dir/app apache/spark-py /opt/spark/bin/spark-submit app/src/main.py
```

- --rm: Removes the container after it stops
- -v `"$(pwd)":/opt/spark/work-dir/app`: Mounts your current directory to `/opt/spark/work-dir/app` in the container (`/opt/spark/work-dir/` is the default path for Spark data files)



### Run a multi-worker job
You'll need the following directory structure.
```
project-root/
├── docker-compose.yml
└── spark/
    ├── spark-script.py
```

You can configure the amount of workers (replicas) this process will spawn. Also make sure to configure their memory capacity according to your system's resources. 

**NOTE:** This uses the `bitnami/spark` image to run the master and worker processes.

Get a docker compose up and running, based on the `docker-compose.yml` configuration file.
```bash
docker compose up -d # -d to detach and use the terminal freely
```

Run the script with the master and workers.
```bash
docker exec -it spark-master spark-submit   --master spark://spark-master:7077   /opt/spark-app/spark-script.py
```

Kill the docker compose process.
```bash
docker compose down -v
```