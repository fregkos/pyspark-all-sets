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
docker run --rm -it -v "$(pwd)":/opt/spark/work-dir/app apache/spark-py /opt/spark/bin/spark-submit app/src/main.py
```
