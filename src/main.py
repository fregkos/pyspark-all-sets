from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


def main():
    # Create a Spark session
    spark = SparkSession.builder.appName("AllSets").getOrCreate()

    try:
        # Read the dataset from CSV
        df = spark.read.csv("app/dataset.csv", header=True, inferSchema=True)

        # Rename and alias the DataFrames before cross join, because it will keep the column names identical
        df1 = df.alias("df1")
        df2 = df.alias("df2")

        cross_df = df1.crossJoin(df2)

        # Dummy test
        result_df = cross_df.withColumn("is_equal", expr("df1.id = df2.id"))
        result_df.show()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
