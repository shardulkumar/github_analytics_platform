from pyspark.sql import SparkSession


def run(spark: SparkSession):
    """
    A sample job that reads the GH Archive data and performs basic exploration.

    Args:
        spark (SparkSession): The entry point for spark functionality.
    """
    # Define the path to your data
    data_path = "data/sample/2025-09-13-1.json.gz"

    # Read the compressed JSON data
    # Spark can read compressed files directly
    df = spark.read.json(data_path)

    # Exploration
    # 1. Print the schema to understand the data structure
    df.printSchema()

    # 2. Show a few rows of the data
    df.show(5, truncate=False)

    # 3. Count the total number of events in this hour.
    event_count = df.count()
    print(f"Total number of events in the sample: {event_count}")
