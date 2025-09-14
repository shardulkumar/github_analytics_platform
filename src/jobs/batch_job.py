from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour


def run(spark: SparkSession, data_path: str, output_path: str):
    """
    The main batch processing job.
    Reads raw GitHub event data, transforms it into STAR schema, and writes the results to Parquet files.

    Args:
        spark (SparkSession): The SparkSession object.
        data_path (str): The path to the input raw data (JSON files).
        output_path (str): The base path to write the output Parquet files.
    """
    print(f"Starting batch job, reading data from {data_path}")

    # Read the raw JSON data
    raw_df = spark.read.json(data_path)

    # Transformations

    # 1. Create a base dataframe with flattened, aliased columns
    # We select key fields from the nested JSON structure
    events_df = raw_df.select(
        col("id").alias("event_id"),
        to_timestamp(col("created_at")).alias("created_at"),
        col("type").alias("event_type"),
        col("actor.id").alias("actor_id"),
        col("actor.login").alias("actor_login"),
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("repo_name"),
        col("payload.action").alias("payload_action"),
        col("payload.pull_request.merged").alias("is_merged"),
    )

    # 2. Create the dimension tables

    # dim_actors: information about the user performing the event
    dim_actors = events_df.select("actor_id", "actor_login").distinct()

    # dim_repositories: information about the repository
    dim_repositories = events_df.select("repo_id", "repo_name").distinct()

    # dim_dates: date and time components extracted from the event timestamp
    dim_dates = (
        events_df.select("created_at")
        .distinct()
        .withColumn("event_date", col("created_at").cast("date"))
        .withColumn("year", year(col("created_at")))
        .withColumn("month", month(col("created_at")))
        .withColumn("day", dayofmonth(col("created_at")))
        .withColumn("hour", hour(col("created_at")))
    )

    # 3. Create the fact table
    # The fact table contains the event measurements and foreign keys to the dimensions
    fact_events = events_df.select(
        "event_id", "created_at", "event_type", "actor_id", "repo_id", "is_merged"
    )

    # Output to parquet
    # Parquet is a columnar storage format, highly efficient for analytical queries.
    # Using "overwrite" mode for now.
    print(f"Writing dim_actors to: {output_path}/dim_actors")
    dim_actors.write.mode("overwrite").parquet(f"{output_path}/dim_actors")

    print(f"Writing dim_repositories to: {output_path}/dim_repositories")
    dim_repositories.write.mode("overwrite").parquet(f"{output_path}/dim_repositories")

    print(f"Writing dim_dates to: {output_path}/dim_dates")
    dim_dates.write.mode("overwrite").parquet(f"{output_path}/dim_dates")

    print(f"Writing fact_events to: {output_path}/fact_events")
    fact_events.write.mode("overwrite").parquet(f"{output_path}/fact_events")

    print("Batch job finished successfully.")
