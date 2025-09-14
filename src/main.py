import argparse
from utils.spark import get_spark_session
from jobs import exploration, batch_job


def main():
    """
    Main entry point for the spark application.
    """
    # Argument parsing
    parser = argparse.ArgumentParser(description="Github Analytics Platform")
    parser.add_argument(
        "--job",
        required=True,
        choices=["exploration", "batch"],
        help="The name of the job to run.",
    )
    parser.add_argument(
        "--env",
        required=False,
        default="dev",
        choices=["dev", "prod"],
        help="The environment to run the job in.",
    )

    # Arguments for data paths
    parser.add_argument("--data-path", help="Path to the input data")
    parser.add_argument("--output-path", help="Path to write the output")

    args = parser.parse_args()

    # Spark initialization
    spark = get_spark_session(env=args.env, app_name=f"github-analytics-{args.job}")

    # Job execution
    if args.job == "exploration":
        print(f"Running '{args.job}' job in '{args.env}' environment...")
        exploration.run(spark)
    elif args.job == "batch":
        if not args.data_path or not args.output_path:
            raise ValueError(
                "--data-path and --output-path are required for the batch job"
            )
        print(f"Running '{args.job}' job in '{args.env}' environment...")
        batch_job.run(spark, args.data_path, args.output_path)
    else:
        raise ValueError(f"Job {args.job} is not a valid job.")

    # Adding an "intentional pause" to inspect Spark UI; History server to be enabled later.
    input("Press 'ENTER' to stop spark application and exit.")
    # Stop SparkSession
    spark.stop()
    print("Job finished successfully.")


if __name__ == "__main__":
    main()
