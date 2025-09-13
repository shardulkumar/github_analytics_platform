import argparse
from utils.spark import get_spark_session
from jobs import exploration


def main():
    """
    Main entry point for the spark application.
    """
    # Argument parsing
    parser = argparse.ArgumentParser(description="Github Analytics Platform")
    parser.add_argument(
        "--job",
        required=True,
        choices=["exploration"],
        help="The name of the job to run."
    )
    parser.add_argument(
        "--env",
        required=False,
        default="dev",
        choices=["dev", "prod"],
        help="The environment to run the job in."
    )
    args = parser.parse_args()

    # Spark initialization
    spark = get_spark_session(env=args.env, app_name=f"github-analyics-{args.job}")

    # Job execution
    if args.job == "exploration":
        print(f"Running '{args.job}' job in '{args.env}' environment...")
        exploration.run(spark)
    else:
        raise ValueError(f"Job {args.job} is not a valid job.")
    # Stop SparkSession
    spark.stop()
    print("Job finished successfully.")


if __name__ == "__main__":
    main()
