from pyspark.sql import SparkSession
import yaml


def get_spark_session(env: str, app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession configured for the given environment.

    This function reads configuration settings from YAML files corresponding
    to a 'base' configuration and a specific environment ('dev' or 'prod').
    It constructs a SparkSession with these settings.

    Args:
        env (str): The runtime environment (e.g., 'dev', 'prod').
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: A configured SparkSession object.
    """
    # Load base and environment-specific configurations
    with open('configs/base.yml', 'r') as f:
        base_config = yaml.safe_load(f)

    with open(f'configs/{env}.yml', 'r') as f:
        env_config = yaml.safe_load(f)

    # Merge configurations, with environment-specific settings overriding base settings
    spark_config = {**base_config.get('spark', {}), **env_config.get('spark', {})}

    # Create the SparkSession builder
    spark_builder = SparkSession.builder.appName(app_name)

    # Apply configurations to the builder
    for key, value in spark_config.items():
        spark_builder.config(key, value)

    # Create (or get) the SparkSession
    spark_session = spark_builder.getOrCreate()

    return spark_session
