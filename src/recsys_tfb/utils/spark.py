from pyspark.sql import SparkSession


def get_or_create_spark_session(
    app_name: str = "recsys_tfb", **spark_configs
) -> SparkSession:
    """Get or create a SparkSession with the given configuration."""
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_configs.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
