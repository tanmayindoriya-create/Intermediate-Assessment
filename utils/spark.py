from pyspark.sql import SparkSession

_spark = None

def get_spark(app_name: str, master: str, configs: dict) -> SparkSession:
    global _spark

    if _spark is None:
        builder = SparkSession.builder.appName(app_name).master(master)
        builder = builder.config(
            "spark.jars",
            "jars/mysql-connector-j-8.3.0.jar"
        )

        for k, v in configs.items():
            builder = builder.config(k, v)

        _spark = builder.getOrCreate()

    return _spark
