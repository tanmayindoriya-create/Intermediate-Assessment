from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    StringType
)


def run(spark, config, logger):
    logger.info("Starting Bronze ingestion: customers")

    raw_path = f"{config['paths']['raw']}/customers.csv"
    bronze_path = f"{config['paths']['bronze']}/customers"
    invalid_path = f"{config['paths']['bronze']}/customers_invalid"

    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("event_ts", StringType(), True),
    ])

    df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(raw_path)
    )

    # Add ingestion metadata
    df = (
        df.withColumn("ingestion_ts", F.current_timestamp())
          .withColumn("ingestion_date", F.to_date("ingestion_ts"))
          .withColumn("source_file", F.input_file_name())
    )

    # Basic structural validation (Bronze only)
    df_valid = df.filter(
        (F.col("customer_id").isNotNull()) &
        (F.col("event_ts").isNotNull())
    )

    df_invalid = df.filter(
        (F.col("customer_id").isNull()) |
        (F.col("event_ts").isNull())
    )

    counts = (
        df.withColumn(
            "is_valid",
            (F.col("customer_id").isNotNull()) &
            (F.col("event_ts").isNotNull())
        )
        .groupBy("is_valid")
        .count()
        .collect()
    )

    logger.info(f"Record breakdown: {counts}")

    # Write valid records
    (
        df_valid.write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(bronze_path)
    )

    # Write invalid separately
    (
        df_invalid.write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(invalid_path)
    )

    logger.info("Completed Bronze ingestion: customers")
