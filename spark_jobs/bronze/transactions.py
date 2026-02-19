from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    DoubleType, StringType
)


def run(spark, config, logger):
    logger.info("Starting Bronze ingestion: transactions")

    raw_path = f"{config['paths']['raw']}/transactions.csv"
    bronze_path = f"{config['paths']['bronze']}/transactions"
    invalid_path = f"{config['paths']['bronze']}/transactions_invalid"

    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("channel", StringType(), True),
    ])

    df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(raw_path)
    )

    df = (
        df.withColumn("ingestion_ts", F.current_timestamp())
          .withColumn("ingestion_date", F.to_date("ingestion_ts"))
          .withColumn("source_file", F.input_file_name())
    )

    df_valid = df.filter(
        (F.col("transaction_id").isNotNull()) &
        (F.col("amount") > 0)
    )

    df_invalid = df.subtract(df_valid)

    counts = (
        df.withColumn(
            "is_valid",
            (F.col("transaction_id").isNotNull()) &
            (F.col("amount") > 0)
        )
        .groupBy("is_valid")
        .count()
        .collect()
    )

    logger.info(f"Record breakdown: {counts}")

    (
        df_valid.write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(bronze_path)
    )

    (
        df_invalid.write
        .mode("append")
        .partitionBy("ingestion_date")
        .parquet(invalid_path)
    )

    logger.info("Completed Bronze ingestion: transactions")
