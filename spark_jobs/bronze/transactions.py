from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType
)


def run(spark, config, logger):
    logger.info("Starting Bronze ingestion: transactions")

    raw_path = f"{config['paths']['raw']}/transactions.csv"
    bronze_path = f"{config['paths']['bronze']}/transactions"

    schema = StructType([
        StructField("transaction_id", IntegerType(), False),
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
        .schema(schema)
        .csv(raw_path)
    )

    total_count = df.count()
    logger.info(f"Raw records read: {total_count}")

    # Drop rows missing primary key
    df_clean = df.dropna(subset=["transaction_id"])

    clean_count = df_clean.count()
    dropped = total_count - clean_count

    logger.info(f"Valid records: {clean_count}")
    logger.info(f"Dropped corrupt records: {dropped}")

    df_clean = df_clean.withColumn("ingestion_ts", F.current_timestamp())

    # Partition by transaction_date (important for analytics & fraud windows)
    (
        df_clean.write
        .mode("append")
        .partitionBy("transaction_date")
        .parquet(bronze_path)
    )

    logger.info("Completed Bronze ingestion: transactions")
