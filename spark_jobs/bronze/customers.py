from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType


def run(spark, config, logger):
    logger.info("Starting Bronze ingestion: customers")

    raw_path = f"{config['paths']['raw']}/customers.csv"
    bronze_path = f"{config['paths']['bronze']}/customers"

    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("effective_from", StringType(), True),
        StructField("effective_to", StringType(), True),
    ])

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(raw_path)
    )

    total_count = df.count()
    logger.info(f"Raw records read: {total_count}")

    df_clean = df.dropna(subset=["customer_id"])

    clean_count = df_clean.count()
    dropped = total_count - clean_count

    logger.info(f"Valid records: {clean_count}")
    logger.info(f"Dropped corrupt records: {dropped}")

    df_clean = df_clean.withColumn("ingestion_ts", F.current_timestamp())

    (
        df_clean.write
        .mode("append")
        .partitionBy("signup_date")
        .parquet(bronze_path)
    )

    logger.info("Completed Bronze ingestion: customers")
