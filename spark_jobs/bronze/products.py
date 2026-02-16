from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


def run(spark, config, logger):
    logger.info("Starting Bronze ingestion: products")

    raw_path = f"{config['paths']['raw']}/products.csv"
    bronze_path = f"{config['paths']['bronze']}/products"

    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
    ])

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(raw_path)
    )

    total_count = df.count()
    logger.info(f"Raw records read: {total_count}")

    df_clean = df.dropna(subset=["product_id"])

    clean_count = df_clean.count()
    dropped = total_count - clean_count

    logger.info(f"Valid records: {clean_count}")
    logger.info(f"Dropped corrupt records: {dropped}")

    df_clean = df_clean.withColumn("ingestion_ts", F.current_timestamp())

    (
        df_clean.write
        .mode("append")
        .parquet(bronze_path)
    )

    logger.info("Completed Bronze ingestion: products")
