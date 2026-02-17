from pyspark.sql import functions as F
from utils.jdbc import write_to_mysql
from utils.config import get_mysql_credentials

def run(spark, config, logger):
    logger.info("Starting Silver transformation: products")

    bronze_path = f"{config['paths']['bronze']}/products"
    silver_path = f"{config['paths']['silver']}/products"

    df = spark.read.parquet(bronze_path)

    total_count = df.count()
    logger.info(f"Bronze records read: {total_count}")

    df_clean = (
        df.dropna(subset=["product_id"])
          .filter(F.col("price") > 0)
    )

    clean_count = df_clean.count()
    logger.info(f"Valid Silver records: {clean_count}")

    (
        df_clean.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    logger.info("Completed Silver transformation: products")

    credentials = get_mysql_credentials()

    write_to_mysql(
        df_clean,
        "dim_product",
        config,
        credentials,
        mode="overwrite"
    )

    logger.info("Saved data to mysql: dim_products")