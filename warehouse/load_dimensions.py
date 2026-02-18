from utils.jdbc import write_to_mysql
from utils.config import get_mysql_credentials

def run(spark, config, logger):
    logger.info("Loading dimensions into MySQL")

    silver_base = config["paths"]["silver"]

    df_customers = spark.read.parquet(f"{silver_base}/customers")
    df_products = spark.read.parquet(f"{silver_base}/products")

    credentials = get_mysql_credentials()

    write_to_mysql(
        df_customers,
        "dim_customer",
        config,
        credentials,
        mode="overwrite"
    )

    write_to_mysql(
        df_products,
        "dim_product",
        config,
        credentials,
        mode="overwrite"
    )

    logger.info("Dimensions loaded successfully")
