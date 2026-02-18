from utils.jdbc import write_to_mysql
from utils.config import get_mysql_credentials

def run(spark, config, logger):
    logger.info("Loading fact table into MySQL")

    silver_base = config["paths"]["silver"]

    df_transactions = spark.read.parquet(f"{silver_base}/transactions")

    df_fact = (
        df_transactions
        .select(
            "transaction_id",
            "customer_id",
            "product_id",
            "amount",
            "transaction_date",
            "status",
            "channel"
        )
    )

    credentials = get_mysql_credentials()

    write_to_mysql(
        df_fact,
        "fact_transactions",
        config,
        credentials,
        mode="overwrite"
    )

    logger.info("Fact table loaded successfully")
