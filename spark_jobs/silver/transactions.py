from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.jdbc import write_to_mysql
from utils.config import get_mysql_credentials


def run(spark, config, logger):
    logger.info("Starting Silver transformation: transactions")

    bronze_path = f"{config['paths']['bronze']}/transactions"
    silver_path = f"{config['paths']['silver']}/transactions"

    df = spark.read.parquet(bronze_path)

    df = df.withColumn(
        "transaction_date",
        F.to_date("transaction_date")
    )

    total_count = df.count()
    logger.info(f"Bronze records read: {total_count}")

    df_valid = (
        df.dropna(subset=["transaction_id", "customer_id", "product_id"])
          .filter(F.col("amount") > 0)
          .filter(F.col("status").isin("completed", "failed"))
    )

    window = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_ts").desc())

    df_dedup = (
        df_valid
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    clean_count = df_dedup.count()
    logger.info(f"Valid Silver records after dedup: {clean_count}")

    (
        df_dedup.write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(silver_path)
    )

    logger.info("Completed Silver transformation: transactions")

    df_fact = (
        df_dedup
        .select(
            F.col("transaction_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("amount"),
            F.col("transaction_date"),
            F.col("status"),
            F.col("channel")
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


    logger.info("saved data to mysql: fact_transactions")
