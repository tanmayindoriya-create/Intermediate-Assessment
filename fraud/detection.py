from pyspark.sql import functions as F

def run(spark, config, logger):
    logger.info("Starting Fraud detection")
    silver_base = config["paths"]["silver"]
    gold_base = config["paths"]["gold"]

    df_txn = spark.read.parquet(f"{silver_base}/transactions")

    df_high_amount = df_txn.filter(F.col("amount") > 5000)

    df_daily_counts = (
        df_txn.groupBy("customer_id", "transaction_date")
        .agg(F.count("*").alias("daily_txn_count"))
    )

    df_suspicious_daily = df_daily_counts.filter(F.col("daily_txn_count") > 10)

    df_fraud = (
        df_high_amount.select("transaction_id", "customer_id", "amount")
        .unionByName(
            df_txn.join(
                df_suspicious_daily.select("customer_id", "transaction_date"),
                on=["customer_id", "transaction_date"],
                how="inner"
            ).select("transaction_id", "customer_id", "amount")
        )
        .dropDuplicates(["transaction_id"])
    )

    df_fraud.write.mode("overwrite").parquet(f"{gold_base}/fraud_flags")

    logger.info("Fraud detection results acquired and stored")
