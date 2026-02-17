from pyspark.sql import functions as F
from pyspark.sql.window import Window


def run(spark, config, logger):
    logger.info("Starting Gold layer analytics")

    silver_base = config["paths"]["silver"]
    gold_base = config["paths"]["gold"]

    df_txn = spark.read.parquet(f"{silver_base}/transactions")
    df_products = spark.read.parquet(f"{silver_base}/products")
    df_customers = spark.read.parquet(f"{silver_base}/customers")


    # monthly revenue
    df_monthly_revenue = (
        df_txn
        .filter(F.col("status") == "completed")
        .withColumn("year_month", F.date_format("transaction_date", "yyyy-MM"))
        .groupBy("year_month")
        .agg(F.sum("amount").alias("total_revenue"))
    )

    df_monthly_revenue.write.mode("overwrite").parquet(f"{gold_base}/monthly_revenue")

    # top products
    df_top_products = (
        df_txn.filter(F.col("status") == "completed")
        .groupBy("product_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_sales")
        )
        .join(df_products, on="product_id", how="left")
        .orderBy(F.desc("total_sales"))
    )

    df_top_products.write.mode("overwrite").parquet(f"{gold_base}/top_products")

    # current customers
    df_current_customers = df_customers.filter(F.col("is_current") == True)

    # regional performance
    df_regional = (
        df_txn.filter(F.col("status") == "completed")
        .join(df_current_customers, on="customer_id", how="left")
        .groupBy("region")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("regional_revenue")
        )
        .orderBy(F.desc("regional_revenue"))
    )

    df_regional.write.mode("overwrite").parquet(f"{gold_base}/regional_performance")

    # customer retention
    df_customer_activity = (
        df_txn.filter(F.col("status") == "completed")
        .groupBy("customer_id")
        .agg(F.count("*").alias("txn_count"))
        .withColumn("is_repeat_customer", F.col("txn_count") > 1)
    )

    df_customer_activity.write.mode("overwrite").parquet(f"{gold_base}/customer_retention")

    logger.info("Completed Gold layer analytics")
