from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.jdbc import write_to_mysql
from utils.config import get_mysql_credentials

def run(spark, config, logger):
    logger.info("Starting Silver transformation: customers (SCD2)")

    bronze_path = f"{config['paths']['bronze']}/customers"
    silver_path = f"{config['paths']['silver']}/customers"

    df_bronze = spark.read.parquet(bronze_path)
    df_bronze = df_bronze \
    .withColumn("signup_date", F.to_date("signup_date")) \
    .withColumn("effective_from", F.to_date("effective_from")) \
    .withColumn("effective_to", F.to_date("effective_to"))

    window = Window.partitionBy("customer_id", "effective_from").orderBy(F.col("ingestion_ts").desc())

    df_latest = (
        df_bronze
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    import os

    if not os.path.exists(silver_path):
        logger.info("Initial SCD2 load (no existing Silver)")

        df_final = df_latest

        (
            df_final.write
            .mode("overwrite")
            .parquet(silver_path)
        )

        credentials = get_mysql_credentials()

        write_to_mysql(
            df_final,
            "dim_customer",
            config,
            credentials,
            mode="overwrite"
        )

        logger.info("Saved data to mysql: dim_customer")
        logger.info("Completed initial Silver load: customers")

        return
   

    df_silver = spark.read.parquet(silver_path)

    df_joined = df_latest.alias("new").join(
        df_silver.filter(F.col("is_current") == True).alias("old"),
        on="customer_id",
        how="left"
    )

    df_changed = df_joined.filter(
        (F.col("old.customer_id").isNull()) |
        (F.col("new.name") != F.col("old.name")) |
        (F.col("new.region") != F.col("old.region"))
    )

    df_old_closed = (
        df_silver.filter(F.col("is_current") == True)
        .join(df_changed.select("customer_id"), on="customer_id")
        .withColumn("is_current", F.lit(False))
        .withColumn("effective_to", F.current_date())
    )

    df_unchanged = df_silver.join(
        df_old_closed.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )


    df_new_versions = (
        df_changed.select("new.*")
        .withColumn("is_current", F.lit(True))
        .withColumn("effective_to", F.lit(None).cast("string"))
    )

    df_final = df_unchanged.unionByName(df_old_closed).unionByName(df_new_versions)
    df_final = df_final.cache()
    df_final.count()

    (
        df_final.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    logger.info("Completed Silver transformation: customers (SCD2)")

    credentials = get_mysql_credentials()

    write_to_mysql(
        df_final,
        "dim_customer",
        config,
        credentials,
        mode="overwrite"
    )

    logger.info("Saved data to mysql: dim_customers")