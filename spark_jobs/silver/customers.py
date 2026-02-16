from pyspark.sql import functions as F
from pyspark.sql.window import Window


def run(spark, config, logger):
    logger.info("Starting Silver transformation: customers (SCD2)")

    bronze_path = f"{config['paths']['bronze']}/customers"
    silver_path = f"{config['paths']['silver']}/customers"

    df_bronze = spark.read.parquet(bronze_path)

    window = Window.partitionBy("customer_id", "effective_from").orderBy(F.col("ingestion_ts").desc())

    df_latest = (
        df_bronze
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    if not spark._jsparkSession.catalog().tableExists("default.customers_silver_temp"):
        logger.info("Initial SCD2 load (no existing Silver)")

        (
            df_latest.write
            .mode("overwrite")
            .parquet(silver_path)
        )

        logger.info("Completed initial Silver load: customers")
        return

    df_silver = spark.read.parquet(silver_path)

    join_cond = ["customer_id"]

    df_joined = df_latest.alias("new").join(
        df_silver.filter(F.col("is_current") == True).alias("old"),
        on=join_cond,
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

    (
        df_final.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    logger.info("Completed Silver transformation: customers (SCD2)")
