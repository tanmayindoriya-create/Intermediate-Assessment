from pyspark.sql import functions as F
from pyspark.sql.window import Window


def run(spark, config, logger):
    logger.info("Starting Silver transformation: customers (SCD2)")

    bronze_path = f"{config['paths']['bronze']}/customers"
    silver_path = f"{config['paths']['silver']}/customers"

    df = spark.read.parquet(bronze_path)

    # Type normalization
    df = (
        df.withColumn("signup_date", F.to_date("signup_date"))
          .withColumn("event_ts", F.to_timestamp("event_ts"))
    )

    # Remove exact duplicates
    df = df.dropDuplicates(["customer_id", "event_ts", "region"])

    # Order events per customer
    base_window = Window.partitionBy("customer_id").orderBy("event_ts")

    # Detect change from previous region
    df = df.withColumn(
        "prev_region",
        F.lag("region").over(base_window)
    )

    df = df.filter(
        (F.col("prev_region").isNull()) |
        (F.col("region") != F.col("prev_region"))
    ).drop("prev_region")

    window_fill = base_window.rowsBetween(Window.unboundedPreceding, 0)

    df = df.withColumn(
        "signup_date",
        F.last("signup_date", ignorenulls=True).over(window_fill)
    )

    # Compute SCD columns
    df = df.withColumn(
        "effective_from",
        F.col("event_ts")
    )

    df = df.withColumn(
        "effective_to",
        F.lead("event_ts").over(base_window)
    )

    df = df.withColumn(
        "is_current",
        F.when(F.col("effective_to").isNull(), True)
         .otherwise(False)
    )

    # Select final columns
    df_final = df.select(
        "customer_id",
        "name",
        "region",
        "signup_date",
        "effective_from",
        "effective_to",
        "is_current"
    )

    (
        df_final.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    logger.info("Completed Silver transformation: customers (SCD2)")
