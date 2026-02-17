from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType


def run(spark, config, logger):
    logger.info("Starting Structured Streaming job")

    input_path = f"{config["stream"]["input"]}"
    output_path = f"{config["stream"]["output"]}"
    checkpoint_path = f"{config["stream"]["checkpoint"]}"

    schema = StructType([
        StructField("transaction_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("amount", DoubleType()),
        StructField("transaction_date", StringType()),
        StructField("status", StringType()),
        StructField("channel", StringType()),
    ])

    df_stream = (
        spark.readStream
        .schema(schema)
        .option("header", True)
        .csv(input_path)
    )

    df_stream = df_stream.withColumn(
        "transaction_date",
        F.to_timestamp("transaction_date")
    )

    # Apply watermark (allow 2 days late data)
    df_watermarked = df_stream.withWatermark("transaction_date", "2 days")

    # Window aggregation (daily revenue per customer)
    df_agg = (
        df_watermarked
        .filter(F.col("status") == "completed")
        .groupBy(
            F.window("transaction_date", "1 day"),
            F.col("customer_id")
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount")
        )
    )

    query = (
        df_agg.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    logger.info("Streaming query started")
    query.awaitTermination()
