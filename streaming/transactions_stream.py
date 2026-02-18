from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType


def run(spark, config, logger):
    logger.info("Starting Structured Streaming job")

    input_path = f"{config["stream"]["input"]}"
    output_path = f"{config["stream"]["output"]}"
    checkpoint_path = f"{config["stream"]["checkpoint"]}"
    late_output_path = f"{config["stream"]["late_output"]}"
    late_checkpoint_path = f"{config["stream"]["late_checkpoint"]}"

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

    df_with_lateness = df_stream.withColumn(
        "is_late",
        F.col("transaction_date") <
        F.current_timestamp() - F.expr("INTERVAL 2 DAYS")
    )

    df_valid = df_with_lateness.filter(~F.col("is_late"))
    df_late = df_with_lateness.filter(F.col("is_late"))

    df_valid = df_valid.withWatermark("transaction_date", "2 days")

    df_deduped = df_valid.dropDuplicates(["transaction_id"])

    df_agg = (
        df_deduped
        .groupBy(
            F.window("transaction_date", "1 day"),
            F.col("customer_id")
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount")
        )
    )

    main_query = (
        df_agg.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    late_query = (
        df_late.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", late_output_path)
        .option("checkpointLocation", late_checkpoint_path)
        .start()
    )

    logger.info("Streaming query started")
    spark.streams.awaitAnyTermination()
