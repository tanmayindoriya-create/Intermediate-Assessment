import argparse

# utils
from utils.config import load_config
from utils.spark import get_spark
from utils.logger import get_logger

# Bronze imports
from spark_jobs.bronze import customers as bronze_customers
from spark_jobs.bronze import products as bronze_products
from spark_jobs.bronze import transactions as bronze_transactions

# Silver imports
from spark_jobs.silver import products as silver_products
from spark_jobs.silver import transactions as silver_transactions
from spark_jobs.silver import customers as silver_customers

# warehouse imports
from warehouse import load_dimensions, load_facts

# gold imports
from spark_jobs.gold import analytics as gold_analytics

# fraud_detection import
from fraud import detection as fraud_detection

# streaming import
from streaming import transactions_stream

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True, choices=["batch","stream","test"])
    args = parser.parse_args()

    config = load_config()
    logger = get_logger()

    spark = get_spark(
        app_name=config["spark"]["app_name"],
        master=config["spark"]["master"],
        configs=config["spark"]["configs"],
    )

    logger.info(f"Running stage: {args.stage}")

    if args.stage == "batch":
        bronze_customers.run(spark, config, logger)
        bronze_products.run(spark, config, logger)
        bronze_transactions.run(spark, config, logger)
        silver_products.run(spark, config, logger)
        silver_transactions.run(spark, config, logger)
        silver_customers.run(spark, config, logger)
        load_dimensions.run(spark, config, logger)
        load_facts.run(spark, config, logger)
        gold_analytics.run(spark, config, logger)
        fraud_detection.run(spark, config, logger)
    elif args.stage == "stream":
        transactions_stream.run(spark, config["paths"], logger)
    elif args.stage == "test":
        pass

if __name__ == "__main__":
    main()
