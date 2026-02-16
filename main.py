import argparse

from utils.config import load_config
from utils.spark import get_spark
from utils.logger import get_logger

# Bronze imports
from spark_jobs.bronze import customers as bronze_customers
from spark_jobs.bronze import products as bronze_products
from spark_jobs.bronze import transactions as bronze_transactions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True, choices=["bronze", "silver", "gold"])
    args = parser.parse_args()

    config = load_config()
    logger = get_logger()

    spark = get_spark(
        app_name=config["spark"]["app_name"],
        master=config["spark"]["master"],
        configs=config["spark"]["configs"],
    )

    logger.info(f"Running stage: {args.stage}")

    # -------- Stage Routing --------
    if args.stage == "bronze":
        bronze_customers.run(spark, config, logger)
        bronze_products.run(spark, config, logger)
        bronze_transactions.run(spark, config, logger)

    # (silver & gold will be added later)


if __name__ == "__main__":
    main()
