import argparse
from utils.config import load_config
from utils.spark import get_spark
from utils.logger import get_logger


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

    # Stage routing will come next


if __name__ == "__main__":
    main()
