import argparse

from pyspark.sql import SparkSession

from music_analytics_pipeline import bronze, gold, silver


STAGES = {
    "bronze": bronze.run,
    "silver": silver.run,
    "gold": gold.run,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one movie analytics stage.")
    parser.add_argument("stage", choices=STAGES)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    STAGES[args.stage](spark)


if __name__ == "__main__":
    main()
