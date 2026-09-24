from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from music_analytics_pipeline.common import write_snapshot_table
from music_analytics_pipeline.config import (
    BRONZE_NAMESPACE,
    IMDB_FILES,
    MOVIELENS_FILES,
    RAW_ROOT,
    SNAPSHOT_DATES,
)


def load_bronze_source(
    spark: SparkSession,
    dataset: str,
    name: str,
    snapshot_date: date,
) -> DataFrame:
    if dataset == "imdb":
        path = f"{RAW_ROOT}/imdb/{name}.tsv.gz"
        reader = spark.read.option("header", True).option("sep", "\t")
        source_dataset = "imdb"
    elif dataset == "movielens":
        path = f"{RAW_ROOT}/movielens/{name}.csv"
        reader = spark.read.option("header", True)
        source_dataset = "movielens_32m"
    else:
        raise ValueError(f"Unsupported source dataset: {dataset}")

    return (
        reader.csv(path)
        .withColumn("_source_file", F.lit(path))
        .withColumn("_source_dataset", F.lit(source_dataset))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_snapshot_date", F.lit(snapshot_date))
    )


def run(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_NAMESPACE}")

    for dataset, names in (("movielens", MOVIELENS_FILES), ("imdb", IMDB_FILES)):
        snapshot_date = SNAPSHOT_DATES[dataset]
        for name in names:
            table_name = f"{dataset}_{name.replace('.', '_')}"
            source_df = load_bronze_source(spark, dataset, name, snapshot_date)
            write_snapshot_table(
                source_df,
                BRONZE_NAMESPACE,
                table_name,
                snapshot_date,
            )


def main() -> None:
    run(SparkSession.builder.getOrCreate())
