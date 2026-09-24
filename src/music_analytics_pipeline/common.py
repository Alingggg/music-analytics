from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from music_analytics_pipeline.config import snapshot_predicate


def read_snapshot(
    spark: SparkSession,
    namespace: str,
    table_name: str,
    snapshot_date: date,
) -> DataFrame:
    return spark.table(f"{namespace}.{table_name}").filter(
        F.col("_source_snapshot_date") == F.lit(snapshot_date)
    )


def write_snapshot_table(
    df: DataFrame,
    namespace: str,
    table_name: str,
    snapshot_date: date,
) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", snapshot_predicate(snapshot_date))
        .saveAsTable(f"{namespace}.{table_name}")
    )
