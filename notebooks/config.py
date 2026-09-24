"""Shared settings for the Bronze, Silver, and Gold Databricks notebooks."""

from datetime import date

CATALOG = "movie_analytics"
RAW_SCHEMA = "raw"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
RAW_VOLUME = "source_files"

RAW_ROOT = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{RAW_VOLUME}"
BRONZE_NAMESPACE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER_NAMESPACE = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD_NAMESPACE = f"{CATALOG}.{GOLD_SCHEMA}"

IMDB_FILES = (
    "name.basics",
    "title.basics",
    "title.crew",
    "title.principals",
    "title.ratings",
)
MOVIELENS_FILES = ("links", "movies", "ratings", "tags")

MOVIELENS_SNAPSHOT_DATE = date(2026, 9, 20)
IMDB_SNAPSHOT_DATE = date(2026, 9, 20)
SNAPSHOT_DATES = {
    "movielens": MOVIELENS_SNAPSHOT_DATE,
    "imdb": IMDB_SNAPSHOT_DATE,
}

INGESTION_METADATA_COLUMNS = (
    "_source_file",
    "_source_dataset",
    "_ingested_at",
    "_source_snapshot_date",
)

IMDB_BRONZE_TABLES = [
    f"{BRONZE_NAMESPACE}.imdb_{name.replace('.', '_')}"
    for name in IMDB_FILES
]


def snapshot_predicate(snapshot_date):
    return f"_source_snapshot_date = '{snapshot_date.isoformat()}'"
