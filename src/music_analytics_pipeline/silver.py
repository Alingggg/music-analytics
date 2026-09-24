from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from music_analytics_pipeline.common import read_snapshot, write_snapshot_table
from music_analytics_pipeline.config import (
    BRONZE_NAMESPACE,
    IMDB_BRONZE_TABLES,
    IMDB_SNAPSHOT_DATE,
    INGESTION_METADATA_COLUMNS,
    MOVIELENS_SNAPSHOT_DATE,
    SILVER_NAMESPACE,
    SNAPSHOT_DATES,
)

def select_typed_columns(df, target_schema):
    typed_columns = [
        F.col(field.name).try_cast(field.dataType).alias(field.name)
        for field in target_schema.fields
    ]

    return df.select(
        *typed_columns,
        *INGESTION_METADATA_COLUMNS,
    )


def drop_snapshot_duplicates(df, *business_keys):
    deduplication_keys = [
        *business_keys,
        "_source_snapshot_date",
    ]

    return df.dropDuplicates(deduplication_keys)

def explode_delimited_bridge(
    df,
    parent_key,
    source_column,
    value_column,
    delimiter=",",
):
    bridge_df = (
        df
        .where(F.col(source_column).isNotNull())
        .select(
            F.col(parent_key),
            F.explode(
                F.split(F.col(source_column), delimiter)
            ).alias(value_column),
            *INGESTION_METADATA_COLUMNS,
        )
        .withColumn(value_column, F.trim(F.col(value_column)))
        .filter(
            F.col(value_column).isNotNull()
            & (F.col(value_column) != "")
        )
    )

    return drop_snapshot_duplicates(
        bridge_df,
        parent_key,
        value_column,
    )

def normalize_imdb_null_markers(df):
    normalized_columns = [
        (
            F.when(F.col(field.name) == r"\N", F.lit(None))
            .otherwise(F.col(field.name))
            .alias(field.name)
        )
        if (
            isinstance(field.dataType, StringType)
            and field.name not in INGESTION_METADATA_COLUMNS
        )
        else F.col(field.name)
        for field in df.schema.fields
    ]
    return df.select(*normalized_columns)


def imdb_tconst_column(imdb_id):
    imdb_id_text = imdb_id.cast("string")
    padded_imdb_id = F.when(
        F.length(imdb_id_text) < 7,
        F.lpad(imdb_id_text, 7, "0"),
    ).otherwise(imdb_id_text)
    return F.when(imdb_id > 0, F.concat(F.lit("tt"), padded_imdb_id))


def build_imdb_models(spark):

    imdb_null_normalized_dfs = {}

    for table_name in IMDB_BRONZE_TABLES:
        bronze_name = table_name.rsplit(".", 1)[-1]
        bronze_df = read_snapshot(
            spark,
            BRONZE_NAMESPACE,
            bronze_name,
            IMDB_SNAPSHOT_DATE,
        )
        imdb_null_normalized_dfs[bronze_name] = normalize_imdb_null_markers(bronze_df)

    imdb_silver_schemas = {
        "imdb_name_basics": StructType([
            StructField("nconst", StringType(), False),
            StructField("primaryName", StringType(), True),
            StructField("birthYear", IntegerType(), True),
            StructField("deathYear", IntegerType(), True),
            StructField("primaryProfession", StringType(), True),
            StructField("knownForTitles", StringType(), True),
        ]),
        "imdb_title_basics": StructType([
            StructField("tconst", StringType(), False),
            StructField("titleType", StringType(), False),
            StructField("primaryTitle", StringType(), False),
            StructField("originalTitle", StringType(), False),
            StructField("isAdult", BooleanType(), False),
            StructField("startYear", IntegerType(), True),
            StructField("endYear", IntegerType(), True),
            StructField("runtimeMinutes", IntegerType(), True),
            StructField("genres", StringType(), True),
        ]),
        "imdb_title_crew": StructType([
            StructField("tconst", StringType(), False),
            StructField("directors", StringType(), True),
            StructField("writers", StringType(), True),
        ]),
        "imdb_title_principals": StructType([
            StructField("tconst", StringType(), False),
            StructField("ordering", IntegerType(), False),
            StructField("nconst", StringType(), False),
            StructField("category", StringType(), False),
            StructField("job", StringType(), True),
            StructField("characters", StringType(), True),
        ]),
        "imdb_title_ratings": StructType([
            StructField("tconst", StringType(), False),
            StructField("averageRating", DoubleType(), False),
            StructField("numVotes", LongType(), False),
        ]),
    }

    imdb_typed_dfs = {}

    for table_name, normalized_df in imdb_null_normalized_dfs.items():
        imdb_typed_dfs[table_name] = select_typed_columns(
            normalized_df,
            imdb_silver_schemas[table_name],
        )

    name_basics_df = imdb_typed_dfs["imdb_name_basics"]

    imdb_name_df = name_basics_df.drop(
        "primaryProfession",
        "knownForTitles",
    )

    imdb_name_profession_df = explode_delimited_bridge(
        name_basics_df,
        parent_key="nconst",
        source_column="primaryProfession",
        value_column="profession",
    )

    imdb_name_known_for_title_df = explode_delimited_bridge(
        name_basics_df,
        parent_key="nconst",
        source_column="knownForTitles",
        value_column="tconst",
    )

    title_basics_df = imdb_typed_dfs["imdb_title_basics"]

    imdb_title_df = title_basics_df.drop("genres")

    imdb_title_genre_df = explode_delimited_bridge(
        title_basics_df,
        parent_key="tconst",
        source_column="genres",
        value_column="genre",
    )

    title_crew_df = imdb_typed_dfs["imdb_title_crew"]

    director_df = (
        explode_delimited_bridge(
            title_crew_df,
            parent_key="tconst",
            source_column="directors",
            value_column="nconst",
        )
        .withColumn("crew_role", F.lit("director"))
    )

    writer_df = (
        explode_delimited_bridge(
            title_crew_df,
            parent_key="tconst",
            source_column="writers",
            value_column="nconst",
        )
        .withColumn("crew_role", F.lit("writer"))
    )

    imdb_title_crew_member_df = (
        director_df
        .unionByName(writer_df)
        .select(
            "tconst",
            "nconst",
            "crew_role",
            *INGESTION_METADATA_COLUMNS,
        )
    )

    title_principals_df = imdb_typed_dfs["imdb_title_principals"]
    imdb_title_principal_df = title_principals_df.drop("characters")

    imdb_title_principal_character_df = (
        title_principals_df
        .where(F.col("characters").isNotNull())
        .withColumn(
            "_parsed_characters",
            F.from_json(
                F.col("characters"),
                ArrayType(StringType()),
            ),
        )
        .select(
            "tconst",
            "ordering",
            F.explode("_parsed_characters").alias("character"),
            *INGESTION_METADATA_COLUMNS,
        )
        .withColumn("character", F.trim(F.col("character")))
        .filter(
            F.col("character").isNotNull()
            & (F.col("character") != "")
        )
    )

    imdb_title_principal_character_df = drop_snapshot_duplicates(
        imdb_title_principal_character_df,
        "tconst",
        "ordering",
        "character",
    )

    imdb_name_df = drop_snapshot_duplicates(imdb_name_df, "nconst")
    imdb_title_df = drop_snapshot_duplicates(imdb_title_df, "tconst")
    imdb_title_crew_member_df = drop_snapshot_duplicates(
        imdb_title_crew_member_df,
        "tconst",
        "nconst",
        "crew_role",
    )
    imdb_title_principal_df = drop_snapshot_duplicates(
        imdb_title_principal_df,
        "tconst",
        "ordering",
    )
    imdb_title_rating_df = drop_snapshot_duplicates(
        imdb_typed_dfs["imdb_title_ratings"],
        "tconst",
    )

    imdb_silver_dfs = {
        "imdb_name": imdb_name_df,
        "imdb_name_profession": imdb_name_profession_df,
        "imdb_name_known_for_title": imdb_name_known_for_title_df,
        "imdb_title": imdb_title_df,
        "imdb_title_genre": imdb_title_genre_df,
        "imdb_title_crew_member": imdb_title_crew_member_df,
        "imdb_title_principal": imdb_title_principal_df,
        "imdb_title_principal_character": (
            imdb_title_principal_character_df
        ),
        "imdb_title_rating": imdb_title_rating_df,
    }

    return imdb_silver_dfs


def build_movielens_models(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    movielens_silver_schemas = {
        "movielens_links": StructType([
            StructField("movieId", LongType(), False),
            StructField("imdbId", LongType(), True),
            StructField("tmdbId", LongType(), True),
        ]),
        "movielens_movies": StructType([
            StructField("movieId", LongType(), False),
            StructField("title", StringType(), False),
            StructField("genres", StringType(), True),
        ]),
        "movielens_ratings": StructType([
            StructField("userId", LongType(), False),
            StructField("movieId", LongType(), False),
            StructField("rating", DoubleType(), False),
            StructField("timestamp", LongType(), False),
        ]),
        "movielens_tags": StructType([
            StructField("userId", LongType(), False),
            StructField("movieId", LongType(), False),
            StructField("tag", StringType(), False),
            StructField("timestamp", LongType(), False),
        ]),
    }

    movielens_typed_dfs = {}

    for table_name, target_schema in movielens_silver_schemas.items():
        bronze_df = read_snapshot(spark, BRONZE_NAMESPACE, table_name, MOVIELENS_SNAPSHOT_DATE)

        movielens_typed_dfs[table_name] = select_typed_columns(
            bronze_df,
            target_schema,
        )

    movies_df = movielens_typed_dfs["movielens_movies"]
    release_year_text = F.regexp_extract(
        F.col("title"),
        r"\((\d{4})\)$",
        1,
    )

    movielens_movie_df = (
        movies_df
        .withColumn("title", F.trim(F.col("title")))
        .withColumn(
            "releaseYear",
            F.when(release_year_text != "", release_year_text.cast("int")),
        )
        .drop("genres")
    )
    movielens_movie_df = drop_snapshot_duplicates(
        movielens_movie_df,
        "movieId",
    )

    movielens_movie_genre_df = explode_delimited_bridge(
        movies_df.where(
            F.col("genres").isNotNull()
            & (F.col("genres") != "(no genres listed)")
        ),
        parent_key="movieId",
        source_column="genres",
        value_column="genre",
        delimiter=r"\|",
    )

    links_df = movielens_typed_dfs["movielens_links"]
    movielens_link_df = (
        links_df
        .withColumn(
            "tconst",
            imdb_tconst_column(F.col("imdbId")),
        )
        .select(
            "movieId",
            "tconst",
            "tmdbId",
            *INGESTION_METADATA_COLUMNS,
        )
    )
    movielens_link_df = drop_snapshot_duplicates(
        movielens_link_df,
        "movieId",
    )

    movielens_rating_df = (
        movielens_typed_dfs["movielens_ratings"]
        .withColumn(
            "ratedAt",
            F.to_timestamp(F.from_unixtime(F.col("timestamp"))),
        )
        .select(
            "userId",
            "movieId",
            "rating",
            "ratedAt",
            *INGESTION_METADATA_COLUMNS,
        )
    )
    movielens_rating_df = drop_snapshot_duplicates(
        movielens_rating_df,
        "userId",
        "movieId",
        "ratedAt",
    )

    movielens_tag_df = (
        movielens_typed_dfs["movielens_tags"]
        .withColumn("tag", F.trim(F.col("tag")))
        .withColumn(
            "taggedAt",
            F.to_timestamp(F.from_unixtime(F.col("timestamp"))),
        )
        .select(
            "userId",
            "movieId",
            "tag",
            "taggedAt",
            *INGESTION_METADATA_COLUMNS,
        )
    )
    movielens_tag_df = drop_snapshot_duplicates(
        movielens_tag_df,
        "userId",
        "movieId",
        "tag",
        "taggedAt",
    )

    movielens_silver_dfs = {
        "movielens_movie": movielens_movie_df,
        "movielens_movie_genre": movielens_movie_genre_df,
        "movielens_link": movielens_link_df,
        "movielens_rating": movielens_rating_df,
        "movielens_tag": movielens_tag_df,
    }

    return movielens_silver_dfs


def run(spark: SparkSession) -> None:
    imdb_silver_dfs = build_imdb_models(spark)
    movielens_silver_dfs = build_movielens_models(spark)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_NAMESPACE}")
    silver_dfs = {**imdb_silver_dfs, **movielens_silver_dfs}
    for table_name, silver_df in silver_dfs.items():
        snapshot_date = SNAPSHOT_DATES[table_name.split("_", 1)[0]]
        silver_snapshot_df = silver_df.filter(
            F.col("_source_snapshot_date") == F.lit(snapshot_date)
        )
        write_snapshot_table(
            silver_snapshot_df,
            SILVER_NAMESPACE,
            table_name,
            snapshot_date,
        )
