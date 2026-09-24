from datetime import datetime, timezone
from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from music_analytics_pipeline.common import read_snapshot
from music_analytics_pipeline.config import (
    GOLD_NAMESPACE,
    IMDB_SNAPSHOT_DATE,
    MOVIELENS_SNAPSHOT_DATE,
    SILVER_NAMESPACE,
)


def add_gold_metadata(
    df,
    *,
    built_at,
    include_movielens=False,
    include_imdb=False,
):
    result = df

    if include_movielens:
        result = result.withColumn(
            "_movielens_snapshot_date",
            F.lit(MOVIELENS_SNAPSHOT_DATE).cast("date"),
        )

    if include_imdb:
        result = result.withColumn(
            "_imdb_snapshot_date",
            F.lit(IMDB_SNAPSHOT_DATE).cast("date"),
        )

    return result.withColumn(
        "_gold_built_at",
        F.lit(built_at).cast("timestamp"),
    )


def safe_weighted_average(value_column, weight_column):
    weighted_sum = F.sum(F.col(value_column) * F.col(weight_column))
    total_weight = F.sum(F.col(weight_column))

    return F.when(total_weight > 0, weighted_sum / total_weight)


def write_gold_table(table_name, df):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_NAMESPACE}.{table_name}")
    )


def build_gold_models(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    add_metadata = partial(
        add_gold_metadata,
        built_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )
    ml_movie_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "movielens_movie",
        MOVIELENS_SNAPSHOT_DATE,
    )
    ml_movie_genre_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "movielens_movie_genre",
        MOVIELENS_SNAPSHOT_DATE,
    )
    ml_link_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "movielens_link",
        MOVIELENS_SNAPSHOT_DATE,
    )
    ml_rating_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "movielens_rating",
        MOVIELENS_SNAPSHOT_DATE,
    )
    ml_tag_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "movielens_tag",
        MOVIELENS_SNAPSHOT_DATE,
    )

    imdb_name_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "imdb_name",
        IMDB_SNAPSHOT_DATE,
    )
    imdb_title_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "imdb_title",
        IMDB_SNAPSHOT_DATE,
    )
    imdb_title_genre_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "imdb_title_genre",
        IMDB_SNAPSHOT_DATE,
    )
    imdb_title_crew_member_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "imdb_title_crew_member",
        IMDB_SNAPSHOT_DATE,
    )
    imdb_title_principal_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "imdb_title_principal",
        IMDB_SNAPSHOT_DATE,
    )
    imdb_title_principal_character_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "imdb_title_principal_character",
        IMDB_SNAPSHOT_DATE,
    )
    imdb_title_rating_df = read_snapshot(
        spark,
        SILVER_NAMESPACE,
        "imdb_title_rating",
        IMDB_SNAPSHOT_DATE,
    )


    movie_link_df = (
        ml_movie_df.alias("movie")
        .join(
            ml_link_df.alias("link"),
            F.col("movie.movieId") == F.col("link.movieId"),
            "left",
        )
        .select(
            F.col("movie.movieId"),
            F.col("movie.title"),
            F.col("movie.releaseYear"),
            F.when(
                F.col("link.tconst").rlike(r"^tt[0-9]+$"),
                F.substring(F.col("link.tconst"), 3, 20).cast("long"),
            ).alias("imdbId"),
            F.col("link.tconst"),
            F.col("link.tmdbId"),
        )
    )

    dim_movie_base_df = (
        movie_link_df.alias("movie")
        .join(
            imdb_title_df.alias("imdb"),
            F.col("movie.tconst") == F.col("imdb.tconst"),
            "left",
        )
        .select(
            F.col("movie.movieId"),
            F.col("movie.imdbId"),
            F.col("movie.tconst"),
            F.col("movie.tmdbId"),
            F.col("movie.title"),
            F.col("imdb.primaryTitle"),
            F.col("imdb.originalTitle"),
            F.col("movie.releaseYear"),
            F.col("imdb.startYear"),
            F.col("imdb.endYear"),
            F.coalesce(
                F.col("imdb.startYear"),
                F.col("movie.releaseYear"),
            ).alias("analysisReleaseYear"),
            F.col("imdb.runtimeMinutes"),
            F.col("imdb.titleType"),
            F.col("imdb.isAdult"),
            F.col("imdb.tconst").isNotNull().alias("isImdbLinked"),
        )
    )

    dim_movie_df = add_metadata(
        dim_movie_base_df,
        include_movielens=True,
        include_imdb=True,
    )

    movie_key_df = dim_movie_base_df.select("movieId")
    title_to_movie_df = (
        dim_movie_base_df
        .where(F.col("tconst").isNotNull())
        .select("movieId", "tconst")
        .distinct()
    )

    movielens_genre_df = (
        ml_movie_genre_df
        .join(movie_key_df, "movieId", "inner")
        .select(
            "movieId",
            F.col("genre"),
            F.lit("movielens").alias("genreSource"),
        )
    )

    imdb_genre_df = (
        imdb_title_genre_df
        .join(title_to_movie_df, "tconst", "inner")
        .select(
            "movieId",
            F.col("genre"),
            F.lit("imdb").alias("genreSource"),
        )
    )

    bridge_movie_genres_base_df = (
        movielens_genre_df
        .unionByName(imdb_genre_df)
        .filter(F.col("genre").isNotNull() & (F.trim(F.col("genre")) != ""))
        .withColumn("genre", F.trim(F.col("genre")))
        .dropDuplicates()
    )

    bridge_movie_genres_df = add_metadata(
        bridge_movie_genres_base_df,
        include_movielens=True,
        include_imdb=True,
    )

    principal_character_df = (
        imdb_title_principal_character_df
        .groupBy("tconst", "ordering")
        .agg(F.array_sort(F.collect_set("character")).alias("characters"))
    )

    principal_credit_df = (
        imdb_title_principal_df.alias("principal")
        .join(
            title_to_movie_df.alias("movie"),
            F.col("principal.tconst") == F.col("movie.tconst"),
            "inner",
        )
        .join(
            principal_character_df.alias("character"),
            (F.col("principal.tconst") == F.col("character.tconst"))
            & (F.col("principal.ordering") == F.col("character.ordering")),
            "left",
        )
        .select(
            F.col("movie.movieId"),
            F.col("principal.nconst"),
            F.lower(F.trim(F.col("principal.category"))).alias("creditRole"),
            F.col("principal.ordering"),
            F.col("principal.job"),
            F.coalesce(
                F.col("character.characters"),
                F.from_json(F.lit("[]"), "array<string>"),
            ).alias("characters"),
            F.array(F.lit("principal")).alias("creditSources"),
        )
    )

    crew_credit_df = (
        imdb_title_crew_member_df.alias("crew")
        .join(
            title_to_movie_df.alias("movie"),
            F.col("crew.tconst") == F.col("movie.tconst"),
            "inner",
        )
        .select(
            F.col("movie.movieId"),
            F.col("crew.nconst"),
            F.lower(F.trim(F.col("crew.crew_role"))).alias("creditRole"),
            F.lit(None).cast("int").alias("ordering"),
            F.lit(None).cast("string").alias("job"),
            F.from_json(F.lit("[]"), "array<string>").alias("characters"),
            F.array(F.lit("crew")).alias("creditSources"),
        )
    )

    fct_movie_person_credit_base_df = (
        principal_credit_df
        .unionByName(crew_credit_df)
        .filter(F.col("nconst").isNotNull() & F.col("creditRole").isNotNull() & (F.col("creditRole") != ""))
        .groupBy("movieId", "nconst", "creditRole")
        .agg(
            F.min("ordering").alias("ordering"),
            F.min("job").alias("job"),
            F.array_sort(F.array_distinct(F.flatten(F.collect_list("characters")))).alias("characters"),
            F.array_sort(F.array_distinct(F.flatten(F.collect_list("creditSources")))).alias("creditSources"),
        )
        .withColumn(
            "creditRoleGroup",
            F.when(
                F.col("creditRole").isin("actor", "actress"),
                F.lit("performer"),
            ).otherwise(F.col("creditRole")),
        )
    )

    fct_movie_person_credit_df = add_metadata(
        fct_movie_person_credit_base_df,
        include_movielens=True,
        include_imdb=True,
    )

    movie_release_df = dim_movie_base_df.select(
        "movieId",
        "analysisReleaseYear",
    )

    rating_movie_age_years = (
        F.year(F.col("rating.ratedAt")) - F.col("movie.analysisReleaseYear")
    )

    fct_movie_rating_df = add_metadata(
        ml_rating_df.alias("rating")
        .join(movie_release_df.alias("movie"), "movieId", "inner")
        .select(
            F.col("rating.userId"),
            F.col("rating.movieId"),
            F.col("rating.ratedAt"),
            F.date_format(F.to_date(F.col("rating.ratedAt")), "yyyyMMdd").cast("int").alias("ratingDateKey"),
            F.col("rating.rating"),
            F.when(rating_movie_age_years >= 0, rating_movie_age_years)
            .cast("int").alias("movieAgeAtRatingYears"),
        ),
        include_movielens=True,
    )

    valid_ml_tag_df = ml_tag_df.filter(
        F.col("userId").isNotNull() & F.col("taggedAt").isNotNull()
    )

    fct_movie_tag_df = add_metadata(
        valid_ml_tag_df.alias("tag")
        .join(movie_key_df, "movieId", "inner")
        .select(
            F.col("tag.userId"),
            F.col("tag.movieId"),
            F.col("tag.tag"),
            F.col("tag.taggedAt"),
            F.date_format(F.to_date(F.col("tag.taggedAt")), "yyyyMMdd").cast("int").alias("tagDateKey"),
        ),
        include_movielens=True,
    )


    rating_aggregate_df = (
        fct_movie_rating_df
        .groupBy("movieId")
        .agg(
            F.avg("rating").alias("movielensAverageRating"),
            F.count(F.lit(1)).cast("long").alias("ratingCount"),
        )
    )

    tag_aggregate_df = (
        fct_movie_tag_df
        .groupBy("movieId")
        .agg(F.count(F.lit(1)).cast("long").alias("tagCount"))
    )

    imdb_movie_rating_df = (
        title_to_movie_df
        .join(imdb_title_rating_df, "tconst", "left")
        .select(
            "movieId",
            F.col("averageRating").alias("imdbAverageRating"),
            F.col("numVotes").alias("imdbNumVotes"),
        )
    )

    mart_movie_performance_df = add_metadata(
        dim_movie_base_df
        .join(rating_aggregate_df, "movieId", "left")
        .join(tag_aggregate_df, "movieId", "left")
        .join(imdb_movie_rating_df, "movieId", "left")
        .fillna({"ratingCount": 0, "tagCount": 0})
        .withColumn("displayTitle", F.coalesce("primaryTitle", "title"))
        .select(
            "movieId",
            "displayTitle",
            "analysisReleaseYear",
            "ratingCount",
            "movielensAverageRating",
            "imdbAverageRating",
            "imdbNumVotes",
            "tagCount",
        ),
        include_movielens=True,
        include_imdb=True,
    )


    movie_genre_performance_df = (
        bridge_movie_genres_base_df
        .join(
            mart_movie_performance_df.select(
                "movieId",
                "analysisReleaseYear",
                "movielensAverageRating",
                "ratingCount",
            ),
            "movieId",
            "inner",
        )
        .filter(F.col("analysisReleaseYear").isNotNull())
    )

    mart_genre_trends_df = add_metadata(
        movie_genre_performance_df
        .groupBy("genreSource", "genre", "analysisReleaseYear")
        .agg(
            F.countDistinct("movieId").cast("long").alias("movieCount"),
            F.sum("ratingCount").cast("long").alias("ratingCount"),
            safe_weighted_average(
                "movielensAverageRating",
                "ratingCount",
            ).alias("averageRating"),
        ),
        include_movielens=True,
        include_imdb=True,
    )

    mart_user_preferences_df = add_metadata(
        fct_movie_rating_df
        .groupBy("userId")
        .agg(
            F.count(F.lit(1)).cast("long").alias("totalRatings"),
            F.avg("rating").alias("averageRatingGiven"),
            F.min("ratedAt").alias("firstRatingAt"),
            F.max("ratedAt").alias("lastRatingAt"),
        ),
        include_movielens=True,
        include_imdb=True,
    )


    credit_movie_performance_df = (
        fct_movie_person_credit_base_df
        .join(
            mart_movie_performance_df.select(
                "movieId",
                "movielensAverageRating",
                "ratingCount",
            ),
            "movieId",
            "left",
        )
    )

    mart_creator_performance_df = add_metadata(
        credit_movie_performance_df
        .groupBy("nconst", "creditRole")
        .agg(
            F.countDistinct("movieId").cast("long").alias("creditedTitleCount"),
            F.countDistinct(
                F.when(F.col("ratingCount") > 0, F.col("movieId"))
            ).cast("long").alias("ratedTitleCount"),
            F.avg("movielensAverageRating").alias("averageTitleRating"),
            F.sum("ratingCount").cast("long").alias("totalRatingCount"),
        )
        .join(
            imdb_name_df.select("nconst", "primaryName"),
            "nconst",
            "left",
        )
        .fillna({"totalRatingCount": 0})
        .select(
            "nconst",
            "primaryName",
            "creditRole",
            "creditedTitleCount",
            "ratedTitleCount",
            "averageTitleRating",
            "totalRatingCount",
        ),
        include_movielens=True,
        include_imdb=True,
    )

    return {
        "dim_movie": dim_movie_df,
        "bridge_movie_genres": bridge_movie_genres_df,
        "fct_movie_rating": fct_movie_rating_df,
        "fct_movie_tag": fct_movie_tag_df,
        "fct_movie_person_credit": fct_movie_person_credit_df,
        "mart_movie_performance": mart_movie_performance_df,
        "mart_genre_trends": mart_genre_trends_df,
        "mart_user_preferences": mart_user_preferences_df,
        "mart_creator_performance": mart_creator_performance_df,
    }


def run(spark: SparkSession) -> None:
    gold_models = build_gold_models(spark)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_NAMESPACE}")
    for table_name, model_df in gold_models.items():
        write_gold_table(table_name, model_df)


def main() -> None:
    run(SparkSession.builder.getOrCreate())
