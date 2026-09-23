# Movie Audience & Film Analytics Platform

**Project type:** Data Engineering + Analytics portfolio project  
**Primary datasets:** MovieLens 32M + IMDb Non-Commercial Datasets  
**Version:** 2026-09-24

---

## 1. Project Goal

Build a reproducible analytics platform that combines large-scale user rating behavior from MovieLens with film, cast, crew, and title metadata from IMDb.

The project should demonstrate more than moving CSV files into a database. It should show:

- ingestion of multiple bulk datasets with different schemas and refresh behavior;
- reliable ID-based integration between independent data sources;
- data cleaning and quality validation;
- dimensional / analytical modeling;
- incremental handling of a source that refreshes regularly;
- efficient analytical querying;
- reusable metrics and marts;
- several meaningful movie-industry and audience-behavior case studies.

The final product is an **analytics-ready movie lakehouse in Databricks** that supports both audience analysis and film-industry analysis.

---

## 2. Why This Dataset Combination

### MovieLens 32M

MovieLens 32M is a stable benchmark dataset containing:

- 32,000,204 ratings;
- 2,000,072 tag applications;
- 87,585 movies;
- 200,948 anonymized users;
- activity from 1995 through October 2023.

Files:

- `ratings.csv`
- `tags.csv`
- `movies.csv`
- `links.csv`

The important part is `links.csv`, which provides IMDb IDs and allows MovieLens movies to be joined to IMDb metadata without fuzzy title matching.

### IMDb Non-Commercial Datasets

IMDb provides bulk gzipped TSV files refreshed daily. Useful files include:

- `title.basics.tsv.gz`
- `title.ratings.tsv.gz`
- `title.crew.tsv.gz`
- `title.principals.tsv.gz`
- `name.basics.tsv.gz`
- optionally `title.akas.tsv.gz`
- optionally `title.episode.tsv.gz`

This adds richer metadata such as:

- title type;
- release year;
- runtime;
- genres;
- IMDb rating and vote count;
- directors and writers;
- principal cast/crew;
- person names and professions.

---

## 3. Core Portfolio Story

> Build a movie analytics warehouse that integrates audience behavior with industry metadata, then use it to study how popularity, ratings, genres, creators, and audience tastes evolve over time.

This gives the project two distinct analytical sides:

1. **Audience analytics** — what people rate, how tastes differ, how ratings evolve.
2. **Film-industry analytics** — genres, creators, collaborations, longevity, and performance.

---

## 4. Recommended Scope

### Required scope

Use:

- MovieLens 32M;
- IMDb `title.basics`;
- IMDb `title.ratings`;
- IMDb `title.crew`;
- IMDb `title.principals`;
- IMDb `name.basics`.

### Optional later additions

Add only if they support a specific analysis:

- `title.akas` for alternative/localized titles;
- `title.episode` for TV-series analysis;
- MovieLens Tag Genome for semantic/tag-based analysis.

Do not add TMDB, OMDb, or another API just to make the source list longer. The core project is already rich enough.

---

## 5. Suggested Technology Stack

A practical stack for this project:

- **Databricks** — workspace for processing, storage, SQL, and notebooks;
- **Unity Catalog** — catalogs, schemas, managed tables, and governance;
- **Volumes** — retained storage for downloaded source files before ingestion;
- **Delta Lake** — Bronze, Silver, and Gold tables;
- **PySpark / Spark SQL** — ingestion, cleaning, transformation, joins, and analytical modeling;
- **Notebooks** — development, exploration, validation, and documentation;
- **Workflows / Jobs** — optional orchestration after the individual pipeline steps work correctly.

### Optional later tools

- **dbt** — optional if the project later benefits from dbt-managed SQL models, tests, and documentation;
- **Power BI / SQL dashboards / notebooks** — final analytics and visualization.

Docker, ClickHouse, and Airflow are outside the platform scope.

The intended flow is:

```text
downloaded source files
        ↓
Databricks Volume
        ↓
Bronze Delta tables
        ↓
Silver Delta tables
        ↓
Gold Delta tables / marts
        ↓
SQL / BI / notebooks
```

The source files, Delta tables, transformations, tests, and analytical queries can all live in the same workspace.

---

## 6. Data Layers

Use a simple three-layer approach.

### Source files in Volumes

Keep the downloaded source files in Volumes before loading them into tables.

Example layout:

```text
/Volumes/movie_analytics/raw/movielens/
/Volumes/movie_analytics/raw/imdb/
```

Possible files:

```text
/Volumes/movie_analytics/raw/movielens/ratings.csv
/Volumes/movie_analytics/raw/movielens/tags.csv
/Volumes/movie_analytics/raw/movielens/movies.csv
/Volumes/movie_analytics/raw/movielens/links.csv

/Volumes/movie_analytics/raw/imdb/title.basics.tsv.gz
/Volumes/movie_analytics/raw/imdb/title.ratings.tsv.gz
/Volumes/movie_analytics/raw/imdb/title.crew.tsv.gz
/Volumes/movie_analytics/raw/imdb/title.principals.tsv.gz
/Volumes/movie_analytics/raw/imdb/name.basics.tsv.gz
```

Volumes are the retained file layer. They are not the Bronze tables themselves.

### Bronze

Load the source records from Volumes into Delta tables without changing their meaning or grain. Each Bronze row should correspond to one source row, and source values should remain in their original representation, stored as strings where practical.

Do not split multi-value fields, explode arrays, flatten nested content, replace source null markers, cast business data types, deduplicate records, or create relationships in Bronze. Add only the metadata needed to trace and replay ingestion. If a source record cannot be read normally, retain its raw content and capture the parsing issue rather than silently dropping it.

Examples:

- `movielens_ratings`
- `movielens_tags`
- `movielens_movies`
- `movielens_links`
- `imdb_title_basics`
- `imdb_title_ratings`
- `imdb_title_crew`
- `imdb_title_principals`
- `imdb_name_basics`

Recommended ingestion metadata:

- `_source_file`
- `_source_dataset`
- `_ingested_at`
- `_source_snapshot_date`
- `_pipeline_run_id`

Bronze datasets should be stored as Delta tables in a Bronze schema, for example:

```text
movie_analytics.bronze.movielens_ratings
movie_analytics.bronze.imdb_title_basics
```

### Silver

Clean and normalize source data into stable types and relationships. Split multi-value or nested fields into analysis-ready rows and tables in this layer.

Examples:

- `stg_movies`
- `stg_ratings`
- `stg_tags`
- `stg_imdb_titles`
- `stg_people`
- `stg_title_principals`
- `stg_title_crew`
- `bridge_movie_genres`
- `bridge_movie_people`

Main tasks:

- convert IMDb `\\N` values to null;
- normalize IMDb IDs consistently;
- split or explode multi-value fields such as genres, directors, writers, and known-for titles;
- flatten nested source content where required;
- convert Unix MovieLens timestamps to UTC timestamps;
- parse years and runtimes;
- deduplicate expected entity keys;
- classify person roles;
- validate MovieLens-to-IMDb links.

### Gold

Build an analysis-facing movie dimension, facts, bridge, and marts as Delta tables.

Example namespaces:

```text
movie_analytics.silver.stg_movies
movie_analytics.silver.stg_ratings
movie_analytics.gold.dim_movie
movie_analytics.gold.fct_movie_rating
```

Gold models:

#### Dimension

- `dim_movie`

#### Facts

- `fct_movie_rating`
- `fct_movie_tag`
- `fct_movie_person_credit`

#### Bridge

- `bridge_movie_genres`

#### Aggregates / marts

- `mart_movie_performance` — one row per `movieId`; title, release year, MovieLens rating count and average, IMDb rating and vote count, and tag count;
- `mart_genre_trends` — one row per `genreSource`, `genre`, and `analysisReleaseYear`; movie count, rating count, and rating-count-weighted average rating;
- `mart_user_preferences` — one row per `userId` with at least one rating; rating count, average rating given, and first and last rating times;
- `mart_creator_performance` — one row per `nconst` and `creditRole`; name, credited and rated title counts, average title rating, and total rating count.

The marts cover recurring movie, genre, user, and creator questions. Calculate rating distributions, collaboration networks, release-cohort longevity, and advanced scores in case-study queries. Persist additional marts when dashboards or reports repeatedly use those results.

The Gold foundation contains one movie dimension, three facts, and the movie-genre bridge. The marts calculate user behavior from the rating fact, use date values stored in the facts, and read genre labels from the bridge. The creator mart obtains `primaryName` from Silver IMDb names. These sources provide the attributes needed for the planned analyses.

Gold table names use `snake_case`. Columns follow the source-compatible convention: source fields use names such as `movieId`, `userId`, `tconst`, `nconst`, `primaryTitle`, and `averageRating`; derived business fields use lower camel case; ingestion metadata is underscore-prefixed.

The Gold notebook selects source snapshots through `MOVIELENS_SNAPSHOT_DATE` and `IMDB_SNAPSHOT_DATE`. Both are set to `2026-09-20`. When a complete newer IMDb snapshot is available, set `IMDB_SNAPSHOT_DATE` to that date. Snapshot metadata in the Gold tables supports traceability and replay.

---

## 7. Key Modeling Decisions

### Movie grain

`dim_movie` should represent one canonical movie/title linked through IMDb's `tconst` where available.

Useful fields:

- `movieId`;
- `imdbId`;
- `tconst`;
- `tmdbId`;
- `title`;
- `primaryTitle`;
- `originalTitle`;
- `releaseYear`;
- `startYear`;
- `endYear`;
- `analysisReleaseYear`;
- `runtimeMinutes`;
- `titleType`;
- `isAdult`;
- `isImdbLinked`.

Silver stores the normalized `tconst` link. Gold derives numeric `imdbId` from that value.

Store MovieLens and IMDb genre values in `bridge_movie_genres` with `movieId`, `genre`, and `genreSource`. The source indicator preserves differences between the two classification systems.

### Rating fact grain

One row = one MovieLens user rating of one movie at one timestamp.

Candidate fields:

- `userId`;
- `movieId`;
- `rating`;
- `ratedAt`;
- `ratingDateKey`;
- `movieAgeAtRatingYears`.

Calculate movie age at rating time in whole years. The sources provide a release year, so a day-level lag would be falsely precise.

Use the UTC `ratedAt` timestamp from Silver as part of the rating fact grain. The tag fact uses Silver `taggedAt`. Silver converts the original Unix seconds to timestamps and deduplicates on these timestamp grains. Gold stores those timestamps in its facts.

### Person-credit grain

One row = one person credited on one MovieLens-linked title in one normalized `creditRole`. Reconcile IMDb principal `category` values with director/writer roles from title crew, and retain principal ordering, job, characters, and credit-source provenance.

This avoids storing comma-separated director/cast IDs in analytical models.

The person-credit fact retains `nconst` and normalized `creditRole`. `mart_creator_performance` looks up `primaryName` from the selected Silver IMDb snapshot. MovieLens `userId` remains in the rating and tag facts and in `mart_user_preferences`; activity boundaries are not modeled as a separate user dimension.

---

## 8. Data Quality Checks

Implement tests that demonstrate engineering discipline.

### Structural tests

- primary keys are unique where expected;
- required IDs are not null;
- MovieLens ratings are within valid range;
- release years are plausible;
- runtime is non-negative;
- timestamps parse correctly.

### Relationship tests

- every MovieLens rating references an existing MovieLens movie;
- every linked IMDb ID resolves when present;
- every principal person ID resolves to `name.basics` where expected;
- bridge tables contain no duplicate relationship rows.

### Business-quality checks

- report percentage of MovieLens movies successfully linked to IMDb;
- report missing release year/runtime rates;
- report movies with conflicting source genres;
- report unusually high/low rating counts;
- detect duplicate titles with different IMDb IDs.

Run Gold quality checks before table writes, display their results in the Gold notebook, and fail the run when any required check fails. Quality results are displayed during the build.

---

## 9. Core Metrics

The Gold marts persist the following measures. Each mart also carries the selected source snapshot dates and Gold build timestamp.

### Movie performance

`movieId`, `displayTitle`, `analysisReleaseYear`, `ratingCount`, `movielensAverageRating`, `imdbAverageRating`, `imdbNumVotes`, and `tagCount`. Keep MovieLens and IMDb ratings separate because they come from different audiences.

### Genre trends

`genreSource`, `genre`, `analysisReleaseYear`, `movieCount`, `ratingCount`, and `averageRating`. Calculate `averageRating` from movie averages weighted by their MovieLens rating counts. Keep `genreSource` because MovieLens and IMDb genre labels can differ.

### User preferences

`userId`, `totalRatings`, `averageRatingGiven`, `firstRatingAt`, and `lastRatingAt`. These describe observed MovieLens rating activity.

### Creator performance

`nconst`, `primaryName`, `creditRole`, `creditedTitleCount`, `ratedTitleCount`, `averageTitleRating`, and `totalRatingCount`. Group by person and credit role so different roles are assessed separately. `averageTitleRating` is the simple average of rated titles' MovieLens averages.

---

## 10. Main Analytics Questions

Build several independent case studies from the same warehouse. The measures below can be calculated from Gold facts and bridges for each analysis.

### Case Study A — Cult classics vs mainstream hits

Goal: distinguish movies that are highly liked by a smaller audience from movies with both high popularity and broad approval.

Possible measures:

- weighted rating;
- rating count percentile;
- rating variance;
- number of tags;
- genre-relative popularity.

Output:

- mainstream hits;
- niche favorites;
- divisive popular films;
- hidden high-rated films.

### Case Study B — How genre popularity and reception changed over time

Analyze by release decade/year:

- number of films;
- number of ratings;
- median rating;
- weighted rating;
- share of total ratings;
- audience growth/decline by genre.

Avoid claiming this represents the entire movie-going population; MovieLens users are a specific dataset population.

### Case Study C — Audience taste segmentation

Create interpretable user segments from behavior such as:

- genre distribution;
- release-decade preference;
- average movie popularity;
- average rating;
- number of ratings.

This can remain analytical clustering; a production recommendation engine is not required.

### Case Study D — Director / actor consistency

Questions:

- which creators have the strongest long-run consistency?
- which have high peaks but high variance?
- which creators specialize in particular genres?
- how does performance change across a career?

Use minimum-title and minimum-rating thresholds to avoid tiny-sample rankings.

### Case Study E — Collaboration networks

Study repeated director-actor or actor-actor collaborations.

Metrics:

- number of shared titles;
- combined audience rating;
- collaboration frequency;
- genre concentration;
- first/last collaboration year.

Potential extension: network analysis outside the warehouse using Python/NetworkX.

### Case Study F — Old movies that remain actively rated

Use rating timestamps versus release year to study catalog longevity.

Possible metric:

`rating_lag_years = rating_year - release_year`

Questions:

- which movies continue receiving ratings decades later?
- which genres have the longest audience tail?
- does high IMDb vote count correlate with long-term MovieLens activity?

---

## 11. Pipeline Stages

### Stage 1 — Dataset acquisition

Download:

1. MovieLens 32M;
2. selected IMDb bulk datasets.

Source files can be downloaded manually.

Record:

- source dataset;
- source file name;
- source snapshot date;
- download date;
- checksum when available.

### Stage 2 — Store source files in Volumes

Store the downloaded files in Volumes.

Example:

```text
/Volumes/movie_analytics/raw/movielens/
/Volumes/movie_analytics/raw/imdb/
```

The Volume acts as the retained source-file layer so the pipeline can be replayed without redownloading the original files.

### Stage 3 — Bronze ingestion

Use PySpark or Spark SQL to load the files from the Volume into Bronze Delta tables. Preserve one row per source record, the source-aligned column structure, and the original field values. Leave multi-value and nested fields intact in this stage.

Requirements:

- source-aligned ingestion schemas, with fields stored as strings where practical;
- unchanged source record grain;
- source metadata;
- idempotent ingestion;
- row-count validation;
- retention and clear reporting of malformed records.

### Stage 4 — Silver standardization

Clean source-specific quirks, split multi-value or nested fields where needed, and produce stable Silver Delta tables.

Main work includes:

- converting IMDb `\N` values to null;
- parsing years and runtimes;
- converting MovieLens Unix timestamps to UTC timestamps;
- standardizing IDs;
- splitting, exploding, or normalizing multi-value fields where required;
- flattening nested content where required;
- deduplicating expected entity keys.

### Stage 5 — Entity integration

Join MovieLens `imdbId` to IMDb `tconst`.

Create an integration report containing:

- linked rows;
- unlinked rows;
- duplicate links;
- malformed IDs;
- coverage percentage.

### Stage 6 — Gold models

Build Delta tables for:

- dimensions;
- facts;
- bridge tables;
- aggregate marts.

These should be the tables used by notebooks, Databricks SQL, or BI tools.

### Stage 7 — Data quality

Implement checks using Spark SQL, notebooks, or native expectations/testing patterns.

Validate:

- row counts;
- required IDs;
- uniqueness where expected;
- valid rating ranges;
- referential relationships;
- source-to-Bronze reconciliation;
- MovieLens-to-IMDb link coverage.

### Stage 8 — Incremental refresh

MovieLens remains a fixed snapshot.

For IMDb refreshes:

1. upload a newer IMDb snapshot to the Volume;
2. ingest it to Bronze with snapshot metadata;
3. compare it with the selected prior snapshot to identify changed records;
4. build the Silver models for the new snapshot and set `IMDB_SNAPSHOT_DATE` in the Gold notebook;
5. validate and overwrite the Gold tables from the selected MovieLens and IMDb snapshots;
6. retain source snapshots in the Volume and Bronze for replay.

### Stage 9 — Orchestration

After the notebook/jobs work independently, optionally create a Workflow to run:

```text
Bronze ingestion
      ↓
Silver transformation
      ↓
integration
      ↓
Gold models, validation, and write
```

### Stage 10 — Analytics and presentation

Build analyses against Gold tables rather than directly against the source files or Bronze tables.

---

## 12. Incremental / Refresh Strategy

MovieLens 32M should be treated as a fixed snapshot.

IMDb is refreshed daily, but a portfolio project does not need to ingest it every day.

Recommended approach:

- perform an initial full IMDb load;
- store each downloaded IMDb snapshot in the Volume;
- preserve `_source_snapshot_date` in Bronze;
- refresh manually or on a weekly/monthly schedule;
- compare incoming records with the existing Silver/Gold state;
- build Silver for the selected IMDb snapshot and overwrite Gold after validation;
- preserve ingestion timestamps.

Gold represents the selected IMDb snapshot. Rebuild the Gold models from that snapshot when IMDb data is refreshed; retained source snapshots in Volumes and Bronze support replay.

Because the source files remain in Volumes, an older snapshot can be replayed without downloading it again.

---

## 13. Repository Structure

The repository should focus on code and documentation. Large source datasets should live in Volumes rather than inside the Git repository.

```text
movie-analytics/
├── README.md
├── notebooks/
│   ├── 01_bronze/
│   ├── 02_silver/
│   ├── 03_integration/
│   ├── 04_gold/
│   └── 05_quality/
├── src/
│   └── movie_analytics/
│       ├── ingestion/
│       ├── transforms/
│       ├── quality/
│       └── config/
├── sql/
│   ├── silver/
│   ├── gold/
│   └── quality/
├── analyses/
├── dashboards/
├── tests/
└── docs/
```

Do not create empty folders just because they appear here. Add components when they are actually needed.

Example catalog organization:

```text
Catalog: movie_analytics

Schemas:
- bronze
- silver
- gold

Volume:
- movie_analytics.raw.source_files
```

Possible Volume paths:

```text
/Volumes/movie_analytics/raw/source_files/movielens/
/Volumes/movie_analytics/raw/source_files/imdb/
```

---

## 14. Milestones

### Milestone 1 — Workspace setup and MovieLens source files

Deliverables:

- Unity Catalog catalog/schema structure;
- Volume for source files;
- MovieLens files uploaded;
- files readable from Spark.

### Milestone 2 — MovieLens Bronze tables

Deliverables:

- Bronze Delta tables for ratings, tags, movies, and links;
- ingestion metadata;
- source-aligned ingestion schemas;
- original source values and record grain preserved;
- row-count validation.

### Milestone 3 — IMDb ingestion

Deliverables:

- selected IMDb source files stored in the Volume;
- Bronze IMDb Delta tables;
- original source values, including `\N` markers, preserved;
- original source record grain preserved;
- source snapshot metadata.

### Milestone 4 — Silver models

Deliverables:

- cleaned MovieLens models;
- cleaned IMDb title/person models;
- normalized identifiers;
- parsed timestamps, years, runtimes, and genres;
- quality checks.

### Milestone 5 — Integrated movie model

Deliverables:

- MovieLens-to-IMDb join;
- `dim_movie`;
- integration coverage report;
- genre bridge.

### Milestone 6 — Audience and creator warehouse

Deliverables:

- rating fact;
- movie-person credits;
- movie aggregates;
- creator aggregates.

### Milestone 7 — Incremental IMDb refresh

Deliverables:

- second IMDb snapshot;
- snapshot metadata;
- repeatable refresh logic;
- idempotent rerun behavior;
- comparison of changed records.

### Milestone 8 — Orchestration and presentation

Deliverables:

- optional workflow;
- dashboard/report;
- architecture/data-flow diagram;
- data dictionary;
- final README with reproducible setup instructions.

---

## 15. Stretch Goals

Only do these after the core project is finished.

- MovieLens Tag Genome analysis;
- user clustering;
- recommendation prototype;
- network graph of collaborations;
- persisted movie-rating-distribution mart;
- persisted collaboration mart;
- persisted release-cohort mart;
- Delta partitioning / clustering performance experiments;
- compare Spark SQL versus PySpark implementations for selected transformations;
- dbt implementation;
- Workflows for automated refreshes;
- Delta history / time-travel demonstrations;
- Auto Loader experiment for file-based ingestion;
- CI checks for Python / SQL / dbt if dbt is added.

---

## 16. What Not to Overbuild

Avoid:

- making a web app before the data platform works;
- adding many APIs just to increase source count;
- building a recommendation ML model before the analytical warehouse is complete;
- creating dozens of dashboards;
- adding external infrastructure such as ClickHouse when Databricks already covers the project needs;
- introducing Kafka when all primary sources are batch files;
- forcing streaming when the sources are downloadable snapshots;
- scheduling a pipeline before the individual notebook/jobs are reliable.

The value should come from **clean integration, strong modeling, reproducibility, performance, and useful analysis**.

---

## 17. Definition of Done

The project is portfolio-ready when another person can understand and reproduce the pipeline by:

1. obtaining the documented source datasets;
2. uploading or placing them in the expected Databricks Volume paths;
3. running Bronze ingestion;
4. building Silver transformations;
5. integrating MovieLens and IMDb through documented IDs;
6. building Gold facts, dimensions, bridges, and marts;
7. running the documented data-quality checks successfully;
8. refreshing IMDb from a newer snapshot without corrupting or duplicating the warehouse;
9. querying documented Gold tables;
10. reproducing the main analyses from the README/notebooks.

The important engineering story is:

```text
bulk source files
      ↓
Databricks Volumes
      ↓
Bronze Delta
      ↓
Silver Delta
      ↓
Gold Delta
      ↓
analytics
```

---

## 18. Source References

- GroupLens MovieLens datasets: https://grouplens.org/datasets/movielens/
- MovieLens 32M README: https://files.grouplens.org/datasets/movielens/ml-32m-README.html
- IMDb Non-Commercial Datasets: https://www.imdb.com/interfaces/
- IMDb dataset downloads: https://datasets.imdbws.com/

Always re-check dataset licenses/terms before publishing raw source data or using the project commercially.
