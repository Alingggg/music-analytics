# Movie analytics pipeline

Run the Databricks notebooks in order:

1. [Bronze ingestion](notebooks/01_bronze/movie_bronze_ingestion.ipynb) loads MovieLens and IMDb source files from the Volume into Bronze Delta tables.
2. [Silver models](notebooks/02_silver/movie_silver_models.ipynb) cleans and normalizes the persisted Bronze tables.
3. [Gold models](notebooks/03_gold/movie_gold_models.ipynb) joins MovieLens and IMDb, validates the models, and writes analysis tables.

Set the source snapshot dates in each notebook before running it. See the [project plan](movie_analytics_project_plan_revised.md) for the data model and refresh workflow.
