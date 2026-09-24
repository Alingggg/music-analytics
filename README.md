# Movie analytics pipeline

Run the Databricks notebooks in order:

1. [Bronze ingestion](notebooks/01_bronze/movie_bronze_ingestion.ipynb) loads MovieLens and IMDb source files from the Volume into Bronze Delta tables.
2. [Silver models](notebooks/02_silver/movie_silver_models.ipynb) cleans and normalizes the persisted Bronze tables.
3. [Gold models](notebooks/03_gold/movie_gold_models.ipynb) joins MovieLens and IMDb, validates the models, and writes analysis tables.

Set `MOVIELENS_SNAPSHOT_DATE` and `IMDB_SNAPSHOT_DATE` once in [config.py](notebooks/config.py) before running the stages. Each notebook imports that file automatically; do not run it separately. An IMDb refresh can use a newer date while MovieLens stays on its original snapshot. See the [project plan](movie_analytics_project_plan_revised.md) for the data model and refresh workflow.
