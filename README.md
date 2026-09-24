# Movie analytics pipeline

The Databricks job in [resources/job.yml](resources/job.yml) installs the project wheel and runs each stage's `main()` entry point in this order:

1. `bronze` loads MovieLens and IMDb source files from the Volume into Bronze Delta tables.
2. `silver` cleans and normalizes the selected Bronze snapshots.
3. `gold` joins MovieLens and IMDb and writes analysis tables.

The three entry points are defined in [pyproject.toml](pyproject.toml) and call [bronze.py](src/music_analytics_pipeline/bronze.py), [silver.py](src/music_analytics_pipeline/silver.py), and [gold.py](src/music_analytics_pipeline/gold.py). Run `databricks bundle deploy -t dev` to build and deploy the wheel, then `databricks bundle run -t dev movie_pipeline` to start the job. Shared configuration and helpers live in `src/music_analytics_pipeline/`.

Set `MOVIELENS_SNAPSHOT_DATE` and `IMDB_SNAPSHOT_DATE` in [config.py](src/music_analytics_pipeline/config.py) before running the stages. An IMDb refresh can use a newer date while MovieLens stays on its original snapshot. The scripts do not include notebook previews or data-quality checks.

See the [project plan](movie_analytics_project_plan_revised.md) for the data model and refresh workflow.
