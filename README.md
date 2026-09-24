# Movie analytics pipeline

Run [run_pipeline.py](src/run_pipeline.py) on Databricks compute once for each stage, in this order:

1. `bronze` loads MovieLens and IMDb source files from the Volume into Bronze Delta tables.
2. `silver` cleans and normalizes the selected Bronze snapshots.
3. `gold` joins MovieLens and IMDb and writes analysis tables.

Pass the stage name as the script's argument, for example `python src/run_pipeline.py bronze`. The three stage modules and their shared configuration and helpers live together in `src/music_analytics_pipeline/`.

Set `MOVIELENS_SNAPSHOT_DATE` and `IMDB_SNAPSHOT_DATE` in [config.py](src/music_analytics_pipeline/config.py) before running the stages. An IMDb refresh can use a newer date while MovieLens stays on its original snapshot. The scripts do not include notebook previews or data-quality checks. Databricks job configuration will be added separately.

See the [project plan](movie_analytics_project_plan_revised.md) for the data model and refresh workflow.
