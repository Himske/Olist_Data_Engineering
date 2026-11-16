# Project instructions

## Project layout

- `spark/olist_ingest.py` is the reusable Bronze ingestion job; `dags/olist_ingestion_dag.py` schedules one task per dataset.
- `data/olist/` contains the nine source CSVs. `config/.env.example` documents storage configuration; `README.md` documents runtime setup.
- This is a starter project: Silver, dbt, and a complete Airflow deployment are not implemented yet.

## Pipeline invariants

- Keep the dataset names and filenames in the Spark job and the dataset list in the DAG consistent.
- Read Bronze CSV columns as strings and retain `_ingested_at`, `_source_file`, `_batch_id`, and `_dataset`; perform type conversion in Silver.
- Write Parquet under `<prefix>/<dataset>/batch_date=<date>` and preserve batch-scoped overwrite behavior so retries do not overwrite other batches.
- Preserve explicit failures for missing or empty input and storage configuration errors. Stop Spark after the job.
- The DAG passes Airflow's `{{ ds }}` as the batch date; the daily schedule is a demonstration over static historical data, not incremental filtering.

## Storage and runtime

- Use Azure Blob Storage with Hadoop WASB, not S3 or MinIO. Local Azurite uses `wasb://<container>@storageemulator`; cloud storage uses `wasbs://<container>@<account>.blob.core.windows.net`.
- Azurite does not emulate ADLS Gen2. Hadoop's emulator mode connects to localhost:10000, so Spark processes accessing it must share Azurite's network namespace.
- Keep the `hadoop-azure` package version aligned with the Hadoop version in the Spark runtime. The DAG currently requests `3.3.4`.
- Do not commit credentials. The DAG inherits storage environment variables; cloud access requires `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY`, while `AZURE_STORAGE_CONTAINER` selects the container.
- Update the README and example configuration when changing setup, paths, or environment variables. Do not assume the example environment file is loaded automatically.

## Validation

- Run focused tests with `python -m pytest tests -v`. Existing tests mock PySpark and do not establish end-to-end Spark or Azurite compatibility.
- For storage or ingestion changes, when the required runtime is available, also run the documented single-dataset `spark-submit` flow and verify the resulting Parquet in Azurite. Report when integration validation cannot be run.
- Python dependencies are listed in `requirements.txt` and `pyproject.toml`; keep them consistent when changing shared dependencies.
