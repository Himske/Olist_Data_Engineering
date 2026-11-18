## Overview
Deliver the Bronze pipeline end to end first: run Azurite and Airflow, ingest `orders`, verify its Parquet output, then run all nine datasets. After Bronze is reliable, add Silver transformations and then dbt.

## Requirements
- Use the existing Compose setup and Azure Blob Storage via Hadoop WASB. The scheduler shares Azurite’s network namespace; preserve this for local storage access.
- Keep all nine dataset names and filenames consistent between the Spark job and DAG. Bronze fields remain strings and include `_ingested_at`, `_source_file`, `_batch_id`, and `_dataset`.
- Write to `<prefix>/<dataset>/batch_date=<date>` with overwrite scoped to that dataset and batch. Preserve explicit failures for missing or empty inputs and storage configuration errors.
- Treat the daily DAG schedule as a demonstration over static historical data. Do not assume `config/.env.example` is loaded automatically or commit credentials.
- `docker setup.md` describes an earlier, different architecture; use the current `compose.yaml` and README instead.

## Implementation steps
1. Create a private root `.env` with the required Airflow/PostgreSQL secrets; keep it untracked.
2. Start the Compose stack and confirm initialization succeeded and Azurite is running.
3. Create the configured Blob container in Azurite.
4. Submit the Spark job for `orders` with a batch date; confirm it succeeds and writes Parquet under `bronze/orders/batch_date=<date>`.
5. Run the `olist_bronze_ingestion` DAG and verify output for all nine datasets.
6. Address the README’s next steps in order: automate container initialization, add source schema/column checks, build Silver type conversions, add row-count and duplicate-key checks, then add dbt models and tests.
7. Decide whether to add failure notifications and CI tests as part of this phase or defer them.

## Tests to add or update
- Add focused tests for missing/empty input failures, metadata and string-column retention, batch-scoped output paths, and agreement between the DAG datasets and Spark source mappings.
- Run `python -m pytest tests -v`.
- When the runtime is available, run the documented single-dataset `spark-submit` flow and verify the Parquet output in Azurite; mocked tests alone do not validate Spark/Azurite integration.