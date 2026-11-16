# Olist PySpark + Airflow Bronze ingestion

This starter project ingests the nine CSV files from the Brazilian Olist public
e-commerce dataset into a MinIO bucket as date-partitioned Parquet files.

## What it does

- One reusable PySpark job handles all nine source datasets.
- Bronze columns are intentionally read as strings; type casting belongs in
  the Silver layer.
- Adds `_ingested_at`, `_source_file`, `_batch_id`, and `_dataset`.
- Airflow runs one task per dataset in parallel, with retries.
- A retry for the same dataset and batch date overwrites only that batch folder.

## Expected source files

Put the extracted Olist CSV files in `data/olist/`:

- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

Download: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Expected runtime layout

The Airflow worker must have:

- `spark-submit` available at `/opt/spark/bin/spark-submit`
- this project mounted at `/opt/airflow/project`
- Olist CSVs mounted at `/opt/airflow/data/olist`
- network access to MinIO using hostname `minio` and port `9000`

The Spark runtime must include Hadoop S3A support. The DAG requests
`org.apache.hadoop:hadoop-aws:3.3.4`; this version must match the Hadoop
version bundled with your Spark distribution. If your Spark image uses a
different Hadoop version, change the package version accordingly.

Create the MinIO bucket `olist-data` before running the DAG. Configure Airflow
Variables `minio_access_key` and `minio_secret_key`, or replace the Jinja
references in the DAG with your preferred secrets backend. Do not commit real
credentials to Git.

## Run one dataset manually

From an environment with PySpark, the matching `hadoop-aws` package, and access
to MinIO:

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  spark/olist_ingest.py \
  --dataset orders \
  --input-dir ./data/olist \
  --batch-date 2018-10-17
```

Output example:

```text
s3a://olist-data/bronze/orders/batch_date=2018-10-17/
```

## Airflow

Copy `dags/olist_ingestion_dag.py` into your Airflow DAGs folder and mount the
Spark script and source data at the paths configured at the top of the DAG.
The DAG runs daily at 03:00 UTC. Because Olist is a static historical dataset,
this schedule is a demonstration: for a realistic incremental simulation,
create daily source extracts or add a parameterized event-date filter in a
later iteration.

## Important next steps

1. Add a MinIO bucket initialization step (or create the bucket in the console).
2. Add source-level schema contracts and expected-column checks.
3. Add a Silver job to cast dates, numeric values, and identifiers.
4. Add row-count and duplicate-key checks.
5. Add dbt models and tests after the Silver tables are available.
6. Add a failure notification channel and CI tests.

## Notes

This is a Bronze ingestion starter, not a complete production deployment.
The DAG assumes a custom Airflow image/container that has Spark installed.
The default MinIO credentials shown in `.env.example` are for local development
only and must be replaced.
