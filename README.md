# Olist PySpark + Airflow Bronze ingestion

This starter project ingests the nine CSV files from the Brazilian Olist public
e-commerce dataset into an Azure Blob Storage container as date-partitioned
Parquet files. Azurite emulates Blob Storage for local development.

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
- access to Azurite at `127.0.0.1:10000` in its own network namespace, or
  network access to an Azure Blob Storage account

The Spark runtime must include Hadoop Azure support. The DAG requests
`org.apache.hadoop:hadoop-azure:3.3.4`; this version must match the Hadoop
version bundled with your Spark distribution. If your Spark image uses a
different Hadoop version, change the package version accordingly.

### Local storage with Azurite

Start Azurite's blob service, for example on the host running Spark:

```bash
docker run -d --name azurite -p 127.0.0.1:10000:10000 \
  -v azurite-data:/data mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0 --location /data
```

Create the `olist-data` container using Azure CLI on that host:

```bash
az storage container create --name olist-data \
  --connection-string 'UseDevelopmentStorage=true'
```

Hadoop WASB's `storageemulator` mode uses the emulator's built-in development
account at `127.0.0.1:10000`; it cannot point to a separate Docker hostname.
Every Spark process accessing storage must share Azurite's network namespace.
For a containerized Airflow worker, arrange for Azurite to run in that same
namespace, and create the container from a process that can reach its port.
Merely putting separate Airflow and Azurite containers on the same Docker
network is not sufficient. Azurite does not emulate ADLS Gen2; this project
uses the Blob Storage WASB connector instead of ABFS.

The default settings target Azurite. `config/.env.example` lists the environment
variables; copying it alone does not load them into Airflow or Spark.

### Azure Blob Storage

Create a container in your Azure storage account, then set
`AZURE_STORAGE_ACCOUNT` to the account name and `AZURE_STORAGE_CONTAINER` to
the container name. Provide `AZURE_STORAGE_KEY` securely to the Spark driver,
for example through the Airflow worker's secrets-backed environment. The DAG
inherits these variables. Never commit real credentials; Hadoop's account key
is passed into Spark configuration, so restrict access to Spark configuration,
logs, and its UI. This starter uses shared-key authentication and the standard
Azure public-cloud Blob endpoint.

## Run one dataset manually

From an environment with PySpark, the matching `hadoop-azure` package, and
access to Azurite:

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-azure:3.3.4 \
  spark/olist_ingest.py \
  --dataset orders \
  --input-dir ./data/olist \
  --batch-date 2018-10-17
```

Output example:

```text
wasb://olist-data@storageemulator/bronze/orders/batch_date=2018-10-17/
```

For Azure Blob Storage the output is
`wasbs://olist-data@<account>.blob.core.windows.net/bronze/orders/batch_date=2018-10-17/`.
The container can also be selected with `--container`.

## Airflow

Copy `dags/olist_ingestion_dag.py` into your Airflow DAGs folder and mount the
Spark script and source data at the paths configured at the top of the DAG.
The DAG runs daily at 03:00 UTC. Because Olist is a static historical dataset,
this schedule is a demonstration: for a realistic incremental simulation,
create daily source extracts or add a parameterized event-date filter in a
later iteration.

## Important next steps

1. Automate Azure Blob container initialization.
2. Add source-level schema contracts and expected-column checks.
3. Add a Silver job to cast dates, numeric values, and identifiers.
4. Add row-count and duplicate-key checks.
5. Add dbt models and tests after the Silver tables are available.
6. Add a failure notification channel and CI tests.

## Notes

This is a Bronze ingestion starter, not a complete production deployment.
The DAG assumes a custom Airflow image/container that has Spark installed.
Azurite's built-in development account is only for local development.
