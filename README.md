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

## Local Docker Compose setup

The Compose stack is for development only. It builds an Airflow image with Java
and PySpark, runs PostgreSQL for Airflow metadata, and starts Azurite. The
LocalExecutor runs up to two tasks at a time. Allow at least 4 GB of Docker
memory (8 GB recommended), plus enough for the Spark jobs.

1. Install Docker with Compose and the Azure CLI. From the project root, create
   an untracked `.env` file containing the following values:

   ```text
   POSTGRES_PASSWORD=<random-hex>
   AIRFLOW_ADMIN_PASSWORD=<random-hex>
   AIRFLOW_FERNET_KEY=<generated-fernet-key>
   AIRFLOW_WEBSERVER_SECRET_KEY=<random-hex>
   ```

   Generate a separate value for each random-hex field using
   `python -c "import secrets; print(secrets.token_hex(32))"` and generate the
   Fernet key with
   `python -c "import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"`.
   Keep these values private and retain them across restarts. Use hex for the
   PostgreSQL password so it is safe in the database connection URL. Changing
   the password in `.env` does not update an existing PostgreSQL volume or
   Airflow admin user.
2. Start the stack and wait for initialization:

   ```bash
   docker compose up --build -d
   docker compose ps
   docker compose logs airflow-init azurite
   ```

   Check that `airflow-init` exited successfully and Azurite is listening
   before proceeding. The first Spark submission also needs internet access to
   download the Hadoop Azure package.
3. Create the default Blob container using Azure CLI on the host:

   ```bash
   az storage container create --name olist-data \
     --connection-string 'UseDevelopmentStorage=true'
   ```

   If you set `AZURE_STORAGE_CONTAINER`, create that container instead.
4. Open http://localhost:8080, sign in as `admin` with
   `AIRFLOW_ADMIN_PASSWORD`, then unpause and trigger `olist_bronze_ingestion`.
   Inspect the task logs in Airflow and list the resulting blobs:

   ```bash
   az storage blob list --container-name olist-data \
     --connection-string 'UseDevelopmentStorage=true' --output table
   ```

Stop the stack with `docker compose down`; the named PostgreSQL, Azurite, and
Airflow log volumes persist. The scheduler shares Azurite's network namespace
because WASB's emulator endpoint is localhost; don't scale or move the scheduler
to a separate namespace without changing the storage integration.

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

### Local storage without Compose

For a Spark process running on the host, start Azurite's blob service:

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
For a separately configured containerized Airflow worker, arrange for Azurite
to run in that same namespace, and create the container from a process that can
reach its port.
Merely putting separate Airflow and Azurite containers on the same Docker
network is not sufficient. Azurite does not emulate ADLS Gen2; this project
uses the Blob Storage WASB connector instead of ABFS.

The default settings target Azurite. `config/.env.example` lists the environment
variables; copying it alone does not load them into Airflow or Spark. Compose
reads optional `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_CONTAINER`, and
`AZURE_STORAGE_KEY` values from the root `.env` or the host environment.

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

The Compose setup mounts the DAG, Spark script, and source data at the required
paths. For another Airflow deployment, copy the DAG into its DAGs folder and
mount the Spark script and source data at the paths configured in the DAG.
After all nine Bronze tasks succeed, the DAG runs one Silver task for the same
batch date. The DAG runs daily at 03:00 UTC. Because Olist is a static
historical dataset, this schedule is a demonstration: for a realistic
incremental simulation, create daily source extracts or add a parameterized
event-date filter in a later iteration.

## Silver layer

The Spark job reads all nine datasets from
`<prefix>/<dataset>/batch_date=<date>` under the `bronze` prefix, then writes
cleaned Parquet to the same layout under the `silver` prefix. It trims string
values and converts empty strings to nulls, converts order/review/shipping
timestamps to timestamps, and converts item prices, freight, and payment values
to decimals with two fractional digits. Optional missing values remain null.
Exact duplicate rows are removed; duplicate or null primary keys and unmatched
foreign keys fail the task rather than silently dropping records. Foreign-key
checks cover orders/customers, order items/orders/products/sellers, payments/
orders, and reviews/orders. Order reviews use `(review_id, order_id)` as a
composite key because review identifiers repeat across different orders in
the supplied source data.

To run it manually after all nine Bronze batches have completed:

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-azure:3.3.4 \
  spark/olist_silver.py \
  --batch-date 2018-10-17
```

Set `BRONZE_PREFIX` or `SILVER_PREFIX` to override the default prefixes. The
job uses the same Azure storage environment variables and credentials as the
Bronze job.

## Important next steps

1. Add row-count and broader source-quality checks.
2. Add dbt models and tests after the Silver tables are available.
3. Add a failure notification channel and CI tests.

## Notes

This is a Bronze ingestion starter, not a complete production deployment.
The Compose stack and image are intended for local development, not production.
Azurite's built-in development account is only for local development.
