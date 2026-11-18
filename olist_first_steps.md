Project files created

I've built the Docker Compose starter project with **Airflow + PySpark + Azurite + PostgreSQL**, replacing the MinIO setup.

**olist_azure_docker_compose_setup.zip**

Complete project · 11 files

Ready

Download the complete project

### **What's included**

- `docker-compose.yml` — Airflow, PostgreSQL and Azurite.
- `docker/airflow/Dockerfile` — custom Airflow image with Java 17, PySpark and Azure Blob SDK.
- `spark/olist_ingest.py` — reusable ingestion script for all nine Olist datasets.
- `dags/olist_ingestion_dag.py` — Airflow DAG with nine ingestion tasks.
- `.env.example` — local environment configuration.
- `README.md` — setup instructions and troubleshooting.

### **Get it running**

**1.** Extract the ZIP.

**2.** Open a terminal in the extracted project directory:

```
cp .env.example .env
mkdir -p logs plugins data/olist
```

**3.** Download and extract the Olist CSV files into `data/olist/`.

**4.** Build the Docker image:

```
docker compose build
```

**5.** Initialize Airflow:

```
docker compose up airflow-init
```

**6.** Start the services:

```
docker compose up -d
```

**7.** Open Airflow:

[Open Airflow UI](http://localhost:8080?utm_source=chatgpt.com)

Default login: `admin` / `admin`.

### **Test the ingestion**

Before triggering the full DAG, test just the `orders` dataset:

```
docker compose exec airflow-scheduler \
  spark-submit --master 'local[2]' \
  /opt/airflow/spark/olist_ingest.py \
  --dataset orders \
  --batch-date 2026-11-17
```

Then inspect the `bronze` container using Azure Storage Explorer.

### **One architectural note**

This version runs Spark in **local mode inside the Airflow container**. That keeps the initial environment simpler and avoids Hadoop connector configuration issues with Azurite. It still uses actual PySpark processing and uploads Parquet through the Azure Blob API. Azurite's development account credentials are documented by Microsoft and are intended for local testing only.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://github.com\&sz=32)

GitHub+1



It is not yet a distributed Spark cluster. We can evolve it into a separate Spark master/worker deployment once the end-to-end pipeline works.

**My recommendation:** get the `orders` task running first. Once it succeeds, check that the Parquet files appear in Azurite before running all nine datasets.