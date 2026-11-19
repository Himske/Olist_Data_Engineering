"""Airflow DAG: ingest Olist CSVs into Bronze, then build Silver Parquet."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DATASETS = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "category_translation",
]

# These paths must exist inside the Airflow worker/scheduler container.
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SCRIPT = "/opt/airflow/project/spark/olist_ingest.py"
SILVER_SCRIPT = "/opt/airflow/project/spark/olist_silver.py"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="olist_bronze_ingestion",
    description="Ingest Olist CSVs into Bronze and build validated Silver Parquet",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["olist", "pyspark", "azure", "bronze"],
) as dag:
    ingestion_tasks = []
    for dataset in DATASETS:
        task = BashOperator(
            task_id=f"ingest_{dataset}",
            bash_command=(
                f"{SPARK_SUBMIT} "
                f"--packages org.apache.hadoop:hadoop-azure:3.3.4 "
                f"{SCRIPT} "
                f"--dataset {dataset} "
                "--batch-date '{{ ds }}' "
                "--input-dir /opt/airflow/data/olist"
            ),
            env={
                "PYSPARK_PYTHON": "python3",
            },
            append_env=True,
            do_xcom_push=False,
        )
        ingestion_tasks.append(task)

    silver_task = BashOperator(
        task_id="build_silver",
        bash_command=(
            f"{SPARK_SUBMIT} "
            "--packages org.apache.hadoop:hadoop-azure:3.3.4 "
            f"{SILVER_SCRIPT} "
            "--batch-date '{{ ds }}'"
        ),
        env={
            "PYSPARK_PYTHON": "python3",
        },
        append_env=True,
        do_xcom_push=False,
    )
    ingestion_tasks >> silver_task
