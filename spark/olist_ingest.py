#!/usr/bin/env python3
"""Ingest one Olist CSV into date-partitioned Azure Blob Storage Parquet."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, choices=sorted(DATASETS))
    parser.add_argument("--input-dir", default=os.getenv("OLIST_INPUT_DIR", "/opt/airflow/data/olist"))
    parser.add_argument(
        "--batch-date",
        default=os.getenv("BATCH_DATE", datetime.now(UTC).date().isoformat()),
    )
    parser.add_argument("--container", default=os.getenv("AZURE_STORAGE_CONTAINER", "olist-data"))
    parser.add_argument("--output-prefix", default=os.getenv("BRONZE_PREFIX", "bronze"))
    return parser.parse_args()


def validate_batch_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("batch-date must use YYYY-MM-DD") from exc


def build_spark() -> SparkSession:
    account = os.getenv("AZURE_STORAGE_ACCOUNT", "storageemulator")
    builder = SparkSession.builder.appName("olist-bronze-ingestion").config("spark.sql.session.timeZone", "UTC")
    if account != "storageemulator":
        key = os.environ.get("AZURE_STORAGE_KEY")
        if not key:
            raise ValueError("AZURE_STORAGE_KEY is required for Azure Blob Storage")
        builder = builder.config(f"spark.hadoop.fs.azure.account.key.{account}.blob.core.windows.net", key)
    return builder.getOrCreate()


def main() -> int:
    args = parse_args()
    batch_date = validate_batch_date(args.batch_date)
    filename = DATASETS[args.dataset]
    source = Path(args.input_dir) / filename

    if not source.is_file():
        print(f"ERROR: source file not found: {source}", file=sys.stderr)
        return 2
    if source.stat().st_size == 0:
        print(f"ERROR: source file is empty: {source}", file=sys.stderr)
        return 2

    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    try:
        # Keep source fields as strings in Bronze; cast and standardize in Silver.
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "FAILFAST")
            .option("encoding", "UTF-8")
            .csv(str(source))
        )

        if not df.columns:
            raise ValueError(f"No columns found in CSV: {source}")

        df = (
            df.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("_batch_id", lit(batch_date))
            .withColumn("_dataset", lit(args.dataset))
        )

        account = os.getenv("AZURE_STORAGE_ACCOUNT", "storageemulator")
        if account == "storageemulator":
            storage = f"wasb://{args.container}@storageemulator"
        else:
            storage = f"wasbs://{args.container}@{account}.blob.core.windows.net"
        output = f"{storage}/{args.output_prefix}/{args.dataset}/batch_date={batch_date}"

        # Overwrite only this dataset's batch-date folder. Retries are idempotent
        # for a given dataset + batch date; other dates remain untouched.
        (df.write.mode("overwrite").format("parquet").save(output))

        print(f"SUCCESS dataset={args.dataset} output={output} rows={df.count()}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
