#!/usr/bin/env python3
"""Transform all Olist Bronze datasets into validated Silver Parquet."""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime

from olist_ingest import DATASETS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

TIMESTAMP_COLUMNS = {
    "orders": (
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ),
    "order_items": ("shipping_limit_date",),
    "order_reviews": ("review_creation_date", "review_answer_timestamp"),
}
DECIMAL_COLUMNS = {
    "order_items": ("price", "freight_value"),
    "order_payments": ("payment_value",),
}
DECIMAL_TYPE = DecimalType(18, 2)

PRIMARY_KEYS = {
    "customers": ("customer_id",),
    "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_id", "order_id"),
    "orders": ("order_id",),
    "products": ("product_id",),
    "sellers": ("seller_id",),
    "category_translation": ("product_category_name",),
}
FOREIGN_KEYS = (
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments", "order_id", "orders", "order_id"),
    ("order_reviews", "order_id", "orders", "order_id"),
)
REQUIRED_VALUE_COLUMNS = {
    "customers": ("customer_unique_id",),
    "order_items": ("price", "freight_value"),
    "order_payments": ("payment_type", "payment_value"),
    "order_reviews": ("review_score",),
    "orders": ("order_status", "order_purchase_timestamp"),
    "sellers": ("seller_state",),
    "category_translation": ("product_category_name_english",),
}
NONNEGATIVE_COLUMNS = {
    "order_items": ("price", "freight_value"),
    "order_payments": ("payment_value",),
}
ACCEPTED_VALUES = {
    ("order_payments", "payment_type"): (
        "credit_card",
        "boleto",
        "voucher",
        "debit_card",
        "not_defined",
    ),
    ("orders", "order_status"): (
        "created",
        "approved",
        "invoiced",
        "processing",
        "shipped",
        "delivered",
        "unavailable",
        "canceled",
    ),
    ("order_reviews", "review_score"): ("1", "2", "3", "4", "5"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-date",
        default=os.getenv("BATCH_DATE", datetime.now(UTC).date().isoformat()),
    )
    parser.add_argument("--container", default=os.getenv("AZURE_STORAGE_CONTAINER", "olist-data"))
    parser.add_argument("--input-prefix", default=os.getenv("BRONZE_PREFIX", "bronze"))
    parser.add_argument("--output-prefix", default=os.getenv("SILVER_PREFIX", "silver"))
    return parser.parse_args()


def validate_batch_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("batch-date must use YYYY-MM-DD") from exc


def build_spark() -> SparkSession:
    account = os.getenv("AZURE_STORAGE_ACCOUNT", "storageemulator")
    builder = SparkSession.builder.appName("olist-silver-transformation").config(
        "spark.sql.session.timeZone", "UTC"
    )
    if account != "storageemulator":
        key = os.environ.get("AZURE_STORAGE_KEY")
        if not key:
            raise ValueError("AZURE_STORAGE_KEY is required for Azure Blob Storage")
        builder = builder.config(f"spark.hadoop.fs.azure.account.key.{account}.blob.core.windows.net", key)
    return builder.getOrCreate()


def require_columns(df: DataFrame, dataset: str, columns: tuple[str, ...]) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"{dataset} is missing required columns: {', '.join(missing)}")


def validate_row_count(row_count: int, dataset: str, layer: str) -> None:
    if row_count == 0:
        raise ValueError(f"{layer} input is empty: {dataset}")


def validate_source_quality(df: DataFrame, dataset: str) -> None:
    for column in REQUIRED_VALUE_COLUMNS.get(dataset, ()):
        require_columns(df, dataset, (column,))
        if df.filter(F.col(column).isNull()).limit(1).count():
            raise ValueError(f"{dataset}.{column} contains null values")

    for (rule_dataset, column), accepted in ACCEPTED_VALUES.items():
        if rule_dataset != dataset:
            continue
        require_columns(df, dataset, (column,))
        invalid = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(*accepted)).limit(1).count()
        if invalid:
            raise ValueError(f"{dataset}.{column} contains an unsupported value")

    for column in NONNEGATIVE_COLUMNS.get(dataset, ()):
        if df.filter(F.col(column) < 0).limit(1).count():
            raise ValueError(f"{dataset}.{column} contains a negative value")


def normalize_missing_values(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            trimmed = F.trim(F.col(field.name))
            df = df.withColumn(field.name, F.when(trimmed == "", F.lit(None)).otherwise(trimmed))
    return df


def cast_columns(df: DataFrame, dataset: str) -> DataFrame:
    for column in TIMESTAMP_COLUMNS.get(dataset, ()):
        require_columns(df, dataset, (column,))
        value = F.to_timestamp(F.col(column))
        invalid = df.filter(F.col(column).isNotNull() & value.isNull()).limit(1).count()
        if invalid:
            raise ValueError(f"{dataset}.{column} contains an invalid timestamp")
        df = df.withColumn(column, value)

    for column in DECIMAL_COLUMNS.get(dataset, ()):
        require_columns(df, dataset, (column,))
        value = F.col(column).cast(DECIMAL_TYPE)
        invalid = df.filter(F.col(column).isNotNull() & value.isNull()).limit(1).count()
        if invalid:
            raise ValueError(f"{dataset}.{column} contains an invalid decimal")
        df = df.withColumn(column, value)
    return df


def clean_dataset(df: DataFrame, dataset: str) -> DataFrame:
    df = normalize_missing_values(df)
    df = cast_columns(df, dataset)
    df = df.dropDuplicates()
    if dataset in PRIMARY_KEYS:
        require_columns(df, dataset, PRIMARY_KEYS[dataset])
    return df


def validate_primary_key(df: DataFrame, dataset: str, columns: tuple[str, ...]) -> None:
    require_columns(df, dataset, columns)
    null_key = None
    for column in columns:
        expression = F.col(column).isNull()
        null_key = expression if null_key is None else null_key | expression
    if df.filter(null_key).limit(1).count():
        raise ValueError(f"{dataset} has null values in primary key ({', '.join(columns)})")

    duplicates = df.groupBy(*columns).count().filter(F.col("count") > 1).limit(1).count()
    if duplicates:
        raise ValueError(f"{dataset} has duplicate primary key values ({', '.join(columns)})")


def validate_foreign_keys(datasets: dict[str, DataFrame]) -> None:
    for child_name, child_column, parent_name, parent_column in FOREIGN_KEYS:
        child = datasets[child_name]
        parent = datasets[parent_name]
        require_columns(child, child_name, (child_column,))
        require_columns(parent, parent_name, (parent_column,))
        parent_keys = parent.select(F.col(parent_column).alias("_parent_key")).distinct()
        unmatched = (
            child.alias("_child")
            .join(
                parent_keys.alias("_parent"),
                F.col(f"_child.{child_column}") == F.col("_parent._parent_key"),
                "left_anti",
            )
            .limit(1)
            .count()
        )
        if unmatched:
            raise ValueError(
                f"{child_name}.{child_column} contains values missing from {parent_name}.{parent_column}"
            )


def storage_root(container: str) -> str:
    account = os.getenv("AZURE_STORAGE_ACCOUNT", "storageemulator")
    if account == "storageemulator":
        return f"wasb://{container}@storageemulator"
    return f"wasbs://{container}@{account}.blob.core.windows.net"


def main() -> int:
    args = parse_args()
    batch_date = validate_batch_date(args.batch_date)
    root = storage_root(args.container)
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    try:
        cleaned: dict[str, DataFrame] = {}
        row_counts: dict[str, tuple[int, int]] = {}
        for dataset in DATASETS:
            input_path = f"{root}/{args.input_prefix}/{dataset}/batch_date={batch_date}"
            df = spark.read.parquet(input_path)
            source_rows = df.count()
            validate_row_count(source_rows, dataset, "Bronze")
            cleaned[dataset] = clean_dataset(df, dataset)
            silver_rows = cleaned[dataset].count()
            validate_row_count(silver_rows, dataset, "Silver")
            row_counts[dataset] = (source_rows, silver_rows)

        for dataset, columns in PRIMARY_KEYS.items():
            validate_primary_key(cleaned[dataset], dataset, columns)
        for dataset, df in cleaned.items():
            validate_source_quality(df, dataset)
        validate_foreign_keys(cleaned)

        for dataset, (source_rows, silver_rows) in row_counts.items():
            print(
                f"QUALITY dataset={dataset} bronze_rows={source_rows} "
                f"silver_rows={silver_rows} rows_removed_by_deduplication={source_rows - silver_rows}"
            )

        for dataset, df in cleaned.items():
            output_path = f"{root}/{args.output_prefix}/{dataset}/batch_date={batch_date}"
            df.write.mode("overwrite").format("parquet").save(output_path)
            print(f"SUCCESS dataset={dataset} output={output_path}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
