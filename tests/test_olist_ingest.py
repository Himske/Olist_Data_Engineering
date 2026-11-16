import importlib.util
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def ingest(monkeypatch):
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = MagicMock()
    functions = types.ModuleType("pyspark.sql.functions")
    functions.current_timestamp = MagicMock()
    functions.input_file_name = MagicMock()
    functions.lit = MagicMock()
    with monkeypatch.context() as mocks:
        mocks.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
        mocks.setitem(sys.modules, "pyspark.sql", sql)
        mocks.setitem(sys.modules, "pyspark.sql.functions", functions)
        spec = importlib.util.spec_from_file_location(
            "olist_ingest", Path(__file__).resolve().parents[1] / "spark" / "olist_ingest.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


@pytest.fixture
def spark_mocks(ingest):
    spark_session = MagicMock()
    builder = MagicMock()
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = spark_session
    ingest.SparkSession.builder = builder
    return builder, spark_session


def test_cloud_requires_key(ingest, spark_mocks):
    builder, _ = spark_mocks
    with patch.dict(os.environ, {"AZURE_STORAGE_ACCOUNT": "example"}, clear=True), pytest.raises(
        ValueError, match="AZURE_STORAGE_KEY"
    ):
        ingest.build_spark()
    builder.getOrCreate.assert_not_called()


@pytest.mark.parametrize(
    ("environment", "root"),
    [
        ({}, "wasb://olist-data@storageemulator"),
        (
            {"AZURE_STORAGE_ACCOUNT": "example", "AZURE_STORAGE_KEY": "test-key",
             "AZURE_STORAGE_CONTAINER": "custom"},
            "wasbs://custom@example.blob.core.windows.net",
        ),
    ],
    ids=["azurite", "azure"],
)
def test_storage_paths_and_cloud_credentials(ingest, spark_mocks, tmp_path, monkeypatch, environment, root):
    builder, spark_session = spark_mocks
    (tmp_path / "olist_orders_dataset.csv").write_text("order_id\n1\n")
    monkeypatch.setattr(
        sys, "argv", ["olist_ingest.py", "--dataset", "orders", "--input-dir", str(tmp_path),
                      "--batch-date", "2018-10-17"]
    )
    frame = spark_session.read.option.return_value.option.return_value.option.return_value.option.return_value.csv.return_value
    frame.columns = ["order_id"]
    frame.withColumn.return_value = frame

    with patch.dict(os.environ, environment, clear=True):
        assert ingest.main() == 0

    frame.write.mode.return_value.format.return_value.save.assert_called_once_with(
        f"{root}/bronze/orders/batch_date=2018-10-17"
    )
    spark_session.stop.assert_called_once()
    if "AZURE_STORAGE_KEY" in environment:
        builder.config.assert_any_call(
            "spark.hadoop.fs.azure.account.key.example.blob.core.windows.net", "test-key"
        )
