import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest


class FakeStringType:
    pass


@pytest.fixture
def silver(monkeypatch):
    sql = types.ModuleType("pyspark.sql")
    sql.DataFrame = type("DataFrame", (), {})
    sql.SparkSession = MagicMock()
    functions = types.ModuleType("pyspark.sql.functions")
    for name in ("col", "current_timestamp", "input_file_name", "lit", "to_timestamp", "trim", "when"):
        setattr(functions, name, MagicMock())
    functions.col.return_value.__gt__.return_value = MagicMock()
    functions.col.return_value.__lt__.return_value = MagicMock()
    spark_types = types.ModuleType("pyspark.sql.types")
    spark_types.DecimalType = lambda precision, scale: ("decimal", precision, scale)
    spark_types.StringType = FakeStringType

    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", spark_types)

    spark_dir = Path(__file__).resolve().parents[1] / "spark"
    monkeypatch.syspath_prepend(str(spark_dir))
    spec = importlib.util.spec_from_file_location("olist_silver", spark_dir / "olist_silver.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, functions


def _order_item_frame(silver):
    module, _ = silver
    names = (
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    )
    frame = MagicMock()
    frame.columns = list(names)
    frame.schema.fields = [SimpleNamespace(name=name, dataType=FakeStringType()) for name in names]
    frame.withColumn.return_value = frame
    frame.filter.return_value.limit.return_value.count.return_value = 0
    frame.dropDuplicates.return_value = frame
    return module, frame


def test_clean_dataset_normalizes_casts_and_deduplicates(silver):
    module, functions = silver
    _, frame = _order_item_frame(silver)

    assert module.clean_dataset(frame, "order_items") is frame

    assert frame.withColumn.call_args_list[0].args[0] == "order_id"
    assert call("shipping_limit_date", functions.to_timestamp.return_value) in frame.withColumn.call_args_list
    assert call("price", functions.col.return_value.cast.return_value) in frame.withColumn.call_args_list
    frame.dropDuplicates.assert_called_once_with()


def test_clean_dataset_fails_on_invalid_timestamp(silver):
    module, _ = silver
    _, frame = _order_item_frame(silver)
    frame.filter.return_value.limit.return_value.count.return_value = 1

    with pytest.raises(ValueError, match="invalid timestamp"):
        module.clean_dataset(frame, "order_items")

    frame.dropDuplicates.assert_not_called()


def test_primary_key_validation_fails_on_duplicates(silver):
    module, functions = silver
    frame = MagicMock()
    frame.columns = ["order_id"]
    frame.filter.return_value.limit.return_value.count.return_value = 0
    frame.groupBy.return_value.count.return_value.filter.return_value.limit.return_value.count.return_value = 1

    with pytest.raises(ValueError, match="duplicate primary key"):
        module.validate_primary_key(frame, "orders", ("order_id",))

    frame.groupBy.assert_called_once_with("order_id")
    functions.col.assert_any_call("count")


def test_foreign_key_validation_fails_on_unmatched_values(silver):
    module, _ = silver
    child = MagicMock()
    parent = MagicMock()
    child.columns = ["customer_id"]
    parent.columns = ["customer_id"]
    child.alias.return_value.join.return_value.limit.return_value.count.return_value = 1

    with pytest.raises(ValueError, match="missing from"):
        module.validate_foreign_keys({"orders": child, "customers": parent})

    child.alias.return_value.join.assert_called_once()
    assert child.alias.return_value.join.call_args.args[2] == "left_anti"


def test_storage_root_keeps_local_and_cloud_wasb_urls(silver, monkeypatch):
    module, _ = silver
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
    assert module.storage_root("olist-data") == "wasb://olist-data@storageemulator"

    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "example")
    assert module.storage_root("custom") == "wasbs://custom@example.blob.core.windows.net"


def test_cloud_storage_requires_a_key(silver, monkeypatch):
    module, _ = silver
    builder = MagicMock()
    builder.appName.return_value.config.return_value = builder
    monkeypatch.setattr(module.SparkSession, "builder", builder)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "example")
    monkeypatch.delenv("AZURE_STORAGE_KEY", raising=False)

    with pytest.raises(ValueError, match="AZURE_STORAGE_KEY"):
        module.build_spark()

    builder.getOrCreate.assert_not_called()


@pytest.mark.parametrize(
    ("row_count", "dataset", "layer"),
    [(1, "orders", "Bronze"), (0, "orders", "Bronze"), (0, "orders", "Silver")],
)
def test_validate_row_count(silver, row_count, dataset, layer):
    module, _ = silver

    if row_count:
        module.validate_row_count(row_count, dataset, layer)
    else:
        with pytest.raises(ValueError, match=f"{layer} input is empty"):
            module.validate_row_count(row_count, dataset, layer)


def test_source_quality_rejects_null_required_values(silver):
    module, functions = silver
    frame = MagicMock()
    frame.columns = ["payment_type", "payment_value"]
    frame.filter.return_value.limit.return_value.count.return_value = 1

    with pytest.raises(ValueError, match="order_payments.payment_type contains null"):
        module.validate_source_quality(frame, "order_payments")

    functions.col.assert_called_with("payment_type")


def test_source_quality_rejects_unsupported_status(silver):
    module, functions = silver
    frame = MagicMock()
    frame.columns = ["order_status", "order_purchase_timestamp"]
    frame.filter.return_value.limit.return_value.count.side_effect = [0, 0, 1]

    with pytest.raises(ValueError, match="orders.order_status contains an unsupported"):
        module.validate_source_quality(frame, "orders")

    functions.col.assert_any_call("order_status")


def test_source_quality_rejects_negative_amounts(silver):
    module, _ = silver
    frame = MagicMock()
    frame.columns = ["price", "freight_value"]
    frame.filter.return_value.limit.return_value.count.side_effect = [0, 0, 1]

    with pytest.raises(ValueError, match="order_items.price contains a negative"):
        module.validate_source_quality(frame, "order_items")
