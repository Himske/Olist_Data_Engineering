import importlib.util
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


sql = types.ModuleType("pyspark.sql")
sql.SparkSession = MagicMock()
functions = types.ModuleType("pyspark.sql.functions")
functions.current_timestamp = MagicMock()
functions.input_file_name = MagicMock()
functions.lit = MagicMock()

with patch.dict(sys.modules, {
    "pyspark": types.ModuleType("pyspark"),
    "pyspark.sql": sql,
    "pyspark.sql.functions": functions,
}):
    spec = importlib.util.spec_from_file_location(
        "olist_ingest", Path(__file__).resolve().parents[1] / "spark" / "olist_ingest.py"
    )
    ingest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ingest)


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.spark_session = MagicMock()
        self.builder = MagicMock()
        self.builder.appName.return_value = self.builder
        self.builder.config.return_value = self.builder
        self.builder.getOrCreate.return_value = self.spark_session
        ingest.SparkSession.builder = self.builder

    def test_cloud_requires_key(self):
        with patch.dict(os.environ, {"AZURE_STORAGE_ACCOUNT": "example"}, clear=True):
            with self.assertRaisesRegex(ValueError, "AZURE_STORAGE_KEY"):
                ingest.build_spark()
            self.builder.getOrCreate.assert_not_called()

    def test_storage_paths_and_cloud_credentials(self):
        cases = [
            ({}, "wasb://olist-data@storageemulator"),
            (
                {"AZURE_STORAGE_ACCOUNT": "example", "AZURE_STORAGE_KEY": "test-key",
                 "AZURE_STORAGE_CONTAINER": "custom"},
                "wasbs://custom@example.blob.core.windows.net",
            ),
        ]
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "olist_orders_dataset.csv").write_text("order_id\n1\n")
            for environment, root in cases:
                with self.subTest(root=root), patch.dict(os.environ, environment, clear=True), patch.object(
                    sys, "argv", ["olist_ingest.py", "--dataset", "orders", "--input-dir", directory,
                                  "--batch-date", "2018-10-17"]
                ):
                    self.builder.reset_mock()
                    self.spark_session.reset_mock()
                    frame = self.spark_session.read.option.return_value.option.return_value.option.return_value.option.return_value.csv.return_value
                    frame.columns = ["order_id"]
                    frame.withColumn.return_value = frame
                    self.assertEqual(ingest.main(), 0)
                    frame.write.mode.return_value.format.return_value.save.assert_called_once_with(
                        f"{root}/bronze/orders/batch_date=2018-10-17"
                    )
                    self.spark_session.stop.assert_called_once()
                    if "AZURE_STORAGE_KEY" in environment:
                        self.builder.config.assert_any_call(
                            "spark.hadoop.fs.azure.account.key.example.blob.core.windows.net", "test-key"
                        )


if __name__ == "__main__":
    unittest.main()
