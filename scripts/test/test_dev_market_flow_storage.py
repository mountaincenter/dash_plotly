from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from server.routers import dev_market_flow


class DevMarketFlowStorageTest(unittest.TestCase):
    def test_development_reads_local_without_s3_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.parquet"
            pd.DataFrame({"value": [1]}).to_parquet(path, index=False)

            with (
                patch.object(dev_market_flow, "USE_S3_DATA", False),
                patch.object(dev_market_flow, "DATA_SOURCE_MODE", "local"),
                patch.object(
                    dev_market_flow,
                    "_read_s3_cached",
                    side_effect=AssertionError("development must not call S3"),
                ) as s3_read,
            ):
                loaded = dev_market_flow._read_optional_parquet(path)
                missing = dev_market_flow._read_optional_parquet(
                    path.with_name("missing.parquet")
                )

            self.assertEqual(loaded["value"].tolist(), [1])
            self.assertIsNone(missing)
            s3_read.assert_not_called()

    def test_production_reads_s3_even_when_local_file_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.parquet"
            pd.DataFrame({"local": [1]}).to_parquet(path, index=False)
            s3_frame = pd.DataFrame({"s3": [2]})

            with (
                patch.object(dev_market_flow, "USE_S3_DATA", True),
                patch.object(dev_market_flow, "DATA_SOURCE_MODE", "s3"),
                patch.object(dev_market_flow, "S3_BUCKET", "test-bucket"),
                patch.object(
                    dev_market_flow,
                    "_read_s3_cached",
                    return_value=s3_frame,
                ) as s3_read,
                patch.object(
                    dev_market_flow.pd,
                    "read_parquet",
                    side_effect=AssertionError("production must not read local parquet"),
                ) as local_read,
            ):
                loaded = dev_market_flow._read_optional_parquet(path)

            self.assertEqual(loaded["s3"].tolist(), [2])
            s3_read.assert_called_once_with(path, kind="parquet")
            local_read.assert_not_called()

    def test_source_token_follows_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.parquet"
            path.write_bytes(b"local")

            with patch.object(dev_market_flow, "USE_S3_DATA", False):
                self.assertTrue(dev_market_flow._source_token(path).startswith("local:"))

            client = Mock()
            client.head_object.return_value = {
                "ETag": "etag",
                "ContentLength": 10,
            }
            with (
                patch.object(dev_market_flow, "USE_S3_DATA", True),
                patch.object(dev_market_flow, "S3_BUCKET", "test-bucket"),
                patch.object(dev_market_flow, "_s3_client", return_value=client),
            ):
                token = dev_market_flow._source_token(path)

            self.assertTrue(token.startswith("s3:"))
            client.head_object.assert_called_once_with(
                Bucket="test-bucket",
                Key=dev_market_flow._s3_key(path.name),
            )


if __name__ == "__main__":
    unittest.main()
