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

    def test_sector_radar_separates_leadership_risk_and_unknown(self) -> None:
        dates = [f"2026-07-{day:02d}" for day in range(20, 25)]
        history_rows: list[dict[str, object]] = []
        for date in dates[:-1]:
            history_rows.extend([
                {
                    "date": date,
                    "ticker": "BANK.T",
                    "code": "BANK",
                    "stock_name": "Bank",
                    "sectors": "銀行業",
                    "rank": 10,
                    "trading_value_billion": 10.0,
                    "open_to_close_pct": 1.0,
                    "is_etf": False,
                },
                {
                    "date": date,
                    "ticker": "ELEC.T",
                    "code": "ELEC",
                    "stock_name": "Electric",
                    "sectors": "電気機器",
                    "rank": 20,
                    "trading_value_billion": 10.0,
                    "open_to_close_pct": -1.0,
                    "is_etf": False,
                },
            ])

        latest = pd.DataFrame([
            {
                "date": dates[-1],
                "ticker": "BANK.T",
                "code": "BANK",
                "stock_name": "Bank",
                "sectors": "銀行業",
                "rank": 5,
                "trading_value_billion": 25.0,
                "open_to_close_pct": 2.0,
                "is_etf": False,
                "is_new_top150": False,
                "rank_change": 5.0,
            },
            {
                "date": dates[-1],
                "ticker": "ELEC.T",
                "code": "ELEC",
                "stock_name": "Electric",
                "sectors": "電気機器",
                "rank": 8,
                "trading_value_billion": 25.0,
                "open_to_close_pct": -3.0,
                "is_etf": False,
                "is_new_top150": False,
                "rank_change": 2.0,
            },
            {
                "date": dates[-1],
                "ticker": "UNKNOWN.T",
                "code": "UNKNOWN",
                "stock_name": "Unknown",
                "sectors": "UNKNOWN",
                "rank": 30,
                "trading_value_billion": 5.0,
                "open_to_close_pct": -9.0,
                "is_etf": False,
                "is_new_top150": True,
                "rank_change": None,
            },
        ])
        history = pd.concat([
            pd.DataFrame(history_rows),
            latest.drop(columns=["is_new_top150", "rank_change"]),
        ], ignore_index=True)

        radar = dev_market_flow._sector_radar(latest, history, dates[-1])
        by_sector = {row["sector"]: row for row in radar["rows"]}

        self.assertTrue(radar["available"])
        self.assertEqual(by_sector["銀行業"]["status"], "leader")
        self.assertEqual(by_sector["電気機器"]["status"], "risk")
        self.assertNotIn("UNKNOWN", by_sector)
        self.assertEqual(radar["coverage"]["unclassified_count"], 1)

    def test_market_direction_uses_broad_market_components(self) -> None:
        latest = pd.DataFrame([
            {
                "rank": rank,
                "trading_value_billion": 10.0,
                "open_to_close_pct": 1.0,
                "is_etf": False,
                "sectors": "銀行業" if rank % 2 else "鉄鋼",
            }
            for rank in range(1, 31)
        ])
        sector_radar = {
            "available": True,
            "coverage": {"classified_turnover_pct": 100.0},
            "rows": [
                {"sector": "銀行業", "avg_open_to_close_pct": 1.0},
                {"sector": "鉄鋼", "avg_open_to_close_pct": 1.5},
            ],
        }
        metric = {
            "ret1_pct": 1.0,
            "ret5_pct": 2.0,
            "ret20_pct": 4.0,
            "vs_sma20_pct": 2.0,
        }

        with patch.object(dev_market_flow, "_index_metric", return_value=metric):
            direction = dev_market_flow._market_direction(latest, sector_radar)

        self.assertGreater(direction["score"], 20.0)
        self.assertEqual(direction["label"], "強気")
        self.assertEqual(
            [component["key"] for component in direction["components"]],
            ["index", "breadth", "sector"],
        )


if __name__ == "__main__":
    unittest.main()
