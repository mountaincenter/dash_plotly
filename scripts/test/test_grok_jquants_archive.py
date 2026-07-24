from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from common_cfg.s3cfg import S3Config
from scripts.lib.grok_jquants_backtest import (
    JQuantsBacktestDataError,
    assert_archive_history_unchanged,
    assert_archive_target_rows_preserved,
    calculate_segment_pnl,
    has_trade_after_entry,
    merge_archive_date,
    normalize_daily_prices,
    normalize_minute_bars,
    session_last_close,
    validate_daily_alignment,
    validate_selection_asof,
)
from scripts.lib.protected_archive_s3 import (
    ProtectedArchiveError,
    download_verified_archive,
    publish_guarded_archive,
    publish_guarded_manifest_entry,
    verify_publish_state,
)
from scripts.pipeline import update_manifest
from scripts.pipeline.save_backtest_to_archive import (
    NO_MARKET_TRADE_DATA_SOURCE,
    build_no_market_trade_backtest_data,
    calculate_phase3_return,
    confirm_no_market_trade,
    validate_batch_coverage,
    validate_result_batch,
)


def minute_frame(times: list[str], prices: list[tuple[float, float, float, float]]) -> pd.DataFrame:
    rows = []
    for value, (clock, ohlc) in enumerate(zip(times, prices), start=1):
        open_, high, low, close = ohlc
        rows.append(
            {
                "ticker": "1234.T",
                "datetime": pd.Timestamp(f"2026-07-10 {clock}"),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 100.0,
                "value": float(value * 1000),
            }
        )
    return pd.DataFrame(rows)


class JQuantsExecutionTests(unittest.TestCase):
    def test_confirm_no_market_trade_requires_null_daily_and_empty_minute(self) -> None:
        class Client:
            def request(self, endpoint: str, params: dict[str, str]):
                if endpoint == "/equities/bars/daily":
                    return {
                        "data": [
                            {
                                "Date": "2026-07-10",
                                "Code": "68980",
                                "O": None,
                                "H": None,
                                "L": None,
                                "C": None,
                                "Vo": None,
                            }
                        ]
                    }
                if endpoint == "/equities/bars/minute":
                    return {"data": []}
                raise AssertionError(endpoint)

        with patch(
            "scripts.pipeline.save_backtest_to_archive.get_jquants_client",
            return_value=Client(),
        ):
            self.assertTrue(
                confirm_no_market_trade("6898.T", pd.Timestamp("2026-07-10"))
            )

    def test_confirm_no_market_trade_rejects_valid_price_or_minute(self) -> None:
        class Client:
            def __init__(
                self,
                *,
                daily_price: int | None,
                minute_rows: list[dict] | None = None,
            ) -> None:
                self.daily_price = daily_price
                self.minute_rows = minute_rows or []

            def request(self, endpoint: str, params: dict[str, str]):
                if endpoint == "/equities/bars/daily":
                    return {
                        "data": [
                            {
                                "Date": "2026-07-10",
                                "Code": "68980",
                                "O": self.daily_price,
                                "H": self.daily_price,
                                "L": self.daily_price,
                                "C": self.daily_price,
                                "Vo": (
                                    100 if self.daily_price is not None else None
                                ),
                            }
                        ]
                    }
                return {"data": self.minute_rows}

        with patch(
            "scripts.pipeline.save_backtest_to_archive.get_jquants_client",
            return_value=Client(daily_price=3650),
        ):
            self.assertFalse(
                confirm_no_market_trade("6898.T", pd.Timestamp("2026-07-10"))
            )
        with patch(
            "scripts.pipeline.save_backtest_to_archive.get_jquants_client",
            return_value=Client(
                daily_price=None,
                minute_rows=[{"Date": "2026-07-10", "Code": "68980"}],
            ),
        ):
            self.assertFalse(
                confirm_no_market_trade("6898.T", pd.Timestamp("2026-07-10"))
            )

    def test_batch_coverage_allows_only_confirmed_symmetric_gap(self) -> None:
        selection = pd.DataFrame([{"ticker": "1234.T"}, {"ticker": "6898.T"}])
        minute = pd.DataFrame(
            [
                {
                    "ticker": "1234.T",
                    "datetime": pd.Timestamp("2026-07-10 09:00"),
                }
            ]
        )
        daily = normalize_daily_prices(
            pd.DataFrame(
                [
                    {
                        "date": "2026-07-10",
                        "ticker": "1234.T",
                        "Open": 100,
                        "High": 100,
                        "Low": 100,
                        "Close": 100,
                        "Volume": 100,
                    }
                ]
            )
        )
        with (
            patch(
                "scripts.pipeline.save_backtest_to_archive.load_jquants_minute",
                return_value=minute,
            ),
            patch(
                "scripts.pipeline.save_backtest_to_archive.load_jquants_daily",
                return_value=daily,
            ),
            patch(
                "scripts.pipeline.save_backtest_to_archive.confirm_no_market_trade",
                return_value=True,
            ) as confirm,
        ):
            result = validate_batch_coverage(
                selection,
                pd.Timestamp("2026-07-10"),
            )
        self.assertEqual(result, {"6898.T"})
        confirm.assert_called_once()

        minute_with_6898 = pd.concat(
            [
                minute,
                pd.DataFrame(
                    [
                        {
                            "ticker": "6898.T",
                            "datetime": pd.Timestamp("2026-07-10 15:30"),
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
        with (
            patch(
                "scripts.pipeline.save_backtest_to_archive.load_jquants_minute",
                return_value=minute_with_6898,
            ),
            patch(
                "scripts.pipeline.save_backtest_to_archive.load_jquants_daily",
                return_value=daily,
            ),
            self.assertRaises(JQuantsBacktestDataError),
        ):
            validate_batch_coverage(selection, pd.Timestamp("2026-07-10"))

    def test_no_market_trade_row_keeps_prices_and_pnl_null(self) -> None:
        daily = normalize_daily_prices(
            pd.DataFrame(
                [
                    {
                        "date": "2026-07-09",
                        "ticker": "6898.T",
                        "Open": 3600,
                        "High": 3650,
                        "Low": 3600,
                        "Close": 3650,
                        "Volume": 300,
                    }
                ]
            )
        )
        with patch(
            "scripts.pipeline.save_backtest_to_archive.load_jquants_daily",
            return_value=daily,
        ):
            row = build_no_market_trade_backtest_data(
                "6898.T",
                pd.Timestamp("2026-07-10"),
            )
        frame = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-07-10",
                    "ticker": "6898.T",
                    **row,
                }
            ]
        )
        self.assertEqual(
            validate_result_batch(frame, "2026-07-10"),
            "2026-07-10",
        )
        self.assertEqual(frame.iloc[0]["data_source"], NO_MARKET_TRADE_DATA_SOURCE)
        self.assertEqual(frame.iloc[0]["prev_close"], 3650)
        self.assertIsNone(frame.iloc[0]["buy_price"])
        self.assertIsNone(frame.iloc[0]["phase2_return"])

        with self.assertRaises(JQuantsBacktestDataError):
            validate_result_batch(
                frame.assign(buy_price=3650),
                "2026-07-10",
            )

    def test_selection_asof_must_predate_target(self) -> None:
        selection = pd.DataFrame(
            [
                {
                    "date": "2026-07-10",
                    "ticker": "1234.T",
                    "price_asof_date": "2026-07-09",
                    "price_source_date": "2026-07-09",
                }
            ]
        )
        validate_selection_asof(selection, "2026-07-10")
        with self.assertRaises(JQuantsBacktestDataError):
            validate_selection_asof(
                selection.assign(price_source_date="2026-07-10"),
                "2026-07-10",
            )

    def test_daily_alignment_and_next_executable_open(self) -> None:
        raw = minute_frame(
            ["09:00", "09:32", "11:30", "15:30"],
            [
                (100.0, 101.0, 99.0, 100.5),
                (102.0, 103.0, 101.0, 102.5),
                (98.0, 99.0, 97.0, 98.5),
                (95.0, 96.0, 94.0, 95.0),
            ],
        )
        bars = normalize_minute_bars(raw, "1234.T", "2026-07-10")
        daily = normalize_daily_prices(
            pd.DataFrame(
                [
                    {
                        "date": "2026-07-09",
                        "ticker": "1234.T",
                        "Open": 99.0,
                        "High": 100.0,
                        "Low": 98.0,
                        "Close": 99.5,
                        "Volume": 300.0,
                    },
                    {
                        "date": "2026-07-10",
                        "ticker": "1234.T",
                        "Open": 100.0,
                        "High": 103.0,
                        "Low": 94.0,
                        "Close": 95.0,
                        "Volume": 400.0,
                    },
                ]
            )
        )
        aggregate, previous_close = validate_daily_alignment(
            bars, daily, "1234.T", "2026-07-10"
        )
        self.assertEqual(aggregate["Open"], 100.0)
        self.assertEqual(previous_close, 99.5)
        segments = calculate_segment_pnl(bars, 100.0)
        self.assertEqual(segments["seg_0930"], -200.0)
        self.assertEqual(segments["seg_1130"], 200.0)
        self.assertEqual(segments["seg_1530"], 500.0)

    def test_delayed_open_requires_entry_before_exit_target(self) -> None:
        raw = minute_frame(
            ["09:30", "10:00", "15:30"],
            [
                (100.0, 100.0, 100.0, 100.0),
                (99.0, 99.0, 99.0, 99.0),
                (98.0, 98.0, 98.0, 98.0),
            ],
        )
        bars = normalize_minute_bars(raw, "1234.T", "2026-07-10")
        segments = calculate_segment_pnl(bars, 100.0)
        self.assertIsNone(segments["seg_0930"])
        self.assertEqual(segments["seg_1000"], 100.0)

    def test_single_timestamp_keeps_close_mark_and_flags_no_round_trip(self) -> None:
        raw = minute_frame(
            ["15:30"],
            [(100.0, 100.0, 100.0, 100.0)],
        )
        bars = normalize_minute_bars(raw, "1234.T", "2026-07-10")
        segments = calculate_segment_pnl(bars, 100.0)
        self.assertFalse(has_trade_after_entry(bars))
        for name, value in segments.items():
            if name == "seg_1530":
                self.assertEqual(value, 0.0)
            else:
                self.assertIsNone(value)
        self.assertIsNone(session_last_close(bars, "09:00", "11:30"))
        self.assertEqual(
            calculate_phase3_return(bars, 100.0, 0.01, -0.01),
            (0.0, False, "hold_until_close"),
        )

    def test_no_trade_at_or_after_target_is_missing(self) -> None:
        raw = minute_frame(
            ["09:00", "09:20"],
            [
                (100.0, 100.0, 100.0, 100.0),
                (99.0, 99.0, 99.0, 99.0),
            ],
        )
        bars = normalize_minute_bars(raw, "1234.T", "2026-07-10")
        self.assertIsNone(calculate_segment_pnl(bars, 100.0)["seg_0930"])

    def test_daily_mismatch_fails_closed(self) -> None:
        raw = minute_frame(
            ["09:00", "15:30"],
            [
                (100.0, 101.0, 99.0, 100.0),
                (100.0, 101.0, 99.0, 100.0),
            ],
        )
        bars = normalize_minute_bars(raw, "1234.T", "2026-07-10")
        daily = normalize_daily_prices(
            pd.DataFrame(
                [
                    {
                        "date": "2026-07-09",
                        "ticker": "1234.T",
                        "Open": 100,
                        "High": 100,
                        "Low": 100,
                        "Close": 100,
                        "Volume": 100,
                    },
                    {
                        "date": "2026-07-10",
                        "ticker": "1234.T",
                        "Open": 105,
                        "High": 105,
                        "Low": 105,
                        "Close": 105,
                        "Volume": 200,
                    },
                ]
            )
        )
        with self.assertRaises(JQuantsBacktestDataError):
            validate_daily_alignment(bars, daily, "1234.T", "2026-07-10")

    def test_archive_merge_replaces_only_latest_date(self) -> None:
        archive = pd.DataFrame(
            [
                {"backtest_date": "2026-07-09", "ticker": "1111.T", "value": 1},
                {"backtest_date": "2026-07-10", "ticker": "2222.T", "value": 2},
            ]
        )
        new = pd.DataFrame(
            [{"backtest_date": "2026-07-10", "ticker": "3333.T", "value": 3}]
        )
        merged = merge_archive_date(archive, new, "2026-07-10")
        self.assertEqual(merged.to_dict("records"), [archive.iloc[0].to_dict(), new.iloc[0].to_dict()])
        assert_archive_history_unchanged(archive, merged, "2026-07-10")
        assert_archive_target_rows_preserved(new, merged, "2026-07-10")
        changed = merged.copy()
        changed.loc[0, "value"] = 999
        with self.assertRaises(JQuantsBacktestDataError):
            assert_archive_history_unchanged(archive, changed, "2026-07-10")
        with self.assertRaises(JQuantsBacktestDataError):
            merge_archive_date(archive, new.assign(backtest_date="2026-07-09"), "2026-07-09")

    def test_archive_guards_allow_only_missing_sentinel_round_trip(self) -> None:
        source = pd.DataFrame(
            {
                "backtest_date": ["2026-07-09", "2026-07-10"],
                "ticker": ["1111.T", "2222.T"],
                "phase1_win": pd.Series([pd.NA, True], dtype="boolean"),
            }
        )
        candidate = source.copy()
        candidate["phase1_win"] = candidate["phase1_win"].astype(object)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "candidate.parquet"
            candidate.to_parquet(path, index=False)
            reloaded = pd.read_parquet(path)

        self.assertIsNone(reloaded.loc[0, "phase1_win"])
        assert_archive_history_unchanged(source, reloaded, "2026-07-11")
        assert_archive_target_rows_preserved(
            source.iloc[[0]].reset_index(drop=True), reloaded, "2026-07-09"
        )

        changed_missing = reloaded.copy()
        changed_missing.loc[0, "phase1_win"] = False
        with self.assertRaises(JQuantsBacktestDataError):
            assert_archive_history_unchanged(source, changed_missing, "2026-07-11")

        changed_value = reloaded.copy()
        changed_value.loc[1, "phase1_win"] = False
        with self.assertRaises(JQuantsBacktestDataError):
            assert_archive_history_unchanged(source, changed_value, "2026-07-11")


class FakeS3:
    def __init__(self, archive_key: str, archive: bytes, manifest_key: str) -> None:
        self.archive_key = archive_key
        self.manifest_key = manifest_key
        self.archive = archive
        self.version = "v1"
        self.metadata: dict[str, str] = {}
        self.checksum: str | None = None
        self.etag = self._etag(archive)
        sha = hashlib.sha256(archive).hexdigest()
        manifest = {
            "files": {
                "backtest/grok_trending_archive.parquet": {
                    "protected": True,
                    "canonical": True,
                    "sha256": sha,
                    "s3_etag": self.etag,
                    "s3_version_id": self.version,
                }
            }
        }
        self.manifest = json.dumps(manifest).encode()
        self.manifest_etag = self._etag(self.manifest)
        self.manifest_version = "m1"
        self.get_requests: list[tuple[str, dict]] = []

    @staticmethod
    def _etag(payload: bytes) -> str:
        return f'"{hashlib.md5(payload).hexdigest()}"'  # nosec: S3 ETag fixture

    def head_object(self, Bucket: str, Key: str, **kwargs):
        if Key == self.archive_key:
            return {
                "ETag": self.etag,
                "VersionId": self.version,
                "Metadata": self.metadata,
                "ChecksumSHA256": self.checksum,
            }
        return {
            "ETag": self.manifest_etag,
            "VersionId": self.manifest_version,
            "ChecksumSHA256": None,
        }

    def get_object(self, Bucket: str, Key: str, **kwargs):
        self.get_requests.append((Key, dict(kwargs)))
        if "VersionId" in kwargs:
            raise PermissionError("s3:GetObjectVersion is not available")
        expected_etag = self.etag if Key == self.archive_key else self.manifest_etag
        if kwargs.get("IfMatch") != expected_etag:
            raise RuntimeError("object precondition failed")
        payload = self.archive if Key == self.archive_key else self.manifest
        return {"Body": io.BytesIO(payload)}

    def put_object(self, Bucket: str, Key: str, Body, **kwargs):
        payload = Body.read() if hasattr(Body, "read") else Body
        if Key == self.manifest_key:
            if kwargs.get("IfMatch") != self.manifest_etag:
                raise RuntimeError("manifest precondition failed")
            self.manifest = payload
            self.manifest_etag = self._etag(payload)
            self.manifest_version = "m2"
            return {
                "ETag": self.manifest_etag,
                "VersionId": self.manifest_version,
                "ChecksumSHA256": kwargs["ChecksumSHA256"],
            }
        if kwargs.get("IfMatch") != self.etag:
            raise RuntimeError("archive precondition failed")
        self.archive = payload
        self.etag = self._etag(payload)
        self.version = "v2"
        self.metadata = kwargs["Metadata"]
        self.checksum = kwargs["ChecksumSHA256"]
        return {
            "ETag": self.etag,
            "VersionId": self.version,
            "ChecksumSHA256": self.checksum,
        }


class ProtectedArchiveS3Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = S3Config(
            bucket="bucket",
            prefix="parquet/",
            region=None,
            profile=None,
            endpoint_url=None,
        )
        self.client = FakeS3(
            "parquet/backtest/grok_trending_archive.parquet",
            b"source archive",
            "parquet/manifest.json",
        )

    def test_guarded_publish_and_receipt_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source_path = Path(directory) / "source.parquet"
            candidate_path = Path(directory) / "candidate.parquet"
            candidate_path.write_bytes(b"candidate archive")
            source = download_verified_archive(
                self.cfg, source_path, client=self.client
            )
            state = publish_guarded_archive(
                self.cfg,
                candidate_path,
                source,
                backtest_date="2026-07-10",
                row_count=10,
                client=self.client,
            )
            manifest_state = publish_guarded_manifest_entry(
                self.cfg,
                source,
                state,
                columns=["backtest_date", "ticker"],
                date_min="2025-11-04",
                date_max="2026-07-10",
                unique_ticker_date_keys=10,
                client=self.client,
            )
            verify_publish_state(self.cfg, state, client=self.client)
            self.assertEqual(state["s3_version_id"], "v2")
            self.assertEqual(self.client.metadata["data-source"], "jquants-1m")
            self.assertEqual(manifest_state["manifest_s3_version_id"], "m2")
            entry = json.loads(self.client.manifest)["files"][
                "backtest/grok_trending_archive.parquet"
            ]
            self.assertEqual(entry["sha256"], state["sha256"])

    def test_download_uses_etag_without_version_read_permission(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source_path = Path(directory) / "source.parquet"
            source = download_verified_archive(
                self.cfg, source_path, client=self.client
            )

        self.assertEqual(source["version_id"], "v1")
        self.assertEqual(
            self.client.get_requests,
            [
                (self.client.archive_key, {"IfMatch": self.client.etag}),
                (
                    self.client.manifest_key,
                    {"IfMatch": self.client.manifest_etag},
                ),
            ],
        )

    def test_stale_source_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source_path = Path(directory) / "source.parquet"
            candidate_path = Path(directory) / "candidate.parquet"
            candidate_path.write_bytes(b"candidate archive")
            source = download_verified_archive(
                self.cfg, source_path, client=self.client
            )
            self.client.etag = '"changed"'
            with self.assertRaises(ProtectedArchiveError):
                publish_guarded_archive(
                    self.cfg,
                    candidate_path,
                    source,
                    backtest_date="2026-07-10",
                    row_count=10,
                    client=self.client,
                )


class ProtectedManifestEntryTests(unittest.TestCase):
    def test_manifest_only_preserves_missing_derived_entry(self) -> None:
        existing_entry = {"exists": True, "row_count": 12, "updated_at": "old"}
        existing = {"files": {"derived.parquet": existing_entry}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(update_manifest, "PARQUET_DIR", root),
                patch.object(update_manifest, "UPLOAD_FILES", ["derived.parquet"]),
                patch.object(update_manifest, "PROTECTED_FILES", []),
            ):
                manifest = update_manifest.generate_manifest(
                    existing,
                    preserve_missing=True,
                )
        self.assertEqual(manifest["files"]["derived.parquet"], existing_entry)

    def test_absent_local_archive_preserves_existing_entry(self) -> None:
        existing = {
            "exists": True,
            "protected": True,
            "canonical": True,
            "sha256": "abc",
            "s3_version_id": "v1",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(update_manifest, "PARQUET_DIR", root):
                entry = update_manifest.get_protected_file_entry(
                    "backtest/grok_trending_archive.parquet", existing
                )
        self.assertEqual(entry, existing)

    def test_changed_local_archive_without_receipt_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "backtest" / "grok_trending_archive.parquet"
            archive.parent.mkdir(parents=True)
            pd.DataFrame(
                [{"backtest_date": "2026-07-10", "ticker": "1234.T"}]
            ).to_parquet(archive, index=False)
            receipt = root / "backtest" / "missing.publish.json"
            with (
                patch.object(update_manifest, "PARQUET_DIR", root),
                patch.object(update_manifest, "ARCHIVE_PUBLISH_STATE_PATH", receipt),
            ):
                with self.assertRaises(RuntimeError):
                    update_manifest.get_protected_file_entry(
                        "backtest/grok_trending_archive.parquet",
                        {
                            "protected": True,
                            "canonical": True,
                            "sha256": "different",
                        },
                    )


if __name__ == "__main__":
    unittest.main()
