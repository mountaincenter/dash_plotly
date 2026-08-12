from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from common_cfg import s3io
from common_cfg.s3cfg import S3Config
from scripts.lib.grok_jquants_backtest import (
    JQuantsBacktestDataError,
    MARKET_CAP_PROVENANCE_COLUMNS,
    align_rows_to_archive_schema,
    assert_archive_history_unchanged,
    assert_archive_schema_unchanged,
    assert_archive_target_rows_preserved,
    assert_parquet_schema_unchanged,
    build_derived_backtest_rows,
    calculate_segment_pnl,
    has_trade_after_entry,
    merge_archive_date,
    normalize_daily_prices,
    normalize_minute_bars,
    session_last_close,
    validate_daily_alignment,
    validate_backtest_execution_states,
    validate_selection_asof,
    validate_selection_market_cap,
    validate_target_daily_corporate_actions,
)
from scripts.lib.protected_archive_s3 import (
    ProtectedArchiveError,
    download_verified_archive,
    publish_guarded_archive,
    publish_guarded_manifest_entry,
    publish_guarded_trending_and_manifest,
)
from scripts.pipeline import update_manifest
from scripts.pipeline import save_backtest_to_archive as save_backtest
from scripts.pipeline.save_backtest_to_archive import (
    assert_parquet_roundtrip_equal,
    calculate_phase3_return,
)
from scripts.pipeline.update_archive_holding_returns import build_holding_returns


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
    def test_parquet_roundtrip_treats_na_and_none_as_same_missing_value(self) -> None:
        expected = pd.DataFrame(
            {
                "jquants_daily_code": pd.Series([pd.NA, pd.NA], dtype="object"),
                "market_cap": [100.0, 200.0],
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "derived.parquet"
            expected.to_parquet(path, index=False)
            actual = pd.read_parquet(path)
        self.assertEqual(actual["jquants_daily_code"].tolist(), [None, None])
        assert_parquet_roundtrip_equal(expected, actual)

    def test_parquet_roundtrip_rejects_non_missing_value_change(self) -> None:
        expected = pd.DataFrame(
            {"jquants_daily_code": [pd.NA, "72030"], "market_cap": [100.0, 200.0]}
        )
        actual = pd.DataFrame(
            {"jquants_daily_code": [None, "72031"], "market_cap": [100.0, 200.0]}
        )
        with self.assertRaises(AssertionError):
            assert_parquet_roundtrip_equal(expected, actual)

    def test_parquet_roundtrip_rejects_missing_position_change(self) -> None:
        expected = pd.DataFrame({"jquants_daily_code": [pd.NA, "72030"]})
        actual = pd.DataFrame({"jquants_daily_code": ["72030", None]})
        with self.assertRaises(AssertionError):
            assert_parquet_roundtrip_equal(expected, actual)

    def test_official_no_market_day_is_resolved_without_invented_prices(self) -> None:
        target = pd.Timestamp("2026-07-24").to_pydatetime()
        grok = pd.DataFrame({"ticker": ["7203.T", "6898.T"]})
        minute = pd.DataFrame(
            [
                {
                    "ticker": "7203.T",
                    "datetime": pd.Timestamp("2026-07-24 09:00"),
                    "open": 3000.0,
                    "high": 3010.0,
                    "low": 2990.0,
                    "close": 3005.0,
                    "volume": 100.0,
                    "value": 300_500.0,
                }
            ]
        )
        daily = pd.DataFrame(
            [
                {
                    "date": "2026-07-23",
                    "ticker": "6898.T",
                    "Open": 100.0,
                    "High": 101.0,
                    "Low": 99.0,
                    "Close": 100.0,
                    "Volume": 10.0,
                },
                {
                    "date": "2026-07-24",
                    "ticker": "7203.T",
                    "Open": 3000.0,
                    "High": 3010.0,
                    "Low": 2990.0,
                    "Close": 3005.0,
                    "Volume": 100.0,
                },
            ]
        )
        features = pd.DataFrame(
            [
                {
                    "trading_date": "2026-07-24",
                    "ticker": "7203.T",
                    "jq_daily_trade_status": "traded",
                },
                {
                    "trading_date": "2026-07-24",
                    "ticker": "6898.T",
                    "jq_daily_trade_status": "no_market_trade",
                },
            ]
        )
        with patch.object(save_backtest, "_jquants_minute_df", minute), patch.object(
            save_backtest,
            "_jquants_daily_df",
            save_backtest.normalize_daily_prices(daily),
        ):
            no_market = save_backtest.validate_batch_coverage(
                grok,
                target,
                features,
            )
            self.assertEqual(no_market, {"6898.T"})
            row = save_backtest.build_no_market_trade_backtest_data(
                "6898.T",
                target,
            )
        self.assertEqual(row["data_source"], "jquants_no_market_trade")
        self.assertEqual(row["jquants_bar_count"], 0)
        self.assertEqual(row["prev_close"], 100.0)
        self.assertIsNone(row["buy_price"])
        self.assertIsNone(row["phase2_return"])
        self.assertTrue(all(row[column] is None for column in save_backtest.SEGMENT_TARGETS))
        no_market_frame = pd.DataFrame([row])
        validate_backtest_execution_states(no_market_frame)
        with self.assertRaisesRegex(
            JQuantsBacktestDataError,
            "invented price",
        ):
            validate_backtest_execution_states(
                no_market_frame.assign(buy_price=1.0)
            )

    def test_pipeline_orders_daily_fields_before_archive_and_final_publish(self) -> None:
        workflow = (Path(__file__).resolve().parents[2] / ".github" / "workflows" / "data-pipeline.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn('JQUANTS_VERSION="1.3.0"', workflow)
        self.assertIn(
            'JQUANTS_SHA256="074b6be27b8e4d343a81af8d207d34e81bd30aee2dd2f5358e6ab903564d2b73"',
            workflow,
        )
        recommendation = workflow.index(
            "- name: Generate trading recommendation (22:00 only)"
        )
        finalization = workflow.index(
            "- name: Publish finalized Grok and manifest (22:15 only)"
        )
        ledger_build = workflow.index(
            "build_grok_jquants_backtest_ledger.py"
        )
        historical_daily = workflow.index(
            "fetch_grok_archive_daily_jquants.py"
        )
        master_build = workflow.index(
            "build_grok_master_jquants_segments.py"
        )
        self.assertLess(ledger_build, historical_daily)
        self.assertLess(historical_daily, master_build)
        self.assertIn(
            '--archive-path "$LEDGER_FILE"',
            workflow,
        )
        self.assertLess(recommendation, finalization)
        self.assertNotRegex(
            workflow,
            r'aws s3 cp\s+data/parquet/grok_trending\.parquet\s+'
            r'"s3://[^\"]+grok_trending\.parquet"',
        )
        self.assertNotRegex(
            workflow,
            r'aws s3 cp\s+(?:\\\s*)?'
            r'data/parquet/backtest/grok_trending_archive\.parquet\s+'
            r'(?:\\\s*)?"s3://',
        )
        derived_script = (
            Path(__file__).resolve().parents[2]
            / "scripts"
            / "pipeline"
            / "save_backtest_to_archive.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("publish_guarded_archive(", derived_script)
        self.assertNotIn(
            "os.replace(candidate_path, BACKTEST_ARCHIVE_PATH)",
            derived_script,
        )
        feature_script = (
            Path(__file__).resolve().parents[2]
            / "scripts"
            / "ml"
            / "feature_engineering.py"
        ).read_text(encoding="utf-8")
        self.assertIn("grok_jquants_backtest_ledger.parquet", feature_script)

        with patch.dict("os.environ", {"SKIP_GROK_GENERATION": "true"}):
            from scripts.run_pipeline_scalping_skip_add_grok import PipelineRunner

            steps = [name for name, _ in PipelineRunner().steps]
        self.assertLess(
            steps.index("pipeline.fetch_watch_daily_jquants"),
            steps.index("pipeline.save_backtest_to_archive"),
        )
        self.assertLess(
            steps.index("pipeline.save_backtest_to_archive"),
            steps.index("pipeline.update_manifest"),
        )

    def test_holding_returns_are_derived_without_mutating_archive(self) -> None:
        archive = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-08-07",
                    "ticker": "7203.T",
                    "buy_price": 3000.0,
                    "canonical_value": 123,
                }
            ]
        )
        original = archive.copy(deep=True)
        prices = pd.DataFrame(
            [
                {"date": "2026-08-10", "ticker": "7203.T", "Close": 2990.0},
                {"date": "2026-08-12", "ticker": "7203.T", "Close": 2980.0},
            ]
        )
        derived = build_holding_returns(archive, prices)
        pd.testing.assert_frame_equal(archive, original)
        self.assertNotIn("canonical_value", derived.columns)
        self.assertEqual(derived.iloc[0]["short_profit_d1"], 1000.0)
        self.assertEqual(derived.iloc[0]["short_profit_d2"], 2000.0)
        self.assertTrue(pd.isna(derived.iloc[0]["short_profit_d3"]))

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

    def test_archive_guards_preserve_missing_value_positions(self) -> None:
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

        assert_archive_history_unchanged(source, reloaded, "2026-07-11")
        changed = reloaded.copy()
        changed.loc[0, "phase1_win"] = False
        with self.assertRaises(JQuantsBacktestDataError):
            assert_archive_history_unchanged(source, changed, "2026-07-11")

    def test_official_market_cap_and_target_corporate_action_gate(self) -> None:
        selection = pd.DataFrame(
            [
                {
                    "date": "2026-08-10",
                    "ticker": "7203.T",
                    "market_cap": 43_493_063_000_000.0,
                    "market_cap_source": "jquants_eq_daily_mktcap_d_minus_1",
                    "jq_market_cap_asof_date": "2026-08-07",
                    "jq_mkt_cap_million_yen_asof": 43_493_063.0,
                    "jq_market_cap_yen_asof": 43_493_063_000_000.0,
                    "jq_ex_rights_type_asof": pd.NA,
                    "jq_adjustment_factor_asof": 1.0,
                    "jq_daily_source_asof": "jquants_api_v2",
                    "jq_daily_fetched_at_asof": "2026-08-07T16:00:00+00:00",
                },
                {
                    "date": "2026-08-10",
                    "ticker": "200A.T",
                    "market_cap": pd.NA,
                    "market_cap_source": "jquants_eq_daily_mktcap_d_minus_1",
                    "jq_market_cap_asof_date": "2026-08-07",
                    "jq_mkt_cap_million_yen_asof": pd.NA,
                    "jq_market_cap_yen_asof": pd.NA,
                    "jq_ex_rights_type_asof": pd.NA,
                    "jq_adjustment_factor_asof": 1.0,
                    "jq_daily_source_asof": "jquants_api_v2",
                    "jq_daily_fetched_at_asof": "2026-08-07T16:00:00+00:00",
                },
            ]
        )
        calendar = pd.DataFrame({"date": ["2026-08-07", "2026-08-10"]})
        target_daily = pd.DataFrame(
            [
                {
                    "trading_date": "2026-08-10",
                    "ticker": "7203.T",
                    "jq_daily_trade_status": "traded",
                    "jq_mkt_cap_million_yen": 44_000_000.0,
                    "jq_market_cap_yen": 44_000_000_000_000.0,
                    "jq_ex_rights_type": 1,
                    "jq_adjustment_factor": 0.5,
                    "source": "jquants_api_v2",
                    "fetched_at": "2026-08-10T16:00:00+00:00",
                },
                {
                    "trading_date": "2026-08-10",
                    "ticker": "200A.T",
                    "jq_daily_trade_status": "traded",
                    "jq_mkt_cap_million_yen": pd.NA,
                    "jq_market_cap_yen": pd.NA,
                    "jq_ex_rights_type": pd.NA,
                    "jq_adjustment_factor": 1.0,
                    "source": "jquants_api_v2",
                    "fetched_at": "2026-08-10T16:00:00+00:00",
                },
            ]
        )

        validate_selection_market_cap(selection, "2026-08-10", calendar)
        validate_target_daily_corporate_actions(
            selection, "2026-08-10", target_daily
        )

        with self.assertRaises(JQuantsBacktestDataError):
            validate_selection_market_cap(
                selection.assign(jq_market_cap_asof_date="2026-08-06"),
                "2026-08-10",
                calendar,
            )
        changed = selection.copy()
        changed.loc[changed["ticker"].eq("7203.T"), "market_cap"] = 1.0
        with self.assertRaises(JQuantsBacktestDataError):
            validate_selection_market_cap(changed, "2026-08-10", calendar)
        with self.assertRaises(JQuantsBacktestDataError):
            validate_target_daily_corporate_actions(
                selection,
                "2026-08-10",
                target_daily[target_daily["ticker"].ne("200A.T")],
            )

        archive = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-08-07",
                    "date": "2026-08-07",
                    "ticker": "7203.T",
                    "market_cap": 1.0,
                    "value": 10.0,
                }
            ]
        )
        new_rows = selection.assign(backtest_date="2026-08-10")
        new_rows["market_cap"] = new_rows["market_cap"].astype("Float64")
        derived = build_derived_backtest_rows(
            archive,
            new_rows,
            "2026-08-10",
            target_daily,
        )
        self.assertEqual(len(derived), 2)
        self.assertEqual(
            derived.columns[: len(archive.columns)].tolist(),
            archive.columns.tolist(),
        )
        toyota = derived[derived["ticker"].eq("7203.T")].iloc[0]
        self.assertEqual(toyota["jq_ex_rights_type_target"], 1)
        self.assertEqual(
            toyota["jq_market_cap_yen_target"], 44_000_000_000_000.0
        )
        etf = derived[derived["ticker"].eq("200A.T")].iloc[0]
        self.assertTrue(pd.isna(etf["jq_market_cap_yen_target"]))

    def test_target_rows_keep_exact_canonical_schema(self) -> None:
        archive = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-08-07",
                    "ticker": "7203.T",
                    "market_cap": 1.0,
                    "value": 10.0,
                }
            ]
        )
        new_rows = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-08-10",
                    "ticker": "7203.T",
                    "market_cap": 43_493_063_000_000.0,
                    "market_cap_source": "jquants_eq_daily_mktcap_d_minus_1",
                    "jq_market_cap_asof_date": "2026-08-07",
                }
            ]
        )
        aligned = align_rows_to_archive_schema(
            archive,
            new_rows,
            allowed_extra_columns=MARKET_CAP_PROVENANCE_COLUMNS,
        )
        self.assertEqual(aligned.columns.tolist(), archive.columns.tolist())
        self.assertEqual(aligned.iloc[0]["market_cap"], 43_493_063_000_000.0)
        self.assertTrue(pd.isna(aligned.iloc[0]["value"]))
        assert_archive_schema_unchanged(
            archive,
            pd.concat([archive, aligned], ignore_index=True),
        )

        with self.assertRaises(JQuantsBacktestDataError):
            align_rows_to_archive_schema(
                archive,
                new_rows.assign(unreviewed_schema_column=1),
                allowed_extra_columns=MARKET_CAP_PROVENANCE_COLUMNS,
            )
        with self.assertRaisesRegex(
            JQuantsBacktestDataError,
            "dtype changed",
        ):
            assert_archive_schema_unchanged(
                archive,
                archive.assign(
                    market_cap=archive["market_cap"].astype("Float64")
                ),
            )

    def test_parquet_schema_guard_includes_pandas_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source_path = Path(temp_dir) / "source.parquet"
            same_path = Path(temp_dir) / "same.parquet"
            changed_path = Path(temp_dir) / "changed.parquet"
            source = pd.DataFrame(
                {"ticker": pd.Series(["7203.T"], dtype="object"), "value": [1.0]}
            )
            source.to_parquet(source_path, index=False)
            source.copy().to_parquet(same_path, index=False)
            source.assign(ticker=source["ticker"].astype("string")).to_parquet(
                changed_path,
                index=False,
            )
            assert_parquet_schema_unchanged(source_path, same_path)
            with self.assertRaisesRegex(
                JQuantsBacktestDataError,
                "Parquet/Arrow schema",
            ):
                assert_parquet_schema_unchanged(source_path, changed_path)


class FakeS3:
    def __init__(self, archive_key: str, archive: bytes, manifest_key: str) -> None:
        self.archive_key = archive_key
        self.manifest_key = manifest_key
        self.archive = archive
        self.source_archive = archive
        self.version = "v1"
        self.metadata: dict[str, str] = {}
        self.checksum: str | None = None
        self.etag = self._etag(archive)
        self.source_etag = self.etag
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
        self.source_manifest = self.manifest
        self.manifest_etag = self._etag(self.manifest)
        self.source_manifest_etag = self.manifest_etag
        self.manifest_version = "m1"
        self.fail_manifest = False
        self.get_requests: list[tuple[str, dict[str, object]]] = []

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

    def get_bucket_versioning(self, Bucket: str):
        return {"Status": "Enabled"}

    def get_object(self, Bucket: str, Key: str, **kwargs):
        self.get_requests.append((Key, dict(kwargs)))
        expected_etag = self.etag if Key == self.archive_key else self.manifest_etag
        if kwargs.get("IfMatch") != expected_etag:
            raise RuntimeError("object precondition failed")
        payload = self.archive if Key == self.archive_key else self.manifest
        return {"Body": io.BytesIO(payload)}

    def put_object(self, Bucket: str, Key: str, Body, **kwargs):
        payload = Body.read() if hasattr(Body, "read") else Body
        if Key == self.manifest_key:
            if self.fail_manifest:
                raise RuntimeError("simulated manifest failure")
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

    def delete_object(self, Bucket: str, Key: str, VersionId: str):
        if Key == self.archive_key and VersionId == "v2":
            self.archive = self.source_archive
            self.etag = self.source_etag
            self.version = "v1"
            self.metadata = {}
            self.checksum = None
            return {"VersionId": VersionId}
        if Key == self.manifest_key and VersionId == "m2":
            self.manifest = self.source_manifest
            self.manifest_etag = self.source_manifest_etag
            self.manifest_version = "m1"
            return {"VersionId": VersionId}
        raise RuntimeError("unexpected rollback target")


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

    def test_verified_archive_download_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source_path = Path(directory) / "source.parquet"
            source = download_verified_archive(
                self.cfg, source_path, client=self.client
            )
            self.assertEqual(source_path.read_bytes(), b"source archive")
            self.assertEqual(source["version_id"], "v1")
            self.assertEqual(self.client.archive, b"source archive")
            self.assertEqual(self.client.version, "v1")
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

    def test_canonical_archive_publication_is_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            candidate_path = Path(directory) / "candidate.parquet"
            candidate_path.write_bytes(b"candidate archive")
            with self.assertRaisesRegex(ProtectedArchiveError, "read-only"):
                publish_guarded_archive(
                    self.cfg,
                    candidate_path,
                    {},
                    backtest_date="2026-07-10",
                    row_count=10,
                    client=self.client,
                )
        self.assertEqual(self.client.archive, b"source archive")
        self.assertEqual(self.client.version, "v1")

    def test_canonical_manifest_advancement_is_disabled(self) -> None:
        with self.assertRaisesRegex(ProtectedArchiveError, "read-only"):
            publish_guarded_manifest_entry(
                self.cfg,
                {},
                {},
                columns=["backtest_date", "ticker"],
                date_min="2025-11-04",
                date_max="2026-07-10",
                unique_ticker_date_keys=10,
                client=self.client,
            )
        self.assertEqual(self.client.archive, b"source archive")
        self.assertEqual(self.client.version, "v1")
        self.assertEqual(self.client.manifest_version, "m1")

    def test_generic_s3_uploader_rejects_canonical_archive(self) -> None:
        protected = Path("data/parquet/backtest/grok_trending_archive.parquet")
        with patch.object(s3io, "_init_s3_client") as init_client:
            self.assertFalse(
                s3io.upload_file(
                    self.cfg,
                    protected,
                    "backtest/grok_trending_archive.parquet",
                )
            )
        init_client.assert_not_called()


class FakeTrendingS3:
    trend_key = "parquet/grok_trending.parquet"
    manifest_key = "parquet/manifest.json"

    def __init__(self) -> None:
        source = b"source trending"
        self.trend_versions = [
            {
                "version": "t1",
                "payload": source,
                "etag": FakeS3._etag(source),
                "metadata": {"sha256": hashlib.sha256(source).hexdigest()},
                "checksum": None,
            }
        ]
        manifest = {
            "files": {
                "grok_trending.parquet": {
                    "sha256": hashlib.sha256(source).hexdigest(),
                    "s3_etag": self.trend_versions[-1]["etag"],
                    "s3_version_id": "t1",
                }
            }
        }
        self.manifest = json.dumps(manifest).encode()
        self.manifest_etag = FakeS3._etag(self.manifest)
        self.manifest_version = "m1"
        self.manifest_checksum: str | None = None
        self.fail_manifest = False
        self.version_counter = 1

    def head_object(self, Bucket: str, Key: str, **kwargs):
        if Key == self.trend_key:
            current = self.trend_versions[-1]
            return {
                "ETag": current["etag"],
                "VersionId": current["version"],
                "Metadata": current["metadata"],
                "ChecksumSHA256": current["checksum"],
            }
        return {
            "ETag": self.manifest_etag,
            "VersionId": self.manifest_version,
            "ChecksumSHA256": self.manifest_checksum,
        }

    def get_bucket_versioning(self, Bucket: str):
        return {"Status": "Enabled"}

    def get_object(self, Bucket: str, Key: str, **kwargs):
        if Key == self.trend_key:
            version = kwargs.get("VersionId")
            item = next(
                value
                for value in self.trend_versions
                if value["version"] == version
            )
            payload = item["payload"]
        else:
            payload = self.manifest
        return {"Body": io.BytesIO(payload)}

    def put_object(self, Bucket: str, Key: str, Body, **kwargs):
        payload = Body.read() if hasattr(Body, "read") else Body
        if Key == self.manifest_key:
            if self.fail_manifest:
                raise RuntimeError("simulated manifest failure")
            if kwargs.get("IfMatch") != self.manifest_etag:
                raise RuntimeError("manifest precondition failed")
            self.manifest = payload
            self.manifest_etag = FakeS3._etag(payload)
            self.manifest_version = "m2"
            self.manifest_checksum = kwargs.get("ChecksumSHA256")
            return {
                "ETag": self.manifest_etag,
                "VersionId": self.manifest_version,
                "ChecksumSHA256": self.manifest_checksum,
            }

        current = self.trend_versions[-1]
        if kwargs.get("IfMatch") != current["etag"]:
            raise RuntimeError("trend precondition failed")
        self.version_counter += 1
        item = {
            "version": f"t{self.version_counter}",
            "payload": payload,
            "etag": FakeS3._etag(payload),
            "metadata": kwargs["Metadata"],
            "checksum": kwargs.get("ChecksumSHA256"),
        }
        self.trend_versions.append(item)
        return {
            "ETag": item["etag"],
            "VersionId": item["version"],
            "ChecksumSHA256": item["checksum"],
        }

    def delete_object(self, Bucket: str, Key: str, VersionId: str):
        if Key != self.trend_key or self.trend_versions[-1]["version"] != VersionId:
            raise RuntimeError("unexpected version rollback")
        self.trend_versions.pop()
        return {"VersionId": VersionId}


class GuardedTrendingPublishTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = S3Config(
            bucket="bucket",
            prefix="parquet/",
            region=None,
            profile=None,
            endpoint_url=None,
        )

    @staticmethod
    def candidate_manifest() -> dict:
        return {
            "files": {
                "grok_trending.parquet": {
                    "exists": True,
                    "row_count": 1,
                    "columns": ["ticker"],
                }
            }
        }

    def test_dataset_is_verified_before_manifest_advances(self) -> None:
        client = FakeTrendingS3()
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "grok_trending.parquet"
            candidate.write_bytes(b"final trending")
            state = publish_guarded_trending_and_manifest(
                self.cfg,
                candidate,
                self.candidate_manifest(),
                entry_metadata={"data_source": "jquants_eq_daily"},
                client=client,
            )
        entry = json.loads(client.manifest)["files"]["grok_trending.parquet"]
        self.assertEqual(entry["sha256"], hashlib.sha256(b"final trending").hexdigest())
        self.assertEqual(entry["s3_version_id"], "t2")
        self.assertEqual(state["manifest_s3_version_id"], "m2")

    def test_manifest_failure_removes_only_new_dataset_version(self) -> None:
        client = FakeTrendingS3()
        source_version = client.trend_versions[-1]["version"]
        source_manifest = client.manifest
        client.fail_manifest = True
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "grok_trending.parquet"
            candidate.write_bytes(b"final trending")
            with self.assertRaises(ProtectedArchiveError):
                publish_guarded_trending_and_manifest(
                    self.cfg,
                    candidate,
                    self.candidate_manifest(),
                    entry_metadata={"data_source": "jquants_eq_daily"},
                    client=client,
                )
        self.assertEqual(client.trend_versions[-1]["version"], source_version)
        self.assertEqual(client.manifest, source_manifest)


class ProtectedManifestEntryTests(unittest.TestCase):
    def test_production_manifest_never_falls_back_to_local(self) -> None:
        with patch(
            "scripts.pipeline.update_manifest.read_remote_manifest",
            side_effect=RuntimeError("S3 unavailable"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Production S3 manifest"):
                update_manifest.load_existing_manifest(use_s3=True)

    def test_storage_mode_mismatch_is_rejected(self) -> None:
        with patch.dict(
            "os.environ",
            {"APP_ENV": "production", "STORAGE_MODE": "local"},
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "storage mode mismatch"):
                update_manifest.resolve_storage_mode()

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

    def test_full_manifest_preserves_cumulative_ledger_until_builder_runs(self) -> None:
        filename = "backtest/grok_jquants_backtest_ledger.parquet"
        existing_entry = {
            "exists": True,
            "row_count": 2968,
            "sha256": "verified-ledger",
        }
        existing = {"files": {filename: existing_entry}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(update_manifest, "PARQUET_DIR", root),
                patch.object(update_manifest, "UPLOAD_FILES", [filename]),
                patch.object(update_manifest, "PROTECTED_FILES", []),
            ):
                manifest = update_manifest.generate_manifest(existing)
        self.assertEqual(manifest["files"][filename], existing_entry)

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

    def test_changed_local_archive_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "backtest" / "grok_trending_archive.parquet"
            archive.parent.mkdir(parents=True)
            pd.DataFrame(
                [{"backtest_date": "2026-07-10", "ticker": "1234.T"}]
            ).to_parquet(archive, index=False)
            with patch.object(update_manifest, "PARQUET_DIR", root):
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
