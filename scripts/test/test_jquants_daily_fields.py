from __future__ import annotations

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import scripts.ml.train_model as train_model
from scripts.ml.train_model import (
    FEATURE_COLUMNS,
    FEATURE_CONTRACT,
    PRICE_HISTORY_SOURCE,
    validate_model_release,
)

from scripts.analysis.build_grok_master_jquants_segments import (
    JQ_ADJUSTMENT_FACTOR_ASOF,
    JQ_ADJUSTMENT_FACTOR_TARGET,
    JQ_CLOSE_ASOF,
    JQ_EX_RIGHTS_TYPE_TARGET,
    JQ_MARKET_CAP_ASOF_DATE,
    JQ_MARKET_CAP_YEN_ASOF,
    JQ_MARKET_CAP_YEN_TARGET,
    JQ_MKT_CAP_MILLION_YEN_ASOF,
    JQ_MKT_CAP_MILLION_YEN_TARGET,
    build_master,
)
from scripts.analysis.validate_grok_jquants_outputs import build_report
from scripts.data.fetch_grok_archive_daily_jquants import (
    load_archive_targets,
    merge_existing as merge_grok_daily,
    normalize_daily_csv,
    raw_csv_supports_daily_fields,
    validate_jquants_daily_schema,
)
from scripts.lib.jquants_daily_fields import (
    DAILY_TRADE_STATUS_NO_MARKET_TRADE,
    DAILY_TRADE_STATUS_TRADED,
    JQ_ADJUSTMENT_FACTOR,
    JQ_DAILY_TRADE_STATUS,
    JQ_EX_RIGHTS_TYPE,
    JQ_MARKET_CAP_YEN,
    JQ_MKT_CAP_MILLION_YEN,
    JQUANTS_DAILY_FIELD_COLUMNS,
    classify_jquants_daily_trade_status,
    normalize_jquants_daily_fields,
)
from scripts.lib.grok_jquants_backtest import JQuantsBacktestDataError
from scripts.ml.feature_engineering import attach_official_market_cap_from_master
from scripts.pipeline.add_market_cap_to_grok_trending import (
    JQ_EX_RIGHTS_TYPE_ASOF,
    JQ_MARKET_CAP_ASOF_DATE as LIVE_JQ_MARKET_CAP_ASOF_DATE,
    JQ_MARKET_CAP_YEN_ASOF as LIVE_JQ_MARKET_CAP_YEN_ASOF,
    MARKET_CAP_SOURCE,
    attach_official_market_cap_asof,
)
from scripts.pipeline.add_ml_prediction_to_grok_trending import (
    validate_model_package,
    validate_model_market_cap_source,
    validate_official_market_cap_input,
)
from scripts.pipeline.fetch_watch_daily_jquants import (
    fetch_daily_bars,
    merge_daily_features,
    normalize_daily_features,
)
from scripts.pipeline.save_backtest_to_archive import (
    validate_or_attach_market_cap_provenance,
)
from scripts.pipeline.generate_grok_prices_max_1d import (
    merge_price_sources,
    validate_current_price_coverage,
)
from scripts.pipeline.update_manifest import validate_finalized_grok_artifact
from scripts.pipeline.build_grok_jquants_backtest_ledger import build_ledger


def raw_daily_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Date": "2026-08-07",
                "Code": "72030",
                "O": 2995.0,
                "H": 3022.0,
                "L": 2964.5,
                "C": 2980.0,
                "Vo": 1_000.0,
                "MktCap": 43_493_063.0,
                "ExRT": None,
                "AdjFactor": 1.0,
            },
            {
                "Date": "2026-08-07",
                "Code": "200A0",
                "O": 4200.0,
                "H": 4300.0,
                "L": 4190.0,
                "C": 4254.0,
                "Vo": 2_000.0,
                "MktCap": None,
                "ExRT": None,
                "AdjFactor": 1.0,
            },
            {
                "Date": "2023-06-29",
                "Code": "94320",
                "O": 170.0,
                "H": 172.0,
                "L": 169.0,
                "C": 171.2,
                "Vo": 3_000.0,
                "MktCap": 10_506_009.0,
                "ExRT": "1",
                "AdjFactor": 0.04,
            },
        ]
    )


class JQuantsDailyFieldTests(unittest.TestCase):
    def test_derived_ledger_appends_only_audited_future_days(self) -> None:
        canonical = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-08-10",
                    "date": "2026-08-10",
                    "ticker": "7203.T",
                    "market_cap": 43_493_063_000_000.0,
                    "value": 1.0,
                }
            ]
        )
        future = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-08-11",
                    "date": "2026-08-11",
                    "ticker": "7203.T",
                    "market_cap": 43_493_063_000_000.0,
                    "value": 2.0,
                    "market_cap_source": "jquants_eq_daily_mktcap_d_minus_1",
                    "jq_market_cap_asof_date": "2026-08-10",
                    "jq_mkt_cap_million_yen_asof": 43_493_063.0,
                    "jq_market_cap_yen_asof": 43_493_063_000_000.0,
                    "jq_ex_rights_type_asof": pd.NA,
                    "jq_adjustment_factor_asof": 1.0,
                    "jq_daily_source_asof": "jquants_api_v2",
                    "jq_daily_fetched_at_asof": "2026-08-10T16:00:00+00:00",
                    "jq_daily_target_date": "2026-08-11",
                    "jq_daily_trade_status_target": "traded",
                    "jq_mkt_cap_million_yen_target": 44_000_000.0,
                    "jq_market_cap_yen_target": 44_000_000_000_000.0,
                    "jq_ex_rights_type_target": pd.NA,
                    "jq_adjustment_factor_target": 1.0,
                    "jq_daily_source_target": "jquants_api_v2",
                    "jq_daily_fetched_at_target": "2026-08-11T16:00:00+00:00",
                }
            ]
        )
        calendar = pd.DataFrame({"date": ["2026-08-10", "2026-08-11"]})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            historical_legacy = root / "grok_trending_20260810.parquet"
            future_path = root / "grok_trending_20260811.parquet"
            canonical.to_parquet(historical_legacy, index=False)
            future.to_parquet(future_path, index=False)
            ledger, receipts = build_ledger(
                canonical,
                [
                    ("2026-08-10", historical_legacy),
                    ("2026-08-11", future_path),
                ],
                calendar,
            )
            self.assertEqual(len(ledger), 2)
            self.assertEqual(len(receipts), 1)
            self.assertEqual(receipts[0]["date"], "2026-08-11")
            pd.testing.assert_frame_equal(
                canonical,
                ledger.iloc[:1].reset_index(drop=True),
                check_dtype=True,
                check_exact=True,
            )
            self.assertEqual(ledger.iloc[1]["value"], 2.0)

            future.drop(columns=["value"]).to_parquet(
                future_path,
                index=False,
            )
            with self.assertRaisesRegex(
                JQuantsBacktestDataError,
                "lacks canonical columns",
            ):
                build_ledger(
                    canonical,
                    [("2026-08-11", future_path)],
                    calendar,
                )

            future.drop(columns=["jq_daily_source_target"]).to_parquet(
                future_path,
                index=False,
            )
            with self.assertRaisesRegex(
                JQuantsBacktestDataError,
                "audit provenance",
            ):
                build_ledger(
                    canonical,
                    [("2026-08-11", future_path)],
                    calendar,
                )

    def test_all_market_daily_request_uses_v2_date_parameter(self) -> None:
        class FakeClient:
            def request_with_pagination(self, endpoint, params, **kwargs):
                self.endpoint = endpoint
                self.params = params
                return raw_daily_fixture().to_dict("records")

        class FakeFetcher:
            client = FakeClient()

            @staticmethod
            def _normalize_columns(frame):
                return frame

        fetcher = FakeFetcher()
        result = fetch_daily_bars(
            fetcher,
            from_date="2026-08-07",
            to_date="2026-08-07",
        )
        self.assertFalse(result.empty)
        self.assertEqual(fetcher.client.params, {"date": "2026-08-07"})
        with self.assertRaisesRegex(ValueError, "exactly one date"):
            fetch_daily_bars(
                fetcher,
                from_date="2026-08-06",
                to_date="2026-08-07",
            )

    def test_normalizes_units_nullability_and_split_fields(self) -> None:
        normalized = normalize_jquants_daily_fields(raw_daily_fixture())

        toyota = normalized[normalized["Code"].eq("72030")].iloc[0]
        self.assertEqual(toyota[JQ_MKT_CAP_MILLION_YEN], 43_493_063.0)
        self.assertEqual(toyota[JQ_MARKET_CAP_YEN], 43_493_063_000_000.0)
        self.assertTrue(pd.isna(toyota[JQ_EX_RIGHTS_TYPE]))

        etf = normalized[normalized["Code"].eq("200A0")].iloc[0]
        self.assertTrue(pd.isna(etf[JQ_MKT_CAP_MILLION_YEN]))
        self.assertTrue(pd.isna(etf[JQ_MARKET_CAP_YEN]))

        split = normalized[normalized["Code"].eq("94320")].iloc[0]
        self.assertEqual(split[JQ_EX_RIGHTS_TYPE], 1)
        self.assertEqual(split[JQ_ADJUSTMENT_FACTOR], 0.04)
        self.assertEqual(str(normalized[JQ_EX_RIGHTS_TYPE].dtype), "Int64")

    def test_daily_trade_status_distinguishes_no_trade_from_partial_data(self) -> None:
        traded = classify_jquants_daily_trade_status(raw_daily_fixture())
        self.assertTrue(traded.eq(DAILY_TRADE_STATUS_TRADED).all())

        no_trade = raw_daily_fixture().iloc[[0]].copy()
        no_trade[["O", "H", "L", "C", "Vo"]] = pd.NA
        status = classify_jquants_daily_trade_status(no_trade)
        self.assertEqual(status.iloc[0], DAILY_TRADE_STATUS_NO_MARKET_TRADE)

        partial = raw_daily_fixture().iloc[[0]].copy()
        partial["C"] = pd.NA
        with self.assertRaisesRegex(ValueError, "partially null"):
            classify_jquants_daily_trade_status(partial)

    def test_rejects_unknown_ex_rights_code(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid J-Quants ExRT"):
            normalize_jquants_daily_fields(
                pd.DataFrame([{"MktCap": 1.0, "ExRT": "9", "AdjFactor": 1.0}])
            )

    def test_watch_sidecar_keeps_etf_null_row_and_old_schema(self) -> None:
        latest = normalize_daily_features(
            raw_daily_fixture(),
            {"7203.T", "200A.T", "9432.T"},
        )
        etf = latest[latest["ticker"].eq("200A.T")].iloc[0]
        self.assertTrue(pd.isna(etf[JQ_MKT_CAP_MILLION_YEN]))
        self.assertEqual(etf[JQ_DAILY_TRADE_STATUS], DAILY_TRADE_STATUS_TRADED)

        old = pd.DataFrame(
            [
                {
                    "trading_date": "2026-08-06",
                    "ticker": "7203.T",
                    "jquants_code": "7203",
                    "source": "old",
                    "fetched_at": "2026-08-06T16:00:00+00:00",
                }
            ]
        )
        merged = merge_daily_features(old, latest)
        self.assertEqual(len(merged), 4)
        for column in JQUANTS_DAILY_FIELD_COLUMNS:
            self.assertIn(column, merged.columns)
        old_row = merged[merged["trading_date"].eq("2026-08-06")].iloc[0]
        self.assertTrue(pd.isna(old_row[JQ_MKT_CAP_MILLION_YEN]))
        self.assertTrue(pd.isna(old_row[JQ_DAILY_TRADE_STATUS]))

    def test_live_market_cap_uses_exact_previous_trading_day_and_keeps_etf(self) -> None:
        grok = pd.DataFrame(
            [
                {"date": "2026-08-10", "ticker": "7203.T", "Close": 3000.0},
                {"date": "2026-08-10", "ticker": "200A.T", "Close": 4300.0},
            ]
        )
        daily = normalize_daily_features(
            raw_daily_fixture(),
            {"7203.T", "200A.T", "9432.T"},
        )
        calendar = pd.DataFrame({"date": ["2026-08-07", "2026-08-10"]})

        attached = attach_official_market_cap_asof(grok, daily, calendar)

        self.assertEqual(len(attached), 2)
        self.assertTrue(
            attached[LIVE_JQ_MARKET_CAP_ASOF_DATE].eq("2026-08-07").all()
        )
        toyota = attached[attached["ticker"].eq("7203.T")].iloc[0]
        self.assertEqual(toyota["market_cap"], 43_493_063_000_000.0)
        self.assertEqual(
            toyota[LIVE_JQ_MARKET_CAP_YEN_ASOF], 43_493_063_000_000.0
        )
        self.assertTrue(pd.isna(toyota[JQ_EX_RIGHTS_TYPE_ASOF]))
        self.assertEqual(toyota["market_cap_source"], MARKET_CAP_SOURCE)

        etf = attached[attached["ticker"].eq("200A.T")].iloc[0]
        self.assertTrue(pd.isna(etf["market_cap"]))
        self.assertEqual(etf["market_cap_source"], MARKET_CAP_SOURCE)
        validate_official_market_cap_input(attached)

        with tempfile.TemporaryDirectory() as directory:
            finalized = attached.assign(prob_up=[0.4, 0.6])
            output = Path(directory) / "grok_trending.parquet"
            calendar_path = Path(directory) / "calendar.parquet"
            finalized.to_parquet(output, index=False)
            calendar.to_parquet(calendar_path, index=False)
            with patch(
                "scripts.pipeline.update_manifest.TRADING_CALENDAR_PATH",
                calendar_path,
            ):
                metadata = validate_finalized_grok_artifact(output)
        self.assertEqual(metadata["row_count"], 2)
        self.assertEqual(metadata["market_cap_asof_date"], "2026-08-07")

    def test_live_market_cap_rejects_same_day_only_source(self) -> None:
        grok = pd.DataFrame(
            [{"date": "2026-08-10", "ticker": "7203.T", "Close": 3000.0}]
        )
        same_day = raw_daily_fixture().iloc[[0]].copy()
        same_day["Date"] = "2026-08-10"
        daily = normalize_daily_features(same_day, {"7203.T"})
        calendar = pd.DataFrame({"date": ["2026-08-07", "2026-08-10"]})

        with self.assertRaisesRegex(ValueError, "D-1 source rows are missing"):
            attach_official_market_cap_asof(grok, daily, calendar)

    def test_legacy_selection_is_upgraded_only_after_exact_official_match(self) -> None:
        legacy = pd.DataFrame(
            [
                {
                    "date": "2026-08-10",
                    "ticker": "7203.T",
                    "market_cap": 43_493_063_000_000.0,
                },
                {
                    "date": "2026-08-10",
                    "ticker": "200A.T",
                    "market_cap": pd.NA,
                },
            ]
        )
        daily = normalize_daily_features(
            raw_daily_fixture(),
            {"7203.T", "200A.T", "9432.T"},
        )
        calendar = pd.DataFrame({"date": ["2026-08-07", "2026-08-10"]})

        upgraded = validate_or_attach_market_cap_provenance(
            legacy,
            pd.Timestamp("2026-08-10").to_pydatetime(),
            daily,
            calendar,
        )
        self.assertEqual(
            upgraded.loc[upgraded["ticker"].eq("7203.T"), "market_cap"].iloc[0],
            43_493_063_000_000.0,
        )
        self.assertTrue(
            upgraded["jq_daily_source_asof"].eq("jquants_api_v2").all()
        )

        with self.assertRaisesRegex(
            JQuantsBacktestDataError,
            "differs from re-fetched official",
        ):
            validate_or_attach_market_cap_provenance(
                legacy.assign(market_cap=[1.0, pd.NA]),
                pd.Timestamp("2026-08-10").to_pydatetime(),
                daily,
                calendar,
            )

    def test_training_market_cap_comes_from_derived_master_not_archive(self) -> None:
        archive = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-08-10",
                    "ticker": "7203.T",
                    "market_cap": 1.0,
                },
                {
                    "backtest_date": "2026-08-10",
                    "ticker": "200A.T",
                    "market_cap": 2.0,
                },
            ]
        )
        master = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-08-10",
                    "ticker": "7203.T",
                    JQ_MARKET_CAP_ASOF_DATE: "2026-08-07",
                    JQ_MKT_CAP_MILLION_YEN_ASOF: 43_493_063.0,
                    JQ_MARKET_CAP_YEN_ASOF: 43_493_063_000_000.0,
                    JQ_ADJUSTMENT_FACTOR_ASOF: 1.0,
                },
                {
                    "backtest_date": "2026-08-10",
                    "ticker": "200A.T",
                    JQ_MARKET_CAP_ASOF_DATE: "2026-08-07",
                    JQ_MKT_CAP_MILLION_YEN_ASOF: pd.NA,
                    JQ_MARKET_CAP_YEN_ASOF: pd.NA,
                    JQ_ADJUSTMENT_FACTOR_ASOF: 1.0,
                },
            ]
        )

        attached = attach_official_market_cap_from_master(archive, master)

        self.assertEqual(len(attached), 2)
        self.assertEqual(attached.iloc[0]["market_cap_archive"], 1.0)
        self.assertEqual(attached.iloc[0]["market_cap"], 43_493_063_000_000.0)
        self.assertTrue(pd.isna(attached.iloc[1]["market_cap"]))
        self.assertEqual(attached.iloc[1]["market_cap_archive"], 2.0)
        self.assertTrue(attached["market_cap_source"].eq(MARKET_CAP_SOURCE).all())

    def test_model_metadata_requires_official_market_cap_source(self) -> None:
        validate_model_market_cap_source(
            {"feature_sources": {"market_cap": MARKET_CAP_SOURCE}}
        )
        with self.assertRaisesRegex(ValueError, "incompatible"):
            validate_model_market_cap_source({})

        with tempfile.TemporaryDirectory() as directory:
            original_model_dir = train_model.MODEL_DIR
            try:
                train_model.MODEL_DIR = Path(directory)
                train_model.save_model(
                    {"dummy": "model"},
                    list(FEATURE_COLUMNS),
                    {"auc_mean": 0.5},
                )
                meta = json.loads(
                    (Path(directory) / "grok_lgbm_meta.json").read_text()
                )
                model_path = Path(directory) / "grok_lgbm_model.pkl"
                validate_model_package(meta, model_path)
            finally:
                train_model.MODEL_DIR = original_model_dir
        self.assertEqual(
            meta["feature_sources"]["market_cap"],
            MARKET_CAP_SOURCE,
        )
        self.assertEqual(len(meta["model_sha256"]), 64)

        with tempfile.TemporaryDirectory() as directory:
            model_path = Path(directory) / "model.pkl"
            model_path.write_bytes(b"wrong model bytes")
            with self.assertRaisesRegex(ValueError, "SHA256"):
                validate_model_package(
                    {
                        "feature_names": list(FEATURE_COLUMNS),
                        "n_features": len(FEATURE_COLUMNS),
                        "feature_contract": FEATURE_CONTRACT,
                        "feature_sources": {
                            "market_cap": MARKET_CAP_SOURCE,
                            "price_history": PRICE_HISTORY_SOURCE,
                        },
                        "model_sha256": "0" * 64,
                    },
                    model_path,
                )

    def test_model_release_gate_rejects_material_regression(self) -> None:
        previous = {
            "feature_contract": FEATURE_CONTRACT,
            "feature_sources": {
                "market_cap": MARKET_CAP_SOURCE,
                "price_history": PRICE_HISTORY_SOURCE,
            },
            "metrics": {
                "auc_mean": 0.55,
                "short_win_rate": 0.60,
                "total_evaluated": 1_000,
            }
        }
        candidate = {
            "feature_contract": FEATURE_CONTRACT,
            "feature_sources": {
                "market_cap": MARKET_CAP_SOURCE,
                "price_history": PRICE_HISTORY_SOURCE,
            },
            "metrics": {
                "auc_mean": 0.56,
                "short_win_rate": 0.61,
                "short_count": 500,
                "short_pnl_total": 1_000_000,
                "short_pf": 1.5,
                "total_evaluated": 1_100,
            },
        }
        validate_model_release(candidate, previous)
        regressed = json.loads(json.dumps(candidate))
        regressed["metrics"]["auc_mean"] = 0.50
        with self.assertRaisesRegex(ValueError, "release gate failed"):
            validate_model_release(regressed, previous)

    def test_grok_price_history_merges_jquants_and_requires_full_coverage(self) -> None:
        dates = pd.bdate_range("2026-06-01", periods=40)
        jquants = pd.DataFrame(
            {
                "date": dates,
                "ticker": "4937.T",
                "Open": 100.0,
                "High": 101.0,
                "Low": 99.0,
                "Close": 100.0,
                "Volume": 1_000.0,
            }
        )
        existing = jquants.iloc[[-1]].assign(Close=123.0)
        merged = merge_price_sources(existing, pd.DataFrame(), jquants)
        self.assertEqual(len(merged), 40)
        self.assertEqual(merged.iloc[-1]["Close"], 123.0)

        trending = pd.DataFrame(
            [{"ticker": "4937.T", "date": dates[-1] + pd.Timedelta(days=3)}]
        )
        validate_current_price_coverage(merged, trending, minimum_rows=35)
        with self.assertRaisesRegex(ValueError, "lack point-in-time"):
            validate_current_price_coverage(
                merged.iloc[:34], trending, minimum_rows=35
            )

    def test_grok_daily_cache_expands_schema_and_detects_old_raw_csv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp_dir = Path(directory)
            new_csv = temp_dir / "new.csv"
            old_csv = temp_dir / "old.csv"
            output = temp_dir / "daily.parquet"
            raw_daily_fixture().to_csv(new_csv, index=False)
            raw_daily_fixture().drop(columns=["MktCap", "ExRT"]).to_csv(
                old_csv, index=False
            )

            self.assertTrue(raw_csv_supports_daily_fields(new_csv))
            self.assertFalse(raw_csv_supports_daily_fields(old_csv))

            normalized = normalize_daily_csv(raw_daily_fixture(), new_csv)
            combined = merge_grok_daily(output, normalized)
            self.assertEqual(len(combined), 3)
            for column in JQUANTS_DAILY_FIELD_COLUMNS:
                self.assertIn(column, combined.columns)

    def test_fetch_rejects_cli_schema_without_new_daily_fields(self) -> None:
        version = Namespace(
            returncode=0,
            stdout="jquants 1.3.0\n",
            stderr="",
        )
        schema = Namespace(
            returncode=0,
            stdout=json.dumps([{"Field": "Date"}, {"Field": "AdjFactor"}]),
            stderr="",
        )
        with patch(
            "scripts.data.fetch_grok_archive_daily_jquants.subprocess.run",
            side_effect=[version, schema],
        ):
            with self.assertRaisesRegex(RuntimeError, "pinned 1.3.0 CLI"):
                validate_jquants_daily_schema({})

    def test_fetch_rejects_unpinned_cli_version(self) -> None:
        result = Namespace(
            returncode=0,
            stdout="jquants 1.0.0\n",
            stderr="",
        )
        with patch(
            "scripts.data.fetch_grok_archive_daily_jquants.subprocess.run",
            return_value=result,
        ):
            with self.assertRaisesRegex(RuntimeError, "pinned release"):
                validate_jquants_daily_schema({})

    def test_derived_master_receives_daily_fields_without_changing_rows(self) -> None:
        archive = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-08-07",
                    "ticker": "7203.T",
                    "_archive_row_id": 0,
                    "_key_backtest_date": "2026-08-07",
                    "_key_ticker": "7203.T",
                }
            ]
        )
        segments = pd.DataFrame(
            [
                {
                    "_key_backtest_date": "2026-08-07",
                    "_key_ticker": "7203.T",
                    "jq_buy_price": 2900.0,
                    "jq_daily_close": 2980.0,
                }
            ]
        )
        daily = pd.DataFrame(
            [
                {
                    "_key_daily_date": "2026-08-06",
                    "_key_ticker": "7203.T",
                    "jq_daily_code": "7203",
                    "close": 2900.0,
                    JQ_MKT_CAP_MILLION_YEN: 42_000_000.0,
                    JQ_MARKET_CAP_YEN: 42_000_000_000_000.0,
                    JQ_EX_RIGHTS_TYPE: pd.NA,
                    JQ_ADJUSTMENT_FACTOR: 1.0,
                    "jq_daily_source": "jquants_cli",
                    "jq_daily_fields_fetched_at": "2026-08-06T16:00:00+00:00",
                },
                {
                    "_key_daily_date": "2026-08-07",
                    "_key_ticker": "7203.T",
                    "jq_daily_code": "7203",
                    "close": 2980.0,
                    JQ_MKT_CAP_MILLION_YEN: 43_493_063.0,
                    JQ_MARKET_CAP_YEN: 43_493_063_000_000.0,
                    JQ_EX_RIGHTS_TYPE: pd.NA,
                    JQ_ADJUSTMENT_FACTOR: 1.0,
                    "jq_daily_source": "jquants_cli",
                    "jq_daily_fields_fetched_at": "2026-08-07T16:00:00+00:00",
                }
            ]
        )

        master = build_master(
            archive,
            segments,
            daily,
            ["2026-08-06", "2026-08-07"],
        )
        self.assertEqual(len(master), 1)
        self.assertEqual(master.iloc[0][JQ_MARKET_CAP_YEN], 43_493_063_000_000.0)
        self.assertEqual(
            master.iloc[0][JQ_MARKET_CAP_YEN_TARGET],
            43_493_063_000_000.0,
        )
        self.assertEqual(master.iloc[0][JQ_MARKET_CAP_ASOF_DATE], "2026-08-06")
        self.assertEqual(master.iloc[0][JQ_MARKET_CAP_YEN_ASOF], 42_000_000_000_000.0)
        self.assertEqual(master.iloc[0][JQ_CLOSE_ASOF], 2900.0)
        self.assertNotIn("close", master.columns)
        self.assertNotIn("_key_backtest_date", master.columns)

    def test_derived_master_marks_official_zero_bar_day_explicitly(self) -> None:
        archive = pd.DataFrame(
            [
                {
                    "backtest_date": "2026-07-24",
                    "ticker": "6898.T",
                    "data_source": "jquants_no_market_trade",
                    "phase1_mark_status": "no_market_trade",
                    "close_execution_status": "no_market_trade",
                    "_archive_row_id": 0,
                    "_key_backtest_date": "2026-07-24",
                    "_key_ticker": "6898.T",
                }
            ]
        )
        daily = pd.DataFrame(
            [
                {
                    "_key_daily_date": "2026-07-23",
                    "_key_ticker": "6898.T",
                    "jq_daily_code": "6898",
                    "close": 3650.0,
                    JQ_MKT_CAP_MILLION_YEN: 2982.0,
                    JQ_MARKET_CAP_YEN: 2_982_000_000.0,
                    JQ_EX_RIGHTS_TYPE: pd.NA,
                    JQ_ADJUSTMENT_FACTOR: 1.0,
                    "jq_daily_source": "jquants_cli",
                    "jq_daily_fields_fetched_at": "2026-07-23T16:00:00+00:00",
                },
                {
                    "_key_daily_date": "2026-07-24",
                    "_key_ticker": "6898.T",
                    "jq_daily_code": "6898",
                    "close": pd.NA,
                    JQ_MKT_CAP_MILLION_YEN: pd.NA,
                    JQ_MARKET_CAP_YEN: pd.NA,
                    JQ_EX_RIGHTS_TYPE: pd.NA,
                    JQ_ADJUSTMENT_FACTOR: 1.0,
                    "jq_daily_source": "jquants_cli",
                    "jq_daily_fields_fetched_at": "2026-07-24T16:00:00+00:00",
                },
            ]
        )
        master = build_master(
            archive,
            pd.DataFrame(
                columns=[
                    "_key_backtest_date",
                    "_key_ticker",
                    "jq_buy_price",
                    "jq_daily_close",
                ]
            ),
            daily,
            ["2026-07-23", "2026-07-24"],
        )
        row = master.iloc[0]
        self.assertEqual(row["jq_bar_count"], 0)
        self.assertEqual(row["jq_open_trade_status"], "no_market_trade")
        self.assertEqual(row["jq_close_execution_status"], "no_market_trade")
        self.assertEqual(row["jq_seg_1530_missing_reason"], "no_market_trade")
        self.assertTrue(pd.isna(row["jq_buy_price"]))

    def test_fetch_plan_includes_previous_trading_date(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp_dir = Path(directory)
            archive_path = temp_dir / "archive.parquet"
            calendar_path = temp_dir / "calendar.parquet"
            pd.DataFrame(
                [
                    {
                        "backtest_date": "2026-08-07",
                        "ticker": "7203.T",
                        "code": "7203",
                    }
                ]
            ).to_parquet(archive_path, index=False)
            pd.DataFrame(
                {"date": ["2026-08-06", "2026-08-07"]}
            ).to_parquet(calendar_path, index=False)

            pairs, dates = load_archive_targets(
                Namespace(
                    archive_path=archive_path,
                    calendar_path=calendar_path,
                    market_date=[],
                    date=None,
                    date_from=None,
                    date_to=None,
                    all_archive=True,
                    max_dates=0,
                    include_selection_asof=True,
                )
            )

            self.assertEqual(len(pairs), 1)
            self.assertEqual(
                dates["trading_date"].tolist(),
                ["2026-08-06", "2026-08-07"],
            )

    def test_validator_accepts_nullable_ex_rights_and_exact_unit_conversion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp_dir = Path(directory)
            frames = {
                "archive": pd.DataFrame(
                    [{"backtest_date": "2026-08-07", "ticker": "7203.T"}]
                ),
                "minute": pd.DataFrame(
                    [
                        {
                            "trading_date": "2026-08-07",
                            "ticker": "7203.T",
                            "datetime": "2026-08-07 09:00:00",
                        },
                        {
                            "trading_date": "2026-08-07",
                            "ticker": "7203.T",
                            "datetime": "2026-08-07 15:30:00",
                        },
                    ]
                ),
                "daily": pd.DataFrame(
                    [
                        {
                            "trading_date": "2026-08-06",
                            "jquants_code": "7203",
                            "close": 2900.0,
                            JQ_MKT_CAP_MILLION_YEN: 42_000_000.0,
                            JQ_MARKET_CAP_YEN: 42_000_000_000_000.0,
                            JQ_EX_RIGHTS_TYPE: pd.NA,
                            JQ_ADJUSTMENT_FACTOR: 1.0,
                        },
                        {
                            "trading_date": "2026-08-07",
                            "jquants_code": "7203",
                            "close": 2980.0,
                            JQ_MKT_CAP_MILLION_YEN: 43_493_063.0,
                            JQ_MARKET_CAP_YEN: 43_493_063_000_000.0,
                            JQ_EX_RIGHTS_TYPE: pd.NA,
                            JQ_ADJUSTMENT_FACTOR: 1.0,
                        }
                    ]
                ),
                "master": pd.DataFrame(
                    [
                        {
                            "backtest_date": "2026-08-07",
                            "ticker": "7203.T",
                            "jq_bar_count": 2,
                            "jq_buy_price": 2900.0,
                            "jq_seg_1530": -8000.0,
                            "jq_close_execution_status": "executable",
                            JQ_MKT_CAP_MILLION_YEN: 43_493_063.0,
                            JQ_MARKET_CAP_YEN: 43_493_063_000_000.0,
                            JQ_EX_RIGHTS_TYPE: pd.NA,
                            JQ_ADJUSTMENT_FACTOR: 1.0,
                            JQ_MKT_CAP_MILLION_YEN_TARGET: 43_493_063.0,
                            JQ_MARKET_CAP_YEN_TARGET: 43_493_063_000_000.0,
                            JQ_EX_RIGHTS_TYPE_TARGET: pd.NA,
                            JQ_ADJUSTMENT_FACTOR_TARGET: 1.0,
                            JQ_MARKET_CAP_ASOF_DATE: "2026-08-06",
                            JQ_MKT_CAP_MILLION_YEN_ASOF: 42_000_000.0,
                            JQ_MARKET_CAP_YEN_ASOF: 42_000_000_000_000.0,
                            JQ_ADJUSTMENT_FACTOR_ASOF: 1.0,
                            JQ_CLOSE_ASOF: 2900.0,
                        }
                    ]
                ),
                "calendar": pd.DataFrame(
                    {"date": ["2026-08-06", "2026-08-07"]}
                ),
            }
            paths: dict[str, Path] = {}
            for name, frame in frames.items():
                path = temp_dir / f"{name}.parquet"
                frame.to_parquet(path, index=False)
                paths[name] = path

            args = Namespace(
                archive_path=paths["archive"],
                minute_path=paths["minute"],
                daily_path=paths["daily"],
                calendar_path=paths["calendar"],
                master_path=paths["master"],
                output_json=temp_dir / "validation.json",
                min_minute_coverage=0.80,
                min_master_coverage_of_minute=0.95,
            )
            report, exit_code = build_report(args)
            self.assertEqual(exit_code, 0)
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["daily"]["mkt_cap_unit_mismatches"], 0)
            self.assertEqual(report["daily"]["selection_asof_coverage"], 1.0)
            self.assertEqual(
                report["master"]["selection_asof_date_mismatches"], 0
            )

            invalid_master = frames["master"].copy()
            invalid_master[JQ_MARKET_CAP_ASOF_DATE] = "2026-08-07"
            invalid_master.to_parquet(paths["master"], index=False)
            invalid_report, invalid_exit_code = build_report(args)
            self.assertEqual(invalid_exit_code, 1)
            self.assertIn(
                "selection as-of dates are not strictly before target: 1",
                invalid_report["failures"],
            )

    def test_validator_counts_explicit_no_market_trade_as_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp_dir = Path(directory)
            frames = {
                "archive": pd.DataFrame(
                    [
                        {
                            "backtest_date": "2026-07-24",
                            "ticker": "6898.T",
                            "data_source": "jquants_no_market_trade",
                            "phase1_mark_status": "no_market_trade",
                            "close_execution_status": "no_market_trade",
                        }
                    ]
                ),
                "minute": pd.DataFrame(
                    columns=["trading_date", "ticker", "datetime"]
                ),
                "daily": pd.DataFrame(
                    [
                        {
                            "trading_date": "2026-07-23",
                            "jquants_code": "6898",
                            "close": 3650.0,
                            JQ_MKT_CAP_MILLION_YEN: 2982.0,
                            JQ_MARKET_CAP_YEN: 2_982_000_000.0,
                            JQ_EX_RIGHTS_TYPE: pd.NA,
                            JQ_ADJUSTMENT_FACTOR: 1.0,
                        },
                        {
                            "trading_date": "2026-07-24",
                            "jquants_code": "6898",
                            "close": pd.NA,
                            JQ_MKT_CAP_MILLION_YEN: pd.NA,
                            JQ_MARKET_CAP_YEN: pd.NA,
                            JQ_EX_RIGHTS_TYPE: pd.NA,
                            JQ_ADJUSTMENT_FACTOR: 1.0,
                        },
                    ]
                ),
                "master": pd.DataFrame(
                    [
                        {
                            "backtest_date": "2026-07-24",
                            "ticker": "6898.T",
                            "jq_bar_count": 0,
                            "jq_buy_price": pd.NA,
                            "jq_seg_1530": pd.NA,
                            "jq_close_execution_status": "no_market_trade",
                            JQ_MKT_CAP_MILLION_YEN: pd.NA,
                            JQ_MARKET_CAP_YEN: pd.NA,
                            JQ_EX_RIGHTS_TYPE: pd.NA,
                            JQ_ADJUSTMENT_FACTOR: 1.0,
                            JQ_MKT_CAP_MILLION_YEN_TARGET: pd.NA,
                            JQ_MARKET_CAP_YEN_TARGET: pd.NA,
                            JQ_EX_RIGHTS_TYPE_TARGET: pd.NA,
                            JQ_ADJUSTMENT_FACTOR_TARGET: 1.0,
                            JQ_MARKET_CAP_ASOF_DATE: "2026-07-23",
                            JQ_MKT_CAP_MILLION_YEN_ASOF: 2982.0,
                            JQ_MARKET_CAP_YEN_ASOF: 2_982_000_000.0,
                            JQ_ADJUSTMENT_FACTOR_ASOF: 1.0,
                            JQ_CLOSE_ASOF: 3650.0,
                        }
                    ]
                ),
                "calendar": pd.DataFrame(
                    {"date": ["2026-07-23", "2026-07-24"]}
                ),
            }
            paths: dict[str, Path] = {}
            for name, frame in frames.items():
                path = temp_dir / f"{name}.parquet"
                frame.to_parquet(path, index=False)
                paths[name] = path
            report, exit_code = build_report(
                Namespace(
                    archive_path=paths["archive"],
                    minute_path=paths["minute"],
                    daily_path=paths["daily"],
                    calendar_path=paths["calendar"],
                    master_path=paths["master"],
                    output_json=temp_dir / "validation.json",
                    min_minute_coverage=0.80,
                    min_master_coverage_of_minute=0.95,
                )
            )
        self.assertEqual(exit_code, 0, report["failures"])
        self.assertEqual(report["minute"]["no_market_trade_keys"], 1)
        self.assertEqual(report["minute"]["unresolved_archive_keys"], 0)
        self.assertEqual(report["minute"]["coverage_of_archive_keys"], 1.0)
        self.assertEqual(
            report["master"]["close_execution_status"],
            {"no_market_trade": 1},
        )


if __name__ == "__main__":
    unittest.main()
