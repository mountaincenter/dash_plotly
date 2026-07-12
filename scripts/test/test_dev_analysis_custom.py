from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import patch

import pandas as pd

from server.services.grok_history import (
    GrokHistoryError,
    LEGACY_INTRADAY_COLUMNS,
    SEGMENT_COLUMNS,
    normalize_grok_history,
)
from server.routers.dev_analysis_custom import (
    PRICE_RANGE_UNKNOWN_LABEL,
    PROB_BIN_LABELS,
    _build_data_scope,
    _calc_bucket_pf,
    _direction_multiplier,
    _filter_executable_rows,
    _max_drawdown,
    assign_prob_bin,
    get_futures_gap_pf,
    get_weekday_risk_matrix,
    get_weekday_panels,
    prepare_data,
)
from server.routers.dev_day_trade_list import (
    build_grok_expected_pf_lookup,
    get_prob_bin,
    get_weekday_rule,
)


class DevAnalysisDirectionTests(unittest.TestCase):
    def test_analysis_segments_are_native_short_pnl(self) -> None:
        self.assertEqual(_direction_multiplier("short"), 1)
        self.assertEqual(_direction_multiplier("long"), -1)

    def test_bucket_pf_uses_the_canonical_direction_multiplier(self) -> None:
        frame = pd.DataFrame({"seg_1530": [100.0] * 5 + [-50.0] * 5})

        short = _calc_bucket_pf(frame, "LOW_PROB_HEAT", direction="short")
        long = _calc_bucket_pf(frame, "LOW_PROB_HEAT", direction="long")

        self.assertEqual(short["n"], 10)
        self.assertEqual(short["avg"], 25)
        self.assertEqual(short["pf"], 2.0)
        self.assertEqual(long["avg"], -25)
        self.assertEqual(long["pf"], 0.5)


class DevAnalysisDrawdownTests(unittest.TestCase):
    def test_initial_loss_is_included_in_max_drawdown(self) -> None:
        self.assertEqual(_max_drawdown(pd.Series([-100.0, 50.0])), -100.0)

    def test_profitable_path_has_zero_drawdown(self) -> None:
        self.assertEqual(_max_drawdown(pd.Series([100.0, 50.0])), 0.0)


class DevAnalysisUniverseTests(unittest.TestCase):
    def test_only_close_executable_rows_enter_the_analysis_universe(self) -> None:
        selected = pd.DataFrame(
            {
                "close_execution_status": [
                    "executable",
                    "mark_only_no_round_trip",
                    None,
                ]
            }
        )

        analysis = _filter_executable_rows(selected)

        self.assertEqual(len(analysis), 1)
        self.assertEqual(analysis.iloc[0]["close_execution_status"], "executable")

    def test_data_scope_separates_selected_and_executable_counts(self) -> None:
        raw = pd.DataFrame(
            {
                "backtest_date": pd.to_datetime(["2025-12-22"] * 3),
                "analysis_source": ["grok_master_jquants_segments"] * 3,
            }
        )
        raw.attrs["analysis_source"] = "grok_master_jquants_segments"
        raw.attrs["price_basis"] = "jquants_minute"
        selected = pd.DataFrame(
            {
                "close_execution_status": [
                    "executable",
                    "mark_only_no_round_trip",
                    None,
                ]
            }
        )
        analysis = _filter_executable_rows(selected)

        scope = _build_data_scope(raw, selected, analysis_df=analysis)

        self.assertEqual(scope["selectedRows"], 3)
        self.assertEqual(scope["executableRows"], 1)
        self.assertEqual(scope["excludedNonExecutableRows"], 2)
        self.assertEqual(scope["analysisUniverse"], "close_executable")

    def test_data_scope_does_not_claim_executable_when_status_is_missing(self) -> None:
        raw = pd.DataFrame(
            {
                "backtest_date": pd.to_datetime(["2025-12-22"]),
                "analysis_source": ["grok_trending_archive"],
            }
        )
        selected = pd.DataFrame({"buy_price": [100.0]})
        analysis = _filter_executable_rows(selected)

        scope = _build_data_scope(raw, selected, analysis_df=analysis)

        self.assertEqual(scope["analysisUniverse"], "execution_status_unavailable")
        self.assertIsNone(scope["executableRows"])
        self.assertIsNone(scope["excludedNonExecutableRows"])

    def test_price_range_uses_prev_close_and_keeps_unknown_explicit(self) -> None:
        frame = pd.DataFrame(
            {
                "backtest_date": pd.to_datetime(["2025-12-22", "2025-12-23"]),
                "buy_price": [9000.0, 2000.0],
                "prev_close": [900.0, None],
                "shortable": [True, True],
                "day_trade": [False, False],
                "day_trade_available_shares": [None, None],
            }
        )
        price_ranges = [
            {"label": "~1,000円", "min": 0, "max": 1000},
            {"label": "1,000~3,000円", "min": 1000, "max": 3000},
        ]

        prepared = prepare_data(frame, price_ranges)

        self.assertEqual(prepared.iloc[0]["price_range"], "~1,000円")
        self.assertEqual(prepared.iloc[1]["price_range"], PRICE_RANGE_UNKNOWN_LABEL)


class DevAnalysisEndpointTests(unittest.TestCase):
    @staticmethod
    def _base_frame() -> pd.DataFrame:
        pnl = [100.0] * 5 + [-50.0] * 5
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-07-06"] * 10),
                "bucket": ["LOW_PROB_HEAT"] * 10,
                "futures_change_pct": [-1.0] * 10,
                "volume": [1000.0] * 10,
                "day_trade_available_shares": [100.0] * 10,
                "phase1_return": [0.01] * 5 + [-0.005] * 5,
                "phase2_return": [0.01] * 5 + [-0.005] * 5,
                "morning_max_gain_pct": [3.0] * 10,
                "morning_max_drawdown_pct": [-4.0] * 10,
                "daily_max_gain_pct": [3.0] * 10,
                "daily_max_drawdown_pct": [-5.0] * 10,
                "seg_1530": pnl,
                "profit_per_100_shares_morning_early": pnl,
                "buy_price": [100.0] * 10,
                "close_execution_status": ["executable"] * 10,
            }
        )
        frame.attrs["data_scope"] = {"scope": "test"}
        return frame

    def test_futures_endpoint_preserves_native_short_pnl(self) -> None:
        frame = self._base_frame()
        with patch(
            "server.routers.dev_analysis_custom._load_analysis_base",
            return_value=frame,
        ):
            response = asyncio.run(
                get_futures_gap_pf(weekday=0, direction="short")
            )

        payload = json.loads(response.body)
        low = payload["rows"][0]["LOW_PROB_HEAT"]
        self.assertEqual(low["avg"], 25)
        self.assertEqual(low["pf"], 2.0)

    def test_weekday_panels_use_short_pnl_and_short_excursions(self) -> None:
        frame = self._base_frame()
        with patch(
            "server.routers.dev_analysis_custom._load_analysis_base",
            return_value=frame,
        ):
            response = asyncio.run(get_weekday_panels(weekday=0, direction="short"))

        payload = json.loads(response.body)
        close = payload["hold_vs_exit"]["seg_1530"]
        self.assertEqual(close["total_pnl"], 250)
        self.assertEqual(close["pf"], 2.0)

        favorable = payload["excursion"]["daily_max_drawdown_pct"]
        adverse = payload["excursion"]["daily_max_gain_pct"]
        self.assertEqual(favorable["label"], "日中最大含み益%")
        self.assertEqual(favorable["p50"], 5.0)
        self.assertEqual(adverse["label"], "日中最大含み損%")
        self.assertEqual(adverse["p50"], -3.0)
        self.assertEqual(payload["hold_vs_exit"]["giveback_pct"], 80.0)

    def test_weekday_panels_keep_long_as_the_inverse_direction(self) -> None:
        frame = self._base_frame()
        with patch(
            "server.routers.dev_analysis_custom._load_analysis_base",
            return_value=frame,
        ):
            response = asyncio.run(get_weekday_panels(weekday=0, direction="long"))

        payload = json.loads(response.body)
        close = payload["hold_vs_exit"]["seg_1530"]
        self.assertEqual(close["total_pnl"], -250)
        self.assertEqual(close["pf"], 0.5)

        favorable = payload["excursion"]["daily_max_gain_pct"]
        adverse = payload["excursion"]["daily_max_drawdown_pct"]
        self.assertEqual(favorable["label"], "日中最大含み益%")
        self.assertEqual(favorable["p50"], 3.0)
        self.assertEqual(adverse["label"], "日中最大含み損%")
        self.assertEqual(adverse["p50"], -5.0)
        self.assertEqual(payload["hold_vs_exit"]["giveback_pct"], 83.3)

    def test_risk_matrix_all_group_keeps_rows_with_missing_prob(self) -> None:
        frame = self._base_frame()
        frame["margin_type"] = "制度信用"
        frame["is_ex0"] = True
        frame["ml_prob"] = [float("nan")] + [0.45] * 9
        frame["ml_prob_live"] = float("nan")
        frame["seg_1030"] = frame["seg_1530"]
        frame["seg_1130"] = frame["seg_1530"]
        frame["seg_1400"] = frame["seg_1530"]

        with patch(
            "server.routers.dev_analysis_custom._load_analysis_base",
            return_value=frame,
        ):
            response = asyncio.run(
                get_weekday_risk_matrix(
                    weekday=0,
                    direction="short",
                    segment_mode="4seg",
                    prob_mode="bin",
                )
            )

        payload = json.loads(response.body)
        all_row = next(
            row
            for row in payload["rows"]
            if row["marginKey"] == "all" and row["probKey"] == "all"
        )
        prob_rows = [
            row
            for row in payload["rows"]
            if row["marginKey"] == "all" and row["probKey"] in PROB_BIN_LABELS
        ]
        self.assertEqual(all_row["count"], 10)
        self.assertEqual(sum(row["count"] for row in prob_rows), 9)


class GrokHistoryNormalizationTests(unittest.TestCase):
    @staticmethod
    def _archive_frame() -> pd.DataFrame:
        frame = pd.DataFrame(
            {
                "backtest_date": pd.to_datetime(["2026-07-09", "2026-07-10"]),
                "ticker": ["1111.T", "2222.T"],
                "buy_price": [100.0, 200.0],
                "close_execution_status": [
                    "executable",
                    "mark_only_no_round_trip",
                ],
                "profit_per_100_shares_phase1": [-999.0, -999.0],
                "profit_per_100_shares_phase2": [-999.0, -999.0],
                "phase1_return": [-9.99, -9.99],
                "phase2_return": [-9.99, -9.99],
                "phase1_win": [False, False],
                "phase2_win": [True, True],
                **{segment: [0.0, 0.0] for segment in SEGMENT_COLUMNS},
            }
        )
        frame.loc[0, "seg_1130"] = 200.0
        frame.loc[0, "seg_1530"] = -100.0
        frame.loc[1, "seg_1130"] = float("nan")
        return frame

    def test_phase_aliases_are_derived_from_canonical_segments(self) -> None:
        normalized = normalize_grok_history(self._archive_frame())

        self.assertEqual(normalized.loc[0, "profit_per_100_shares_phase1"], 200.0)
        self.assertEqual(normalized.loc[0, "sell_price"], 98.0)
        self.assertEqual(normalized.loc[0, "phase1_return"], 0.02)
        self.assertTrue(normalized.loc[0, "phase1_win"])
        self.assertTrue(pd.isna(normalized.loc[1, "profit_per_100_shares_phase1"]))
        self.assertTrue(pd.isna(normalized.loc[1, "sell_price"]))
        self.assertEqual(normalized.loc[0, "profit_per_100_shares_phase2"], -100.0)
        self.assertEqual(normalized.loc[0, "phase2_return"], -0.01)
        self.assertFalse(normalized.loc[0, "phase2_win"])
        self.assertFalse(normalized.loc[1, "phase2_win"])
        self.assertEqual(normalized.attrs["analysis_source"], "grok_trending_archive")
        self.assertEqual(normalized.attrs["price_basis"], "jquants_minute")
        self.assertEqual(normalized.attrs["close_executable_rows"], 1)
        self.assertEqual(normalized.attrs["close_mark_only_rows"], 1)

    def test_missing_canonical_segment_fails_closed(self) -> None:
        frame = self._archive_frame().drop(columns=["seg_1130"])

        with self.assertRaises(GrokHistoryError):
            normalize_grok_history(frame)

    def test_legacy_intraday_metrics_are_hidden_and_daily_excursion_is_rebuilt(self) -> None:
        frame = self._archive_frame()
        frame["high"] = [110.0, 220.0]
        frame["low"] = [90.0, 180.0]
        for column in LEGACY_INTRADAY_COLUMNS:
            frame[column] = -999.0

        normalized = normalize_grok_history(frame)

        for column in LEGACY_INTRADAY_COLUMNS:
            self.assertNotIn(column, normalized.columns)
        self.assertEqual(normalized.loc[0, "daily_max_gain_pct"], 10.0)
        self.assertEqual(normalized.loc[0, "daily_max_drawdown_pct"], -10.0)
        self.assertEqual(
            set(normalized.attrs["disabled_legacy_intraday_columns"]),
            set(LEGACY_INTRADAY_COLUMNS),
        )

    def test_analysis_loader_uses_only_the_canonical_archive_service(self) -> None:
        normalized = normalize_grok_history(self._archive_frame())
        with patch(
            "server.routers.dev_analysis_custom.load_grok_history",
            return_value=normalized,
        ) as loader:
            from server.routers.dev_analysis_custom import load_archive

            loaded = load_archive()

        loader.assert_called_once()
        self.assertEqual(loaded.attrs["analysis_source"], "grok_trending_archive")

    def test_recommendations_loader_uses_only_the_canonical_archive_service(self) -> None:
        normalized = normalize_grok_history(self._archive_frame())
        with patch(
            "server.routers.dev_day_trade_list.load_grok_history",
            return_value=normalized,
        ) as loader:
            from server.routers.dev_day_trade_list import load_grok_expected_pf_history

            loaded = load_grok_expected_pf_history()

        loader.assert_called_once()
        self.assertEqual(loaded.attrs["analysis_source"], "grok_trending_archive")


class GrokProbabilityBoundaryTests(unittest.TestCase):
    def test_prob_bins_align_with_heat_regime_boundaries(self) -> None:
        cases = {
            0.0: "0.0-0.1",
            0.399999: "0.3-0.4",
            0.4: "0.4-0.5",
            0.5: "0.5-0.6",
            0.999999: "0.9-1.0",
            1.0: "0.9-1.0",
        }
        for probability, expected in cases.items():
            with self.subTest(probability=probability):
                self.assertEqual(assign_prob_bin(probability), expected)
                self.assertEqual(get_prob_bin(probability), expected)

    def test_expected_pf_and_weekday_pf_exclude_mark_only_rows(self) -> None:
        frame = pd.DataFrame(
            {
                "backtest_date": pd.to_datetime(["2026-07-06"] * 3),
                "ticker": ["1111.T", "2222.T", "3333.T"],
                "buy_price": [100.0] * 3,
                "seg_1530": [100.0, -50.0, 0.0],
                "ml_prob": [0.45] * 3,
                "ml_prob_live": [float("nan")] * 3,
                "shortable": [True] * 3,
                "day_trade": [True] * 3,
                "ng": [False] * 3,
                "day_trade_available_shares": [100.0] * 3,
                "close_execution_status": [
                    "executable",
                    "executable",
                    "mark_only_no_round_trip",
                ],
            }
        )

        lookup = build_grok_expected_pf_lookup(frame)
        metrics = lookup[
            (
                "weekday+credit+prob",
                ("月曜日", "制度信用", "0.4-0.5"),
            )
        ]
        weekday = get_weekday_rule(pd.Timestamp("2026-07-06"), frame)

        self.assertEqual(metrics["n"], 2)
        self.assertEqual(metrics["pf"], 2.0)
        self.assertEqual(metrics["mark_only_n"], 0)
        self.assertEqual(weekday["n"], 2)
        self.assertEqual(weekday["pf"], 2.0)
        self.assertEqual(weekday["pnl"], 50.0)

if __name__ == "__main__":
    unittest.main()
