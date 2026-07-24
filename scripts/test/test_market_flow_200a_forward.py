from __future__ import annotations

import unittest

import pandas as pd

from scripts.pipeline.generate_market_flow_200a_forward import (
    FORWARD_COLUMNS,
    STRATEGY_VERSION,
    build_forward_rows,
    build_phase_status,
    merge_forward,
)


TIMES = ("09:00", "09:30", "09:31", "10:00", "10:01", "11:30", "14:00", "14:01", "15:30")
TICKERS = ("200A.T", "2644.T", "285A.T", "8035.T")
SEMICON_TICKERS = {"285A.T", "8035.T"}


def _day(
    date: str,
    *,
    mode: str,
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for ticker in TICKERS:
        session_open = 100.0
        for index, time in enumerate(TIMES):
            close = 100.0
            vwap = 100.0
            if mode == "short_0930" and ticker == "200A.T" and time >= "09:30":
                close = 95.0 if time < "14:00" else 90.0
                vwap = 98.0
            elif mode == "short_1000":
                if ticker in {"200A.T", "2644.T", "285A.T", "8035.T"} and time >= "10:00":
                    close = 95.0 if time < "11:30" else 92.0
                    vwap = 98.0
            elif mode == "long_1400" and time >= "14:00":
                close = 105.0 if time < "15:30" else 108.0
                vwap = 102.0
            records.append(
                {
                    "trading_date": date,
                    "time": time,
                    "ticker": ticker,
                    "datetime": pd.Timestamp(f"{date} {time}"),
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "cum_value": float((index + 1) * 1_000_000),
                    "session_open": session_open,
                    "session_vwap": vwap,
                    "ret_from_open_pct": (close / session_open - 1.0) * 100.0,
                }
            )
    return pd.DataFrame(records)


class MarketFlowForwardRouteTests(unittest.TestCase):
    def test_primary_route_precedence_and_fixed_exits(self) -> None:
        features = pd.concat(
            [
                _day("2026-07-23", mode="short_0930"),
                _day("2026-07-24", mode="short_1000"),
                _day("2026-07-27", mode="long_1400"),
            ],
            ignore_index=True,
        )

        result = build_forward_rows(
            features,
            SEMICON_TICKERS,
            start_date="2026-07-23",
            recorded_at="2026-07-27T16:45:00+09:00",
        )

        self.assertEqual(len(result), 9)
        primary = result[result["primary_selected"]].sort_values("trade_date")
        self.assertEqual(
            primary["rule_id"].tolist(),
            [
                "S_0930_SELF_TO_1400",
                "S_1000_BREADTH_TO_1130",
                "L_1400_STRICT_TO_1530",
            ],
        )
        self.assertEqual(primary["execution_status"].tolist(), ["completed"] * 3)
        self.assertEqual(primary["entry_time"].tolist(), ["09:31", "10:01", "14:01"])
        self.assertTrue(primary["net_return_bps"].gt(0).all())

    def test_signal_uses_only_bars_through_the_signal_time(self) -> None:
        features = _day("2026-07-23", mode="long_1400")
        result = build_forward_rows(features, SEMICON_TICKERS, start_date="2026-07-23")

        early = result[result["rule_id"].eq("S_0930_SELF_TO_1400")].iloc[0]
        late = result[result["rule_id"].eq("L_1400_STRICT_TO_1530")].iloc[0]
        self.assertFalse(bool(early["triggered"]))
        self.assertTrue(bool(late["triggered"]))
        self.assertEqual(late["entry_time"], "14:01")


class MarketFlowForwardPersistenceTests(unittest.TestCase):
    def test_merge_is_idempotent_by_strategy_date_and_rule(self) -> None:
        generated = build_forward_rows(
            _day("2026-07-23", mode="short_0930"),
            SEMICON_TICKERS,
            start_date="2026-07-23",
            recorded_at="2026-07-23T16:45:00+09:00",
        )
        replacement = generated.copy()
        replacement["recorded_at"] = "2026-07-23T17:00:00+09:00"

        merged = merge_forward(generated, replacement)

        self.assertEqual(len(merged), 3)
        self.assertEqual(set(merged["recorded_at"]), {"2026-07-23T17:00:00+09:00"})
        self.assertEqual(list(merged.columns), FORWARD_COLUMNS)

    def test_phase3_remains_blocked_before_twenty_primary_signals(self) -> None:
        forward = build_forward_rows(
            _day("2026-07-23", mode="short_0930"),
            SEMICON_TICKERS,
            start_date="2026-07-23",
        )

        status = build_phase_status(forward)

        self.assertEqual(status["strategy_version"], STRATEGY_VERSION)
        self.assertEqual(status["mode"], "phase2_forward_shadow")
        self.assertFalse(status["phase3_live_eligible"])
        self.assertIn("sample_count", status["blocked_reasons"])

    def test_phase3_unlocks_only_after_all_gate_checks_pass(self) -> None:
        rows: list[dict[str, object]] = []
        for index in range(20):
            row = {column: None for column in FORWARD_COLUMNS}
            row.update(
                {
                    "strategy_version": STRATEGY_VERSION,
                    "trade_date": (pd.Timestamp("2026-07-23") + pd.Timedelta(days=index)).strftime("%Y-%m-%d"),
                    "rule_id": "S_0930_SELF_TO_1400",
                    "priority": 1,
                    "side": "short",
                    "signal_time": "09:30",
                    "exit_time": "14:00",
                    "signal_available": True,
                    "triggered": True,
                    "primary_selected": True,
                    "execution_status": "completed",
                    "net_return_bps": 10.0,
                    "shadow_pnl_yen_30": 150.0,
                    "phase3_stage1_pnl_yen_1": 5.0,
                    "recorded_at": "2026-08-31T16:45:00+09:00",
                }
            )
            rows.append(row)

        status = build_phase_status(pd.DataFrame(rows))

        self.assertTrue(status["phase3_live_eligible"])
        self.assertEqual(status["mode"], "phase3_live_eligible")
        self.assertEqual(status["blocked_reasons"], [])


if __name__ == "__main__":
    unittest.main()
