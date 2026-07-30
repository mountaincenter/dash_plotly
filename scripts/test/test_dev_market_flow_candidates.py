from __future__ import annotations

import unittest

import pandas as pd

from server.routers.dev_market_flow import (
    _directional_candidate_lens,
    _latest_atr14,
)


LATEST_DATE = "2026-07-30"
FLOW_DATES = ["2026-07-28", "2026-07-29", LATEST_DATE]


def _prices(tickers: list[str], *, end: str = LATEST_DATE) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ticker_index, ticker in enumerate(tickers):
        for day_index, date in enumerate(pd.bdate_range(end=end, periods=16)):
            close = 100.0 + ticker_index * 5.0 + day_index * 0.2
            rows.append({
                "ticker": ticker,
                "date": date,
                "High": close + 1.0,
                "Low": close - 1.0,
                "Close": close,
            })
    return pd.DataFrame(rows)


def _flow_frame() -> tuple[pd.DataFrame, pd.DataFrame]:
    definitions = [
        ("1001.T", "保険A", "保険業", False, False, 10.0, 0.8),
        ("1002.T", "保険B", "保険業", False, False, 8.0, 0.5),
        ("200A.T", "半導体ETF", "ETF", True, True, 20.0, 1.0),
        ("285A.T", "半導体", "電気機器", False, True, 30.0, 1.5),
    ]
    rows: list[dict[str, object]] = []
    for date_index, date in enumerate(FLOW_DATES):
        for rank, definition in enumerate(definitions, start=1):
            ticker, name, sector, is_etf, is_semiconductor, turnover, oc = definition
            rows.append({
                "date": date,
                "rank": rank + 30,
                "rank_change": 1.0,
                "ticker": ticker,
                "code": ticker.replace(".T", ""),
                "stock_name": name,
                "sectors": sector,
                "trading_value_billion": turnover + date_index,
                "open_to_close_pct": oc,
                "is_etf": is_etf,
                "is_semiconductor": is_semiconductor,
                "is_new_top150": False,
                "consecutive_days_in_top150": 3,
            })
    recent = pd.DataFrame(rows)
    latest = recent[recent["date"].eq(LATEST_DATE)].copy()
    return latest, recent


class MarketFlowAtrTests(unittest.TestCase):
    def test_atr_requires_a_fresh_latest_price(self) -> None:
        fresh = _latest_atr14(_prices(["1001.T"]), LATEST_DATE)
        stale = _latest_atr14(_prices(["1001.T"], end="2026-07-29"), LATEST_DATE)

        self.assertEqual(fresh.iloc[0]["price_date"], LATEST_DATE)
        self.assertGreater(float(fresh.iloc[0]["atr14_pct"]), 0)
        self.assertTrue(pd.isna(stale.iloc[0]["atr14_pct"]))


class MarketFlowCandidateLensTests(unittest.TestCase):
    def test_candidates_exclude_semiconductors_and_etfs(self) -> None:
        latest, recent = _flow_frame()

        result = _directional_candidate_lens(
            latest,
            recent,
            FLOW_DATES,
            _prices(["1001.T", "1002.T", "200A.T", "285A.T"]),
        )

        tickers = {row["ticker"] for row in result["candidates"]}
        self.assertEqual(tickers, {"1001.T", "1002.T"})
        self.assertTrue(all(row["direction"] == "long" for row in result["candidates"]))
        self.assertTrue(all(row["flow_status"] == "confirmed" for row in result["candidates"]))
        self.assertTrue(
            all(row["selection_status"] in {"qualified", "watch"} for row in result["candidates"])
        )
        self.assertEqual(result["coverage"]["eligible_count"], 2)
        self.assertEqual(result["coverage"]["atr_available_count"], 2)

    def test_zero_percent_sector_breadth_confirms_short_flow(self) -> None:
        latest, recent = _flow_frame()
        eligible = recent["ticker"].isin({"1001.T", "1002.T"})
        recent.loc[eligible, "open_to_close_pct"] *= -1
        latest = recent[recent["date"].eq(LATEST_DATE)].copy()

        result = _directional_candidate_lens(
            latest,
            recent,
            FLOW_DATES,
            _prices(["1001.T", "1002.T"]),
        )

        self.assertTrue(all(row["direction"] == "short" for row in result["candidates"]))
        self.assertTrue(all(row["flow_status"] == "confirmed" for row in result["candidates"]))

    def test_missing_atr_is_explicitly_unverified(self) -> None:
        latest, recent = _flow_frame()

        result = _directional_candidate_lens(
            latest,
            recent,
            FLOW_DATES,
            _prices(["1001.T"]),
        )
        by_ticker = {row["ticker"]: row for row in result["candidates"]}

        self.assertEqual(by_ticker["1002.T"]["selection_status"], "unverified")
        self.assertEqual(by_ticker["1002.T"]["calm_status"], "unverified")
        self.assertIn("ATR14未取得", by_ticker["1002.T"]["risk_reasons"])


if __name__ == "__main__":
    unittest.main()
