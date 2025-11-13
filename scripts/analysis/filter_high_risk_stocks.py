#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
バックテストの知見を元に、明日の選定銘柄から除外すべき高リスク銘柄を選定

除外基準:
1. 前日急騰 (+15%以上)
2. 前日急落 (-10%以下)
3. 出来高異常 (平均の10倍以上)
4. 過去バックテストで勝率が低かった特徴
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))


def load_data():
    """Load all required data"""
    print("[1/4] Loading latest grok_trending.parquet...")
    grok_latest = pd.read_parquet("/tmp/grok_trending_latest.parquet")
    print(f"      Loaded {len(grok_latest)} stocks")

    print("[2/4] Loading 5m price data...")
    prices_5m = pd.read_parquet("/tmp/prices_60d_5m.parquet")
    print(f"      Loaded {len(prices_5m)} rows")

    print("[3/4] Loading daily price data...")
    prices_1d = pd.read_parquet("/tmp/prices_max_1d.parquet")
    print(f"      Loaded {len(prices_1d)} rows")

    print("[4/4] Loading backtest archive (v1.3)...")
    archive_path = project_root / "data" / "parquet" / "backtest" / "v1.3_grok_trending_archive_with_backtest.parquet"
    backtest_archive = pd.read_parquet(archive_path)
    print(f"      Loaded {len(backtest_archive)} backtest records")

    return grok_latest, prices_5m, prices_1d, backtest_archive


def calculate_previous_day_metrics(ticker: str, prices_1d: pd.DataFrame) -> dict:
    """Calculate previous day's price change and volume"""
    ticker_data = prices_1d[prices_1d["ticker"] == ticker].copy()

    if ticker_data.empty:
        return None

    ticker_data["date"] = pd.to_datetime(ticker_data["date"])
    ticker_data = ticker_data.sort_values("date")

    # Get last 2 trading days
    if len(ticker_data) < 2:
        return None

    latest = ticker_data.iloc[-1]
    previous = ticker_data.iloc[-2]

    # Calculate metrics
    price_change_pct = ((latest["Close"] - previous["Close"]) / previous["Close"]) * 100

    # Average volume (last 20 days)
    if len(ticker_data) >= 20:
        avg_volume = ticker_data.iloc[-20:]["Volume"].mean()
    else:
        avg_volume = ticker_data["Volume"].mean()

    volume_ratio = latest["Volume"] / avg_volume if avg_volume > 0 else 0

    return {
        "latest_close": latest["Close"],
        "latest_date": latest["date"],
        "price_change_pct": price_change_pct,
        "latest_volume": latest["Volume"],
        "avg_volume": avg_volume,
        "volume_ratio": volume_ratio
    }


def calculate_intraday_volatility(ticker: str, prices_5m: pd.DataFrame) -> dict:
    """Calculate intraday volatility for latest trading day"""
    ticker_5m = prices_5m[prices_5m["ticker"] == ticker].copy()

    if ticker_5m.empty:
        return None

    ticker_5m["date"] = pd.to_datetime(ticker_5m["date"])
    ticker_5m = ticker_5m.sort_values("date")

    # Get latest trading day
    latest_date = ticker_5m["date"].dt.date.max()
    latest_day = ticker_5m[ticker_5m["date"].dt.date == latest_date]

    if latest_day.empty:
        return None

    # Calculate intraday range
    high = latest_day["High"].max()
    low = latest_day["Low"].min()
    open_price = latest_day.iloc[0]["Open"]

    intraday_range_pct = ((high - low) / open_price) * 100 if open_price > 0 else 0

    return {
        "intraday_range_pct": intraday_range_pct,
        "high": high,
        "low": low
    }


def get_backtest_stats(ticker: str, backtest_archive: pd.DataFrame) -> dict:
    """Get historical backtest statistics for ticker"""
    ticker_history = backtest_archive[backtest_archive["ticker"] == ticker]

    if ticker_history.empty:
        return None

    phase1_data = ticker_history[ticker_history["phase1_return"].notna()]
    phase2_data = ticker_history[ticker_history["phase2_return"].notna()]

    stats = {}

    if not phase1_data.empty:
        stats["phase1_avg_return"] = phase1_data["phase1_return"].mean()
        stats["phase1_win_rate"] = phase1_data["phase1_win"].sum() / len(phase1_data)
        stats["phase1_count"] = len(phase1_data)

    if not phase2_data.empty:
        stats["phase2_avg_return"] = phase2_data["phase2_return"].mean()
        stats["phase2_win_rate"] = phase2_data["phase2_win"].sum() / len(phase2_data)
        stats["phase2_count"] = len(phase2_data)

    return stats if stats else None


def determine_exclusion_reasons(row: dict, prev_day: dict, intraday: dict, backtest_stats: dict) -> list:
    """Determine if stock should be excluded and why"""
    reasons = []

    # Check previous day metrics
    if prev_day:
        if prev_day["price_change_pct"] >= 15:
            reasons.append(f"前日急騰 (+{prev_day['price_change_pct']:.1f}%)")

        if prev_day["price_change_pct"] <= -10:
            reasons.append(f"前日急落 ({prev_day['price_change_pct']:.1f}%)")

        if prev_day["volume_ratio"] >= 10:
            reasons.append(f"出来高異常 ({prev_day['volume_ratio']:.1f}倍)")

    # Check intraday volatility
    if intraday:
        if intraday["intraday_range_pct"] >= 20:
            reasons.append(f"日中ボラ大 ({intraday['intraday_range_pct']:.1f}%)")

    # Check backtest history
    if backtest_stats:
        # Phase2 勝率が30%未満
        if "phase2_win_rate" in backtest_stats:
            if backtest_stats["phase2_win_rate"] < 0.3 and backtest_stats["phase2_count"] >= 2:
                reasons.append(f"過去勝率低 (Phase2: {backtest_stats['phase2_win_rate']*100:.0f}%, {backtest_stats['phase2_count']}回)")

        # Phase2 平均リターンがマイナス
        if "phase2_avg_return" in backtest_stats:
            if backtest_stats["phase2_avg_return"] < -0.02 and backtest_stats["phase2_count"] >= 2:
                reasons.append(f"過去平均赤字 (Phase2: {backtest_stats['phase2_avg_return']*100:.1f}%)")

    return reasons


def main():
    print("=" * 80)
    print("高リスク銘柄の除外判定 (バックテスト知見ベース)")
    print("=" * 80)
    print()

    # Load data
    grok_latest, prices_5m, prices_1d, backtest_archive = load_data()
    print()

    print("=" * 80)
    print("分析開始")
    print("=" * 80)
    print()

    results = []

    for idx, row in grok_latest.iterrows():
        ticker = row["ticker"]
        stock_name = row.get("stock_name", "")

        print(f"[{idx+1}/{len(grok_latest)}] {ticker} - {stock_name}")

        # Get metrics
        prev_day = calculate_previous_day_metrics(ticker, prices_1d)
        intraday = calculate_intraday_volatility(ticker, prices_5m)
        backtest_stats = get_backtest_stats(ticker, backtest_archive)

        # Determine exclusion
        exclusion_reasons = determine_exclusion_reasons(row, prev_day, intraday, backtest_stats)

        result = {
            "ticker": ticker,
            "stock_name": stock_name,
            "should_exclude": len(exclusion_reasons) > 0,
            "exclusion_reasons": ", ".join(exclusion_reasons) if exclusion_reasons else "",
            "price_change_pct": prev_day["price_change_pct"] if prev_day else None,
            "volume_ratio": prev_day["volume_ratio"] if prev_day else None,
            "intraday_range_pct": intraday["intraday_range_pct"] if intraday else None,
            "backtest_phase2_win_rate": backtest_stats.get("phase2_win_rate") if backtest_stats else None,
            "backtest_phase2_count": backtest_stats.get("phase2_count") if backtest_stats else None
        }

        results.append(result)

        if exclusion_reasons:
            print(f"  ⚠️  除外推奨: {', '.join(exclusion_reasons)}")
        else:
            print(f"  ✅ OK")

        # Print details
        if prev_day:
            print(f"      前日変化率: {prev_day['price_change_pct']:+.2f}%, 出来高比: {prev_day['volume_ratio']:.2f}x")
        if intraday:
            print(f"      日中レンジ: {intraday['intraday_range_pct']:.2f}%")
        if backtest_stats and "phase2_win_rate" in backtest_stats:
            print(f"      過去実績: Phase2勝率 {backtest_stats['phase2_win_rate']*100:.1f}% ({backtest_stats['phase2_count']}回)")

        print()

    # Create DataFrame
    df_results = pd.DataFrame(results)

    # Summary
    print("=" * 80)
    print("📊 サマリー")
    print("=" * 80)
    print()

    excluded = df_results[df_results["should_exclude"]]
    remaining = df_results[~df_results["should_exclude"]]

    print(f"総銘柄数: {len(df_results)}")
    print(f"除外推奨: {len(excluded)}")
    print(f"残存銘柄: {len(remaining)}")
    print()

    if len(excluded) > 0:
        print("除外推奨銘柄:")
        for idx, row in excluded.iterrows():
            print(f"  ❌ {row['ticker']} - {row['stock_name']}")
            print(f"      理由: {row['exclusion_reasons']}")
        print()

    if len(remaining) > 0:
        print("取引推奨銘柄:")
        for idx, row in remaining.iterrows():
            print(f"  ✅ {row['ticker']} - {row['stock_name']}")
        print()

    # Save results
    output_dir = project_root / "test_output"
    output_dir.mkdir(exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    output_file = output_dir / f"filtered_stocks_{today}.parquet"

    # Save only remaining stocks
    remaining_stocks = grok_latest[grok_latest["ticker"].isin(remaining["ticker"])]
    remaining_stocks.to_parquet(output_file, index=False)

    print(f"✅ 保存完了: {output_file}")
    print(f"   除外後銘柄数: {len(remaining_stocks)}")

    # Also save exclusion report
    report_file = output_dir / f"exclusion_report_{today}.parquet"
    df_results.to_parquet(report_file, index=False)
    print(f"✅ 除外レポート: {report_file}")
    print()


if __name__ == "__main__":
    main()
