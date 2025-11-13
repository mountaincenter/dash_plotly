#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
損切り戦略のバックテスト検証（-3% vs -4%）

Phase3: 寄り付き買い → -3%で損切り or 大引けで利確
Phase4: 寄り付き買い → -4%で損切り or 大引けで利確

index（日経平均・TOPIX）の動きも考慮して分析
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, time

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))


def load_data():
    """Load backtest archive and price data"""
    print("[1/2] Loading v1.3 backtest archive...")
    archive_path = project_root / "data" / "parquet" / "backtest" / "v1.3_grok_trending_archive_with_backtest.parquet"
    backtest = pd.read_parquet(archive_path)
    print(f"      Loaded {len(backtest)} records")

    print("[2/2] Loading v1.2 archive (for more samples)...")
    v1_2_path = project_root / "data" / "parquet" / "backtest" / "grok_trending_archive_with_phase4.parquet"
    if v1_2_path.exists():
        backtest_v1_2 = pd.read_parquet(v1_2_path)
        print(f"      Loaded {len(backtest_v1_2)} records")
    else:
        backtest_v1_2 = None
        print(f"      v1.2 archive not found")

    return backtest, backtest_v1_2


def fetch_5m_data_yfinance(ticker: str, backtest_date: str) -> pd.DataFrame:
    """Fetch 5m intraday data from yfinance"""
    import yfinance as yf

    try:
        end_date = datetime.strptime(backtest_date, "%Y-%m-%d") + pd.Timedelta(days=1)
        start_date = end_date - pd.Timedelta(days=5)

        data = yf.download(
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="5m",
            progress=False,
            auto_adjust=True
        )

        if data.empty:
            return None

        # Handle multi-index columns
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        # Convert UTC to JST
        data.index = pd.to_datetime(data.index)
        if data.index.tz is not None:
            data.index = data.index.tz_convert('Asia/Tokyo')

        # Filter to target date
        target_data = data[data.index.date == datetime.strptime(backtest_date, "%Y-%m-%d").date()]

        if target_data.empty:
            return None

        return target_data

    except Exception as e:
        return None


def calculate_stop_loss_exit(ticker: str, backtest_date: str, entry_price: float,
                              stop_loss_pct: float = -3.0) -> dict:
    """
    Calculate exit price with stop loss strategy using yfinance

    Returns:
        dict with exit_price, exit_time, exit_reason
    """
    # Fetch 5m data from yfinance
    day_data = fetch_5m_data_yfinance(ticker, backtest_date)

    if day_data is None or day_data.empty:
        return None

    # Filter to trading hours (9:00-15:00)
    day_data = day_data[
        (day_data.index.time >= time(9, 0)) &
        (day_data.index.time <= time(15, 0))
    ].copy()

    if day_data.empty:
        return None

    # Calculate return for each 5m candle
    day_data["return_pct"] = ((day_data["Close"] - entry_price) / entry_price) * 100

    # Check if stop loss is triggered
    stop_loss_triggered = day_data[day_data["return_pct"] <= stop_loss_pct]

    if not stop_loss_triggered.empty:
        # Stop loss triggered - exit at first occurrence
        exit_row = stop_loss_triggered.iloc[0]
        return {
            "exit_price": exit_row["Close"],
            "exit_time": exit_row.name,
            "exit_reason": "stop_loss",
            "exit_return_pct": exit_row["return_pct"]
        }
    else:
        # No stop loss - hold until market close (15:00)
        close_data = day_data[day_data.index.time >= time(15, 0)]
        if close_data.empty:
            # Use last available data
            exit_row = day_data.iloc[-1]
        else:
            exit_row = close_data.iloc[0]

        return {
            "exit_price": exit_row["Close"],
            "exit_time": exit_row.name,
            "exit_reason": "market_close",
            "exit_return_pct": exit_row["return_pct"]
        }


def get_index_performance(backtest_date: str) -> dict:
    """Get Nikkei 225 performance for the day (simplified)"""
    return {}


def main():
    print("=" * 80)
    print("3%損切り戦略のバックテスト検証")
    print("=" * 80)
    print()

    # Load data
    backtest, backtest_v1_2 = load_data()
    print()

    # Combine v1.3 and v1.2 data
    all_records = []

    # v1.3 records
    v1_3_records = backtest[backtest["buy_price"].notna()].copy()
    v1_3_records["version"] = "v1.3"
    all_records.append(v1_3_records)

    # v1.2 records (if available)
    if backtest_v1_2 is not None:
        v1_2_records = backtest_v1_2[backtest_v1_2["buy_price"].notna()].copy()
        v1_2_records["version"] = "v1.2"
        all_records.append(v1_2_records)

    valid_records = pd.concat(all_records, ignore_index=True)
    print(f"有効なバックテスト記録: {len(valid_records)} (v1.3: {len(v1_3_records)}, v1.2: {len(v1_2_records) if backtest_v1_2 is not None else 0})")
    print()

    print("=" * 80)
    print("Phase3（-3%損切り）& Phase4（-4%損切り）戦略の検証開始")
    print("=" * 80)
    print()

    results = []

    for idx, row in valid_records.iterrows():
        ticker = row["ticker"]
        stock_name = row.get("stock_name", "")
        backtest_date = row["backtest_date"]
        buy_price = row["buy_price"]

        version = row.get("version", "unknown")
        print(f"[{idx+1}/{len(valid_records)}] {ticker} - {stock_name} ({backtest_date}, {version})")

        # Calculate Phase3 with -3% stop loss
        phase3_result = calculate_stop_loss_exit(
            ticker, backtest_date, buy_price, stop_loss_pct=-3.0
        )

        # Calculate Phase4 with -4% stop loss
        phase4_result = calculate_stop_loss_exit(
            ticker, backtest_date, buy_price, stop_loss_pct=-4.0
        )

        # Get index performance
        index_perf = get_index_performance(backtest_date)

        result = {
            "ticker": ticker,
            "stock_name": stock_name,
            "backtest_date": backtest_date,
            "buy_price": buy_price,
            "phase1_return": row.get("phase1_return"),
            "phase2_return": row.get("phase2_return"),
            **index_perf
        }

        # Phase3 (-3%)
        if phase3_result:
            exit_price = phase3_result["exit_price"]
            phase3_return = ((exit_price - buy_price) / buy_price)
            phase3_win = phase3_return > 0
            profit_per_100_shares_phase3 = (exit_price - buy_price) * 100

            result.update({
                "phase3_return": phase3_return,
                "phase3_exit_reason": phase3_result["exit_reason"],
                "phase3_exit_time": phase3_result["exit_time"],
                "phase3_win": phase3_win,
                "profit_per_100_shares_phase3": profit_per_100_shares_phase3
            })

            reason_emoji = "🛑" if phase3_result["exit_reason"] == "stop_loss" else "📈"
            win_emoji = "✅" if phase3_win else "❌"

            print(f"  {win_emoji} Phase3 (-3%): {phase3_return*100:+.2f}% (¥{profit_per_100_shares_phase3:+,.0f}/100株)")
            print(f"     {reason_emoji} {phase3_result['exit_reason']} @ {phase3_result['exit_time'].strftime('%H:%M')}")
        else:
            result.update({
                "phase3_return": None,
                "phase3_exit_reason": None,
                "phase3_exit_time": None,
                "phase3_win": None,
                "profit_per_100_shares_phase3": None
            })
            print(f"  ⚠️  Phase3: 5分足データなし")

        # Phase4 (-4%)
        if phase4_result:
            exit_price = phase4_result["exit_price"]
            phase4_return = ((exit_price - buy_price) / buy_price)
            phase4_win = phase4_return > 0
            profit_per_100_shares_phase4 = (exit_price - buy_price) * 100

            result.update({
                "phase4_return": phase4_return,
                "phase4_exit_reason": phase4_result["exit_reason"],
                "phase4_exit_time": phase4_result["exit_time"],
                "phase4_win": phase4_win,
                "profit_per_100_shares_phase4": profit_per_100_shares_phase4
            })

            reason_emoji = "🛑" if phase4_result["exit_reason"] == "stop_loss" else "📈"
            win_emoji = "✅" if phase4_win else "❌"

            print(f"  {win_emoji} Phase4 (-4%): {phase4_return*100:+.2f}% (¥{profit_per_100_shares_phase4:+,.0f}/100株)")
            print(f"     {reason_emoji} {phase4_result['exit_reason']} @ {phase4_result['exit_time'].strftime('%H:%M')}")
        else:
            result.update({
                "phase4_return": None,
                "phase4_exit_reason": None,
                "phase4_exit_time": None,
                "phase4_win": None,
                "profit_per_100_shares_phase4": None
            })
            print(f"  ⚠️  Phase4: 5分足データなし")

        results.append(result)
        print()

        # Rate limit for yfinance
        import time as time_module
        time_module.sleep(0.3)

    # Create DataFrame
    df_results = pd.DataFrame(results)

    # Compare strategies
    print("=" * 80)
    print("📊 戦略比較サマリー")
    print("=" * 80)
    print()

    phase1_available = df_results["phase1_return"].notna().sum()
    phase2_available = df_results["phase2_return"].notna().sum()
    phase3_available = df_results["phase3_return"].notna().sum()
    phase4_available = df_results["phase4_return"].notna().sum()

    print(f"総記録数: {len(df_results)}")
    print(f"Phase1データ: {phase1_available}")
    print(f"Phase2データ: {phase2_available}")
    print(f"Phase3データ: {phase3_available}")
    print(f"Phase4データ: {phase4_available}")
    print()

    # Phase1 stats
    if phase1_available > 0:
        phase1_avg_return = df_results["phase1_return"].mean() * 100
        phase1_win_rate = (df_results["phase1_return"] > 0).sum() / phase1_available * 100
        print(f"【Phase1: 寄り付き → 11:30前場引け】")
        print(f"  勝率: {phase1_win_rate:.1f}%")
        print(f"  平均リターン: {phase1_avg_return:+.2f}%")
        print()

    # Phase2 stats
    if phase2_available > 0:
        phase2_avg_return = df_results["phase2_return"].mean() * 100
        phase2_win_rate = (df_results["phase2_return"] > 0).sum() / phase2_available * 100
        phase2_avg_profit = df_results[df_results["phase2_return"].notna()].apply(
            lambda x: (x["buy_price"] * (1 + x["phase2_return"]) - x["buy_price"]) * 100, axis=1
        ).mean()
        print(f"【Phase2: 寄り付き → 大引け（損切りなし）】")
        print(f"  勝率: {phase2_win_rate:.1f}%")
        print(f"  平均リターン: {phase2_avg_return:+.2f}%")
        print(f"  平均利益: ¥{phase2_avg_profit:+,.0f}/100株")
        print()

    # Phase3 stats (-3%)
    if phase3_available > 0:
        phase3_data = df_results[df_results["phase3_return"].notna()]
        phase3_avg_return = phase3_data["phase3_return"].mean() * 100
        phase3_win_rate = (phase3_data["phase3_return"] > 0).sum() / phase3_available * 100
        phase3_avg_profit = phase3_data["profit_per_100_shares_phase3"].mean()

        stop_loss_count_3 = (phase3_data["phase3_exit_reason"] == "stop_loss").sum()
        stop_loss_rate_3 = stop_loss_count_3 / phase3_available * 100

        print(f"【Phase3: 寄り付き → -3%損切り or 大引け】")
        print(f"  勝率: {phase3_win_rate:.1f}%")
        print(f"  平均リターン: {phase3_avg_return:+.2f}%")
        print(f"  平均利益: ¥{phase3_avg_profit:+,.0f}/100株")
        print(f"  損切り発動率: {stop_loss_rate_3:.1f}% ({stop_loss_count_3}/{phase3_available})")
        print()

    # Phase4 stats (-4%)
    if phase4_available > 0:
        phase4_data = df_results[df_results["phase4_return"].notna()]
        phase4_avg_return = phase4_data["phase4_return"].mean() * 100
        phase4_win_rate = (phase4_data["phase4_return"] > 0).sum() / phase4_available * 100
        phase4_avg_profit = phase4_data["profit_per_100_shares_phase4"].mean()

        stop_loss_count_4 = (phase4_data["phase4_exit_reason"] == "stop_loss").sum()
        stop_loss_rate_4 = stop_loss_count_4 / phase4_available * 100

        print(f"【Phase4: 寄り付き → -4%損切り or 大引け】")
        print(f"  勝率: {phase4_win_rate:.1f}%")
        print(f"  平均リターン: {phase4_avg_return:+.2f}%")
        print(f"  平均利益: ¥{phase4_avg_profit:+,.0f}/100株")
        print(f"  損切り発動率: {stop_loss_rate_4:.1f}% ({stop_loss_count_4}/{phase4_available})")
        print()

    # Comparison
    if phase2_available > 0 and phase3_available > 0 and phase4_available > 0:
        print("=" * 80)
        print("🔍 全戦略比較")
        print("=" * 80)
        print()

        # Compare same stocks only
        comparison = df_results[
            df_results["phase2_return"].notna() &
            df_results["phase3_return"].notna() &
            df_results["phase4_return"].notna()
        ].copy()

        if len(comparison) > 0:
            print(f"比較可能な銘柄数: {len(comparison)}")
            print()

            # Calculate profits
            comparison["phase2_profit"] = comparison.apply(
                lambda x: (x["buy_price"] * (1 + x["phase2_return"]) - x["buy_price"]) * 100, axis=1
            )

            # Phase2 vs Phase3
            phase2_better_3 = (comparison["phase2_profit"] > comparison["profit_per_100_shares_phase3"]).sum()
            phase3_better = (comparison["profit_per_100_shares_phase3"] > comparison["phase2_profit"]).sum()

            print(f"【Phase2 (損切りなし) vs Phase3 (-3%損切り)】")
            print(f"  Phase2が有利: {phase2_better_3} ({phase2_better_3/len(comparison)*100:.1f}%)")
            print(f"  Phase3が有利: {phase3_better} ({phase3_better/len(comparison)*100:.1f}%)")

            comparison["profit_diff_3"] = comparison["profit_per_100_shares_phase3"] - comparison["phase2_profit"]
            avg_diff_3 = comparison["profit_diff_3"].mean()
            print(f"  平均利益差: ¥{avg_diff_3:+,.0f}/100株")
            print()

            # Phase2 vs Phase4
            phase2_better_4 = (comparison["phase2_profit"] > comparison["profit_per_100_shares_phase4"]).sum()
            phase4_better = (comparison["profit_per_100_shares_phase4"] > comparison["phase2_profit"]).sum()

            print(f"【Phase2 (損切りなし) vs Phase4 (-4%損切り)】")
            print(f"  Phase2が有利: {phase2_better_4} ({phase2_better_4/len(comparison)*100:.1f}%)")
            print(f"  Phase4が有利: {phase4_better} ({phase4_better/len(comparison)*100:.1f}%)")

            comparison["profit_diff_4"] = comparison["profit_per_100_shares_phase4"] - comparison["phase2_profit"]
            avg_diff_4 = comparison["profit_diff_4"].mean()
            print(f"  平均利益差: ¥{avg_diff_4:+,.0f}/100株")
            print()

            # Phase3 vs Phase4
            phase3_better_4 = (comparison["profit_per_100_shares_phase3"] > comparison["profit_per_100_shares_phase4"]).sum()
            phase4_better_3 = (comparison["profit_per_100_shares_phase4"] > comparison["profit_per_100_shares_phase3"]).sum()

            print(f"【Phase3 (-3%損切り) vs Phase4 (-4%損切り)】")
            print(f"  Phase3が有利: {phase3_better_4} ({phase3_better_4/len(comparison)*100:.1f}%)")
            print(f"  Phase4が有利: {phase4_better_3} ({phase4_better_3/len(comparison)*100:.1f}%)")

            comparison["profit_diff_3_4"] = comparison["profit_per_100_shares_phase4"] - comparison["profit_per_100_shares_phase3"]
            avg_diff_3_4 = comparison["profit_diff_3_4"].mean()
            print(f"  平均利益差: ¥{avg_diff_3_4:+,.0f}/100株")
            print()

            # Best strategy
            print("=" * 80)
            print("🏆 最適戦略")
            print("=" * 80)
            print()

            strategies = {
                "Phase2 (損切りなし)": comparison["phase2_profit"].mean(),
                "Phase3 (-3%損切り)": comparison["profit_per_100_shares_phase3"].mean(),
                "Phase4 (-4%損切り)": comparison["profit_per_100_shares_phase4"].mean()
            }

            best_strategy = max(strategies, key=strategies.get)
            best_profit = strategies[best_strategy]

            for name, profit in sorted(strategies.items(), key=lambda x: x[1], reverse=True):
                emoji = "🥇" if name == best_strategy else "  "
                print(f"{emoji} {name}: ¥{profit:+,.0f}/100株")

            print()
            print(f"✅ 推奨: {best_strategy}")

    # Save results
    output_dir = project_root / "test_output"
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "backtest_stop_loss_comparison.parquet"
    df_results.to_parquet(output_file, index=False)

    print()
    print(f"✅ 保存完了: {output_file}")
    print()


if __name__ == "__main__":
    main()
