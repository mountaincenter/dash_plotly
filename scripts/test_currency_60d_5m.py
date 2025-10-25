#!/usr/bin/env python3
"""
為替データの取得可能期間・インターバル検証
yfinanceで60日5分足が取得できるか確認
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

CURRENCY_TICKERS = ["JPY=X", "EURJPY=X"]

# 検証パターン
TEST_PATTERNS = [
    {"period": "5d", "interval": "5m", "name": "5日・5分足"},
    {"period": "10d", "interval": "5m", "name": "10日・5分足"},
    {"period": "30d", "interval": "5m", "name": "30日・5分足"},
    {"period": "60d", "interval": "5m", "name": "60日・5分足"},
    {"period": "1mo", "interval": "5m", "name": "1ヶ月・5分足"},
    {"period": "3mo", "interval": "5m", "name": "3ヶ月・5分足"},
    {"period": "60d", "interval": "1h", "name": "60日・1時間足"},
    {"period": "3mo", "interval": "1h", "name": "3ヶ月・1時間足"},
]


def test_currency_periods():
    """為替データの取得可能期間を検証"""

    print("=" * 100)
    print("為替データ取得可能期間・インターバル検証")
    print("=" * 100)
    print()

    results = []

    for ticker in CURRENCY_TICKERS:
        print(f"\n{'=' * 100}")
        print(f"ティッカー: {ticker}")
        print(f"{'=' * 100}\n")

        for pattern in TEST_PATTERNS:
            period = pattern["period"]
            interval = pattern["interval"]
            name = pattern["name"]

            print(f"{name:20s} (period={period:4s}, interval={interval:3s}) ... ", end="", flush=True)

            try:
                # データ取得
                df = yf.download(
                    ticker,
                    period=period,
                    interval=interval,
                    progress=False,
                    auto_adjust=False,
                )

                # MultiIndex列を単純化
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.droplevel(1)

                if df.empty:
                    print("❌ データなし")
                    results.append({
                        "ticker": ticker,
                        "pattern": name,
                        "period": period,
                        "interval": interval,
                        "status": "empty",
                        "records": 0,
                        "days": 0,
                        "size_mb": 0,
                    })
                    continue

                # データサマリー
                records = len(df)
                date_range = (df.index.max() - df.index.min()).days
                size_mb = df.memory_usage(deep=True).sum() / 1024 / 1024

                print(f"✅ {records:,}レコード, {date_range}日間, {size_mb:.2f}MB")

                # 詳細情報（5分足のみ）
                if interval == "5m":
                    # 1日あたりのレコード数
                    records_per_day = records / max(date_range, 1)
                    print(f"{'':22s} → 1日平均: {records_per_day:.1f}本/日", end="")

                    # 24時間取引の場合の理論値
                    theoretical_24h = 24 * 60 / 5  # 288本/日
                    coverage_pct = (records_per_day / theoretical_24h) * 100
                    print(f" (理論値288本の {coverage_pct:.1f}%)")

                results.append({
                    "ticker": ticker,
                    "pattern": name,
                    "period": period,
                    "interval": interval,
                    "status": "success",
                    "records": records,
                    "days": date_range,
                    "size_mb": size_mb,
                })

            except Exception as e:
                print(f"❌ エラー: {str(e)[:50]}")
                results.append({
                    "ticker": ticker,
                    "pattern": name,
                    "period": period,
                    "interval": interval,
                    "status": "error",
                    "error": str(e),
                    "records": 0,
                    "days": 0,
                    "size_mb": 0,
                })

    # サマリー
    print("\n\n" + "=" * 100)
    print("検証結果サマリー")
    print("=" * 100)
    print()

    results_df = pd.DataFrame(results)

    # 成功したパターン
    success_df = results_df[results_df["status"] == "success"]

    if not success_df.empty:
        print("✅ 取得成功パターン:")
        print()
        for _, row in success_df.iterrows():
            print(f"  {row['ticker']:10s} {row['pattern']:20s} → {row['records']:,}レコード, {row['size_mb']:.2f}MB")

    print()

    # 失敗したパターン
    failed_df = results_df[results_df["status"].isin(["empty", "error"])]

    if not failed_df.empty:
        print("❌ 取得失敗パターン:")
        print()
        for _, row in failed_df.iterrows():
            reason = row.get("error", "データなし")
            print(f"  {row['ticker']:10s} {row['pattern']:20s} → {reason}")

    print()

    # 推奨構成
    print("=" * 100)
    print("💡 推奨parquet構成")
    print("=" * 100)
    print()

    # 60日5分足の検証結果
    pattern_60d_5m = results_df[
        (results_df["period"] == "60d") &
        (results_df["interval"] == "5m") &
        (results_df["status"] == "success")
    ]

    if not pattern_60d_5m.empty:
        avg_size = pattern_60d_5m["size_mb"].mean()
        avg_records = pattern_60d_5m["records"].mean()

        print(f"60日5分足: ✅ 取得可能")
        print(f"  - 平均レコード数: {avg_records:,.0f}")
        print(f"  - 平均ファイルサイズ: {avg_size:.2f}MB")
        print()

        if avg_size > 10:
            print("  ⚠️  ファイルサイズが大きいため、以下の対策を推奨:")
            print("    1. 日本株取引時間のみに絞る (9:00-15:30 JST)")
            print("    2. 期間を短縮 (30日、または3ヶ月1時間足)")
            print("    3. 1時間足に変更")
        else:
            print("  ✅ ファイルサイズは許容範囲内")
            print("  → currency_prices_60d_5m.parquet として保存可能")

    else:
        print(f"60日5分足: ❌ 取得不可")
        print()
        print("代替案:")

        # 代替パターンを探す
        alt_5m = results_df[
            (results_df["interval"] == "5m") &
            (results_df["status"] == "success")
        ].sort_values("records", ascending=False)

        if not alt_5m.empty:
            best_5m = alt_5m.iloc[0]
            print(f"  1. {best_5m['pattern']} (最大取得可能)")

        alt_60d = results_df[
            (results_df["period"] == "60d") &
            (results_df["status"] == "success")
        ]

        if not alt_60d.empty:
            best_60d = alt_60d.iloc[0]
            print(f"  2. {best_60d['pattern']} (60日で取得可能)")

    print()


if __name__ == "__main__":
    test_currency_periods()
