#!/usr/bin/env python3
"""
為替データ構造の検証スクリプト
yfinanceで為替データを取得し、株式との違いを確認
"""

import yfinance as yf
import pandas as pd
from pathlib import Path

# 検証する為替ペア
CURRENCY_TICKERS = ["JPY=X", "EURJPY=X"]

# データ取得パターン
TEST_CASES = [
    {"period": "5d", "interval": "1d", "name": "5日間・日足"},
    {"period": "5d", "interval": "5m", "name": "5日間・5分足"},
]


def test_currency_data():
    """為替データ構造を検証"""

    print("=" * 80)
    print("為替データ構造検証")
    print("=" * 80)
    print()

    for ticker in CURRENCY_TICKERS:
        print(f"\n{'=' * 80}")
        print(f"ティッカー: {ticker}")
        print(f"{'=' * 80}\n")

        for test_case in TEST_CASES:
            period = test_case["period"]
            interval = test_case["interval"]
            name = test_case["name"]

            print(f"\n{'-' * 80}")
            print(f"パターン: {name} (period={period}, interval={interval})")
            print(f"{'-' * 80}\n")

            try:
                # データ取得
                df = yf.download(
                    ticker,
                    period=period,
                    interval=interval,
                    progress=False,
                    auto_adjust=False,  # 調整なし
                )

                # MultiIndex列を単純化
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.droplevel(1)

                if df.empty:
                    print(f"⚠️  データが取得できませんでした")
                    continue

                # 基本情報
                print(f"📊 データサマリー:")
                print(f"  - レコード数: {len(df)}")
                print(f"  - 期間: {df.index.min()} 〜 {df.index.max()}")
                print(f"  - カラム: {list(df.columns)}")
                print()

                # データ型
                print(f"📋 カラムのデータ型:")
                for col in df.columns:
                    print(f"  - {col}: {df[col].dtype}")
                print()

                # 欠損値チェック
                print(f"🔍 欠損値チェック:")
                missing = df.isnull().sum()
                for col in df.columns:
                    if missing[col] > 0:
                        print(f"  - {col}: {missing[col]} / {len(df)} ({missing[col]/len(df)*100:.1f}%)")
                if missing.sum() == 0:
                    print(f"  ✅ 欠損値なし")
                print()

                # サンプルデータ（最初の3行）
                print(f"📄 サンプルデータ（最初の3行）:")
                print(df.head(3).to_string())
                print()

                # 統計情報
                print(f"📈 統計情報:")
                print(df.describe().to_string())
                print()

                # 株式データとの比較
                print(f"🔄 株式データとの比較:")
                stock_columns = ["Open", "High", "Low", "Close", "Volume", "Adj Close"]
                currency_columns = list(df.columns)

                print(f"  株式の標準カラム: {stock_columns}")
                print(f"  為替のカラム: {currency_columns}")
                print()

                # カラムの有無チェック
                for col in stock_columns:
                    if col in currency_columns:
                        print(f"  ✅ {col}: 存在")
                    else:
                        print(f"  ❌ {col}: なし")
                print()

                # Volumeの分析（為替と株式で意味が異なる可能性）
                if "Volume" in df.columns:
                    vol_stats = df["Volume"].describe()
                    print(f"📊 Volumeの分析:")
                    print(f"  - 平均: {vol_stats['mean']:,.0f}")
                    print(f"  - 最小: {vol_stats['min']:,.0f}")
                    print(f"  - 最大: {vol_stats['max']:,.0f}")
                    print(f"  - ゼロの数: {(df['Volume'] == 0).sum()}")

                    if (df['Volume'] == 0).sum() == len(df):
                        print(f"  ⚠️  全てのVolumeがゼロ（為替では意味がない可能性）")
                    print()

                # Adj Closeの分析
                if "Adj Close" in df.columns and "Close" in df.columns:
                    adj_diff = (df["Adj Close"] - df["Close"]).abs().sum()
                    print(f"📊 Adj Closeの分析:")
                    print(f"  - CloseとAdj Closeの差分合計: {adj_diff:.6f}")
                    if adj_diff < 0.001:
                        print(f"  ✅ CloseとAdj Closeはほぼ同じ（為替では配当調整不要）")
                    print()

            except Exception as e:
                print(f"❌ エラー: {e}")
                import traceback
                traceback.print_exc()
                print()

    print("\n" + "=" * 80)
    print("検証完了")
    print("=" * 80)


def compare_stock_vs_currency():
    """株式と為替のデータ構造を比較"""

    print("\n\n" + "=" * 80)
    print("株式 vs 為替 比較検証")
    print("=" * 80)
    print()

    # 株式データ取得
    print("📈 株式データ取得: 7203.T (トヨタ自動車)")
    stock_df = yf.download("7203.T", period="5d", interval="1d", progress=False, auto_adjust=False)
    if isinstance(stock_df.columns, pd.MultiIndex):
        stock_df.columns = stock_df.columns.droplevel(1)

    print(f"  カラム: {list(stock_df.columns)}")
    print(f"  Volume: {stock_df['Volume'].mean():,.0f} (平均)")
    print()

    # 為替データ取得
    print("💱 為替データ取得: JPY=X (ドル円)")
    currency_df = yf.download("JPY=X", period="5d", interval="1d", progress=False, auto_adjust=False)
    if isinstance(currency_df.columns, pd.MultiIndex):
        currency_df.columns = currency_df.columns.droplevel(1)

    print(f"  カラム: {list(currency_df.columns)}")
    if "Volume" in currency_df.columns:
        print(f"  Volume: {currency_df['Volume'].mean():,.0f} (平均)")
    print()

    # 推奨カラム
    print("=" * 80)
    print("💡 推奨カラム構成")
    print("=" * 80)
    print()

    print("株式データ:")
    print("  - Open, High, Low, Close: OHLC価格")
    print("  - Volume: 出来高（重要）")
    print("  - Adj Close: 配当調整後価格（テクニカル分析用）")
    print()

    print("為替データ:")
    print("  - Open, High, Low, Close: OHLC価格")
    print("  - Volume: 為替では意味がない可能性（ゼロまたは無視）")
    print("  - Adj Close: 配当がないため、Closeと同じ")
    print()

    print("推奨:")
    print("  為替parquetには以下のカラムを保存:")
    print("  - date (index)")
    print("  - ticker")
    print("  - Open")
    print("  - High")
    print("  - Low")
    print("  - Close")
    print("  ❌ Volume: 除外（為替では意味なし）")
    print("  ❌ Adj Close: 除外（Closeと同じ）")
    print()


if __name__ == "__main__":
    test_currency_data()
    compare_stock_vs_currency()
