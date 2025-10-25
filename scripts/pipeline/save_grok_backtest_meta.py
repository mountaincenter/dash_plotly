#!/usr/bin/env python3
"""
save_grok_backtest_meta.py
Grok選定のバックテストメタ情報を保存

バックテスト結果から以下の情報を抽出してparquetに保存:
- 5日間の勝率・平均リターン
- Top5戦略 vs Top10戦略の比較
- 前場戦略 vs デイリー戦略の比較
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR

def generate_grok_backtest_meta() -> pd.DataFrame:
    """
    バックテスト結果からメタ情報を生成

    Returns:
        pd.DataFrame: バックテストメタ情報
    """
    # 最新のバックテスト結果を読み込み
    backtest_dir = ROOT / "data/parquet/backtest_results"

    if not backtest_dir.exists():
        print("[WARN] No backtest results found")
        return pd.DataFrame()

    latest_result = sorted(backtest_dir.glob("*/summary.csv"))
    if not latest_result:
        print("[WARN] No summary.csv found in backtest results")
        return pd.DataFrame()

    latest_result_dir = latest_result[-1].parent

    print(f"[INFO] Loading backtest results from: {latest_result_dir}")

    # summary.csvを読み込み
    df = pd.read_csv(latest_result_dir / "summary.csv")

    # 基本統計
    total_stocks = len(df)
    unique_stocks = df['ticker'].nunique()
    date_range = f"{df['target_date'].min()} to {df['target_date'].max()}"

    # デイリー戦略の統計
    daily_win_rate = (df['daily_change_pct'] > 0).sum() / len(df) * 100
    daily_avg_return = df['daily_change_pct'].mean()

    # 前場戦略の統計
    morning_win_rate = (df['morning_change_pct'] > 0).sum() / len(df) * 100
    morning_avg_return = df['morning_change_pct'].mean()

    # プレミアム言及効果
    has_mention_df = df[df['has_mention'] == True]
    no_mention_df = df[df['has_mention'] == False]

    mention_win_rate = (has_mention_df['morning_change_pct'] > 0).sum() / len(has_mention_df) * 100 if len(has_mention_df) > 0 else 0
    no_mention_win_rate = (no_mention_df['morning_change_pct'] > 0).sum() / len(no_mention_df) * 100 if len(no_mention_df) > 0 else 0

    # Top5戦略のシミュレーション結果（最新のCSVから読み込み）
    top5_csv = latest_result_dir / "top5_selection_details.csv"

    if top5_csv.exists():
        df_top5 = pd.read_csv(top5_csv)
        top5_morning_win_rate = (df_top5['morning_change_pct'] > 0).sum() / len(df_top5) * 100
        top5_morning_avg_return = df_top5['morning_change_pct'].mean()
    else:
        top5_morning_win_rate = 0
        top5_morning_avg_return = 0

    # メタ情報DataFrame
    meta_data = {
        "metric": [
            "total_stocks",
            "unique_stocks",
            "date_range",
            "daily_win_rate",
            "daily_avg_return",
            "morning_win_rate",
            "morning_avg_return",
            "top5_morning_win_rate",
            "top5_morning_avg_return",
            "mention_win_rate",
            "no_mention_win_rate",
            "backtest_date",
        ],
        "value": [
            str(total_stocks),
            str(unique_stocks),
            date_range,
            f"{daily_win_rate:.1f}%",
            f"{daily_avg_return:.2f}%",
            f"{morning_win_rate:.1f}%",
            f"{morning_avg_return:.2f}%",
            f"{top5_morning_win_rate:.1f}%",
            f"{top5_morning_avg_return:.2f}%",
            f"{mention_win_rate:.1f}%",
            f"{no_mention_win_rate:.1f}%",
            latest_result_dir.name,
        ]
    }

    df_meta = pd.DataFrame(meta_data)

    print(f"[OK] Generated backtest meta: {len(df_meta)} metrics")
    return df_meta


def main():
    """メイン処理"""
    print("=" * 60)
    print("Save Grok Backtest Meta")
    print("=" * 60)

    df_meta = generate_grok_backtest_meta()

    if df_meta.empty:
        print("[WARN] No backtest meta generated")
        return 0

    # 保存
    output_file = PARQUET_DIR / "grok_backtest_meta.parquet"
    df_meta.to_parquet(output_file, index=False)

    print(f"\n[OK] Saved: {output_file}")
    print(f"\nBacktest Summary:")
    print("=" * 60)
    for _, row in df_meta.iterrows():
        print(f"{row['metric']:30} : {row['value']}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
