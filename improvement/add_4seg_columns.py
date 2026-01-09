"""
4区分カラム追加スクリプト（使い捨て）

grok_trending_archive.parquet 及び grok_trending_YYYYMMDD.parquet に
空のカラムを追加:
- profit_per_100_shares_morning_early  (前場前半 9:00-10:30)
- profit_per_100_shares_afternoon_early (後場前半 12:30-14:00)
"""

import pandas as pd
import numpy as np
from pathlib import Path

BACKTEST_DIR = Path(__file__).resolve().parent.parent / "data" / "parquet" / "backtest"
ARCHIVE_PATH = BACKTEST_DIR / "grok_trending_archive.parquet"


def main():
    print("=== 4区分カラム追加（空カラム） ===")

    # アーカイブ読み込み
    print("\nアーカイブ読み込み中...")
    archive = pd.read_parquet(ARCHIVE_PATH)
    print(f"  {len(archive)}件")
    print(f"  既存カラム数: {len(archive.columns)}")

    # 既存カラム確認
    if "profit_per_100_shares_morning_early" in archive.columns:
        print("\n[WARN] profit_per_100_shares_morning_early は既に存在します")
    else:
        archive["profit_per_100_shares_morning_early"] = np.nan
        print("\n[ADD] profit_per_100_shares_morning_early を追加")

    if "profit_per_100_shares_afternoon_early" in archive.columns:
        print("[WARN] profit_per_100_shares_afternoon_early は既に存在します")
    else:
        archive["profit_per_100_shares_afternoon_early"] = np.nan
        print("[ADD] profit_per_100_shares_afternoon_early を追加")

    print(f"\n  新カラム数: {len(archive.columns)}")

    # 保存確認
    print(f"\n保存先: {ARCHIVE_PATH}")
    confirm = input("保存しますか？ (y/n): ")

    if confirm.lower() == "y":
        archive.to_parquet(ARCHIVE_PATH, index=False)
        print("アーカイブ保存完了")

        # 日次ファイルも更新
        print("\n日次ファイルも更新中...")
        daily_files = list(BACKTEST_DIR.glob("grok_trending_2*.parquet"))
        for daily_file in daily_files:
            df_daily = pd.read_parquet(daily_file)
            if "profit_per_100_shares_morning_early" not in df_daily.columns:
                df_daily["profit_per_100_shares_morning_early"] = np.nan
            if "profit_per_100_shares_afternoon_early" not in df_daily.columns:
                df_daily["profit_per_100_shares_afternoon_early"] = np.nan
            df_daily.to_parquet(daily_file, index=False)
            print(f"  更新: {daily_file.name}")

        print("\n完了")
    else:
        print("保存をスキップしました")


if __name__ == "__main__":
    main()
