#!/usr/bin/env python3
"""
grok_trending_archive に11時間区分の利益カラムを追加するスクリプト

対象: improvement/grok_trending_archive_11seg.parquet（本番には触らない）

時間区分と対応するカラム名:
  seg_0930: -9:30 (9:30時点)
  seg_1000: 9:30-10:00 (10:00時点)
  seg_1030: 10:00-10:30 (10:30時点)
  seg_1100: 10:30-11:00 (11:00時点)
  seg_1130: 11:00-11:30 (前場引け、sell_priceを使用)
  seg_1300: 12:30-13:00 (13:00時点)
  seg_1330: 13:00-13:30 (13:30時点)
  seg_1400: 13:30-14:00 (14:00時点)
  seg_1430: 14:00-14:30 (14:30時点)
  seg_1500: 14:30-15:00 (15:00時点)
  seg_1530: 15:00-15:30 (大引け、daily_closeを使用)

利益計算:
  (buy_price - 各時間の価格) * 100 (ショート基準、正=利益)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, time

# パス設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "parquet" / "backtest"
IMPROVEMENT_DIR = DATA_DIR / "improvement"

# 入出力ファイル
INPUT_ARCHIVE = IMPROVEMENT_DIR / "grok_trending_archive_11seg.parquet"
OUTPUT_ARCHIVE = IMPROVEMENT_DIR / "grok_trending_archive_11seg.parquet"

# 5分足ファイル
M5_FILES = [
    DATA_DIR / "grok_5m_60d_20251230.parquet",
    DATA_DIR / "grok_5m_60d_20260130.parquet",
]

# 時間区分定義（セグメント名, 取得する時刻）
TIME_SEGMENTS = [
    ("seg_0930", time(9, 30)),
    ("seg_1000", time(10, 0)),
    ("seg_1030", time(10, 30)),
    ("seg_1100", time(11, 0)),
    ("seg_1130", None),  # sell_price使用
    ("seg_1300", time(13, 0)),
    ("seg_1330", time(13, 30)),
    ("seg_1400", time(14, 0)),
    ("seg_1430", time(14, 30)),
    ("seg_1500", time(15, 0)),
    ("seg_1530", None),  # daily_close使用
]


def load_5m_data():
    """5分足データを読み込み"""
    print("5分足データ読み込み中...")

    dfs = []
    for f in M5_FILES:
        if f.exists():
            df = pd.read_parquet(f)
            print(f"  {f.name}: {len(df)}件")
            dfs.append(df)

    if not dfs:
        raise FileNotFoundError("5分足データが見つかりません")

    m5 = pd.concat(dfs, ignore_index=True)

    # タイムゾーン処理
    m5["datetime"] = pd.to_datetime(m5["datetime"], utc=True)
    m5["datetime"] = m5["datetime"].dt.tz_convert("Asia/Tokyo").dt.tz_localize(None)

    # 重複除去
    m5 = m5.drop_duplicates(subset=["datetime", "ticker"])
    print(f"  合計（重複除去後）: {len(m5)}件")

    return m5


def get_price_at_time(m5_ticker_day, target_time):
    """
    指定時刻の価格を取得
    datetime=target_time のバーの open を使用（その時点の価格）

    5分足の datetime はバーの開始時刻なので:
    - datetime=9:30 の open = 9:30時点の価格
    - datetime=9:30 の close = 9:35時点の価格
    """
    if m5_ticker_day.empty:
        return None

    target_dt = m5_ticker_day["datetime"].iloc[0].replace(
        hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0
    )

    # target_time のバーの open を取得
    exact_match = m5_ticker_day[m5_ticker_day["datetime"] == target_dt]

    if exact_match.empty:
        return None

    return exact_match.iloc[0]["open"]


def add_time_segment_columns(archive, m5):
    """11時間区分の利益カラムを追加"""
    print("\n11時間区分の利益計算中...")

    # 日付ごと・銘柄ごとに5分足をグループ化
    m5["date"] = m5["datetime"].dt.date
    m5_grouped = m5.groupby(["ticker", "date"])

    # 新カラムを初期化
    for seg_name, _ in TIME_SEGMENTS:
        archive[seg_name] = np.nan

    total = len(archive)
    for idx, row in archive.iterrows():
        if idx % 100 == 0:
            print(f"  {idx}/{total}")

        ticker = row["ticker"]
        backtest_date = pd.to_datetime(row["backtest_date"]).date()
        buy_price = row["buy_price"]

        if pd.isna(buy_price):
            continue

        # 5分足データ取得
        try:
            m5_ticker_day = m5_grouped.get_group((ticker, backtest_date))
        except KeyError:
            m5_ticker_day = pd.DataFrame()

        # 各時間区分の利益を計算
        for seg_name, seg_time in TIME_SEGMENTS:
            if seg_name == "seg_1130":
                # 前場引け = sell_price
                price = row.get("sell_price")
            elif seg_name == "seg_1530":
                # 大引け = daily_close
                price = row.get("daily_close")
            else:
                # 5分足から取得
                price = get_price_at_time(m5_ticker_day, seg_time) if not m5_ticker_day.empty else None

            if price is not None and not pd.isna(price):
                # ショート利益 = (売り建値 - 決済価格) * 100
                profit = (buy_price - price) * 100
                archive.at[idx, seg_name] = profit

    return archive


def main():
    print("=" * 60)
    print("11時間区分カラム追加スクリプト")
    print("=" * 60)
    print(f"入力: {INPUT_ARCHIVE}")
    print(f"出力: {OUTPUT_ARCHIVE}")
    print()

    # Archive読み込み
    print("Archive読み込み中...")
    archive = pd.read_parquet(INPUT_ARCHIVE)
    print(f"  レコード数: {len(archive)}")
    print(f"  既存カラム: {list(archive.columns)}")

    # 5分足読み込み
    m5 = load_5m_data()

    # 11時間区分カラム追加
    archive = add_time_segment_columns(archive, m5)

    # 保存
    print(f"\n保存中: {OUTPUT_ARCHIVE}")
    archive.to_parquet(OUTPUT_ARCHIVE, index=False)

    # 確認
    new_cols = [col for col in archive.columns if col.startswith("seg_")]
    print(f"\n追加されたカラム: {new_cols}")

    # サマリー
    print("\n" + "=" * 60)
    print("サマリー（全体平均）")
    print("=" * 60)
    for seg_name, _ in TIME_SEGMENTS:
        valid = archive[seg_name].dropna()
        if len(valid) > 0:
            mean = valid.mean()
            win_rate = (valid > 0).mean() * 100
            print(f"  {seg_name}: 平均{mean:+,.0f}円 (n={len(valid)}, 勝率{win_rate:.1f}%)")
        else:
            print(f"  {seg_name}: データなし")

    print("\n完了")


if __name__ == "__main__":
    main()
