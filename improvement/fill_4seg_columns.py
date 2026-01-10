"""
4区分カラムにデータ投入スクリプト（使い捨て）

profit_per_100_shares_morning_early, profit_per_100_shares_afternoon_early を計算

ルール:
- morning_early (9:00-10:30): 10:25の終値。なければ次時間帯の最早Open
- afternoon_early (12:30-14:50): 14:45の終値。なければ次時間帯の最早Open
- ストップ高安（5分足データなし）: NaN
- 利益 = (exit_price - entry_price) * 100 （ロング基準）
"""

import pandas as pd
import numpy as np
from pathlib import Path

BACKTEST_DIR = Path(__file__).resolve().parent.parent / "data" / "parquet" / "backtest"
ARCHIVE_PATH = BACKTEST_DIR / "grok_trending_archive.parquet"

# 5分足ファイル
PRICES_5M_FILES = [
    BACKTEST_DIR / "grok_5m_60d_20251230.parquet",
    BACKTEST_DIR / "grok_5m_60d_20260110.parquet",
]


def load_5m_prices():
    """5分足データ読み込み（複数ファイルをマージ）"""
    dfs = []
    for f in PRICES_5M_FILES:
        if f.exists():
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert("Asia/Tokyo")
            df["date"] = df["datetime"].dt.date
            df["time"] = df["datetime"].dt.strftime("%H:%M")
            df = df.rename(columns={
                "open": "Open", "high": "High", "low": "Low",
                "close": "Close", "volume": "Volume"
            })
            dfs.append(df)
            print(f"  {f.name}: {len(df)}行")

    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    # 重複削除（同じticker, datetime）
    merged = merged.drop_duplicates(subset=["ticker", "datetime"], keep="last")
    return merged


def get_exit_price(day_data, start_time, end_time, next_start_time=None):
    """
    時間帯の終値を取得
    データがなければ次時間帯の最早Openを返す
    次時間帯にもデータなければNone（ストップ高安継続）
    """
    # 指定時間帯のデータ
    slot_data = day_data[
        (day_data["time"] >= start_time) &
        (day_data["time"] <= end_time)
    ].sort_values("time")

    if len(slot_data) > 0:
        return float(slot_data.iloc[-1]["Close"])

    # 次時間帯の最早Open（次時間帯にデータがある場合のみ）
    if next_start_time:
        next_data = day_data[day_data["time"] >= next_start_time].sort_values("time")
        if len(next_data) > 0:
            return float(next_data.iloc[0]["Open"])

    # 次時間帯にもデータなし → NaN（ストップ高安継続）
    return None


def calc_adjustment_factor(day_data, daily_close):
    """分割調整係数を計算"""
    if pd.isna(daily_close) or daily_close <= 0:
        return 1.0

    last_data = day_data[day_data["time"] >= "14:55"]
    if len(last_data) == 0:
        last_data = day_data[day_data["time"] >= "14:30"]

    if len(last_data) > 0:
        last_close = last_data.iloc[-1]["Close"]
        if last_close > 0:
            ratio = daily_close / last_close
            if ratio > 1.5 or ratio < 0.7:
                return ratio
    return 1.0


def calculate_profits(archive, prices_5m):
    """4区分の利益を計算"""
    morning_early_profits = []
    afternoon_early_profits = []

    for idx, row in archive.iterrows():
        ticker = row["ticker"]
        sel_date = pd.to_datetime(row["selection_date"]).date()
        entry_price = row["buy_price"]
        daily_close = row.get("daily_close")

        # 5分足データ取得
        day_data = prices_5m[
            (prices_5m["ticker"] == ticker) &
            (prices_5m["date"] == sel_date)
        ].copy()

        # データなし or entry_priceなし → NaN（ストップ高安）
        if len(day_data) == 0 or pd.isna(entry_price):
            morning_early_profits.append(np.nan)
            afternoon_early_profits.append(np.nan)
            continue

        # 分割調整係数
        adjustment = calc_adjustment_factor(day_data, daily_close)

        # 前場前半 (9:00-10:30)
        # 10:25の終値、なければ10:30以降の最早Open
        morning_early_exit = get_exit_price(day_data, "09:00", "10:25", "10:30")
        if morning_early_exit is not None:
            morning_early_exit_adj = morning_early_exit * adjustment
            profit_morning = (morning_early_exit_adj - entry_price) * 100
            morning_early_profits.append(profit_morning)
        else:
            morning_early_profits.append(np.nan)

        # 後場前半 (12:30-14:50)
        # 14:45の終値、なければ14:50以降の最早Open
        afternoon_early_exit = get_exit_price(day_data, "12:30", "14:45", "14:50")
        if afternoon_early_exit is not None:
            afternoon_early_exit_adj = afternoon_early_exit * adjustment
            profit_afternoon = (afternoon_early_exit_adj - entry_price) * 100
            afternoon_early_profits.append(profit_afternoon)
        else:
            afternoon_early_profits.append(np.nan)

    return morning_early_profits, afternoon_early_profits


def main():
    print("=== 4区分カラムデータ投入 ===")

    # 5分足データ読み込み
    print("\n5分足データ読み込み中...")
    prices_5m = load_5m_prices()
    print(f"  合計: {len(prices_5m)}行, {prices_5m['ticker'].nunique()}銘柄")

    # アーカイブ読み込み
    print("\nアーカイブ読み込み中...")
    archive = pd.read_parquet(ARCHIVE_PATH)
    print(f"  {len(archive)}件")

    # 計算
    print("\n利益計算中...")
    morning_early, afternoon_early = calculate_profits(archive, prices_5m)

    # 更新
    archive["profit_per_100_shares_morning_early"] = morning_early
    archive["profit_per_100_shares_afternoon_early"] = afternoon_early

    # 統計
    valid_morning = archive["profit_per_100_shares_morning_early"].dropna()
    valid_afternoon = archive["profit_per_100_shares_afternoon_early"].dropna()

    print(f"\n=== 結果 ===")
    print(f"前場前半 (morning_early):")
    print(f"  有効データ: {len(valid_morning)}/{len(archive)}件 ({len(valid_morning)/len(archive)*100:.1f}%)")
    if len(valid_morning) > 0:
        print(f"  合計利益: {valid_morning.sum():+,.0f}円 (ロング基準)")
        print(f"  勝率: {(valid_morning > 0).sum() / len(valid_morning) * 100:.1f}%")

    print(f"\n後場前半 (afternoon_early):")
    print(f"  有効データ: {len(valid_afternoon)}/{len(archive)}件 ({len(valid_afternoon)/len(archive)*100:.1f}%)")
    if len(valid_afternoon) > 0:
        print(f"  合計利益: {valid_afternoon.sum():+,.0f}円 (ロング基準)")
        print(f"  勝率: {(valid_afternoon > 0).sum() / len(valid_afternoon) * 100:.1f}%")

    # 既存phase1/phase2との比較
    print(f"\n=== 既存カラムとの比較 (ロング基準) ===")
    p1 = archive["profit_per_100_shares_phase1"].dropna()
    p2 = archive["profit_per_100_shares_phase2"].dropna()
    print(f"phase1 (前場引け): {p1.sum():+,.0f}円, 勝率{(p1>0).sum()/len(p1)*100:.1f}%")
    print(f"phase2 (大引け): {p2.sum():+,.0f}円, 勝率{(p2>0).sum()/len(p2)*100:.1f}%")

    # 保存確認
    print(f"\n保存先: {ARCHIVE_PATH}")
    confirm = input("保存しますか？ (y/n): ")

    if confirm.lower() == "y":
        archive.to_parquet(ARCHIVE_PATH, index=False)
        print("アーカイブ保存完了")

        # 日次ファイルも更新
        print("\n日次ファイルも更新中...")
        daily_files = list(BACKTEST_DIR.glob("grok_trending_2*.parquet"))
        updated = 0
        for daily_file in daily_files:
            df_daily = pd.read_parquet(daily_file)
            date_str = daily_file.stem.replace("grok_trending_", "")
            date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            matched = archive[archive["backtest_date"] == date_formatted]
            if len(matched) > 0:
                # 既存カラムを削除してマージ
                for col in ["profit_per_100_shares_morning_early", "profit_per_100_shares_afternoon_early"]:
                    if col in df_daily.columns:
                        df_daily = df_daily.drop(columns=[col])

                merge_cols = matched[["ticker", "profit_per_100_shares_morning_early", "profit_per_100_shares_afternoon_early"]]
                df_daily = df_daily.merge(merge_cols, on="ticker", how="left")
                df_daily.to_parquet(daily_file, index=False)
                updated += 1

        print(f"  {updated}ファイル更新")
        print("\n完了")
    else:
        print("保存をスキップしました")


if __name__ == "__main__":
    main()
