#!/usr/bin/env python3
"""
日中分析データの事前計算

prices_60d_5m.parquet + prices_max_1d.parquet から
全銘柄の日中分析データを事前計算して保存する

終値は prices_max_1d.parquet の Close を 15:30 として使用
"""

from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import sys

# プロジェクトルート
BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR / "scripts"))

# ファイルパス
PRICES_5M_PATH = BASE_DIR / "data" / "parquet" / "prices_60d_5m.parquet"
PRICES_1D_PATH = BASE_DIR / "data" / "parquet" / "prices_max_1d.parquet"
ALL_STOCKS_PATH = BASE_DIR / "data" / "parquet" / "all_stocks.parquet"
OUTPUT_PATH = BASE_DIR / "data" / "parquet" / "intraday_analysis.parquet"

# 曜日名
WEEKDAY_NAMES = ["月", "火", "水", "木", "金", "土", "日"]


def load_data():
    """データ読み込み"""
    print(f"[LOAD] {PRICES_5M_PATH}")
    df_5m = pd.read_parquet(PRICES_5M_PATH)

    print(f"[LOAD] {PRICES_1D_PATH}")
    df_1d = pd.read_parquet(PRICES_1D_PATH)

    # date列の型を統一
    if "date" in df_5m.columns:
        df_5m["date"] = pd.to_datetime(df_5m["date"])
    if "date" in df_1d.columns:
        df_1d["date"] = pd.to_datetime(df_1d["date"])

    return df_5m, df_1d


def calc_intraday_for_ticker(ticker: str, df_5m: pd.DataFrame, df_1d: pd.DataFrame) -> list:
    """1銘柄の日中分析を計算"""
    # 銘柄でフィルタ
    t_5m = df_5m[df_5m["ticker"] == ticker].copy()
    t_1d = df_1d[df_1d["ticker"] == ticker].copy()

    if len(t_5m) == 0 or len(t_1d) == 0:
        return []

    # 日付リスト（5分足に存在する日付、新しい順）
    t_5m["date_only"] = t_5m["date"].dt.date
    dates = sorted(t_5m["date_only"].unique(), reverse=True)

    # 日足をdate_onlyでインデックス化・ソート
    t_1d["date_only"] = t_1d["date"].dt.date
    t_1d_sorted = t_1d.sort_values("date_only").copy()
    t_1d_indexed = t_1d.set_index("date_only")

    result = []

    # 各日付を処理（新しい順）
    for date in dates:
        # 日足データを取得
        if date not in t_1d_indexed.index:
            continue

        day_1d = t_1d_indexed.loc[date]
        if isinstance(day_1d, pd.DataFrame):
            day_1d = day_1d.iloc[0]

        # 前日終値をprices_max_1d.parquetから直接取得
        prev_days = t_1d_sorted[t_1d_sorted["date_only"] < date]
        prev_close = prev_days.iloc[-1]["Close"] if len(prev_days) > 0 else None

        # 5分足データ
        day_5m = t_5m[t_5m["date_only"] == date].copy()

        if len(day_5m) == 0:
            continue

        # 日足のCloseを15:30として追加（終値補正）
        close_1d = day_1d["Close"]

        # 高値・安値の時間を計算（NaN対応）
        high_series = day_5m["High"].dropna()
        low_series = day_5m["Low"].dropna()

        if len(high_series) == 0 or len(low_series) == 0:
            prev_close = close_1d
            continue

        high_idx = high_series.idxmax()
        low_idx = low_series.idxmin()
        high_time = day_5m.loc[high_idx, "date"].strftime("%H:%M")
        low_time = day_5m.loc[low_idx, "date"].strftime("%H:%M")

        # 前場終値（11:30以前の最後の有効なClose）
        am_data = day_5m[(day_5m["date"].dt.hour < 12) & (day_5m["Close"].notna())]
        am_close = am_data.sort_values("date").iloc[-1]["Close"] if len(am_data) > 0 else None

        # 曜日
        weekday = pd.Timestamp(date).weekday()
        weekday_name = WEEKDAY_NAMES[weekday] if weekday < 7 else ""

        # 価格データ
        open_price = day_1d["Open"]
        high_price = day_1d["High"]
        low_price = day_1d["Low"]

        # PnL計算（ロングベース、終値は日足のCloseを使用）
        am_pnl = int(am_close - open_price) if am_close is not None and not pd.isna(am_close) and not pd.isna(open_price) else None
        day_pnl = int(close_1d - open_price) if not pd.isna(close_1d) and not pd.isna(open_price) else None

        def safe_int(val):
            if val is None or pd.isna(val):
                return None
            return int(val)

        result.append({
            "ticker": ticker,
            "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
            "dayOfWeek": weekday_name,
            "prevClose": safe_int(prev_close),
            "open": safe_int(open_price),
            "high": safe_int(high_price),
            "highTime": high_time,
            "low": safe_int(low_price),
            "lowTime": low_time,
            "amClose": safe_int(am_close),
            "amPnl": am_pnl,
            "close": safe_int(close_1d),
            "dayPnl": day_pnl,
            "volatility": safe_int(high_price - low_price) if not pd.isna(high_price) and not pd.isna(low_price) else None,
        })

    # 既に新しい順
    return result


def main() -> int:
    print(f"[START] {datetime.now().isoformat()}")

    # データ読み込み
    df_5m, df_1d = load_data()

    # all_stocks.parquetから対象銘柄を取得
    print(f"[LOAD] {ALL_STOCKS_PATH}")
    df_all_stocks = pd.read_parquet(ALL_STOCKS_PATH)
    tickers = sorted(df_all_stocks["ticker"].unique())
    print(f"[INFO] {len(tickers)} tickers to process")

    # 全銘柄分を計算
    all_results = []
    for i, ticker in enumerate(tickers):
        if (i + 1) % 50 == 0:
            print(f"[PROGRESS] {i + 1}/{len(tickers)}")

        rows = calc_intraday_for_ticker(ticker, df_5m, df_1d)
        all_results.extend(rows)

    # DataFrameに変換
    df_result = pd.DataFrame(all_results)
    print(f"[INFO] Total rows: {len(df_result)}")

    # 保存
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_result.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)
    print(f"[SAVE] {OUTPUT_PATH}")

    print(f"[DONE] {datetime.now().isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
