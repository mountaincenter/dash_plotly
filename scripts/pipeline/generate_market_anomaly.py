#!/usr/bin/env python3
"""
generate_market_anomaly.py
カレンダーアノマリー分析（N225 / TOPIX ETF）

index_prices_max_1d.parquet から ^N225 と 1306.T を抽出し、
月別・週別・曜日別・月初月末効果・来週予報を生成する。
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

PARQUET_DIR = ROOT / "data" / "parquet"
OUTPUT_PATH = PARQUET_DIR / "market_anomaly.parquet"
SOURCE_PATH = PARQUET_DIR / "index_prices_max_1d.parquet"

# 分析対象と開始年
TARGETS: Dict[str, int] = {
    "^N225": 2000,
    "1306.T": 2008,
}

JST = timezone(timedelta(hours=9))
TODAY = datetime.now(JST).date()


def _next_monday(today: date) -> date:
    """今日より後の次の月曜を返す。週末/週末前の来週予報用。"""
    days_until_monday = 7 - today.weekday()
    if days_until_monday <= 0:
        days_until_monday += 7
    return today + timedelta(days=days_until_monday)


FORECAST_START = _next_monday(TODAY)
NEXT_WEEK = FORECAST_START.isocalendar()[1]
CURRENT_MONTH = FORECAST_START.month
CURRENT_MONTH_LABEL = f"{CURRENT_MONTH}月"


def load_data() -> Dict[str, pd.DataFrame]:
    """parquetを読み込み、ticker別にリターンを計算して返す"""
    df = pd.read_parquet(SOURCE_PATH)

    # カラム名を正規化
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    elif "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
        df["date"] = pd.to_datetime(df["date"])

    if "Ticker" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"Ticker": "ticker"})

    result: Dict[str, pd.DataFrame] = {}
    for ticker, start_year in TARGETS.items():
        mask = df["ticker"] == ticker
        sub = df.loc[mask, ["date", "Close"]].copy()
        sub = sub.sort_values("date").reset_index(drop=True)
        sub = sub[sub["date"].dt.year >= start_year].copy()
        sub["return_pct"] = sub["Close"].pct_change(fill_method=None) * 100
        sub = sub.dropna(subset=["return_pct"])
        sub["year"] = sub["date"].dt.year
        sub["month"] = sub["date"].dt.month
        sub["iso_week"] = sub["date"].dt.isocalendar().week.astype(int)
        sub["weekday"] = sub["date"].dt.weekday  # 0=Mon, 4=Fri
        result[ticker] = sub

    return result


# ──────────────────────────────────────────────
# A. 月別リターンヒートマップ (year x month)
# ──────────────────────────────────────────────
def calc_monthly_returns(df: pd.DataFrame) -> pd.DataFrame:
    """年×月の月間リターン。最終行に average, win_rate"""
    monthly = df.groupby(["year", "month"])["return_pct"].sum().unstack(fill_value=np.nan)
    monthly.columns = [int(c) for c in monthly.columns]
    # 全月揃える
    for m in range(1, 13):
        if m not in monthly.columns:
            monthly[m] = np.nan
    monthly = monthly[sorted(monthly.columns)]

    avg = monthly.mean()
    win = (monthly > 0).sum() / monthly.notna().sum() * 100
    stats = pd.DataFrame([avg, win], index=["average", "win_rate"])
    return pd.concat([monthly, stats])


# ──────────────────────────────────────────────
# B. 週別リターンヒートマップ (year x ISO week)
# ──────────────────────────────────────────────
def calc_weekly_returns(df: pd.DataFrame) -> pd.DataFrame:
    """年×ISO週の週間リターン"""
    weekly = df.groupby(["year", "iso_week"])["return_pct"].sum().unstack(fill_value=np.nan)
    weekly.columns = [int(c) for c in weekly.columns]
    for w in range(1, 54):
        if w not in weekly.columns:
            weekly[w] = np.nan
    weekly = weekly[sorted(weekly.columns)]

    avg = weekly.mean()
    win = (weekly > 0).sum() / weekly.notna().sum() * 100
    stats = pd.DataFrame([avg, win], index=["average", "win_rate"])
    return pd.concat([weekly, stats])


# ──────────────────────────────────────────────
# C. 曜日別パフォーマンス
# ──────────────────────────────────────────────
def calc_dow_performance(df: pd.DataFrame) -> pd.DataFrame:
    """曜日別リターン統計（全期間/直近5年/直近10年）"""
    labels = {0: "月曜", 1: "火曜", 2: "水曜", 3: "木曜", 4: "金曜"}
    current_year = TODAY.year

    rows = []
    for wd in range(5):
        sub_all = df[df["weekday"] == wd]["return_pct"]
        sub_5y = df[(df["weekday"] == wd) & (df["year"] >= current_year - 5)]["return_pct"]
        sub_10y = df[(df["weekday"] == wd) & (df["year"] >= current_year - 10)]["return_pct"]

        rows.append({
            "weekday": labels[wd],
            "avg_all": sub_all.mean(),
            "median_all": sub_all.median(),
            "win_rate_all": (sub_all > 0).sum() / len(sub_all) * 100 if len(sub_all) else np.nan,
            "count_all": len(sub_all),
            "avg_5y": sub_5y.mean(),
            "median_5y": sub_5y.median(),
            "win_rate_5y": (sub_5y > 0).sum() / len(sub_5y) * 100 if len(sub_5y) else np.nan,
            "count_5y": len(sub_5y),
            "avg_10y": sub_10y.mean(),
            "median_10y": sub_10y.median(),
            "win_rate_10y": (sub_10y > 0).sum() / len(sub_10y) * 100 if len(sub_10y) else np.nan,
            "count_10y": len(sub_10y),
        })

    return pd.DataFrame(rows)


# ──────────────────────────────────────────────
# D. 月初/月末効果
# ──────────────────────────────────────────────
def calc_month_position_effect(df: pd.DataFrame) -> pd.DataFrame:
    """月初5日・月末5日の平均日次リターン"""
    df_sorted = df.sort_values("date").copy()

    # 月内の営業日番号（前から）
    df_sorted["bday_of_month"] = df_sorted.groupby(["year", "month"]).cumcount() + 1
    # 月内の営業日番号（後ろから）
    df_sorted["bday_from_end"] = df_sorted.groupby(["year", "month"]).cumcount(ascending=False) + 1

    first5 = df_sorted[df_sorted["bday_of_month"] <= 5]["return_pct"]
    last5 = df_sorted[df_sorted["bday_from_end"] <= 5]["return_pct"]
    middle = df_sorted[(df_sorted["bday_of_month"] > 5) & (df_sorted["bday_from_end"] > 5)]["return_pct"]

    rows = []
    for label, subset in [("月初5日", first5), ("月末5日", last5), ("中間", middle)]:
        rows.append({
            "position": label,
            "avg_return": subset.mean(),
            "median_return": subset.median(),
            "win_rate": (subset > 0).sum() / len(subset) * 100 if len(subset) else np.nan,
            "count": len(subset),
        })

    return pd.DataFrame(rows)


# ──────────────────────────────────────────────
# E. 来週予報データ
# ──────────────────────────────────────────────
def calc_next_week_forecast(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """来週の過去実績、対象月サマリー、月曜特性"""
    # 対象週の年別リターン
    week_data = df[df["iso_week"] == NEXT_WEEK].groupby("year")["return_pct"].sum()
    week_summary = pd.DataFrame({
        "year": week_data.index,
        "week_return_pct": week_data.values,
    })

    # 対象月のアノマリー
    month_data = df[df["month"] == CURRENT_MONTH].groupby("year")["return_pct"].sum()
    month_summary = pd.DataFrame({
        "year": month_data.index,
        "month_return_pct": month_data.values,
    })

    # 月曜 × 対象月 × 対象週 の重複
    monday_month_week = df[
        (df["weekday"] == 0) & (df["month"] == CURRENT_MONTH) & (df["iso_week"] == NEXT_WEEK)
    ]["return_pct"]

    # 対象月最初の営業日（月初効果）
    month_first_days = df[df["month"] == CURRENT_MONTH].sort_values("date")
    month_first_day = month_first_days.groupby("year").first()["return_pct"]

    overlap_df = pd.DataFrame({
        "metric": [
            f"Week {NEXT_WEEK} 平均",
            f"Week {NEXT_WEEK} 中央値",
            f"Week {NEXT_WEEK} 勝率",
            f"Week {NEXT_WEEK} サンプル数",
            f"{CURRENT_MONTH_LABEL} 平均",
            f"{CURRENT_MONTH_LABEL} 中央値",
            f"{CURRENT_MONTH_LABEL} 勝率",
            f"月曜×{CURRENT_MONTH_LABEL}×Week{NEXT_WEEK} 平均",
            f"月曜×{CURRENT_MONTH_LABEL}×Week{NEXT_WEEK} サンプル数",
            f"{CURRENT_MONTH_LABEL}初日 平均",
            f"{CURRENT_MONTH_LABEL}初日 勝率",
        ],
        "value": [
            week_data.mean(),
            week_data.median(),
            (week_data > 0).sum() / len(week_data) * 100 if len(week_data) else np.nan,
            len(week_data),
            month_data.mean(),
            month_data.median(),
            (month_data > 0).sum() / len(month_data) * 100 if len(month_data) else np.nan,
            monday_month_week.mean() if len(monday_month_week) else np.nan,
            len(monday_month_week),
            month_first_day.mean(),
            (month_first_day > 0).sum() / len(month_first_day) * 100 if len(month_first_day) else np.nan,
        ],
    })

    return {
        "week_history": week_summary,
        "month_history": month_summary,
        "overlap": overlap_df,
    }


# ──────────────────────────────────────────────
# 保存
# ──────────────────────────────────────────────
def save_all_tables(all_tables: list[Tuple[str, pd.DataFrame]]) -> None:
    """全テーブルを table_name 列付きで1つの parquet に保存"""
    frames = []
    for name, tbl in all_tables:
        flat = tbl.reset_index()
        flat.insert(0, "table_name", name)
        # 全列を文字列に変換（heterogeneous types 対応）
        for col in flat.columns:
            flat[col] = flat[col].astype(str)
        frames.append(flat)

    combined = pd.concat(frames, ignore_index=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)
    print(f"\n[SAVE] {OUTPUT_PATH} ({len(combined)} rows)")


# ──────────────────────────────────────────────
# ターミナル表示
# ──────────────────────────────────────────────
def print_section(title: str, width: int = 72) -> None:
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}")


def print_weekly_forecast(ticker: str, df: pd.DataFrame, forecast: Dict) -> None:
    """来週の予報を表示"""
    week_hist = forecast["week_history"]
    month_hist = forecast["month_history"]
    overlap = forecast["overlap"]

    print_section(
        f"[{ticker}] 来週のアノマリー予報 "
        f"(Week {NEXT_WEEK}, {FORECAST_START.year}年{CURRENT_MONTH_LABEL})"
    )

    # 対象週 過去実績
    print(f"\n  ■ Week {NEXT_WEEK} 過去実績 (週間リターン%)")
    if not week_hist.empty:
        recent = week_hist.tail(10)
        for _, row in recent.iterrows():
            yr = int(row["year"])
            ret = row["week_return_pct"]
            sign = "+" if ret > 0 else ""
            bar = "+" * max(0, int(ret / 0.5)) if ret > 0 else "-" * max(0, int(-ret / 0.5))
            print(f"    {yr}: {sign}{ret:6.2f}% {bar}")

        avg = week_hist["week_return_pct"].mean()
        med = week_hist["week_return_pct"].median()
        wr = (week_hist["week_return_pct"] > 0).sum() / len(week_hist) * 100
        print(f"    ---")
        print(f"    平均: {avg:+.2f}%  中央値: {med:+.2f}%  勝率: {wr:.0f}% ({len(week_hist)}年)")

    # 対象月アノマリー
    print(f"\n  ■ {CURRENT_MONTH_LABEL}アノマリー (月間リターン%)")
    if not month_hist.empty:
        recent_month = month_hist.tail(10)
        for _, row in recent_month.iterrows():
            yr = int(row["year"])
            ret = row["month_return_pct"]
            sign = "+" if ret > 0 else ""
            print(f"    {yr}: {sign}{ret:6.2f}%")

        mavg = month_hist["month_return_pct"].mean()
        mmed = month_hist["month_return_pct"].median()
        mwr = (month_hist["month_return_pct"] > 0).sum() / len(month_hist) * 100
        print(f"    ---")
        print(f"    平均: {mavg:+.2f}%  中央値: {mmed:+.2f}%  勝率: {mwr:.0f}% ({len(month_hist)}年)")

    # 重複条件
    print(f"\n  ■ 複合条件")
    for _, row in overlap.iterrows():
        val = row["value"]
        try:
            val_f = float(val)
            if "勝率" in row["metric"]:
                print(f"    {row['metric']}: {val_f:.1f}%")
            elif "サンプル" in row["metric"]:
                print(f"    {row['metric']}: {int(val_f)}")
            else:
                print(f"    {row['metric']}: {val_f:+.3f}%")
        except (ValueError, TypeError):
            print(f"    {row['metric']}: {val}")


def print_dow_table(ticker: str, dow_df: pd.DataFrame) -> None:
    """曜日別パフォーマンス表示"""
    print_section(f"[{ticker}] 曜日別パフォーマンス")
    print(f"  {'曜日':<6} {'全期間平均':>10} {'直近5年':>10} {'直近10年':>10} {'勝率(全)':>10} {'勝率(5Y)':>10}")
    print(f"  {'-'*66}")
    for _, row in dow_df.iterrows():
        print(
            f"  {row['weekday']:<6}"
            f" {row['avg_all']:>+10.3f}"
            f" {row['avg_5y']:>+10.3f}"
            f" {row['avg_10y']:>+10.3f}"
            f" {row['win_rate_all']:>9.1f}%"
            f" {row['win_rate_5y']:>9.1f}%"
        )


def print_month_position(ticker: str, pos_df: pd.DataFrame) -> None:
    """月初/月末効果表示"""
    print_section(f"[{ticker}] 月初/月末効果")
    print(f"  {'区分':<10} {'平均':>10} {'中央値':>10} {'勝率':>10} {'件数':>8}")
    print(f"  {'-'*52}")
    for _, row in pos_df.iterrows():
        print(
            f"  {row['position']:<10}"
            f" {row['avg_return']:>+10.4f}"
            f" {row['median_return']:>+10.4f}"
            f" {row['win_rate']:>9.1f}%"
            f" {int(row['count']):>8}"
        )


def print_monthly_heatmap_summary(ticker: str, monthly_df: pd.DataFrame) -> None:
    """月別リターンの要約（average行のみ）"""
    if "average" in monthly_df.index:
        avg_row = monthly_df.loc["average"]
        win_row = monthly_df.loc["win_rate"]
        month_names = ["1月", "2月", "3月", "4月", "5月", "6月",
                       "7月", "8月", "9月", "10月", "11月", "12月"]
        print_section(f"[{ticker}] 月別アノマリー (平均月間リターン%)")
        print(f"  {'月':<6} {'平均':>8} {'勝率':>8}")
        print(f"  {'-'*24}")
        for m in range(1, 13):
            if m in avg_row.index:
                a = avg_row[m]
                w = win_row[m]
                marker = " <<" if m == CURRENT_MONTH else ""
                if not np.isnan(a):
                    print(f"  {month_names[m-1]:<6} {a:>+7.2f}% {w:>7.0f}%{marker}")


# ──────────────────────────────────────────────
# main
# ──────────────────────────────────────────────
def main() -> int:
    print("=" * 72)
    print("  カレンダーアノマリー分析")
    print(f"  分析日: {TODAY}  予報開始日: {FORECAST_START}  来週: Week {NEXT_WEEK}")
    print("=" * 72)

    if not SOURCE_PATH.exists():
        print(f"[ERROR] {SOURCE_PATH} が見つかりません")
        return 1

    data = load_data()

    all_tables: list[Tuple[str, pd.DataFrame]] = []

    for ticker, start_year in TARGETS.items():
        df = data[ticker]
        label = ticker.replace("^", "").replace(".", "_")
        print(f"\n[INFO] {ticker}: {len(df)} rows ({start_year}~)")

        # A. 月別リターン
        monthly = calc_monthly_returns(df)
        all_tables.append((f"{label}_monthly", monthly))
        print_monthly_heatmap_summary(ticker, monthly)

        # B. 週別リターン
        weekly = calc_weekly_returns(df)
        all_tables.append((f"{label}_weekly", weekly))

        # C. 曜日別
        dow = calc_dow_performance(df)
        all_tables.append((f"{label}_dow", dow))
        print_dow_table(ticker, dow)

        # D. 月初/月末効果
        pos = calc_month_position_effect(df)
        all_tables.append((f"{label}_position", pos))
        print_month_position(ticker, pos)

        # E. 来週予報
        forecast = calc_next_week_forecast(df)
        all_tables.append((f"{label}_week_history", forecast["week_history"]))
        all_tables.append((f"{label}_month_history", forecast["month_history"]))
        all_tables.append((f"{label}_overlap", forecast["overlap"]))
        print_weekly_forecast(ticker, df, forecast)

    # 保存
    save_all_tables(all_tables)

    # S3にアップロード
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            s3_key = "market_anomaly.parquet"
            if upload_file(cfg, OUTPUT_PATH, s3_key):
                print(f"[OK] Uploaded to S3: {s3_key}")
            else:
                print(f"[WARN] S3 upload returned False for {s3_key}")
        else:
            print("[INFO] S3 not configured, skipping upload")
    except Exception as e:
        print(f"[WARN] S3 upload failed (non-critical): {e}")

    # 最終サマリー
    print_section("来週のアノマリー予報まとめ")
    for ticker in TARGETS:
        df = data[ticker]
        label = ticker

        # 対象週
        week_returns = df[df["iso_week"] == NEXT_WEEK].groupby("year")["return_pct"].sum()
        week_avg = week_returns.mean() if len(week_returns) else 0
        week_wr = (week_returns > 0).sum() / len(week_returns) * 100 if len(week_returns) else 0

        # 対象月
        month_returns = df[df["month"] == CURRENT_MONTH].groupby("year")["return_pct"].sum()
        month_avg = month_returns.mean() if len(month_returns) else 0
        month_wr = (month_returns > 0).sum() / len(month_returns) * 100 if len(month_returns) else 0

        # 月曜
        mon = df[df["weekday"] == 0]["return_pct"]
        mon_avg = mon.mean() if len(mon) else 0

        # 月初効果
        df_s = df.sort_values("date").copy()
        df_s["bday_of_month"] = df_s.groupby(["year", "month"]).cumcount() + 1
        first5 = df_s[df_s["bday_of_month"] <= 5]["return_pct"]
        first5_avg = first5.mean() if len(first5) else 0

        print(f"\n  [{label}]")
        print(f"    Week {NEXT_WEEK} 過去平均: {week_avg:+.2f}%  勝率: {week_wr:.0f}%")
        print(f"    {CURRENT_MONTH_LABEL}アノマリー: {month_avg:+.2f}%  勝率: {month_wr:.0f}%")
        print(f"    月曜平均:        {mon_avg:+.3f}%")
        print(f"    月初5日平均:     {first5_avg:+.3f}%")

        # 判定
        signals = []
        if week_avg > 0 and week_wr >= 50:
            signals.append(f"Week{NEXT_WEEK}↑")
        elif week_avg < 0 and week_wr < 50:
            signals.append(f"Week{NEXT_WEEK}↓")
        if month_avg > 0 and month_wr >= 50:
            signals.append(f"{CURRENT_MONTH_LABEL}↑")
        elif month_avg < 0 and month_wr < 50:
            signals.append(f"{CURRENT_MONTH_LABEL}↓")
        if first5_avg > 0:
            signals.append("月初効果↑")

        if signals:
            direction = "やや上昇バイアス" if sum(1 for s in signals if "↑" in s) > sum(1 for s in signals if "↓" in s) else "やや下落バイアス"
            print(f"    → シグナル: {', '.join(signals)} → {direction}")
        else:
            print(f"    → 明確なバイアスなし")

    print(f"\n{'=' * 72}")
    print(f"  完了 - {OUTPUT_PATH}")
    print(f"{'=' * 72}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
