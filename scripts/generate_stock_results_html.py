#!/usr/bin/env python3
"""
株式取引結果の集計・ターミナル出力・Parquet生成・S3アップロード
取得日/建日が2025/11/04以降のデータを抽出
"""

import pandas as pd
import json
import sys
from pathlib import Path
from datetime import datetime

# common_cfg をインポート
sys.path.insert(0, str(Path(__file__).parent.parent))
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file, download_file

# パス設定
BASE_DIR = Path(__file__).parent.parent
CSV_PATH = BASE_DIR / "data" / "csv" / "stock_results.csv"
NIKKEIVI_CSV_PATH = BASE_DIR / "data" / "csv" / "nikkeivi.csv"
PARQUET_DIR = BASE_DIR / "data" / "parquet"
PARQUET_PATH = PARQUET_DIR / "stock_results.parquet"
NIKKEIVI_PARQUET_PATH = PARQUET_DIR / "nikkei_vi_max_1d.parquet"
MANIFEST_PATH = PARQUET_DIR / "manifest.json"

# CSV読み込み
df = pd.read_csv(CSV_PATH, encoding="utf-8")

# 取得日/建日が"-"のものは、約定日で補完
df.loc[df["取得日/建日"] == "-", "取得日/建日"] = df.loc[df["取得日/建日"] == "-", "約定日"]

# 取得日/建日が"-"のものを除外してから日付型に変換
df = df[df["取得日/建日"] != "-"].copy()
df["取得日/建日"] = pd.to_datetime(df["取得日/建日"], format="%Y/%m/%d")

# 2025/11/04以降のデータを抽出
cutoff_date = datetime(2025, 11, 4)
df_filtered = df[df["取得日/建日"] >= cutoff_date].copy()

# 約定日も日付型に変換（ソート用）
df_filtered["約定日"] = pd.to_datetime(df_filtered["約定日"], format="%Y/%m/%d")

# 週・月カラム追加
df_filtered["週"] = df_filtered["約定日"].dt.strftime("%Y/W%W")
df_filtered["月"] = df_filtered["約定日"].dt.strftime("%Y/%m")

# 約定日でソート（新しい順）
df_filtered = df_filtered.sort_values("約定日", ascending=False)

# 実現損益の数値変換（カンマ除去）
df_filtered["実現損益_num"] = df_filtered["実現損益(円)"].str.replace(",", "").astype(float)

# ロング/ショート判定（売埋/売付=ロング、買埋=ショート）
df_filtered["position_type"] = df_filtered["取引"].apply(
    lambda x: "ショート" if x == "買埋" else "ロング"
)

# 数値変換（集計前に必要）
df_filtered["単価_num"] = df_filtered["単価(円)"].str.replace(",", "").astype(float)
df_filtered["取得価額_num"] = df_filtered["平均取得価額(円)"].str.replace(",", "").astype(float)

# 銘柄別日別売買別集計（先に作成）
daily_stock = df_filtered.groupby(["約定日", "コード", "銘柄名", "position_type"]).agg({
    "取得価額_num": "mean",
    "単価_num": "mean",
    "実現損益_num": "sum",
    "数量(株/口)": lambda x: pd.to_numeric(x.astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int).sum()
}).reset_index()

daily_stock.columns = ["約定日", "コード", "銘柄名", "売買", "平均取得価額", "平均単価", "実現損益", "数量"]

# 週別集計
weekly_stock = df_filtered.groupby(["週", "コード", "銘柄名", "position_type"]).agg({
    "取得価額_num": "mean",
    "単価_num": "mean",
    "実現損益_num": "sum",
    "数量(株/口)": lambda x: pd.to_numeric(x.astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int).sum()
}).reset_index()
weekly_stock.columns = ["週", "コード", "銘柄名", "売買", "平均取得価額", "平均単価", "実現損益", "数量"]

# 月別集計
monthly_stock = df_filtered.groupby(["月", "コード", "銘柄名", "position_type"]).agg({
    "取得価額_num": "mean",
    "単価_num": "mean",
    "実現損益_num": "sum",
    "数量(株/口)": lambda x: pd.to_numeric(x.astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int).sum()
}).reset_index()
monthly_stock.columns = ["月", "コード", "銘柄名", "売買", "平均取得価額", "平均単価", "実現損益", "数量"]

# 全体集計（銘柄別日別売買別ベース）
total_profit = daily_stock["実現損益"].sum()
win_count = (daily_stock["実現損益"] > 0).sum()
lose_count = (daily_stock["実現損益"] < 0).sum()
total_count = len(daily_stock)
win_rate = win_count / total_count * 100 if total_count > 0 else 0

# ロング/ショート別集計（銘柄別日別売買別ベース）
long_df = daily_stock[daily_stock["売買"] == "ロング"]
short_df = daily_stock[daily_stock["売買"] == "ショート"]

long_profit = long_df["実現損益"].sum() if len(long_df) > 0 else 0
long_count = len(long_df)
long_win = (long_df["実現損益"] > 0).sum() if len(long_df) > 0 else 0
long_lose = (long_df["実現損益"] < 0).sum() if len(long_df) > 0 else 0
long_win_rate = long_win / long_count * 100 if long_count > 0 else 0

short_profit = short_df["実現損益"].sum() if len(short_df) > 0 else 0
short_count = len(short_df)
short_win = (short_df["実現損益"] > 0).sum() if len(short_df) > 0 else 0
short_lose = (short_df["実現損益"] < 0).sum() if len(short_df) > 0 else 0
short_win_rate = short_win / short_count * 100 if short_count > 0 else 0

# ソート（新しい順）
daily_stock = daily_stock.sort_values("約定日", ascending=False)

# =============================================================================
# ターミナル出力
# =============================================================================

def fmt_pl(v):
    return f"\033[32m{v:+,.0f}円\033[0m" if v >= 0 else f"\033[31m{v:+,.0f}円\033[0m"

print(f"抽出件数: {total_count}件")
print(f"合計損益: {fmt_pl(total_profit)}")
print(f"ロング: {long_count}件 / {fmt_pl(long_profit)}")
print(f"ショート: {short_count}件 / {fmt_pl(short_profit)}")

# --- 日経VI ---
vi_df = None
if NIKKEIVI_CSV_PATH.exists():
    vi_df = pd.read_csv(NIKKEIVI_CSV_PATH)
    vi_df.columns = ["date", "open", "high", "low", "close"]
    vi_df["date"] = pd.to_datetime(vi_df["date"])
    vi_df = vi_df.sort_values("date").reset_index(drop=True)
    latest_vi = vi_df.iloc[-1]
    print(f"\n日経VI: {latest_vi['close']:.2f}（{latest_vi['date'].strftime('%Y/%m/%d')}）高値{latest_vi['high']:.2f} 安値{latest_vi['low']:.2f}")

# 最新の日別/週別/月別
daily_totals = daily_stock.groupby("約定日")["実現損益"].sum().sort_index(ascending=False)
latest_date = daily_totals.index[0]
d = latest_date
print(f"{d.month}月{d.day}日: {fmt_pl(daily_totals.iloc[0])}")

weekly_totals = weekly_stock.groupby("週")["実現損益"].sum()
latest_week = sorted(weekly_totals.index, reverse=True)[0]
week_num = latest_week.split("W")[1]
print(f"{d.month}月W{int(week_num)}週: {fmt_pl(weekly_totals[latest_week])}")

monthly_totals = monthly_stock.groupby("月")["実現損益"].sum()
latest_month = sorted(monthly_totals.index, reverse=True)[0]
print(f"{d.month}月: {fmt_pl(monthly_totals[latest_month])}")

# =============================================================================
# Parquetファイル生成・S3アップロード
# =============================================================================

# サマリー統計を含むDataFrameを作成
summary_data = {
    "metric": [
        "total_profit", "total_count", "win_count", "lose_count", "win_rate",
        "long_profit", "long_count", "long_win", "long_lose", "long_win_rate",
        "short_profit", "short_count", "short_win", "short_lose", "short_win_rate"
    ],
    "value": [
        total_profit, total_count, win_count, lose_count, win_rate,
        long_profit, long_count, long_win, long_lose, long_win_rate,
        short_profit, short_count, short_win, short_lose, short_win_rate
    ]
}
summary_df = pd.DataFrame(summary_data)

# --- 戦略タグ付け ---
BACKTEST_DIR = PARQUET_DIR / "backtest"

# Grok アーカイブ + 当日分 grok_trending から (date, ticker) ペアを取得
grok_set = set()
grok_arc_path = BACKTEST_DIR / "grok_trending_archive.parquet"
if grok_arc_path.exists():
    try:
        grok_arc = pd.read_parquet(grok_arc_path)
        grok_arc["backtest_date"] = pd.to_datetime(grok_arc["backtest_date"])
        grok_set = set(zip(
            grok_arc["backtest_date"].dt.date,
            grok_arc["ticker"].str.replace(".T", "", regex=False)
        ))
    except Exception as e:
        print(f"戦略タグ: Grokアーカイブ読み込み失敗: {e}")

# grok_trending（当日・未アーカイブ分）も追加
grok_trending_path = PARQUET_DIR / "grok_trending.parquet"
if grok_trending_path.exists():
    try:
        gt = pd.read_parquet(grok_trending_path)
        gt["date"] = pd.to_datetime(gt["date"])
        gt_set = set(zip(
            gt["date"].dt.date,
            gt["ticker"].str.replace(".T", "", regex=False)
        ))
        grok_set |= gt_set
    except Exception as e:
        print(f"戦略タグ: grok_trending読み込み失敗: {e}")

print(f"戦略タグ: Grok合計 {len(grok_set)} 件読み込み")


def tag_strategy(row):
    trade_date = row["約定日"]
    # 12/22より前は全てLLM
    if trade_date < pd.Timestamp("2025-12-22"):
        return "llm"
    # 2/24以降でgrok_setに不一致 → granville
    key = (trade_date.date(), str(row["コード"]))
    if trade_date >= pd.Timestamp("2026-02-24") and key not in grok_set:
        return "granville"
    # 12/22以降はgrok（照合一致 or 12/22-2/23の全件）
    return "grok"

daily_stock["戦略"] = daily_stock.apply(tag_strategy, axis=1)
strategy_counts = daily_stock["戦略"].value_counts()
print(f"戦略タグ付け完了: {dict(strategy_counts)}")

# 戦略別集計をサマリーに追加
for strategy in ["grok", "granville", "llm"]:
    s_df = daily_stock[daily_stock["戦略"] == strategy]
    s_profit = s_df["実現損益"].sum() if len(s_df) > 0 else 0
    s_count = len(s_df)
    s_win = (s_df["実現損益"] > 0).sum() if len(s_df) > 0 else 0
    s_win_rate = s_win / s_count * 100 if s_count > 0 else 0
    for metric, val in [
        (f"{strategy}_profit", s_profit),
        (f"{strategy}_count", s_count),
        (f"{strategy}_win", s_win),
        (f"{strategy}_win_rate", s_win_rate),
    ]:
        summary_df = pd.concat([summary_df, pd.DataFrame({"metric": [metric], "value": [val]})], ignore_index=True)

# daily_stock を保存用に整形
parquet_df = daily_stock.copy()

# Parquetファイル出力
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
parquet_df.to_parquet(PARQUET_PATH, index=False)
print(f"\nParquetファイルを出力しました: {PARQUET_PATH}")
print(f"  行数: {len(parquet_df)}行")

# サマリーも別ファイルで保存
summary_path = PARQUET_DIR / "stock_results_summary.parquet"
summary_df.to_parquet(summary_path, index=False)

# 日経VI CSV → Parquet
if vi_df is not None:
    vi_df.to_parquet(NIKKEIVI_PARQUET_PATH, index=False)

# S3からmanifest.jsonをダウンロードして最新を取得
s3_cfg = load_s3_config()

if s3_cfg.bucket:
    # S3から一時ファイルにダウンロード
    temp_manifest = PARQUET_DIR / "manifest.json.s3tmp"
    downloaded = download_file(s3_cfg, "manifest.json", temp_manifest)

    if downloaded and temp_manifest.exists():
        with open(temp_manifest, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        temp_manifest.unlink()
    else:
        if MANIFEST_PATH.exists():
            with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        else:
            manifest = {"generated_at": None, "files": {}}
else:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = {"generated_at": None, "files": {}}

now = datetime.now().isoformat()

manifest["files"]["stock_results.parquet"] = {
    "exists": True,
    "size_bytes": PARQUET_PATH.stat().st_size,
    "row_count": len(parquet_df),
    "columns": list(parquet_df.columns),
    "updated_at": now
}

manifest["files"]["stock_results_summary.parquet"] = {
    "exists": True,
    "size_bytes": summary_path.stat().st_size,
    "row_count": len(summary_df),
    "columns": list(summary_df.columns),
    "updated_at": now
}

if vi_df is not None and NIKKEIVI_PARQUET_PATH.exists():
    manifest["files"]["nikkei_vi_max_1d.parquet"] = {
        "exists": True,
        "size_bytes": NIKKEIVI_PARQUET_PATH.stat().st_size,
        "row_count": len(vi_df),
        "columns": list(vi_df.columns),
        "updated_at": now
    }

manifest["generated_at"] = now

with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, ensure_ascii=False)

# S3アップロード
if s3_cfg.bucket:
    upload_file(s3_cfg, PARQUET_PATH, "stock_results.parquet")
    upload_file(s3_cfg, summary_path, "stock_results_summary.parquet")
    if NIKKEIVI_PARQUET_PATH.exists():
        upload_file(s3_cfg, NIKKEIVI_PARQUET_PATH, "nikkei_vi_max_1d.parquet")
    upload_file(s3_cfg, MANIFEST_PATH, "manifest.json")

    import urllib.request
    import urllib.error

    API_URL = "https://muuq3bv2n2.ap-northeast-1.awsapprunner.com/api/dev/stock-results/refresh"
    try:
        req = urllib.request.Request(API_URL, method="POST")
        with urllib.request.urlopen(req, timeout=30) as response:
            result = response.read().decode("utf-8")
            print(f"[OK] キャッシュリフレッシュ完了: {result}")
    except (urllib.error.URLError, Exception) as e:
        print(f"[WARNING] キャッシュリフレッシュ失敗: {e}")

# =============================================================================
# MarketSpeed CSV → Parquet 変換（hold_stocks / orders / credit_status）
# =============================================================================

CSV_DIR = BASE_DIR / "data" / "csv"


def convert_hold_stocks() -> pd.DataFrame | None:
    """hold_stocks.csv → hold_stocks.parquet（実保有ポジション）"""
    path = CSV_DIR / "hold_stocks.csv"
    if not path.exists():
        print("[SKIP] hold_stocks.csv not found")
        return None

    df = pd.read_csv(path, encoding="utf-8")
    if df.empty:
        print("[SKIP] hold_stocks.csv is empty")
        return None

    num_cols = {
        "建玉数量合計(株/口)": "quantity",
        "建玉金額合計(円)": "cost_total",
        "時価(円)": "current_price",
        "時価評価額(円)": "market_value",
        "評価損益額合計(円)": "unrealized_pnl",
        "評価損益率(%)": "unrealized_pct",
    }
    out = pd.DataFrame()
    out["ticker"] = df["コード"].astype(str)
    out["stock_name"] = df["銘柄名"]
    out["direction"] = df["売買"]
    out["margin_type"] = df["信用区分"]
    out["deadline"] = df["弁済期限"]

    for src, dst in num_cols.items():
        if src in df.columns:
            out[dst] = df[src].astype(str).str.replace(",", "").astype(float)

    if "直近期日" in df.columns:
        out["expiry_date"] = df["直近期日"]

    out["as_of"] = datetime.now().strftime("%Y-%m-%d")

    out_path = PARQUET_DIR / "hold_stocks.parquet"
    out.to_parquet(out_path, index=False)
    print(f"[OK] hold_stocks.parquet: {len(out)}件")
    return out


def convert_orders() -> pd.DataFrame | None:
    """order.csv → orders.parquet（注文履歴）"""
    path = CSV_DIR / "order.csv"
    if not path.exists():
        print("[SKIP] order.csv not found")
        return None

    df = pd.read_csv(path, encoding="utf-8")
    if df.empty:
        print("[SKIP] order.csv is empty")
        return None

    out = pd.DataFrame()
    out["order_no"] = df["受付No"].astype(str)
    out["status"] = df["通常注文状況"]
    out["ticker"] = df["コード"].astype(str)
    out["stock_name"] = df["銘柄名"]
    out["trade_type"] = df["取引"]
    out["direction"] = df["売買"]
    out["exec_condition"] = df["執行条件"]
    out["order_qty"] = df["注文数量(株/口)"].astype(str).str.replace(",", "").astype(int)
    out["filled_qty"] = df["約定数量(株/口)"].astype(str).str.replace(",", "").astype(int)
    out["order_price"] = df["注文単価(円)"]
    out["margin_type"] = df["信用区分"]
    out["order_datetime"] = df["発注/受注日時"]
    out["is_filled"] = out["status"] == "約定"

    out_path = PARQUET_DIR / "orders.parquet"
    out.to_parquet(out_path, index=False)
    print(f"[OK] orders.parquet: {len(out)}件 (約定: {out['is_filled'].sum()}件)")
    return out


def convert_credit_capacity() -> dict | None:
    """credit_capacity.csv → credit_status.parquet（資産状況）"""
    path = CSV_DIR / "credit_capacity.csv"
    if not path.exists():
        print("[SKIP] credit_capacity.csv not found")
        return None

    df = pd.read_csv(path, encoding="utf-8")
    if df.empty:
        print("[SKIP] credit_capacity.csv is empty")
        return None

    # date,available_margin 形式
    if "available_margin" in df.columns:
        margin = float(df.iloc[-1]["available_margin"])
        out = pd.DataFrame([{
            "asset": "available_margin",
            "value": margin,
            "as_of": str(df.iloc[-1].get("date", datetime.now().strftime("%Y-%m-%d"))),
        }])
        out_path = PARQUET_DIR / "credit_status.parquet"
        out.to_parquet(out_path, index=False)
        print(f"[OK] credit_status.parquet: 余力 ¥{margin:,.0f}")
        return {"available_margin": margin, "source": "simple_csv"}

    # MarketSpeed資産内訳形式
    out_rows = []
    for _, row in df.iterrows():
        name = str(row.iloc[0])
        val_str = str(row.iloc[1]).replace(",", "")
        try:
            val = float(val_str)
        except ValueError:
            val = 0.0
        out_rows.append({"asset": name, "value": val})

    out = pd.DataFrame(out_rows)
    out["as_of"] = datetime.now().strftime("%Y-%m-%d")

    credit_row = out[out["asset"].str.contains("信用", na=False)]
    margin = float(credit_row["value"].iloc[0]) if not credit_row.empty else 0.0

    out_path = PARQUET_DIR / "credit_status.parquet"
    out.to_parquet(out_path, index=False)
    print(f"[OK] credit_status.parquet: {len(out)}行, 信用保証金 ¥{margin:,.0f}")
    return {"available_margin": margin, "source": "marketspeed"}


print("\n" + "=" * 60)
print("MarketSpeed CSV → Parquet 変換")
print("=" * 60)
hold = convert_hold_stocks()
orders = convert_orders()
credit = convert_credit_capacity()

# S3アップロード（MarketSpeed parquets）
if s3_cfg.bucket:
    for fname in ["hold_stocks.parquet", "orders.parquet", "credit_status.parquet"]:
        fpath = PARQUET_DIR / fname
        if fpath.exists():
            upload_file(s3_cfg, fpath, fname)

    # manifest 更新
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for fname, label in [
        ("hold_stocks.parquet", "実保有ポジション"),
        ("orders.parquet", "注文履歴"),
        ("credit_status.parquet", "資産状況"),
    ]:
        fpath = PARQUET_DIR / fname
        if fpath.exists():
            fdf = pd.read_parquet(fpath)
            manifest["files"][fname] = {
                "exists": True,
                "size_bytes": fpath.stat().st_size,
                "row_count": len(fdf),
                "columns": list(fdf.columns),
                "updated_at": now,
            }

    manifest["generated_at"] = now
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    upload_file(s3_cfg, MANIFEST_PATH, "manifest.json")
    print("[OK] S3 manifest updated")
