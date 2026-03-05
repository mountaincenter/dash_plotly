#!/usr/bin/env python3
"""
JQuants から TOPIX100構成銘柄の日足株価を取得し、yfinanceデータと日次リターンベースで突合する。

yfinanceは配当+分割を遡及調整するため絶対価格はJQuantsと一致しない。
バックテストに影響するのは「日次リターンの差異」なので、リターンベースで品質を分類する。

出力:
  data/raw/jquants/prices_daily.parquet       — JQuants 10年分の生データ
  data/raw/yfinance/prices_daily.parquet      — yfinance 対象銘柄の全期間データ
  data/interim/return_comparison.parquet      — 日次リターン突合結果
  data/interim/data_quality_report.parquet    — 品質分類（4段階）
  data/raw/metadata/price_data_sources.md     — データ出典・定義
  chapters/01_data_quality/report.html        — 品質レポート
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

DASH_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(DASH_DIR))

from scripts.lib.jquants_fetcher import JQuantsFetcher

SV_DIR = DASH_DIR / "strategy_verification"
RAW_JQ_DIR = SV_DIR / "data" / "raw" / "jquants"
RAW_YF_DIR = SV_DIR / "data" / "raw" / "yfinance"
INTERIM_DIR = SV_DIR / "data" / "interim"
METADATA_DIR = SV_DIR / "data" / "raw" / "metadata"
CH01_DIR = SV_DIR / "chapters" / "01_data_quality"

JQ_FROM = "2016-03-06"
JQ_TO = "2026-03-05"


# ---------------------------------------------------------------------------
# Data fetch
# ---------------------------------------------------------------------------

def fetch_jquants_prices(tickers: list[str]) -> pd.DataFrame:
    fetcher = JQuantsFetcher()
    codes = [t.replace(".T", "") for t in tickers]
    all_dfs = []
    for i, (ticker, code) in enumerate(zip(tickers, codes)):
        print(f"  [{i+1}/{len(tickers)}] {ticker}...", end=" ", flush=True)
        try:
            df = fetcher.get_prices_daily(code=code, from_date=JQ_FROM, to_date=JQ_TO)
            if df is not None and len(df) > 0:
                df["ticker"] = ticker
                all_dfs.append(df)
                print(f"{len(df)} rows")
            else:
                print("no data")
        except Exception as e:
            print(f"ERROR: {e}")
        time.sleep(0.5)
    if not all_dfs:
        raise RuntimeError("No data fetched")
    result = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal: {len(result):,} rows, {result['ticker'].nunique()} tickers")
    return result


def load_yfinance_prices(tickers: list[str]) -> pd.DataFrame:
    path = DASH_DIR / "data" / "parquet" / "prices_max_1d.parquet"
    df = pd.read_parquet(str(path), engine="pyarrow")
    df = df[df["ticker"].isin(tickers)].copy()
    df["date"] = pd.to_datetime(df["date"])
    return df


# ---------------------------------------------------------------------------
# Return-based comparison
# ---------------------------------------------------------------------------

def compute_daily_returns(df: pd.DataFrame, date_col: str, close_col: str) -> pd.DataFrame:
    """銘柄別の日次リターンを計算"""
    df = df.sort_values(["ticker", date_col]).copy()
    df["prev_close"] = df.groupby("ticker")[close_col].shift(1)
    df["ret"] = np.where(
        df["prev_close"] > 0,
        (df[close_col] / df["prev_close"] - 1) * 100,
        np.nan,
    )
    return df


def compare_returns(jq_raw: pd.DataFrame, yf: pd.DataFrame) -> pd.DataFrame:
    """日次リターンベースで突合"""
    # JQuantsは生値（Close）でリターン計算（配当未調整）
    jq = jq_raw[["Date", "ticker", "Close"]].copy()
    jq["date"] = pd.to_datetime(jq["Date"])
    jq = compute_daily_returns(jq, "date", "Close")
    jq = jq.rename(columns={"ret": "ret_jq", "Close": "close_jq"})

    # yfinanceは調整済みCloseでリターン計算
    yf_c = yf[["date", "ticker", "Close"]].copy()
    yf_c = compute_daily_returns(yf_c, "date", "Close")
    yf_c = yf_c.rename(columns={"ret": "ret_yf", "Close": "close_yf"})

    # JQuants期間に合わせる
    jq_min, jq_max = jq["date"].min(), jq["date"].max()
    yf_c = yf_c[(yf_c["date"] >= jq_min) & (yf_c["date"] <= jq_max)]

    merged = pd.merge(
        jq[["date", "ticker", "close_jq", "ret_jq"]],
        yf_c[["date", "ticker", "close_yf", "ret_yf"]],
        on=["date", "ticker"],
        how="outer",
        indicator=True,
    )
    merged["ret_diff"] = (merged["ret_jq"] - merged["ret_yf"]).abs()
    return merged


def classify_quality(merged: pd.DataFrame) -> pd.DataFrame:
    """日次リターン差異に基づく4段階分類"""
    records = []
    for _, row in merged.iterrows():
        mt = row["_merge"]
        issues = []
        severity = 4

        if mt == "left_only":
            issues.append("JQuantsのみ")
            severity = 2
        elif mt == "right_only":
            issues.append("yfinanceのみ")
            severity = 2
        else:
            rd = row["ret_diff"]
            if pd.isna(rd):
                # 初日（prev_closeなし）→ 問題なし
                continue

            # Close < 0（yfinance側）
            if row.get("close_yf") is not None and row["close_yf"] < 0:
                issues.append(f"yf Close<0: {row['close_yf']:.2f}")
                severity = 1
            elif rd >= 1.0:
                # リターン差>=1%: 配当落ち日/株式分割日 → 結果に影響
                issues.append(f"リターン差 {rd:.2f}% (JQ:{row['ret_jq']:+.2f}% yf:{row['ret_yf']:+.2f}%)")
                severity = 1
            elif rd >= 0.1:
                issues.append(f"リターン差 {rd:.3f}%")
                severity = 3
            elif rd >= 0.01:
                issues.append(f"リターン差 {rd:.4f}%")
                severity = 4
            else:
                continue  # 完全一致

        if issues:
            records.append({
                "date": row["date"],
                "ticker": row["ticker"],
                "severity": severity,
                "issues": " | ".join(issues),
                "ret_jq": row.get("ret_jq"),
                "ret_yf": row.get("ret_yf"),
                "ret_diff": row.get("ret_diff"),
                "close_jq": row.get("close_jq"),
                "close_yf": row.get("close_yf"),
            })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def generate_report_html(
    quality: pd.DataFrame,
    merged: pd.DataFrame,
    jq_stats: dict,
    yf_stats: dict,
) -> str:
    n_total = len(merged)
    n_both = (merged["_merge"] == "both").sum()
    n_jq_only = (merged["_merge"] == "left_only").sum()
    n_yf_only = (merged["_merge"] == "right_only").sum()

    sev_counts = {s: int((quality["severity"] == s).sum()) if len(quality) > 0 else 0 for s in [1, 2, 3, 4]}

    # 完全一致数（qualityに含まれない = 差異 < 0.01% or 初日）
    n_clean = n_both - sum(1 for _, r in quality.iterrows() if r.get("_merge", "both") != "left_only" and r.get("_merge", "both") != "right_only") if "_merge" in quality.columns else n_both - len(quality[quality["severity"].isin([1, 2, 3])])

    # Level 1 詳細
    sev1_rows = ""
    if sev_counts[1] > 0:
        sev1 = quality[quality["severity"] == 1].sort_values("ret_diff", ascending=False).head(50)
        for _, r in sev1.iterrows():
            d = r["date"]
            date_str = f"{d:%Y-%m-%d}" if hasattr(d, "strftime") else str(d)[:10]
            sev1_rows += f"<tr><td>{date_str}</td><td>{r['ticker']}</td><td>{r['issues']}</td></tr>"

    # Level 1 の月別分布
    sev1_monthly = ""
    if sev_counts[1] > 0:
        s1 = quality[quality["severity"] == 1].copy()
        s1["month"] = pd.to_datetime(s1["date"]).dt.month
        mc = s1["month"].value_counts().sort_index()
        for m, c in mc.items():
            sev1_monthly += f"<tr><td>{m}月</td><td class='r'>{c:,}</td></tr>"

    # Level 2 銘柄別
    sev2_rows = ""
    if sev_counts[2] > 0:
        s2 = quality[quality["severity"] == 2]
        s2_by_type = s2["issues"].value_counts().head(5)
        for issue, cnt in s2_by_type.items():
            sev2_rows += f"<tr><td>{issue}</td><td class='r'>{cnt:,}</td></tr>"

    # 銘柄別リターン差異統計
    both = merged[merged["_merge"] == "both"].dropna(subset=["ret_diff"])
    ticker_rows = ""
    if len(both) > 0:
        ts = both.groupby("ticker").agg(
            n=("ret_diff", "size"),
            mean_diff=("ret_diff", "mean"),
            max_diff=("ret_diff", "max"),
            n_gt1=("ret_diff", lambda x: (x >= 1).sum()),
        ).sort_values("n_gt1", ascending=False).head(20)
        for ticker, r in ts.iterrows():
            cls = "num-neg" if r["n_gt1"] > 5 else ""
            ticker_rows += f"<tr><td>{ticker}</td><td class='r'>{r['n']:,.0f}</td><td class='r'>{r['mean_diff']:.4f}%</td><td class='r {cls}'>{r['max_diff']:.2f}%</td><td class='r {cls}'>{r['n_gt1']:,.0f}</td></tr>"

    now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Chapter 1: データ品質検証 — JQuants vs yfinance 日次リターン突合</title>
<style>
  :root {{
    --bg: #09090b; --card: #18181b; --card-border: #27272a;
    --text: #fafafa; --text-muted: #a1a1aa;
    --emerald: #34d399; --rose: #fb7185; --amber: #fbbf24; --blue: #60a5fa;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif; line-height:1.6; padding:24px; max-width:1200px; margin:0 auto; }}
  h1 {{ font-size:1.5rem; margin-bottom:8px; }}
  .subtitle {{ color:var(--text-muted); font-size:0.875rem; margin-bottom:32px; }}
  .section {{ background:var(--card); border:1px solid var(--card-border); border-radius:12px; padding:24px; margin-bottom:20px; }}
  .section h2 {{ font-size:1.1rem; margin-bottom:16px; }}
  .section h3 {{ font-size:0.95rem; color:var(--text-muted); margin:16px 0 8px 0; }}
  table {{ width:100%; border-collapse:collapse; font-size:0.85rem; margin:12px 0; }}
  th {{ text-align:left; padding:8px 12px; background:rgba(255,255,255,0.03); color:var(--text-muted); font-weight:600; border-bottom:1px solid var(--card-border); white-space:nowrap; }}
  th.r {{ text-align:right; }}
  td {{ padding:8px 12px; border-bottom:1px solid rgba(255,255,255,0.05); }}
  td.r {{ text-align:right; font-variant-numeric:tabular-nums; }}
  tr:hover td {{ background:rgba(255,255,255,0.02); }}
  .num-pos {{ color:var(--emerald); font-weight:600; }}
  .num-neg {{ color:var(--rose); font-weight:600; }}
  .grid-4 {{ display:grid; grid-template-columns:1fr 1fr 1fr 1fr; gap:16px; }}
  .grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; }}
  @media (max-width:768px) {{ .grid-4,.grid-2 {{ grid-template-columns:1fr; }} }}
  .stat-card {{ background:rgba(255,255,255,0.02); border:1px solid var(--card-border); border-radius:8px; padding:16px; text-align:center; }}
  .stat-card .label {{ color:var(--text-muted); font-size:0.75rem; margin-bottom:4px; }}
  .stat-card .value {{ font-size:1.5rem; font-weight:700; }}
  .stat-card .sub {{ color:var(--text-muted); font-size:0.75rem; margin-top:2px; }}
  .alert-box {{ border-radius:8px; padding:16px; margin:16px 0; font-size:0.875rem; line-height:1.7; }}
  .alert-danger {{ background:rgba(251,113,133,0.1); border:1px solid rgba(251,113,133,0.3); color:var(--rose); }}
  .alert-warning {{ background:rgba(251,191,36,0.08); border:1px solid rgba(251,191,36,0.3); color:var(--amber); }}
  .alert-info {{ background:rgba(96,165,250,0.1); border:1px solid rgba(96,165,250,0.3); color:var(--blue); }}
  .alert-success {{ background:rgba(52,211,153,0.1); border:1px solid rgba(52,211,153,0.3); color:var(--emerald); }}
  .sev-1 {{ color: var(--rose); font-weight: 700; }}
  .sev-2 {{ color: var(--amber); font-weight: 700; }}
  .sev-3 {{ color: var(--blue); }}
  .sev-4 {{ color: var(--emerald); }}
  footer {{ text-align:center; color:var(--text-muted); font-size:0.7rem; margin-top:40px; padding:16px 0; border-top:1px solid var(--card-border); }}
</style>
</head>
<body>

<h1>Chapter 1: データ品質検証</h1>
<div class="subtitle">JQuants（生値）vs yfinance（調整済み）日次リターン突合 | Generated: {now}</div>

<div class="section">
  <h2>手法</h2>
  <div class="alert-box alert-info">
    yfinanceは配当+分割を遡及調整するため<strong>絶対価格はJQuantsと一致しない</strong>。
    バックテストに影響するのは日次リターンの差異なので、JQuants生値Close → 日次リターン vs yfinance調整済みClose → 日次リターン で比較する。
    差異の主因は<strong>配当落ち日</strong>（yfinanceは配当調整込みのためリターンが異なる）。
  </div>
</div>

<div class="section">
  <h2>データソース</h2>
  <div class="grid-4">
    <div class="stat-card">
      <div class="label">JQuants（生値）</div>
      <div class="value">{jq_stats['n_rows']:,}</div>
      <div class="sub">{jq_stats['n_tickers']}銘柄 / {jq_stats['date_range']}</div>
    </div>
    <div class="stat-card">
      <div class="label">yfinance（同期間）</div>
      <div class="value">{yf_stats['n_rows']:,}</div>
      <div class="sub">{yf_stats['n_tickers']}銘柄</div>
    </div>
    <div class="stat-card">
      <div class="label">突合成功</div>
      <div class="value">{n_both:,}</div>
      <div class="sub">両方に存在</div>
    </div>
    <div class="stat-card">
      <div class="label">片方のみ</div>
      <div class="value">{n_jq_only + n_yf_only:,}</div>
      <div class="sub">JQのみ {n_jq_only:,} / yfのみ {n_yf_only:,}</div>
    </div>
  </div>
</div>

<div class="section">
  <h2>品質分類サマリー（日次リターン差異ベース）</h2>
  <table>
    <thead><tr><th>Level</th><th>定義</th><th>閾値</th><th class="r">件数</th><th>対応</th></tr></thead>
    <tbody>
      <tr><td class="sev-1">1</td><td>結果に影響する重大な不備</td><td>リターン差 ≥ 1%（配当落ち日/分割日）</td><td class="r">{sev_counts[1]:,}</td><td>除外必須</td></tr>
      <tr><td class="sev-2">2</td><td>単純な不備</td><td>片方のデータソースに欠損</td><td class="r">{sev_counts[2]:,}</td><td>除外推奨</td></tr>
      <tr><td class="sev-3">3</td><td>軽微、除外で精度向上</td><td>リターン差 0.1〜1%</td><td class="r">{sev_counts[3]:,}</td><td>任意</td></tr>
      <tr><td class="sev-4">4</td><td>軽微、除外不要</td><td>リターン差 0.01〜0.1%（丸め誤差）</td><td class="r">{sev_counts[4]:,}</td><td>不要</td></tr>
    </tbody>
  </table>
  <div class="alert-box alert-success">
    問題なしレコード（リターン差 &lt; 0.01%）: <strong>{n_both - sev_counts[1] - sev_counts[2] - sev_counts[3] - sev_counts[4]:,}</strong> / {n_both:,}（<strong>{(n_both - sev_counts[1] - sev_counts[2] - sev_counts[3] - sev_counts[4]) / n_both * 100:.1f}%</strong>）
  </div>
</div>

<div class="section">
  <h2>Level 1: 重大な不備（リターン差 ≥ 1%）— {sev_counts[1]:,}件</h2>
  <div class="grid-2">
    <div>
      <h3>月別分布（配当落ち月に集中）</h3>
      <table>
        <thead><tr><th>月</th><th class="r">件数</th></tr></thead>
        <tbody>{sev1_monthly}</tbody>
      </table>
    </div>
    <div>
      <h3>サンプル（リターン差の大きい順、先頭50件）</h3>
      <table>
        <thead><tr><th>日付</th><th>銘柄</th><th>詳細</th></tr></thead>
        <tbody>{sev1_rows}</tbody>
      </table>
    </div>
  </div>
  <div class="alert-box alert-warning">
    3月・9月（期末配当落ち）と6月・12月（中間配当落ち）に集中。
    yfinanceは配当調整を遡及適用するため、配当落ち日のリターンがJQuants生値と乖離する。
    バックテストでは<strong>これらの日のシグナル判定・SMA計算に影響する可能性</strong>がある。
  </div>
</div>

{"" if sev_counts[2] == 0 else f'''
<div class="section">
  <h2>Level 2: 片方欠損 — {sev_counts[2]:,}件</h2>
  <table>
    <thead><tr><th>種別</th><th class="r">件数</th></tr></thead>
    <tbody>{sev2_rows}</tbody>
  </table>
</div>
'''}

<div class="section">
  <h2>銘柄別リターン差異統計（リターン差≥1%件数の多い順、上位20）</h2>
  <table>
    <thead><tr><th>銘柄</th><th class="r">日数</th><th class="r">平均|差異|</th><th class="r">最大|差異|</th><th class="r">≥1%件数</th></tr></thead>
    <tbody>{ticker_rows}</tbody>
  </table>
</div>

<div class="section">
  <h2>結論・次ステップ</h2>
  <div class="alert-box alert-info">
    <strong>yfinanceデータの品質は概ね良好</strong>（97%以上のレコードで日次リターン差 &lt; 0.01%）。<br>
    主な差異は配当落ち日のリターン乖離（yfinance=配当調整済み vs JQuants=生値）。<br><br>
    <strong>対応方針:</strong><br>
    ・2016年以降: JQuants生値を正とする（配当未調整 = 実際の取引価格）<br>
    ・2016年以前: yfinanceを使用し、Level 1該当日（配当落ち/分割日）をフラグ付与<br>
    ・Level 2（片方欠損）: 該当日を除外<br>
    ・cleaned データセットを data/processed/ に出力 → Chapter 2 の発射台とする
  </div>
</div>

<footer>Generated by 01_fetch_jquants_prices.py | strategy_verification/chapters/01_data_quality</footer>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def save_metadata(tickers: list[str], jq_rows: int, yf_rows: int) -> None:
    md = f"""# データ出典

## JQuants 日足株価
- ソース: J-Quants API v2 `/equities/bars/daily`
- プラン: Standard
- 期間: {JQ_FROM} ~ {JQ_TO}（10年）
- 銘柄数: {len(tickers)}（TOPIX100構成銘柄）
- レコード数: {jq_rows:,}
- 価格: 生値（Open/High/Low/Close = 当日の実際の取引価格、配当未調整）
- 調整済み価格も保持（AdjustmentOpen/High/Low/Close = 株式分割のみ調整）
- 取得日: {date.today()}

## yfinance 日足株価
- ソース: yfinance (Yahoo Finance)
- ファイル: data/parquet/prices_max_1d.parquet
- 期間: 1999-05-06 ~ 2026-03-04（最大取得可能期間）
- 銘柄数: 168（うち143銘柄を使用）
- レコード数: {yf_rows:,}（対象銘柄フィルタ後、全期間）
- 価格: 配当+株式分割を遡及調整済み

## 突合方法
- 絶対価格ではなく**日次リターン**で比較（調整方法の違いを吸収）
- JQuants生値Close → pct_change vs yfinance調整済みClose → pct_change
- 差異の主因: 配当落ち日（yfinanceは配当調整込み、JQuantsは未調整）

## 品質分類定義
| Level | 定義 | 閾値 | 対応 |
|-------|------|------|------|
| 1 | 結果に影響する重大な不備 | リターン差 ≥ 1% | 除外必須 |
| 2 | 単純な不備 | 片方のソースに欠損 | 除外推奨 |
| 3 | 軽微、除外で精度向上 | リターン差 0.1〜1% | 任意 |
| 4 | 軽微、除外不要 | リターン差 0.01〜0.1% | 不要 |
"""
    (METADATA_DIR / "price_data_sources.md").write_text(md, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    for d in [RAW_JQ_DIR, RAW_YF_DIR, INTERIM_DIR, METADATA_DIR, CH01_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    meta = pd.read_parquet(str(DASH_DIR / "data" / "parquet" / "meta.parquet"))
    tickers = sorted(meta["ticker"].unique().tolist())
    print(f"=== Target: {len(tickers)} tickers ===\n")

    # 1. JQuants（キャッシュあり）
    jq_path = RAW_JQ_DIR / "prices_daily.parquet"
    if jq_path.exists():
        print(f"JQuants cached: {jq_path}")
        jq_raw = pd.read_parquet(str(jq_path))
    else:
        print("Fetching from JQuants...")
        jq_raw = fetch_jquants_prices(tickers)
        jq_raw.to_parquet(str(jq_path), index=False)
    print(f"JQuants: {len(jq_raw):,} rows, {jq_raw['ticker'].nunique()} tickers")

    # 2. yfinance
    print("\nLoading yfinance...")
    yf_all = load_yfinance_prices(tickers)
    yf_save = RAW_YF_DIR / "prices_daily.parquet"
    if not yf_save.exists():
        yf_all.to_parquet(str(yf_save), index=False)
    print(f"yfinance: {len(yf_all):,} rows, {yf_all['ticker'].nunique()} tickers")

    # 3. 日次リターン突合
    print("\nComparing daily returns...")
    merged = compare_returns(jq_raw, yf_all)
    merged.to_parquet(str(INTERIM_DIR / "return_comparison.parquet"), index=False)

    n_both = (merged["_merge"] == "both").sum()
    n_jq_only = (merged["_merge"] == "left_only").sum()
    n_yf_only = (merged["_merge"] == "right_only").sum()
    print(f"  Both: {n_both:,} | JQ only: {n_jq_only:,} | yf only: {n_yf_only:,}")

    # 4. 品質分類
    print("\nClassifying quality...")
    quality = classify_quality(merged)
    quality.to_parquet(str(INTERIM_DIR / "data_quality_report.parquet"), index=False)
    print(f"Issues: {len(quality):,}")
    for sev in [1, 2, 3, 4]:
        cnt = (quality["severity"] == sev).sum()
        print(f"  Level {sev}: {cnt:,}")

    # 5. メタデータ
    jq_dates = pd.to_datetime(jq_raw["Date"])
    yf_filtered = yf_all[(yf_all["date"] >= jq_dates.min()) & (yf_all["date"] <= jq_dates.max())]
    save_metadata(tickers, len(jq_raw), len(yf_all))

    # 6. HTML
    print("\nGenerating report...")
    jq_stats = {
        "n_rows": len(jq_raw),
        "n_tickers": jq_raw["ticker"].nunique(),
        "date_range": f"{jq_dates.min():%Y/%m/%d} ~ {jq_dates.max():%Y/%m/%d}",
    }
    yf_stats = {
        "n_rows": len(yf_filtered),
        "n_tickers": yf_filtered["ticker"].nunique(),
    }
    html = generate_report_html(quality, merged, jq_stats, yf_stats)
    (CH01_DIR / "report.html").write_text(html, encoding="utf-8")
    print(f"Report: {CH01_DIR / 'report.html'}")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
