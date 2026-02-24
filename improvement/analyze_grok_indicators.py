#!/usr/bin/env python3
"""
grok銘柄選定時の各指標（ATR, RSI9日足, RSI9五分足, Q*）と
ショート損益（-phase2_return）の関連分析。

成果物: improvement/output/grok_indicator_analysis.html
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import timedelta

import numpy as np
import pandas as pd
import joblib
import json

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── パス ──
ARCHIVE = ROOT / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
GROK_PRICES = ROOT / "data" / "parquet" / "grok_prices_max_1d.parquet"
INDEX_PRICES = ROOT / "data" / "parquet" / "index_prices_max_1d.parquet"
FUTURES_PRICES = ROOT / "data" / "parquet" / "futures_prices_max_1d.parquet"
CURRENCY_PRICES = ROOT / "data" / "parquet" / "currency_prices_max_1d.parquet"
ML_MODEL = ROOT / "models" / "grok_lgbm_model.pkl"
ML_META = ROOT / "models" / "grok_lgbm_meta.json"
PARQUET_5M_FILES = sorted((ROOT / "data" / "parquet" / "backtest").glob("grok_5m_60d_*.parquet"))
OUTPUT = ROOT / "improvement" / "output" / "grok_indicator_analysis.html"


# =====================================================================
# RSI 計算（楽天証券方式 = Wilder's RSI）
# =====================================================================
def calc_rsi_series(closes: pd.Series, period: int = 9) -> pd.Series:
    """Wilder's RSI を返す。"""
    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()

    # Wilder smoothing (exponential)
    # rolling の最初の有効値は index=period (diff()でindex 0がNaN → min_periods分ずれる)
    # そのためループは period+1 から開始し、index=period の rolling mean をシードとする
    for i in range(period + 1, len(closes)):
        avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (period - 1) + loss.iloc[i]) / period

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    return rsi


# =====================================================================
# 5分足 RSI 計算
# =====================================================================
def load_5m_parquets() -> pd.DataFrame:
    """parquet の 5分足をすべて結合して返す。"""
    frames = []
    for f in PARQUET_5M_FILES:
        df = pd.read_parquet(f)
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)
        df["date"] = df["datetime"].dt.date
        frames.append(df[["datetime", "open", "high", "low", "close", "volume", "ticker", "date"]])
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["datetime", "ticker"])
    return combined


def download_5m_yfinance(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """yfinance で5分足を取得。"""
    import yfinance as yf

    frames = []
    for ticker in tickers:
        try:
            df = yf.download(ticker, start=start, end=end, interval="5m",
                             progress=False, auto_adjust=True)
            if df.empty:
                continue
            # Handle multi-level columns from yfinance
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.reset_index()
            # Rename columns to match parquet format
            col_map = {}
            for c in df.columns:
                cl = str(c).lower()
                if cl == "datetime" or cl == "date":
                    col_map[c] = "datetime"
                elif cl == "open":
                    col_map[c] = "open"
                elif cl == "high":
                    col_map[c] = "high"
                elif cl == "low":
                    col_map[c] = "low"
                elif cl == "close":
                    col_map[c] = "close"
                elif cl == "volume":
                    col_map[c] = "volume"
            df = df.rename(columns=col_map)
            if "datetime" not in df.columns and "Datetime" in df.columns:
                df = df.rename(columns={"Datetime": "datetime"})
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)
            df["ticker"] = ticker
            df["date"] = df["datetime"].dt.date
            frames.append(df[["datetime", "open", "high", "low", "close", "volume", "ticker", "date"]])
        except Exception as e:
            print(f"  [WARN] yfinance {ticker}: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def compute_5m_rsi_for_record(
    ticker: str,
    target_date,  # backtest_date - 1 business day
    daily_close: float,
    all_5m: pd.DataFrame,
) -> float | None:
    """指定銘柄・日付の5分足RSI9を計算。最終バーの終値を日足終値に置換。"""
    target_date_val = pd.Timestamp(target_date).date() if not isinstance(target_date, type(pd.Timestamp("2020-01-01").date())) else target_date

    ticker_data = all_5m[all_5m["ticker"] == ticker].copy()
    if ticker_data.empty:
        return None

    # target_date以前のデータ（RSI安定化のため数日分）
    ticker_data = ticker_data[ticker_data["date"] <= target_date_val].sort_values("datetime")
    if len(ticker_data) < 20:
        return None

    # target_date の最終バーの close を daily_close に置換
    target_mask = ticker_data["date"] == target_date_val
    if target_mask.any():
        last_idx = ticker_data[target_mask].index[-1]
        ticker_data.loc[last_idx, "close"] = daily_close

    closes = ticker_data["close"].reset_index(drop=True)
    rsi = calc_rsi_series(closes, period=9)
    last_rsi = rsi.iloc[-1]
    return float(last_rsi) if not np.isnan(last_rsi) else None


# =====================================================================
# Q* (quintile) 計算
# =====================================================================
def get_quintile(prob: float) -> str:
    if prob <= 0.32:
        return "Q1"
    elif prob <= 0.40:
        return "Q2"
    elif prob <= 0.48:
        return "Q3"
    elif prob <= 0.55:
        return "Q4"
    else:
        return "Q5"


def load_market_data() -> dict:
    market_data = {}
    if INDEX_PRICES.exists():
        idx_df = pd.read_parquet(INDEX_PRICES)
        idx_df["date"] = pd.to_datetime(idx_df["date"])
        for key, ticker in [("nikkei", "^N225"), ("topix", "1306.T")]:
            df = idx_df[idx_df["ticker"] == ticker].copy()
            market_data[key] = df.sort_values("date").reset_index(drop=True)
    if FUTURES_PRICES.exists():
        fut_df = pd.read_parquet(FUTURES_PRICES)
        fut_df["date"] = pd.to_datetime(fut_df["date"])
        market_data["futures"] = fut_df[fut_df["ticker"] == "NKD=F"].sort_values("date").reset_index(drop=True)
    if CURRENCY_PRICES.exists():
        cur_df = pd.read_parquet(CURRENCY_PRICES)
        cur_df["date"] = pd.to_datetime(cur_df["date"])
        market_data["usdjpy"] = cur_df[cur_df["ticker"] == "JPY=X"].sort_values("date").reset_index(drop=True)
    return market_data


def calc_market_features(target_date: pd.Timestamp, market_data: dict) -> dict:
    features = {}
    for key in ["nikkei", "topix", "futures", "usdjpy"]:
        if key not in market_data:
            features[f"{key}_vol_5d"] = np.nan
            features[f"{key}_ret_5d"] = np.nan
            features[f"{key}_ma5_dev"] = np.nan
            continue
        df = market_data[key]
        df_past = df[df["date"] < target_date].tail(30)
        if len(df_past) < 5:
            features[f"{key}_vol_5d"] = np.nan
            features[f"{key}_ret_5d"] = np.nan
            features[f"{key}_ma5_dev"] = np.nan
            continue
        closes = df_past["Close"].values
        returns = np.diff(closes) / closes[:-1]
        features[f"{key}_vol_5d"] = float(np.std(returns[-5:]) * 100) if len(returns) >= 5 else np.nan
        features[f"{key}_ret_5d"] = float((closes[-1] - closes[-5]) / closes[-5] * 100) if len(closes) >= 5 else np.nan
        ma5 = np.mean(closes[-5:])
        features[f"{key}_ma5_dev"] = float((closes[-1] - ma5) / ma5 * 100)
    return features


def calc_price_features(ticker: str, target_date: pd.Timestamp, prices_df: pd.DataFrame) -> dict | None:
    tp = prices_df[
        (prices_df["ticker"] == ticker) & (prices_df["date"] < target_date)
    ].sort_values("date").tail(60).dropna(subset=["Close"])
    if len(tp) < 5:
        return None
    closes = tp["Close"].values
    volumes = tp["Volume"].values
    highs = tp["High"].values
    lows = tp["Low"].values
    features = {}
    returns = np.diff(closes) / closes[:-1]
    features["volatility_5d"] = float(np.std(returns[-5:]) * 100) if len(returns) >= 5 else np.nan
    features["volatility_10d"] = float(np.std(returns[-10:]) * 100) if len(returns) >= 10 else np.nan
    features["volatility_20d"] = float(np.std(returns[-20:]) * 100) if len(returns) >= 20 else np.nan
    ma5 = np.mean(closes[-5:])
    ma25 = np.mean(closes[-25:]) if len(closes) >= 25 else np.nan
    features["ma5_deviation"] = float((closes[-1] - ma5) / ma5 * 100)
    features["ma25_deviation"] = float((closes[-1] - ma25) / ma25 * 100) if not np.isnan(ma25) else np.nan
    features["prev_day_return"] = float((closes[-1] - closes[-2]) / closes[-2] * 100) if len(closes) >= 2 else np.nan
    features["return_5d"] = float((closes[-1] - closes[-5]) / closes[-5] * 100) if len(closes) >= 5 else np.nan
    features["return_10d"] = float((closes[-1] - closes[-10]) / closes[-10] * 100) if len(closes) >= 10 else np.nan
    if len(volumes) >= 5:
        avg_vol = np.mean(volumes[-5:])
        features["volume_ratio_5d"] = float(volumes[-1] / avg_vol) if avg_vol > 0 else np.nan
    else:
        features["volume_ratio_5d"] = np.nan
    if len(highs) >= 5 and len(lows) >= 5:
        mn = np.min(lows[-5:])
        features["price_range_5d"] = float((np.max(highs[-5:]) - mn) / mn * 100) if mn > 0 else np.nan
    else:
        features["price_range_5d"] = np.nan
    return features


def compute_quintile_for_archive(arc: pd.DataFrame) -> tuple[list, list]:
    """archive全レコードに対してMLモデルで prob_up, quintile を計算"""
    model = joblib.load(ML_MODEL)
    with open(ML_META, "r") as f:
        meta = json.load(f)
    feature_names = meta["feature_names"]

    prices_df = pd.read_parquet(GROK_PRICES)
    prices_df["date"] = pd.to_datetime(prices_df["date"])
    market_data = load_market_data()

    # 日付ごとに市場特徴量をキャッシュ
    market_cache: dict[str, dict] = {}

    prob_up_list = []
    quintile_list = []

    for idx, row in arc.iterrows():
        sel_date = pd.Timestamp(row["selection_date"])
        sel_key = str(sel_date.date())
        ticker = row["ticker"]

        if sel_key not in market_cache:
            market_cache[sel_key] = calc_market_features(sel_date, market_data)

        mkt_feat = market_cache[sel_key]

        existing = {
            "grok_rank": row.get("grok_rank"),
            "selection_score": row.get("selection_score"),
            "buy_price": row.get("buy_price"),
            "market_cap": row.get("market_cap"),
            "atr14_pct": row.get("atr14_pct"),
            "vol_ratio": row.get("vol_ratio"),
            "rsi9": row.get("rsi9"),
            "weekday": row.get("weekday"),
            "nikkei_change_pct": row.get("nikkei_change_pct"),
            "futures_change_pct": row.get("futures_change_pct"),
            "shortable": 1 if row.get("shortable") else 0,
            "day_trade": 1 if row.get("day_trade") else 0,
        }

        price_feat = calc_price_features(ticker, sel_date, prices_df)
        if price_feat is None:
            prob_up_list.append(np.nan)
            quintile_list.append(None)
            continue

        all_feat = {**existing, **price_feat, **mkt_feat}
        vec = []
        for fn in feature_names:
            v = all_feat.get(fn)
            if v is None or (isinstance(v, float) and np.isnan(v)):
                vec.append(0)
            else:
                vec.append(float(v))

        try:
            X = pd.DataFrame([vec], columns=feature_names)
            if "weekday" in X.columns:
                X["weekday"] = X["weekday"].astype("category")
            prob = float(model.predict_proba(X)[0][1])
            prob_up_list.append(round(prob, 3))
            quintile_list.append(get_quintile(prob))
        except Exception:
            prob_up_list.append(np.nan)
            quintile_list.append(None)

    return prob_up_list, quintile_list


# =====================================================================
# 前営業日を求める
# =====================================================================
def prev_business_day(d):
    """dの前営業日（土日スキップ、祝日は未考慮）"""
    from datetime import date as dt_date
    d = pd.Timestamp(d).date() if not isinstance(d, dt_date) else d
    d -= timedelta(days=1)
    while d.weekday() >= 5:  # 5=sat, 6=sun
        d -= timedelta(days=1)
    return d


# =====================================================================
# 分析ユーティリティ
# =====================================================================
def bin_analysis(df: pd.DataFrame, col: str, bins: list, labels: list) -> pd.DataFrame:
    """指標をビン分割し、ショート勝率・平均損益を集計"""
    df = df.dropna(subset=[col, "short_return"])
    df["bin"] = pd.cut(df[col], bins=bins, labels=labels, include_lowest=True)
    agg = df.groupby("bin", observed=False).agg(
        count=("short_return", "size"),
        short_win_rate=("short_win", "mean"),
        avg_short_return=("short_return", "mean"),
        avg_short_profit_100=("short_profit_100", "mean"),
        median_short_return=("short_return", "median"),
    ).reset_index()
    agg["short_win_rate"] = (agg["short_win_rate"] * 100).round(1)
    agg["avg_short_return"] = (agg["avg_short_return"] * 100).round(2)
    agg["median_short_return"] = (agg["median_short_return"] * 100).round(2)
    agg["avg_short_profit_100"] = agg["avg_short_profit_100"].round(0).astype(int)
    return agg


def quintile_analysis(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["quintile", "short_return"])
    agg = df.groupby("quintile", observed=False).agg(
        count=("short_return", "size"),
        short_win_rate=("short_win", "mean"),
        avg_short_return=("short_return", "mean"),
        avg_short_profit_100=("short_profit_100", "mean"),
        median_short_return=("short_return", "median"),
    ).reset_index()
    agg["short_win_rate"] = (agg["short_win_rate"] * 100).round(1)
    agg["avg_short_return"] = (agg["avg_short_return"] * 100).round(2)
    agg["median_short_return"] = (agg["median_short_return"] * 100).round(2)
    agg["avg_short_profit_100"] = agg["avg_short_profit_100"].round(0).astype(int)
    # sort Q1-Q5
    q_order = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    agg["quintile"] = pd.Categorical(agg["quintile"], categories=q_order, ordered=True)
    return agg.sort_values("quintile").reset_index(drop=True)


# =====================================================================
# HTML 生成
# =====================================================================
def render_table(title: str, df: pd.DataFrame, col_label: str) -> str:
    """分析テーブルをHTMLで描画。"""
    # 勝率に応じた色
    def wr_color(v):
        if v >= 60:
            return "#4ade80"  # green
        elif v >= 50:
            return "#fbbf24"  # yellow
        else:
            return "#f87171"  # red

    def ret_color(v):
        if v > 0:
            return "#4ade80"
        elif v == 0:
            return "#9ca3af"
        else:
            return "#f87171"

    rows_html = ""
    for _, r in df.iterrows():
        wrc = wr_color(r["short_win_rate"])
        rc = ret_color(r["avg_short_return"])
        mc = ret_color(r["median_short_return"])
        pc = ret_color(r["avg_short_profit_100"])
        rows_html += f"""
        <tr>
            <td>{r.iloc[0]}</td>
            <td>{r['count']}</td>
            <td style="color:{wrc};font-weight:bold">{r['short_win_rate']}%</td>
            <td style="color:{rc}">{r['avg_short_return']:+.2f}%</td>
            <td style="color:{mc}">{r['median_short_return']:+.2f}%</td>
            <td style="color:{pc}">{r['avg_short_profit_100']:+,}円</td>
        </tr>"""

    return f"""
    <div class="section">
        <h2>{title}</h2>
        <table>
            <thead>
                <tr>
                    <th>{col_label}</th>
                    <th>件数</th>
                    <th>ショート勝率</th>
                    <th>平均リターン</th>
                    <th>中央値</th>
                    <th>平均損益/100株</th>
                </tr>
            </thead>
            <tbody>{rows_html}</tbody>
        </table>
    </div>"""


def generate_html(
    arc: pd.DataFrame,
    atr_table: pd.DataFrame,
    rsi9_table: pd.DataFrame,
    rsi5m_table: pd.DataFrame,
    q_table: pd.DataFrame,
) -> str:
    total = len(arc)
    valid_short = arc["short_return"].notna().sum()
    avg_wr = (arc["short_win"].mean() * 100) if valid_short > 0 else 0
    avg_ret = (arc["short_return"].mean() * 100) if valid_short > 0 else 0

    summary = f"""
    <div class="summary">
        <div class="stat"><span class="stat-label">対象レコード</span><span class="stat-value">{total}</span></div>
        <div class="stat"><span class="stat-label">有効（phase2あり）</span><span class="stat-value">{valid_short}</span></div>
        <div class="stat"><span class="stat-label">全体ショート勝率</span><span class="stat-value">{avg_wr:.1f}%</span></div>
        <div class="stat"><span class="stat-label">全体平均リターン</span><span class="stat-value">{avg_ret:+.2f}%</span></div>
    </div>"""

    atr_html = render_table("ATR (14日, %)", atr_table, "ATR帯")
    rsi9_html = render_table("RSI 9 日足", rsi9_table, "RSI帯")
    rsi5m_html = render_table("RSI 9 五分足（前日終値時点）", rsi5m_table, "RSI帯")
    q_html = render_table("Quintile（ML予測）", q_table, "Q*")

    # 端緒判定
    def judge(tbl: pd.DataFrame) -> str:
        if len(tbl) < 2:
            return "データ不足"
        wrs = tbl["short_win_rate"].values
        rets = tbl["avg_short_return"].values
        spread_wr = max(wrs) - min(wrs)
        spread_ret = max(rets) - min(rets)
        if spread_wr >= 15 or spread_ret >= 3:
            return '<span style="color:#4ade80;font-weight:bold">端緒あり</span>'
        elif spread_wr >= 8 or spread_ret >= 1.5:
            return '<span style="color:#fbbf24">やや端緒あり</span>'
        else:
            return '<span style="color:#f87171">端緒なし</span>'

    verdict = f"""
    <div class="section">
        <h2>端緒判定サマリー</h2>
        <table>
            <thead><tr><th>指標</th><th>勝率スプレッド</th><th>リターンスプレッド</th><th>判定</th></tr></thead>
            <tbody>"""

    for name, tbl in [("ATR", atr_table), ("RSI9日足", rsi9_table), ("RSI9五分足", rsi5m_table), ("Q*", q_table)]:
        if len(tbl) >= 2:
            wrs = tbl["short_win_rate"].values
            rets = tbl["avg_short_return"].values
            sw = f"{max(wrs) - min(wrs):.1f}pp"
            sr = f"{max(rets) - min(rets):.2f}pp"
        else:
            sw = sr = "-"
        verdict += f"<tr><td>{name}</td><td>{sw}</td><td>{sr}</td><td>{judge(tbl)}</td></tr>"
    verdict += "</tbody></table></div>"

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<title>Grok銘柄 指標端緒分析</title>
<style>
body {{ background: #0f172a; color: #e2e8f0; font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; }}
h1 {{ color: #f8fafc; border-bottom: 2px solid #3b82f6; padding-bottom: 8px; }}
h2 {{ color: #93c5fd; margin-top: 32px; }}
.summary {{ display: flex; gap: 24px; margin: 16px 0; flex-wrap: wrap; }}
.stat {{ background: #1e293b; border-radius: 8px; padding: 12px 20px; min-width: 150px; }}
.stat-label {{ display: block; font-size: 12px; color: #94a3b8; }}
.stat-value {{ display: block; font-size: 22px; font-weight: bold; color: #f1f5f9; margin-top: 4px; }}
.section {{ margin-bottom: 32px; }}
table {{ border-collapse: collapse; width: 100%; max-width: 900px; }}
th {{ background: #1e293b; color: #94a3b8; padding: 8px 14px; text-align: left; font-size: 13px; border-bottom: 1px solid #334155; }}
td {{ padding: 8px 14px; border-bottom: 1px solid #1e293b; font-size: 14px; }}
tr:hover {{ background: #1e293b; }}
.note {{ color: #64748b; font-size: 12px; margin-top: 8px; }}
</style>
</head>
<body>
<h1>Grok銘柄 指標端緒分析</h1>
<p class="note">対象: grok_trending_archive（{arc['selection_date'].min()} 〜 {arc['selection_date'].max()}）<br>
成果指標: ショート損益 = -phase2_return（寄付売→大引け買）</p>
{summary}
{verdict}
{atr_html}
{rsi9_html}
{rsi5m_html}
{q_html}
<p class="note">生成日時: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p>
</body>
</html>"""
    return html


# =====================================================================
# main
# =====================================================================
def main():
    print("=== Grok銘柄 指標端緒分析 ===\n")

    # 1. Archive 読み込み
    print("[1/6] Archive 読み込み...")
    arc = pd.read_parquet(ARCHIVE)
    arc["short_return"] = -arc["phase2_return"]
    arc["short_win"] = arc["short_return"] > 0
    arc["short_profit_100"] = -arc["profit_per_100_shares_phase2"]
    print(f"  {len(arc)} レコード, {arc['selection_date'].nunique()} 日")

    # 2. ATR 分析
    print("\n[2/6] ATR 分析...")
    atr_bins = [0, 2, 4, 6, 8, 10, 15, 100]
    atr_labels = ["0-2%", "2-4%", "4-6%", "6-8%", "8-10%", "10-15%", "15%+"]
    atr_table = bin_analysis(arc, "atr14_pct", atr_bins, atr_labels)
    print(atr_table.to_string(index=False))

    # 3. RSI9 日足分析
    print("\n[3/6] RSI9 日足分析...")
    rsi_bins = [0, 20, 30, 40, 50, 60, 70, 80, 100]
    rsi_labels = ["0-20", "20-30", "30-40", "40-50", "50-60", "60-70", "70-80", "80-100"]
    rsi9_table = bin_analysis(arc, "rsi9", rsi_bins, rsi_labels)
    print(rsi9_table.to_string(index=False))

    # 4. 5分足RSI9 計算
    print("\n[4/6] 5分足RSI9 計算...")
    all_5m = load_5m_parquets()
    print(f"  parquet 5m: {len(all_5m)} rows")

    # yfinance で不足分を取得
    max_parquet_date = pd.Timestamp("2026-02-06").date()
    need_yf_mask = arc["backtest_date"].apply(lambda d: pd.Timestamp(d).date() > max_parquet_date)
    need_yf = arc[need_yf_mask]
    if len(need_yf) > 0:
        yf_tickers = sorted(need_yf["ticker"].unique())
        print(f"  yfinance 取得: {len(yf_tickers)} 銘柄 (2/7〜2/19)")
        yf_5m = download_5m_yfinance(yf_tickers, "2026-01-30", "2026-02-20")
        if not yf_5m.empty:
            all_5m = pd.concat([all_5m, yf_5m], ignore_index=True).drop_duplicates(
                subset=["datetime", "ticker"]
            )
            print(f"  yfinance 追加後: {len(all_5m)} rows")

    # 各レコードの5分足RSI9を計算
    rsi_5m_values = []
    computed = 0
    for idx, row in arc.iterrows():
        bd = row["backtest_date"]
        prev_bd = prev_business_day(bd)
        prev_close = row.get("prev_close")
        if prev_close is None or (isinstance(prev_close, float) and np.isnan(prev_close)):
            rsi_5m_values.append(np.nan)
            continue
        rsi_val = compute_5m_rsi_for_record(row["ticker"], prev_bd, prev_close, all_5m)
        rsi_5m_values.append(rsi_val if rsi_val is not None else np.nan)
        if rsi_val is not None:
            computed += 1
    arc["rsi9_5m"] = rsi_5m_values
    print(f"  5分足RSI9 計算完了: {computed}/{len(arc)}")

    rsi5m_table = bin_analysis(arc, "rsi9_5m", rsi_bins, rsi_labels)
    print(rsi5m_table.to_string(index=False))

    # 5. Q* 計算
    print("\n[5/6] Q*（ML予測）計算...")
    prob_up_list, quintile_list = compute_quintile_for_archive(arc)
    arc["prob_up"] = prob_up_list
    arc["quintile"] = quintile_list
    valid_q = arc["quintile"].notna().sum()
    print(f"  Q* 計算完了: {valid_q}/{len(arc)}")
    q_table = quintile_analysis(arc)
    print(q_table.to_string(index=False))

    # 5.5 計算済みデータ保存（再分析用）
    enriched_path = ROOT / "improvement" / "output" / "grok_archive_enriched.parquet"
    arc.to_parquet(enriched_path, index=False)
    print(f"\n  計算済みデータ保存: {enriched_path}")

    # 5.6 クロス分析: Q1/Q2 × 5分足RSI
    print("\n[5.5/6] クロス分析: Q* × 5分足RSI...")
    cross = arc.dropna(subset=["quintile", "rsi9_5m", "short_return"]).copy()
    for q_filter, q_label in [("Q1", "Q1"), ("Q2", "Q2"), ("Q1Q2", "Q1+Q2")]:
        if q_filter == "Q1Q2":
            subset = cross[cross["quintile"].isin(["Q1", "Q2"])]
        else:
            subset = cross[cross["quintile"] == q_filter]
        if len(subset) == 0:
            continue
        print(f"\n  === {q_label} (n={len(subset)}) ===")
        for threshold in [10, 20, 30]:
            low = subset[subset["rsi9_5m"] < threshold]
            high = subset[subset["rsi9_5m"] >= threshold]
            if len(low) > 0:
                wr_low = low["short_win"].mean() * 100
                ret_low = low["short_return"].mean() * 100
                pnl_low = low["short_profit_100"].mean()
                # 朝リバ指標: morning_max_gain_pct (始値から高値までの上昇率)
                riba_low = low["morning_max_gain_pct"].mean() * 100 if "morning_max_gain_pct" in low.columns else float("nan")
            else:
                wr_low = ret_low = pnl_low = riba_low = float("nan")
            if len(high) > 0:
                wr_high = high["short_win"].mean() * 100
                ret_high = high["short_return"].mean() * 100
                pnl_high = high["short_profit_100"].mean()
                riba_high = high["morning_max_gain_pct"].mean() * 100 if "morning_max_gain_pct" in high.columns else float("nan")
            else:
                wr_high = ret_high = pnl_high = riba_high = float("nan")
            print(f"    RSI5m < {threshold}: n={len(low):3d}, 勝率={wr_low:5.1f}%, 平均損益={ret_low:+6.2f}%, 100株PnL={pnl_low:+8.0f}円, 朝リバ={riba_low:5.2f}%")
            print(f"    RSI5m >= {threshold}: n={len(high):3d}, 勝率={wr_high:5.1f}%, 平均損益={ret_high:+6.2f}%, 100株PnL={pnl_high:+8.0f}円, 朝リバ={riba_high:5.2f}%")

    # 6. HTML 生成
    print("\n[6/6] HTML 生成...")
    html = generate_html(arc, atr_table, rsi9_table, rsi5m_table, q_table)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"  出力: {OUTPUT}")
    print("\n=== 完了 ===")


if __name__ == "__main__":
    main()
