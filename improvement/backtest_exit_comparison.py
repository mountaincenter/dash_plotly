#!/usr/bin/env python3
"""
Granville LONG戦略: Exit方式 5パターン比較検証
===============================================
A. ベースライン（SLなし、シグナルexit、MAX_HOLD=60）
B. SL-3% + trail50%（MAX_HOLD=60）
C. SL-3% + シグナルexit（trailなし、MAX_HOLD=60）
D. SL-3% + シグナルexit + 7日キャップ
E. SL-3% + シグナルexit + 10日キャップ

出力: improvement/output/granville_exit_comparison.html
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = ROOT / "data" / "parquet"
OUTPUT_PATH = ROOT / "improvement" / "output" / "granville_exit_comparison.html"

DEV_THRESHOLD = -4.0
BAD_SECTORS = ["医薬品", "輸送用機器", "小売業", "その他製品", "陸運業", "サービス業"]
MAX_PRICE = 20000

PATTERNS: dict[str, dict] = {
    "A": {"label": "A. ベースライン（SLなし）", "sl_pct": 0, "trail_pct": 0, "max_hold": 60},
    "B": {"label": "B. SL-3% + trail50%", "sl_pct": 3, "trail_pct": 0.5, "max_hold": 60},
    "C": {"label": "C. SL-3%（trailなし）★", "sl_pct": 3, "trail_pct": 0, "max_hold": 60},
    "D": {"label": "D. SL-3% + 7日キャップ", "sl_pct": 3, "trail_pct": 0, "max_hold": 7},
    "E": {"label": "E. SL-3% + 10日キャップ", "sl_pct": 3, "trail_pct": 0, "max_hold": 10},
}

PERIODS = [
    ("全期間", None),
    ("2015-2026", "2015-01-01"),
    ("直近2年", "2024-03-01"),
]

REGIMES = [
    ("全体", None),
    ("Uptrend", "uptrend"),
    ("Downtrend", "downtrend"),
]


def load_data() -> pd.DataFrame:
    t0 = time.time()
    m = pd.read_parquet(PARQUET_DIR / "meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)

    p = pd.read_parquet(PARQUET_DIR / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(m["ticker"].tolist())].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["prev_close"] = g["Close"].shift(1)
    ps["dev"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps = ps.dropna(subset=["sma20"])

    ps.loc[:, "up_day"] = ps["Close"] > ps["prev_close"]

    # N225 uptrend
    idx = pd.read_parquet(PARQUET_DIR / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225"})
    nk["nk_sma20"] = nk["nk225"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225"] > nk["nk_sma20"]
    ps = ps.merge(nk[["date", "market_uptrend"]], on="date", how="left")
    ps = ps.merge(m[["ticker", "sectors", "stock_name"]], on="ticker", how="left")

    # フィルター
    ps = ps[~ps["sectors"].isin(BAD_SECTORS)]
    ps = ps[ps["Close"] < MAX_PRICE]

    print(f"Data loaded: {len(ps):,} rows, {ps['ticker'].nunique()} tickers in {time.time()-t0:.1f}s")
    return ps


def backtest_pattern(
    df: pd.DataFrame,
    sl_pct: float,
    trail_pct: float,
    max_hold: int,
) -> pd.DataFrame:
    """
    シグナル: dev < DEV_THRESHOLD & up_day
    Entry: 翌営業日Open
    Exit: SL/trail → signal(Close>=SMA20→翌Open) → expire
    同一銘柄重複除去
    """
    results = []

    for ticker in df["ticker"].unique():
        tk = df[df["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        dates = tk["date"].values
        opens = tk["Open"].values
        closes = tk["Close"].values
        highs = tk["High"].values
        lows = tk["Low"].values
        sma20s = tk["sma20"].values
        devs = tk["dev"].values
        up_days = tk["up_day"].values
        uptrends = tk["market_uptrend"].values
        n = len(tk)

        in_position = False
        position_exit_idx = -1

        for i in range(n):
            if in_position and i > position_exit_idx:
                in_position = False

            # シグナル: dev < threshold & up_day
            if not (devs[i] < DEV_THRESHOLD and up_days[i]):
                continue
            if in_position:
                continue

            # エントリー: 翌営業日Open
            entry_idx = i + 1
            if entry_idx >= n:
                continue
            entry_price = opens[entry_idx]
            if np.isnan(entry_price) or entry_price <= 0:
                continue

            # SL価格
            sl_price = entry_price * (1 - sl_pct / 100) if sl_pct > 0 else 0
            max_price = entry_price

            exit_idx = None
            exit_type = "expire"

            for j in range(entry_idx, min(entry_idx + max_hold, n)):
                c = closes[j]
                s = sma20s[j]
                h = highs[j]
                lo = lows[j]

                if np.isnan(c) or np.isnan(s):
                    continue

                # SL判定（日中Low）— エントリー当日もチェック
                if sl_pct > 0 and lo <= sl_price:
                    exit_idx = j
                    exit_type = "SL"
                    break

                # エントリー当日はSLのみ。trail/signal exitは翌日以降
                if j == entry_idx:
                    continue

                # trailing SL更新（翌日以降のみ）
                if trail_pct > 0 and h > max_price:
                    max_price = h
                    profit = max_price - entry_price
                    if profit > 0:
                        trail_sl = entry_price + profit * trail_pct
                        if trail_sl > sl_price:
                            sl_price = trail_sl

                # trailing SL判定（翌日以降のみ）
                if trail_pct > 0 and sl_price > 0 and lo <= sl_price:
                    exit_idx = j
                    exit_type = "trail"
                    break

                # シグナルexit: Close >= SMA20 → 翌営業日Open
                if c >= s:
                    if j + 1 < n:
                        exit_idx = j + 1
                        exit_type = "signal"
                    else:
                        exit_idx = j
                        exit_type = "signal"
                    break

            if exit_idx is None:
                exit_idx = min(entry_idx + max_hold, n - 1)
                exit_type = "expire"

            # SL/trailは当日のSL価格で約定
            if exit_type in ("SL", "trail"):
                exit_price = sl_price
            else:
                exit_price = opens[exit_idx] if not np.isnan(opens[exit_idx]) else closes[exit_idx]

            if np.isnan(exit_price) or exit_price <= 0:
                continue

            ret_pct = (exit_price / entry_price - 1) * 100
            pnl = int(round(entry_price * 100 * ret_pct / 100))
            hold_days = int(exit_idx - entry_idx)

            in_position = True
            position_exit_idx = exit_idx

            ut = uptrends[i]
            regime = "uptrend" if ut else "downtrend" if ut is not None and not ut else "unknown"

            results.append({
                "ticker": ticker,
                "signal_date": dates[i],
                "entry_date": dates[entry_idx],
                "exit_date": dates[exit_idx],
                "entry_price": round(float(entry_price), 1),
                "exit_price": round(float(exit_price), 1),
                "ret_pct": round(ret_pct, 3),
                "pnl": pnl,
                "hold_days": hold_days,
                "exit_type": exit_type,
                "regime": regime,
                "dev_at_signal": round(float(devs[i]), 2),
            })

    return pd.DataFrame(results)


def calc_stats(trades: pd.DataFrame) -> dict:
    if len(trades) == 0:
        return {
            "n": 0, "wr": 0, "avg": 0, "pf": 0, "pnl": 0, "hold": 0,
            "daily_eff": 0, "sl_rate": 0, "trail_rate": 0, "signal_rate": 0, "expire_rate": 0,
        }
    n = len(trades)
    wins = trades[trades["ret_pct"] > 0]
    losses = trades[trades["ret_pct"] <= 0]
    ws = wins["ret_pct"].sum()
    ls = abs(losses["ret_pct"].sum())
    avg = trades["ret_pct"].mean()
    hold = trades["hold_days"].mean()
    daily_eff = avg / hold if hold > 0 else 0

    return {
        "n": n,
        "wr": round(wins.shape[0] / n * 100, 1),
        "avg": round(avg, 2),
        "pf": round(ws / ls, 2) if ls > 0 else 999,
        "pnl": round(trades["pnl"].sum() / 10000, 1),
        "hold": round(hold, 1),
        "daily_eff": round(daily_eff, 4),
        "sl_rate": round((trades["exit_type"] == "SL").mean() * 100, 1),
        "trail_rate": round((trades["exit_type"] == "trail").mean() * 100, 1),
        "signal_rate": round((trades["exit_type"] == "signal").mean() * 100, 1),
        "expire_rate": round((trades["exit_type"] == "expire").mean() * 100, 1),
    }


def generate_html(all_results: dict[str, dict]) -> str:
    """HTML生成"""

    def fmt(v, key: str) -> str:
        if key == "n":
            return f"{v:,d}"
        if key == "wr":
            return f"{v:.1f}%"
        if key == "avg":
            return f"{v:+.2f}%"
        if key == "pf":
            return f"{v:.2f}" if v < 900 else "∞"
        if key == "pnl":
            return f"{v:+,.1f}"
        if key == "hold":
            return f"{v:.1f}"
        if key == "daily_eff":
            return f"{v:+.4f}"
        if key in ("sl_rate", "trail_rate", "signal_rate", "expire_rate"):
            return f"{v:.1f}%"
        return str(v)

    def highlight_best_worst(values: list[float], key: str, higher_better: bool = True) -> list[str]:
        """ベスト=green/bold, ワースト=red"""
        real = [(i, v) for i, v in enumerate(values) if v != 0 or key == "pnl"]
        if not real:
            return [""] * len(values)
        classes = [""] * len(values)
        vals = [v for _, v in real]
        if len(set(vals)) <= 1:
            return classes
        if higher_better:
            best_v = max(vals)
            worst_v = min(vals)
        else:
            best_v = min(vals)
            worst_v = max(vals)
        for idx, v in real:
            if v == best_v:
                classes[idx] = "best"
            elif v == worst_v:
                classes[idx] = "worst"
        return classes

    pat_keys = list(PATTERNS.keys())

    # --- Section 1: Executive Summary ---
    def make_summary_table(period_label: str, period_start: str | None, regime_label: str, regime_filter: str | None) -> str:
        rows_data = []
        for pk in pat_keys:
            stats = all_results[pk]["stats"].get((period_label, regime_label), {})
            rows_data.append(stats)

        metrics = [
            ("件数", "n", True), ("勝率", "wr", True), ("平均%", "avg", True),
            ("PF", "pf", True), ("PnL(万)", "pnl", True), ("保有日数", "hold", False),
            ("日次効率", "daily_eff", True),
            ("SL率", "sl_rate", False), ("trail率", "trail_rate", False),
            ("signal率", "signal_rate", True), ("expire率", "expire_rate", False),
        ]

        html = '<table class="compact">\n<thead><tr><th>指標</th>'
        for pk in pat_keys:
            html += f'<th>{PATTERNS[pk]["label"]}</th>'
        html += '</tr></thead>\n<tbody>\n'

        for metric_label, metric_key, higher_better in metrics:
            vals = [r.get(metric_key, 0) for r in rows_data]
            classes = highlight_best_worst(vals, metric_key, higher_better)
            html += f'<tr><td class="metric">{metric_label}</td>'
            for i, pk in enumerate(pat_keys):
                v = rows_data[i].get(metric_key, 0)
                cls = f' class="{classes[i]}"' if classes[i] else ""
                html += f'<td{cls}>{fmt(v, metric_key)}</td>'
            html += '</tr>\n'

        html += '</tbody></table>\n'
        return html

    # --- Section 2: Period x Regime Matrix ---
    def make_matrix_table() -> str:
        html = ""
        for pk in pat_keys:
            html += f'<h3>{PATTERNS[pk]["label"]}</h3>\n'
            html += '<table class="compact">\n<thead><tr><th>期間 / レジーム</th>'
            for rl, _ in REGIMES:
                html += f'<th colspan="4">{rl}</th>'
            html += '</tr>\n<tr><th></th>'
            for _ in REGIMES:
                html += '<th>件数</th><th>勝率</th><th>PF</th><th>PnL(万)</th>'
            html += '</tr></thead>\n<tbody>\n'

            for pl, _ in PERIODS:
                html += f'<tr><td class="metric">{pl}</td>'
                for rl, _ in REGIMES:
                    s = all_results[pk]["stats"].get((pl, rl), {})
                    n = s.get("n", 0)
                    wr = s.get("wr", 0)
                    pf = s.get("pf", 0)
                    pnl = s.get("pnl", 0)
                    pnl_cls = "best" if pnl > 0 else "worst" if pnl < 0 else ""
                    pnl_attr = f' class="{pnl_cls}"' if pnl_cls else ""
                    html += f'<td>{n:,d}</td><td>{wr:.1f}%</td><td>{pf:.2f}</td><td{pnl_attr}>{pnl:+,.1f}</td>'
                html += '</tr>\n'
            html += '</tbody></table>\n'
        return html

    # --- Section 3: Year-by-year PnL ---
    def make_yearly_table() -> str:
        html = '<table class="compact">\n<thead><tr><th>年</th>'
        for pk in pat_keys:
            html += f'<th>{pk}</th>'
        html += '</tr></thead>\n<tbody>\n'

        for year in range(2015, 2027):
            html += f'<tr><td class="metric">{year}</td>'
            for pk in pat_keys:
                pnl = all_results[pk]["yearly_pnl"].get(year, 0)
                cls = "best" if pnl > 0 else "worst" if pnl < 0 else ""
                attr = f' class="{cls}"' if cls else ""
                html += f'<td{attr}>{pnl:+,.1f}</td>'
            html += '</tr>\n'

        # 合計
        html += '<tr class="total"><td class="metric">合計</td>'
        for pk in pat_keys:
            total = sum(all_results[pk]["yearly_pnl"].values())
            cls = "best" if total > 0 else "worst" if total < 0 else ""
            attr = f' class="{cls}"' if cls else ""
            html += f'<td{attr}>{total:+,.1f}</td>'
        html += '</tr>\n'

        html += '</tbody></table>\n'
        return html

    # --- Section 4: Exit breakdown ---
    def make_exit_breakdown() -> str:
        html = '<table class="compact">\n<thead><tr><th>パターン</th>'
        html += '<th>SL</th><th>trail</th><th>signal</th><th>expire</th></tr></thead>\n<tbody>\n'
        for pk in pat_keys:
            s = all_results[pk]["stats"].get(("全期間", "全体"), {})
            html += f'<tr><td class="metric">{PATTERNS[pk]["label"]}</td>'
            html += f'<td>{s.get("sl_rate", 0):.1f}%</td>'
            html += f'<td>{s.get("trail_rate", 0):.1f}%</td>'
            html += f'<td>{s.get("signal_rate", 0):.1f}%</td>'
            html += f'<td>{s.get("expire_rate", 0):.1f}%</td>'
            html += '</tr>\n'
        html += '</tbody></table>\n'
        return html

    # --- Section 5: Conclusion ---
    def make_conclusion() -> str:
        # データから結論を導出
        recent_stats = {}
        for pk in pat_keys:
            recent_stats[pk] = all_results[pk]["stats"].get(("直近2年", "全体"), {})

        all_stats = {}
        for pk in pat_keys:
            all_stats[pk] = all_results[pk]["stats"].get(("全期間", "全体"), {})

        # 直近2年PnL最大
        best_recent = max(pat_keys, key=lambda k: recent_stats[k].get("pnl", -9999))
        # 全期間PF最大
        best_pf = max(pat_keys, key=lambda k: all_stats[k].get("pf", 0))
        # 日次効率最大
        best_eff = max(pat_keys, key=lambda k: recent_stats[k].get("daily_eff", -9999))

        html = '<div class="conclusion">\n'
        html += '<ul>\n'
        html += f'<li><strong>直近2年PnL最大</strong>: {PATTERNS[best_recent]["label"]} '
        html += f'({recent_stats[best_recent].get("pnl", 0):+,.1f}万)</li>\n'
        html += f'<li><strong>全期間PF最大</strong>: {PATTERNS[best_pf]["label"]} '
        html += f'(PF {all_stats[best_pf].get("pf", 0):.2f})</li>\n'
        html += f'<li><strong>直近2年 日次効率最大</strong>: {PATTERNS[best_eff]["label"]} '
        html += f'({recent_stats[best_eff].get("daily_eff", 0):+.4f}%/日)</li>\n'

        # 各パターンの特徴
        html += '</ul>\n<h4>各パターンの特徴</h4>\n<ul>\n'
        html += '<li><strong>A</strong>: SLなしで大きな利益を取れるが、ドローダウンが大きい</li>\n'
        html += '<li><strong>B</strong>: trail50%で利益を確保するが、早期trail exitで取りこぼす場合も</li>\n'
        html += '<li><strong>C</strong>: SL-3%で損失を限定しつつ、signal exitで利益を最大化</li>\n'
        html += '<li><strong>D</strong>: 7日キャップで資金回転を高めるが、signal exitの機会を逃す</li>\n'
        html += '<li><strong>E</strong>: 10日キャップはDとCの中間</li>\n'
        html += '</ul>\n'

        html += '<h4>推奨</h4>\n<ul>\n'

        # PFとPnLで判定
        c_pf_all = all_stats.get("C", {}).get("pf", 0)
        c_pnl_recent = recent_stats.get("C", {}).get("pnl", 0)
        c_eff_recent = recent_stats.get("C", {}).get("daily_eff", 0)

        # 年別安定性チェック
        yearly = all_results.get("C", {}).get("yearly_pnl", {})
        win_years = sum(1 for v in yearly.values() if v > 0)
        total_years = len([y for y in yearly.keys() if y >= 2015])

        html += f'<li>C（SL-3%、trailなし）の全期間PF={c_pf_all:.2f}、'
        html += f'直近2年PnL={c_pnl_recent:+,.1f}万、日次効率={c_eff_recent:+.4f}%/日</li>\n'
        html += f'<li>2015-2026の年勝率: {win_years}/{total_years}年</li>\n'

        d_eff = recent_stats.get("D", {}).get("daily_eff", 0)
        e_eff = recent_stats.get("E", {}).get("daily_eff", 0)
        html += f'<li>D/Eの日次効率: D={d_eff:+.4f}%/日, E={e_eff:+.4f}%/日 → 資金回転重視なら検討</li>\n'

        html += '</ul>\n</div>\n'
        return html

    # --- Assemble HTML ---
    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<title>Granville LONG Exit方式 5パターン比較</title>
<style>
body {{
    background: #1a1a2e;
    color: #e0e0e0;
    font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
    margin: 20px 40px;
    line-height: 1.6;
}}
h1 {{
    color: #00d4ff;
    border-bottom: 2px solid #00d4ff;
    padding-bottom: 8px;
    font-size: 1.6em;
}}
h2 {{
    color: #ffcc00;
    margin-top: 40px;
    font-size: 1.3em;
}}
h3 {{
    color: #aaa;
    margin-top: 20px;
    font-size: 1.1em;
}}
h4 {{
    color: #ccc;
    margin-top: 16px;
}}
table.compact {{
    border-collapse: collapse;
    font-size: 13px;
    margin: 10px 0 20px 0;
    min-width: 600px;
}}
table.compact th {{
    background: #16213e;
    color: #aaa;
    padding: 6px 10px;
    border: 1px solid #333;
    text-align: center;
    font-weight: 600;
    white-space: nowrap;
}}
table.compact td {{
    padding: 5px 10px;
    border: 1px solid #333;
    text-align: right;
    white-space: nowrap;
}}
table.compact td.metric {{
    text-align: left;
    font-weight: 600;
    color: #ccc;
    background: #16213e;
}}
table.compact tr:hover {{
    background: rgba(255,255,255,0.05);
}}
table.compact tr.total {{
    border-top: 2px solid #555;
    font-weight: bold;
}}
.best {{
    color: #00ff88 !important;
    font-weight: bold;
}}
.worst {{
    color: #ff4444 !important;
}}
.section {{
    margin-bottom: 40px;
}}
.params {{
    background: #16213e;
    padding: 12px 20px;
    border-radius: 6px;
    margin: 10px 0 20px 0;
    font-size: 13px;
    color: #aaa;
}}
.conclusion {{
    background: #16213e;
    padding: 16px 24px;
    border-radius: 8px;
    border-left: 4px solid #00d4ff;
}}
.conclusion li {{
    margin: 6px 0;
}}
</style>
</head>
<body>
<h1>Granville LONG Exit方式 5パターン比較</h1>
<div class="params">
    シグナル: SMA20乖離率 &lt; {DEV_THRESHOLD}% &amp; 陽線（Close &gt; prev_close）<br>
    フィルター: BAD_SECTORS除外, 株価 &lt; &yen;{MAX_PRICE:,d}<br>
    エントリー: シグナル翌営業日Open / 同一銘柄重複除去
</div>

<div class="section">
<h2>1. エグゼクティブサマリー（直近2年 / 全体）</h2>
<h3>直近2年（2024-03以降）/ 全体</h3>
{make_summary_table("直近2年", "2024-03-01", "全体", None)}
<h3>全期間 / 全体</h3>
{make_summary_table("全期間", None, "全体", None)}
</div>

<div class="section">
<h2>2. 期間 x レジーム別マトリクス</h2>
{make_matrix_table()}
</div>

<div class="section">
<h2>3. 年別PnL推移（万円）2015-2026</h2>
{make_yearly_table()}
</div>

<div class="section">
<h2>4. Exit内訳（全期間 / 全体）</h2>
{make_exit_breakdown()}
</div>

<div class="section">
<h2>5. 結論</h2>
{make_conclusion()}
</div>

</body>
</html>
"""
    return html


def main():
    t0 = time.time()
    print("=" * 80)
    print("Granville LONG Exit方式 5パターン比較")
    print("=" * 80)

    ps = load_data()

    all_results: dict[str, dict] = {}

    for pk, cfg in PATTERNS.items():
        print(f"\n--- {cfg['label']} ---")
        trades = backtest_pattern(
            ps,
            sl_pct=cfg["sl_pct"],
            trail_pct=cfg["trail_pct"],
            max_hold=cfg["max_hold"],
        )
        print(f"  {len(trades):,d} trades")

        # 統計計算
        stats = {}
        for pl, pstart in PERIODS:
            t = trades[trades["signal_date"] >= pstart] if pstart else trades
            for rl, rfilter in REGIMES:
                sub = t[t["regime"] == rfilter] if rfilter else t
                stats[(pl, rl)] = calc_stats(sub)

        # 年別PnL（2015-2026）
        yearly_pnl = {}
        if not trades.empty:
            trades_tmp = trades.copy()
            trades_tmp["year"] = pd.to_datetime(trades_tmp["signal_date"]).dt.year
            for year in range(2015, 2027):
                yt = trades_tmp[trades_tmp["year"] == year]
                yearly_pnl[year] = round(yt["pnl"].sum() / 10000, 1) if not yt.empty else 0

        all_results[pk] = {
            "trades": trades,
            "stats": stats,
            "yearly_pnl": yearly_pnl,
        }

        # 簡易表示
        s = stats.get(("直近2年", "全体"), {})
        print(f"  直近2年: n={s.get('n', 0)}, wr={s.get('wr', 0):.1f}%, "
              f"PF={s.get('pf', 0):.2f}, PnL={s.get('pnl', 0):+,.1f}万, "
              f"hold={s.get('hold', 0):.1f}d, eff={s.get('daily_eff', 0):+.4f}")

    # HTML生成
    html = generate_html(all_results)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"\n[OK] HTML saved → {OUTPUT_PATH}")
    print(f"Total time: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
