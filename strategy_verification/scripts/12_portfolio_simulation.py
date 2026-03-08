#!/usr/bin/env python3
"""
12_portfolio_simulation.py
===========================
Chapter 6: 資金制約付きポートフォリオシミュレーション

Ch5-2で各出口手法の「単体性能」が確定した。
本章では信用余力465万の制約下で、時系列ポートフォリオシミュレーションを実行し、
「実運用で最も資産が増える出口戦略」を特定する。

シミュレーション仕様:
  - 初期信用余力: 465万円
  - 1銘柄100株単位、注文時余力=ストップ高価格×100、約定後建玉額=Open×100
  - シグナル到着順に余力があれば建てる
  - 出口戦略に従いexitしたら建玉額+損益が余力に戻る
  - 同日複数シグナル → 優先順位ルールで選別
  - 28年間(1999-2026)の最終資産を比較

入力:
  - strategy_verification/data/processed/trades_cleaned.parquet
  - strategy_verification/data/processed/prices_cleaned.parquet

出力:
  - strategy_verification/chapters/06_portfolio_simulation/report.html
"""
from __future__ import annotations

import json
import time
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

ROOT = Path(__file__).resolve().parents[2]
SV_DIR = ROOT / "strategy_verification"
PROCESSED = SV_DIR / "data" / "processed"
REPORT_DIR = SV_DIR / "chapters" / "06_portfolio_simulation"

RULES = ["B1", "B2", "B3", "B4"]
MAX_HOLD = 60
PROPOSED_SLS: dict[str, float] = {"B1": 3.0, "B2": 3.0, "B3": 2.5, "B4": 999.0}

INITIAL_CAPITAL = 4_650_000  # 465万円

# 東証 制限値幅テーブル: (基準価格上限, 制限値幅)
_LIMIT_TABLE = [
    (100, 30), (200, 50), (500, 80), (700, 100),
    (1000, 150), (1500, 300), (2000, 400), (3000, 500),
    (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000),
    (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000),
    (100000, 15000), (150000, 30000), (200000, 40000), (300000, 50000),
    (500000, 70000), (700000, 100000), (1000000, 150000),
]


def _get_upper_limit_price(prev_close: float) -> float:
    """前日終値からストップ高価格を返す。"""
    for threshold, limit in _LIMIT_TABLE:
        if prev_close < threshold:
            return prev_close + limit
    return prev_close + 150000


# ============================================================
# 11_exit_methods_survey.py からインポート
# ============================================================

def _import_ch5_2():
    """Ch5-2のエンジンを動的インポート。"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ch5_2", SV_DIR / "scripts" / "11_exit_methods_survey.py")
    mod = importlib.util.module_from_spec(spec)
    # __file__を設定してROOT等のパス解決を正しくする
    mod.__file__ = str(SV_DIR / "scripts" / "11_exit_methods_survey.py")
    spec.loader.exec_module(mod)
    return mod


# ============================================================
# Portfolio Simulator
# ============================================================

class Position:
    """保有ポジション。"""
    __slots__ = ("ticker", "rule", "entry_date", "entry_idx", "entry_price",
                 "cost", "sl_price", "method", "param", "day_count",
                 "max_high", "partial_exited", "trailing_active", "trail_high",
                 "prev_macd_hist")

    def __init__(self, ticker: str, rule: str, entry_date, entry_idx: int,
                 entry_price: float, method: str, param: float, sl_pct: float):
        self.ticker = ticker
        self.rule = rule
        self.entry_date = entry_date
        self.entry_idx = entry_idx
        self.entry_price = entry_price
        self.cost = entry_price * 100  # 100株
        self.sl_price = entry_price * (1 - sl_pct / 100) if sl_pct < 900 else 0.0
        self.method = method
        self.param = param
        self.day_count = 0
        self.max_high = entry_price
        self.partial_exited = False
        self.trailing_active = False
        self.trail_high = entry_price
        self.prev_macd_hist = None


def simulate_portfolio(
    signals: pd.DataFrame,
    price_lookup: dict,
    method: str,
    param: float,
    initial_capital: int = INITIAL_CAPITAL,
    priority: str = "rule_b4_first",
    max_positions: int = 999,
) -> dict:
    """
    時系列ポートフォリオシミュレーション。

    signals: entry_date, ticker, rule, entry_price を含むDF（日付ソート済み）
    priority: 同日複数シグナル時の優先度
      "rule_b4_first" — B4>B1>B3>B2（PF順）
      "cheapest"      — 建玉額が小さい順（枠を多く使える）

    Returns: 統計量辞書
    """
    signals = signals.sort_values("entry_date").reset_index(drop=True)

    capital = float(initial_capital)
    positions: list[Position] = []
    total_pnl = 0.0
    total_trades = 0
    total_wins = 0
    missed_signals = 0
    trade_log: list[dict] = []  # 年別集計用

    # 全取引日のユニークリスト
    all_dates = sorted(signals["entry_date"].unique())

    # 日付 → シグナルリスト
    date_signals = signals.groupby("entry_date")

    rule_priority = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}

    for current_date in all_dates:
        current_date_np = np.datetime64(current_date)

        # 1. 既存ポジションの日次更新 & exit判定
        closed_positions = []
        surviving = []

        for pos in positions:
            if pos.ticker not in price_lookup:
                surviving.append(pos)
                continue

            pl = price_lookup[pos.ticker]
            ci = pos.entry_idx + pos.day_count
            if ci >= len(pl["dates"]):
                # データ終端 → 強制exit
                last_ci = len(pl["dates"]) - 1
                exit_price = pl["closes"][last_ci]
                ret_pct = (exit_price / pos.entry_price - 1) * 100
                pnl = pos.cost * ret_pct / 100
                closed_positions.append((pos, exit_price, pnl, ret_pct, "data_end"))
                continue

            # 現在の日付がcurrent_dateと一致するか確認
            c_high = pl["highs"][ci]
            c_low = pl["lows"][ci]
            c_close = pl["closes"][ci]
            c_open = pl["opens"][ci]

            d = pos.day_count
            exited = False
            exit_price = None
            exit_reason = ""

            # SLチェック
            if pos.sl_price > 0 and c_low <= pos.sl_price:
                exit_price = pos.sl_price
                exit_reason = "SL"
                exited = True

            if not exited and d > 0:
                pos.max_high = max(pos.max_high, c_high)

                # 各exitメソッド
                if method == "fixed_N":
                    n = int(param)
                    if d >= n:
                        exit_price = c_open if d == n else c_close
                        exit_reason = f"fixed_{n}d"
                        exited = True

                elif method == "n_day_high":
                    n = int(param)
                    key = f"donchian_high_{n}" if n in (20, 60) else None
                    if key and key in pl:
                        hi = pl[key][ci]
                    else:
                        start = max(0, ci - n + 1)
                        hi = pl["highs"][start:ci + 1].max()
                    if not np.isnan(hi) and c_high >= hi:
                        exit_price = hi
                        exit_reason = f"high_{n}d"
                        exited = True

                elif method == "atr_spike":
                    atr_val = pl["atr14"][ci]
                    atr_ma = pl["atr14_ma20"][ci]
                    if (not np.isnan(atr_val) and not np.isnan(atr_ma)
                            and atr_ma > 0 and atr_val > atr_ma * param):
                        if ci + 1 < len(pl["dates"]):
                            exit_price = pl["opens"][ci + 1]
                        else:
                            exit_price = c_close
                        exit_reason = f"atr_spike_{param}x"
                        exited = True

                elif method == "time_stop":
                    n = int(param)
                    if d >= n and c_close < pos.entry_price:
                        if ci + 1 < len(pl["dates"]):
                            exit_price = pl["opens"][ci + 1]
                        else:
                            exit_price = c_close
                        exit_reason = f"time_stop_{n}d"
                        exited = True

                elif method == "stoch_cross":
                    k_val = pl["stoch_k"][ci]
                    d_val = pl["stoch_d"][ci]
                    if (not np.isnan(k_val) and not np.isnan(d_val)
                            and k_val > 80 and k_val < d_val):
                        if ci + 1 < len(pl["dates"]):
                            exit_price = pl["opens"][ci + 1]
                        else:
                            exit_price = c_close
                        exit_reason = "stoch_cross"
                        exited = True

                elif method == "donchian":
                    n = int(param)
                    key = f"donchian_low_{n}"
                    if key in pl:
                        don_low = pl[key][ci]
                    else:
                        start = max(0, ci - n + 1)
                        don_low = pl["lows"][start:ci + 1].min()
                    if not np.isnan(don_low) and c_close < don_low:
                        if ci + 1 < len(pl["dates"]):
                            exit_price = pl["opens"][ci + 1]
                        else:
                            exit_price = c_close
                        exit_reason = f"donchian_{n}d"
                        exited = True

                elif method == "trail_pct":
                    trail_trigger = pos.max_high * (1 - param / 100)
                    if c_close <= trail_trigger:
                        if ci + 1 < len(pl["dates"]):
                            exit_price = pl["opens"][ci + 1]
                        else:
                            exit_price = c_close
                        exit_reason = f"trail_{param}pct"
                        exited = True

                elif method == "ma_envelope":
                    sma_val = pl["sma20"][ci]
                    if not np.isnan(sma_val):
                        envelope_upper = sma_val * (1 + param / 100)
                        if c_high >= envelope_upper:
                            exit_price = envelope_upper
                            exit_reason = f"envelope_{param}pct"
                            exited = True

                elif method == "partial_profit":
                    target_10 = pos.entry_price * 1.10
                    if not pos.partial_exited and c_high >= target_10:
                        pos.partial_exited = True
                    if pos.partial_exited:
                        trail_trigger = pos.max_high * (1 - param / 100)
                        if c_close <= trail_trigger:
                            if ci + 1 < len(pl["dates"]):
                                trail_exit = pl["opens"][ci + 1]
                            else:
                                trail_exit = c_close
                            exit_price = (target_10 + trail_exit) / 2
                            exit_reason = f"partial_{param}pct"
                            exited = True

                # MAX_HOLD超過
                if not exited and d >= MAX_HOLD:
                    exit_price = c_close
                    exit_reason = "expire"
                    exited = True

            if d == 0:
                pos.max_high = max(pos.max_high, c_high)
                # MACD hist初期化
                if method == "macd_hist_rev" and "macd_hist" in pl:
                    h = pl["macd_hist"][ci]
                    if not np.isnan(h):
                        pos.prev_macd_hist = h

            if exited and exit_price is not None:
                ret_pct = (exit_price / pos.entry_price - 1) * 100
                pnl = pos.cost * ret_pct / 100
                closed_positions.append((pos, exit_price, pnl, ret_pct, exit_reason))
            else:
                pos.day_count += 1
                surviving.append(pos)

        # 資金回収
        for pos, _, pnl, ret_pct, reason in closed_positions:
            capital += pos.cost + pnl
            total_pnl += pnl
            total_trades += 1
            if ret_pct > 0:
                total_wins += 1
            trade_log.append({
                "year": pd.Timestamp(pos.entry_date).year,
                "pnl": pnl,
                "ret_pct": ret_pct,
                "hold_days": pos.day_count,
                "rule": pos.rule,
                "reason": reason,
            })

        positions = surviving

        # 2. 新規シグナルの処理
        if current_date in date_signals.groups:
            day_signals = date_signals.get_group(current_date).copy()

            # 優先順位ソート
            if priority == "rule_b4_first":
                day_signals["_priority"] = day_signals["rule"].map(rule_priority).fillna(99)
                day_signals = day_signals.sort_values("_priority")
            elif priority == "cheapest":
                day_signals["_cost"] = day_signals["entry_price"] * 100
                day_signals = day_signals.sort_values("_cost")

            for _, sig in day_signals.iterrows():
                ticker = sig["ticker"]
                if ticker not in price_lookup:
                    continue

                pl = price_lookup[ticker]
                entry_mask = pl["dates"] == np.datetime64(current_date)
                if not entry_mask.any():
                    missed_signals += 1
                    continue
                entry_idx = int(np.where(entry_mask)[0][0])

                # 注文時必要余力 = ストップ高価格 × 100株
                prev_close = pl["prev_closes"][entry_idx]
                upper_limit = _get_upper_limit_price(prev_close)
                required_margin = upper_limit * 100

                if required_margin > capital or required_margin <= 0:
                    missed_signals += 1
                    continue

                if len(positions) >= max_positions:
                    missed_signals += 1
                    continue

                # 同一銘柄の重複ポジションを避ける
                if any(p.ticker == ticker for p in positions):
                    missed_signals += 1
                    continue

                # 約定: 実際のコスト = Open × 100
                actual_cost = float(sig["entry_price"]) * 100
                sl = PROPOSED_SLS.get(sig["rule"], 999.0)
                pos = Position(
                    ticker=ticker, rule=sig["rule"],
                    entry_date=current_date, entry_idx=entry_idx,
                    entry_price=float(sig["entry_price"]),
                    method=method, param=param, sl_pct=sl,
                )
                # 注文時に上限額を拘束、約定後に差額を返却
                capital -= required_margin
                capital += (required_margin - actual_cost)  # = capital -= actual_cost
                positions.append(pos)

    # 残存ポジションの強制決済
    for pos in positions:
        if pos.ticker not in price_lookup:
            capital += pos.cost
            continue
        pl = price_lookup[pos.ticker]
        last_ci = min(pos.entry_idx + pos.day_count, len(pl["dates"]) - 1)
        exit_price = pl["closes"][last_ci]
        ret_pct = (exit_price / pos.entry_price - 1) * 100
        pnl = pos.cost * ret_pct / 100
        capital += pos.cost + pnl
        total_pnl += pnl
        total_trades += 1
        if ret_pct > 0:
            total_wins += 1
        trade_log.append({
            "year": pd.Timestamp(pos.entry_date).year,
            "pnl": pnl,
            "ret_pct": ret_pct,
            "hold_days": pos.day_count,
            "rule": pos.rule,
            "reason": "final_close",
        })

    # 年別集計
    log_df = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()
    yearly = {}
    if len(log_df) > 0:
        for year, grp in log_df.groupby("year"):
            rets = grp["ret_pct"].values
            pnls_y = grp["pnl"].values
            wins = rets > 0
            gw = rets[wins].sum()
            gl = abs(rets[~wins].sum())
            yearly[int(year)] = {
                "n": len(grp),
                "wr": round(wins.mean() * 100, 1),
                "pf": round(gw / gl if gl > 0 else 999, 2),
                "pnl": round(pnls_y.sum(), 0),
                "pnl_m": round(pnls_y.sum() / 10000, 1),
            }

    # ルール別集計
    rule_stats = {}
    if len(log_df) > 0:
        for rule in RULES:
            rgrp = log_df[log_df["rule"] == rule]
            if len(rgrp) == 0:
                rule_stats[rule] = {"n": 0, "pnl_m": 0, "wr": 0, "pf": 0}
                continue
            rets = rgrp["ret_pct"].values
            pnls_r = rgrp["pnl"].values
            wins = rets > 0
            gw = rets[wins].sum()
            gl = abs(rets[~wins].sum())
            rule_stats[rule] = {
                "n": len(rgrp),
                "pnl_m": round(pnls_r.sum() / 10000, 1),
                "wr": round(wins.mean() * 100, 1),
                "pf": round(gw / gl if gl > 0 else 999, 2),
            }

    final_capital = capital
    total_signals = total_trades + missed_signals
    exec_rate = total_trades / total_signals * 100 if total_signals > 0 else 0

    return {
        "final_capital": round(final_capital, 0),
        "final_capital_m": round(final_capital / 10000, 1),
        "total_pnl": round(total_pnl, 0),
        "total_pnl_m": round(total_pnl / 10000, 1),
        "total_return_pct": round((final_capital / initial_capital - 1) * 100, 1),
        "total_trades": total_trades,
        "missed_signals": missed_signals,
        "exec_rate": round(exec_rate, 1),
        "win_rate": round(total_wins / total_trades * 100, 1) if total_trades > 0 else 0,
        "yearly": yearly,
        "rule_stats": rule_stats,
    }


# ============================================================
# HTML Helpers
# ============================================================

def _stat_card(label: str, value: str, sub: str = "", tone: str = "") -> str:
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    cls = {"pos": "card-pos", "neg": "card-neg", "warn": "card-warn"}.get(tone, "")
    return (f'<div class="stat-card {cls}"><div class="label">{label}</div>'
            f'<div class="value">{value}</div>{sub_html}</div>')


def _table_html(headers: list[str], rows: list[list], highlight_col: int | None = None) -> str:
    ths = "".join(f"<th>{h}</th>" for h in headers)
    best_idx = -1
    if highlight_col is not None:
        vals = []
        for r in rows:
            try:
                raw = (str(r[highlight_col]).replace("万", "").replace(",", "")
                       .replace("+", "").replace("<b>", "").replace("</b>", "")
                       .replace("%", ""))
                vals.append(float(raw))
            except (ValueError, IndexError):
                vals.append(-9999)
        if vals:
            best_idx = vals.index(max(vals))
    trs = []
    for i, row in enumerate(rows):
        cls = ' class="best-row"' if i == best_idx else ""
        tds = "".join(f"<td>{c}</td>" for c in row)
        trs.append(f"<tr{cls}>{tds}</tr>")
    return f'<table><thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table>'


def _insight_box(text: str) -> str:
    return f'<div class="insight-box">{text}</div>'


def _section(title: str, content: str) -> str:
    return f'<section><h2>{title}</h2>{content}</section>'


def _plotly_line(div_id: str, traces: list[dict], title: str = "",
                 yaxis_title: str = "", height: int = 350) -> str:
    data = json.dumps(traces)
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
        "margin": {"t": 40, "b": 50, "l": 70, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 13}},
        "yaxis": {"title": yaxis_title},
        "legend": {"orientation": "h", "y": -0.15},
    })
    return (f'<div id="{div_id}" style="height:{height}px"></div>\n'
            f'<script>Plotly.newPlot("{div_id}",{data},{layout},{{responsive:true}})</script>')


def _plotly_bar(div_id: str, labels: list[str], values: list[float],
                title: str = "", yaxis_title: str = "", height: int = 300) -> str:
    colors = ["rgba(52,211,153,0.7)" if v > 0 else "rgba(248,113,113,0.7)" for v in values]
    traces = json.dumps([{"x": labels, "y": values, "type": "bar",
                          "marker": {"color": colors}}])
    layout = json.dumps({
        "template": "plotly_dark",
        "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
        "margin": {"t": 40, "b": 50, "l": 60, "r": 20},
        "font": {"size": 11, "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 13}},
        "yaxis": {"title": yaxis_title},
    })
    return (f'<div id="{div_id}" style="height:{height}px"></div>\n'
            f'<script>Plotly.newPlot("{div_id}",{traces},{layout},{{responsive:true}})</script>')


# ============================================================
# Main
# ============================================================

def main() -> None:
    t0 = time.time()
    print("=" * 60)
    print("Ch6: Portfolio Simulation — 資金制約付きポートフォリオシミュレーション")
    print(f"  初期信用余力: {INITIAL_CAPITAL:,}円 ({INITIAL_CAPITAL/10000:.0f}万)")
    print("=" * 60)

    # ---- Load data ----
    print("\n[1/4] Loading data...")
    trades = pd.read_parquet(PROCESSED / "trades_cleaned.parquet")
    prices = pd.read_parquet(PROCESSED / "prices_cleaned.parquet")
    long = trades[trades["direction"] == "LONG"].copy()
    print(f"  LONG trades: {len(long):,}")

    # ---- Build price lookup ----
    print("[2/4] Building price lookup with indicators...")
    t1 = time.time()
    ch5_2 = _import_ch5_2()
    price_lookup = ch5_2.build_price_lookup_v2(prices)
    # 前日終値を追加（制限値幅計算用）
    for ticker in price_lookup:
        closes = price_lookup[ticker]["closes"]
        prev_closes = np.empty_like(closes)
        prev_closes[0] = closes[0]
        prev_closes[1:] = closes[:-1]
        price_lookup[ticker]["prev_closes"] = prev_closes
    print(f"  Tickers: {len(price_lookup)} ({time.time()-t1:.1f}s)")

    # ---- Define strategies to compare ----
    strategies = [
        ("固定60日", "fixed_N", 60),
        ("固定45日", "fixed_N", 45),
        ("固定30日", "fixed_N", 30),
        ("固定20日", "fixed_N", 20),
        ("固定10日", "fixed_N", 10),
        ("20日高値", "n_day_high", 20),
        ("60日高値", "n_day_high", 60),
        ("ATR急拡大2x", "atr_spike", 2),
        ("TimeStop30d", "time_stop", 30),
        ("Stoch%K>80", "stoch_cross", 0),
        ("Donchian20d", "donchian", 20),
        ("Trail20%", "trail_pct", 20),
        ("Envelope15%", "ma_envelope", 15),
        ("半分+10%+Trail10%", "partial_profit", 10),
    ]

    # ---- Run simulations ----
    print(f"[3/4] Running {len(strategies)} portfolio simulations...")
    results: dict[str, dict] = {}
    for i, (name, method, param) in enumerate(strategies):
        t_s = time.time()
        r = simulate_portfolio(long, price_lookup, method, param,
                               initial_capital=INITIAL_CAPITAL,
                               priority="rule_b4_first")
        results[name] = r
        print(f"  [{i+1}/{len(strategies)}] {name:20s} "
              f"最終={r['final_capital_m']:+,.0f}万 "
              f"PnL={r['total_pnl_m']:+,.0f}万 "
              f"執行率={r['exec_rate']}% "
              f"({time.time()-t_s:.1f}s)")

    # ---- Build report ----
    print("[4/4] Building HTML report...")
    sections_html = []

    # === Section 1: Overview ===
    s1 = ""
    s1 += _insight_box(
        f"<b>シミュレーション条件</b><br>"
        f"・初期信用余力: {INITIAL_CAPITAL:,}円（{INITIAL_CAPITAL/10000:.0f}万）<br>"
        f"・全ポジション清算ベース（投信除外）<br>"
        f"・1銘柄100株単位、信用買い<br>"
        f"・注文時必要余力 = ストップ高価格(前日終値+制限値幅)×100株<br>"
        f"・約定後の実コスト = 始値×100株（差額は余力に返却）<br>"
        f"・同日複数シグナル → B4>B1>B3>B2の優先順で余力の範囲内で建てる<br>"
        f"・同一銘柄の重複保有なし<br>"
        f"・SL: B1/B2=-3%, B3=-2.5%, B4=なし<br>"
        f"・MAX_HOLD=60日"
    )

    # メイン比較テーブル
    comp_rows = []
    for name, _, _ in strategies:
        r = results[name]
        comp_rows.append([
            name,
            f'{r["final_capital_m"]:+,.0f}万',
            f'{r["total_pnl_m"]:+,.0f}万',
            f'{r["total_return_pct"]:+,.1f}%',
            f'{r["total_trades"]:,}',
            f'{r["missed_signals"]:,}',
            f'{r["exec_rate"]}%',
            f'{r["win_rate"]}%',
        ])
    s1 += _table_html(
        ["出口戦略", "最終資産", "総PnL", "総リターン", "執行数", "見送り数", "執行率", "WR"],
        comp_rows, highlight_col=1,
    )

    # 最終資産バーチャート
    names = [name for name, _, _ in strategies]
    finals = [results[name]["total_pnl_m"] for name in names]
    s1 += _plotly_bar("bar_final", names, finals,
                      title="出口戦略別 総PnL（資金制約付き）", yaxis_title="PnL(万)")

    # 執行率バーチャート
    exec_rates = [results[name]["exec_rate"] for name in names]
    s1 += _plotly_bar("bar_exec", names, exec_rates,
                      title="出口戦略別 シグナル執行率(%)", yaxis_title="執行率(%)",
                      height=250)

    sections_html.append(_section("1. 出口戦略比較（資金制約付き）", s1))

    # === Section 2: 年別推移 ===
    s2 = ""
    # 累積PnLの年別推移ライン
    traces = []
    colors = [
        "#60a5fa", "#34d399", "#f87171", "#fbbf24", "#a78bfa",
        "#fb923c", "#22d3ee", "#f472b6", "#84cc16", "#e879f9",
        "#38bdf8", "#4ade80", "#ef4444", "#eab308",
    ]
    for idx, (name, _, _) in enumerate(strategies):
        r = results[name]
        if not r["yearly"]:
            continue
        years = sorted(r["yearly"].keys())
        cum_pnl = []
        running = 0
        for y in years:
            running += r["yearly"][y]["pnl_m"]
            cum_pnl.append(round(running, 1))
        traces.append({
            "x": [str(y) for y in years],
            "y": cum_pnl,
            "type": "scatter",
            "mode": "lines+markers",
            "name": name,
            "line": {"color": colors[idx % len(colors)], "width": 2},
            "marker": {"size": 4},
        })
    s2 += _plotly_line("line_cumulative", traces,
                       title="累積PnL推移（資金制約付き）",
                       yaxis_title="累積PnL(万)", height=450)

    # Top3の年別テーブル
    top3 = sorted(strategies, key=lambda s: results[s[0]]["total_pnl_m"], reverse=True)[:3]
    for name, _, _ in top3:
        r = results[name]
        s2 += f"<h3>{name}</h3>"
        if not r["yearly"]:
            s2 += "<p>データなし</p>"
            continue
        years = sorted(r["yearly"].keys())
        y_rows = []
        win_years = 0
        for y in years:
            ys = r["yearly"][y]
            if ys["pnl_m"] > 0:
                win_years += 1
            y_rows.append([
                str(y), f'{ys["n"]:,}', f'{ys["wr"]}%',
                f'{ys["pf"]:.2f}', f'{ys["pnl_m"]:+,.0f}万',
            ])
        s2 += f"<p>年勝率: {win_years}/{len(years)}</p>"
        s2 += _table_html(["年", "執行数", "WR", "PF", "PnL"], y_rows, highlight_col=4)

    sections_html.append(_section("2. 年別推移", s2))

    # === Section 3: ルール別貢献 ===
    s3 = ""
    for name, _, _ in top3:
        r = results[name]
        rs = r["rule_stats"]
        s3 += f"<h3>{name}</h3>"
        rule_rows = []
        for rule in RULES:
            if rule in rs:
                rule_rows.append([
                    rule,
                    f'{rs[rule]["n"]:,}',
                    f'{rs[rule]["wr"]}%',
                    f'{rs[rule]["pf"]:.2f}',
                    f'{rs[rule]["pnl_m"]:+,.0f}万',
                ])
        s3 += _table_html(["Rule", "執行数", "WR", "PF", "PnL"], rule_rows, highlight_col=4)

    sections_html.append(_section("3. ルール別貢献度（Top3戦略）", s3))

    # === Section 4: 資金効率分析 ===
    s4 = ""
    eff_rows = []
    for name, _, _ in strategies:
        r = results[name]
        years_count = len(r["yearly"]) if r["yearly"] else 1
        annual_pnl = r["total_pnl_m"] / years_count
        annual_return = r["total_return_pct"] / years_count
        capital_turnover = r["total_trades"] / years_count
        eff_rows.append([
            name,
            f'{r["total_pnl_m"]:+,.0f}万',
            f'{annual_pnl:+,.1f}万',
            f'{annual_return:+,.1f}%',
            f'{capital_turnover:.0f}回',
            f'{r["exec_rate"]}%',
        ])
    s4 += _table_html(
        ["戦略", "総PnL", "年平均PnL", "年平均Return", "年平均回転数", "執行率"],
        eff_rows, highlight_col=1,
    )

    # Ch5-2単体 vs Ch6ポートフォリオの比較
    s4 += "<h3>Ch5-2（資金無制限）vs Ch6（465万制約）</h3>"
    ch5_2_pnls = {
        "固定60日": 25830, "20日高値": 17320, "60日高値": 19137,
        "ATR急拡大2x": 25986, "TimeStop30d": 23600,
    }
    diff_rows = []
    for name in ch5_2_pnls:
        if name in results:
            ch5_val = ch5_2_pnls[name]
            ch6_val = results[name]["total_pnl_m"]
            ratio = ch6_val / ch5_val * 100 if ch5_val > 0 else 0
            diff_rows.append([
                name,
                f'{ch5_val:+,.0f}万',
                f'{ch6_val:+,.0f}万',
                f'{ch6_val - ch5_val:+,.0f}万',
                f'{ratio:.0f}%',
            ])
    s4 += _table_html(
        ["戦略", "Ch5-2(無制限)", "Ch6(465万)", "差分", "実現率"],
        diff_rows, highlight_col=4,
    )
    s4 += _insight_box(
        "実現率 = Ch6のPnL / Ch5-2のPnL × 100。"
        "資金制約により理論値の何%が実際に獲得できるかを示す。"
        "短期exit戦略ほど実現率が高い傾向がある（資金回転が速いため）。"
    )
    sections_html.append(_section("4. 資金効率分析", s4))

    # === Section 5: 結論 ===
    s5 = ""
    # ランキング
    ranked = sorted(strategies, key=lambda s: results[s[0]]["total_pnl_m"], reverse=True)
    rank_rows = []
    for i, (name, method, param) in enumerate(ranked):
        r = results[name]
        rank_rows.append([
            f"#{i+1}", name,
            f'{r["final_capital_m"]:+,.0f}万',
            f'{r["total_pnl_m"]:+,.0f}万',
            f'{r["total_return_pct"]:+,.1f}%',
            f'{r["exec_rate"]}%',
        ])
    s5 += _table_html(
        ["Rank", "戦略", "最終資産", "総PnL", "総Return", "執行率"],
        rank_rows, highlight_col=3,
    )

    # Top1のサマリーカード
    top1_name = ranked[0][0]
    top1 = results[top1_name]
    cards = [
        _stat_card("最適戦略", top1_name, "", "pos"),
        _stat_card("最終資産", f'{top1["final_capital_m"]:+,.0f}万',
                   f'初期465万 → {top1["total_return_pct"]:+,.1f}%', "pos"),
        _stat_card("総PnL", f'{top1["total_pnl_m"]:+,.0f}万',
                   f'{top1["total_trades"]:,}回執行', "pos"),
        _stat_card("執行率", f'{top1["exec_rate"]}%',
                   f'見送り{top1["missed_signals"]:,}回', "warn"),
    ]
    s5 += f'<div class="card-grid">{"".join(cards)}</div>'

    s5 += _insight_box(
        "<b>結論:</b><br>"
        "・資金無制限(Ch5-2)と資金制約付き(Ch6)では最適戦略が異なりうる<br>"
        "・短期exit(高回転)は1トレードPnLで劣るが、資金回転で巻き返す<br>"
        "・長期exit(固定60日等)はPnL最大だが、シグナル見送りが大量発生<br>"
        "・実運用では「執行率×1トレードPnL」の積が最終的な資産を決める"
    )
    sections_html.append(_section("5. 結論", s5))

    # ========== Assemble HTML ==========
    body = "\n".join(sections_html)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    elapsed = time.time() - t0

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ch6 Portfolio Simulation — Granville Strategy Verification</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
:root {{
  --bg: #0f1117; --card: #1a1d27; --border: #2a2d3a;
  --text: #e2e8f0; --muted: #8892a8; --primary: #60a5fa;
  --pos: #34d399; --neg: #f87171; --warn: #fbbf24;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: var(--bg); color: var(--text);
  line-height: 1.6; padding: 20px; max-width: 1400px; margin: 0 auto;
}}
h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
h2 {{ font-size: 1.1rem; color: var(--primary); margin: 24px 0 12px; border-bottom: 1px solid var(--border); padding-bottom: 6px; }}
h3 {{ font-size: 0.95rem; color: var(--muted); margin: 16px 0 8px; }}
p {{ margin: 6px 0; font-size: 0.82rem; }}
section {{ margin-bottom: 24px; }}
.meta {{ font-size: 0.75rem; color: var(--muted); margin-bottom: 16px; }}
.card-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 12px 0; }}
.stat-card {{
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 12px; text-align: center;
}}
.stat-card .label {{ font-size: 0.75rem; color: var(--muted); }}
.stat-card .value {{ font-size: 1.3rem; font-weight: 700; margin: 4px 0; }}
.stat-card .sub {{ font-size: 0.7rem; color: var(--muted); }}
.card-pos .value {{ color: var(--pos); }}
.card-neg .value {{ color: var(--neg); }}
.card-warn .value {{ color: var(--warn); }}
table {{
  width: 100%; border-collapse: collapse; font-size: 0.8rem;
  margin: 10px 0; background: var(--card);
}}
th, td {{ padding: 6px 10px; border: 1px solid var(--border); text-align: right; }}
th {{ background: #1e2130; color: var(--primary); font-weight: 600; text-align: center; }}
td:first-child {{ text-align: left; font-weight: 500; }}
.best-row {{ background: rgba(96, 165, 250, 0.12); }}
.insight-box {{
  background: rgba(96, 165, 250, 0.08); border-left: 3px solid var(--primary);
  padding: 10px 14px; margin: 12px 0; font-size: 0.82rem;
  border-radius: 0 6px 6px 0; line-height: 1.7;
}}
@media (max-width: 768px) {{
  .card-grid {{ grid-template-columns: repeat(2, 1fr); }}
  table {{ font-size: 0.7rem; }}
  th, td {{ padding: 4px 6px; }}
}}
</style>
</head>
<body>
<h1>Chapter 6: Portfolio Simulation — 資金制約付きポートフォリオシミュレーション</h1>
<div class="meta">Generated: {now} | 初期余力: {INITIAL_CAPITAL/10000:.0f}万円 | {len(strategies)}戦略比較 | {len(long):,} signals | Runtime: {elapsed:.0f}s</div>
{body}
</body>
</html>"""

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / "report.html"
    out.write_text(html, encoding="utf-8")
    print(f"\n{'='*60}")
    print(f"[OK] Report saved: {out}")
    print(f"  Size: {out.stat().st_size / 1024:.0f} KB")
    print(f"  Runtime: {elapsed:.0f}s ({elapsed/60:.1f}min)")


if __name__ == "__main__":
    main()
