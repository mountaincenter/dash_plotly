#!/usr/bin/env python3
"""
18_portfolio_simulation_topix.py
=================================
TOPIX 1,660銘柄版 資金制約付きポートフォリオシミュレーション

エントリー優先順位方法（ML/ルール優先/RSI低い順/証拠金安い順/ランダム）を
実際の資金制約下で日次シミュレーションし比較する。

パラメータ:
  - 初期資金: 465万円
  - 単元: 100株
  - 必要証拠金: upper_limit(entry_price) * 100
  - 最大ポジション数: 3, 5, 10, 50(実質資金上限のみ)
  - 出口: 現行シグナルexit（trades_cleanedの結果をそのまま使用）
"""
from __future__ import annotations

import warnings
import time
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[2]
SV_DIR = ROOT / "strategy_verification"
PRIORITY_DIR = SV_DIR / "chapters" / "09_entry_priority"
REPORT_DIR = SV_DIR / "chapters" / "10_portfolio_simulation"

INITIAL_CAPITAL = 4_650_000  # 465万円
POSITION_LIMITS = [10, 15, 16, 20]


def simulate_portfolio(
    trades_df: pd.DataFrame,
    rank_col: str,
    rank_ascending: bool,
    max_positions: int,
) -> dict:
    """
    日次ポートフォリオシミュレーション。

    毎日:
      1. exit_date <= 当日 のポジションを決済（証拠金+PnL回収）
      2. 当日のシグナルを rank_col でソート
      3. ポジション枠と資金が許す限りエントリー
    """
    df = trades_df.dropna(subset=[rank_col]).copy()
    df["entry_date"] = pd.to_datetime(df["entry_date"])
    df["exit_date"] = pd.to_datetime(df["exit_date"])

    # 事前にentry_dateごとにグループ化・ソートし、タプルリストに変換
    entry_groups: dict[pd.Timestamp, list[tuple]] = {}
    for entry_date, grp in df.groupby("entry_date"):
        sorted_grp = grp.sort_values(rank_col, ascending=rank_ascending)
        entry_groups[entry_date] = list(zip(
            sorted_grp["exit_date"],
            sorted_grp["margin"],
            sorted_grp["pnl"],
            sorted_grp["rule"],
            sorted_grp["win"],
        ))

    # イベント日 = entry日 ∪ exit日
    all_dates = sorted(set(df["entry_date"].unique()) | set(df["exit_date"].dropna().unique()))

    capital = float(INITIAL_CAPITAL)
    open_positions: list[tuple] = []  # (exit_date, margin, pnl)

    realized_pnl = 0.0
    n_trades = 0
    n_wins = 0
    n_skipped_pos = 0
    n_skipped_cap = 0
    peak = float(INITIAL_CAPITAL)
    max_dd = 0.0

    yearly_pnl: dict[int, float] = {}
    yearly_trades: dict[int, int] = {}
    yearly_wins: dict[int, int] = {}
    rule_counts: dict[str, int] = {"B1": 0, "B2": 0, "B3": 0, "B4": 0}
    margin_sum = 0.0

    for date in all_dates:
        # 1. 決済
        still_open = []
        for exit_dt, margin, pnl in open_positions:
            if exit_dt <= date:
                capital += margin + pnl
                realized_pnl += pnl
                n_trades += 1
                yr = exit_dt.year
                yearly_pnl[yr] = yearly_pnl.get(yr, 0) + pnl
                yearly_trades[yr] = yearly_trades.get(yr, 0) + 1
                if pnl > 0:
                    n_wins += 1
                    yearly_wins[yr] = yearly_wins.get(yr, 0) + 1
            else:
                still_open.append((exit_dt, margin, pnl))
        open_positions = still_open

        # 2. 新規エントリー
        if date in entry_groups:
            for exit_dt, margin, pnl, rule, win in entry_groups[date]:
                if len(open_positions) >= max_positions:
                    n_skipped_pos += 1
                    continue
                if capital >= margin:
                    capital -= margin
                    open_positions.append((exit_dt, margin, pnl))
                    rule_counts[rule] = rule_counts.get(rule, 0) + 1
                    margin_sum += margin
                else:
                    n_skipped_cap += 1

        # 3. Drawdown (実現損益ベース)
        equity = INITIAL_CAPITAL + realized_pnl
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    # 残ポジション決済
    for exit_dt, margin, pnl in open_positions:
        capital += margin + pnl
        realized_pnl += pnl
        n_trades += 1
        yr = exit_dt.year
        yearly_pnl[yr] = yearly_pnl.get(yr, 0) + pnl
        yearly_trades[yr] = yearly_trades.get(yr, 0) + 1
        if pnl > 0:
            n_wins += 1
            yearly_wins[yr] = yearly_wins.get(yr, 0) + 1

    total_taken = sum(rule_counts.values())

    return {
        "total_pnl": realized_pnl,
        "n_trades": n_trades,
        "win_rate": n_wins / n_trades if n_trades > 0 else 0,
        "avg_pnl": realized_pnl / n_trades if n_trades > 0 else 0,
        "avg_margin": margin_sum / total_taken if total_taken > 0 else 0,
        "max_dd_pct": max_dd,
        "final_equity": INITIAL_CAPITAL + realized_pnl,
        "return_pct": realized_pnl / INITIAL_CAPITAL * 100,
        "n_skipped_pos": n_skipped_pos,
        "n_skipped_cap": n_skipped_cap,
        "yearly_pnl": yearly_pnl,
        "yearly_trades": yearly_trades,
        "yearly_wins": yearly_wins,
        "rule_counts": rule_counts,
    }


def main():
    t0 = time.time()

    # ========== 1. データ読込 ==========
    print("[1/3] Loading data...")
    features = pd.read_parquet(PRIORITY_DIR / "trades_with_features_topix.parquet")
    ml_preds = pd.read_parquet(PRIORITY_DIR / "ml_predictions_topix.parquet")

    # ML scoreをマージ
    features["entry_date_str"] = features["entry_date"].astype(str)
    ml_sub = ml_preds[["ticker", "entry_date_str", "rule", "ml_score"]].copy()
    df = features.merge(ml_sub, on=["ticker", "entry_date_str", "rule"], how="inner")

    # exit_dateが欠損のレコードを除外
    df = df.dropna(subset=["exit_date"]).copy()

    # ルール優先ランク
    rule_rank = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}
    df["rank_rule"] = df["rule"].map(rule_rank)

    # ランダムランク
    rng = np.random.RandomState(42)
    df["random_rank"] = rng.permutation(len(df))

    print(f"  Trades: {len(df):,}")
    print(f"  Period: {df['entry_date'].min()} ~ {df['entry_date'].max()}")
    print(f"  Avg margin: ¥{df['margin'].mean():,.0f}")

    # ========== 2. シミュレーション ==========
    print(f"\n[2/3] Running simulations (5 methods × {len(POSITION_LIMITS)} limits)...")
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    methods = [
        ("ML予測", "ml_score", False),
        ("RSI低い順", "rsi14", True),
        ("証拠金安い順", "margin", True),
        ("ルール優先", "rank_rule", True),
        ("ランダム", "random_rank", True),
    ]

    results = []

    for max_pos in POSITION_LIMITS:
        print(f"\n  === Max {max_pos} positions ===")
        for method_name, rank_col, rank_asc in methods:
            t1 = time.time()
            r = simulate_portfolio(df, rank_col, rank_asc, max_pos)
            r["method"] = method_name
            r["max_positions"] = max_pos
            results.append(r)
            elapsed = time.time() - t1
            print(
                f"    {method_name:12s}: PnL={r['total_pnl']:>+12,.0f}  "
                f"trades={r['n_trades']:>5,}  WR={r['win_rate']:.1%}  "
                f"avgPnL={r['avg_pnl']:>+8,.0f}  DD={r['max_dd_pct']:.1f}%  "
                f"skip(pos={r['n_skipped_pos']:,}/cap={r['n_skipped_cap']:,})  "
                f"({elapsed:.1f}s)"
            )

    # ========== 3. レポート ==========
    print(f"\n[3/3] Results")

    # --- 3a. サマリーテーブル ---
    print(f"\n{'方法':>12} {'MaxPos':>6} | {'累計PnL':>14} {'取引数':>7} {'勝率':>6} "
          f"{'平均PnL':>10} {'平均証拠金':>10} {'最大DD':>7} {'リターン':>9}")
    print("=" * 100)

    for r in results:
        print(
            f"{r['method']:>12} {r['max_positions']:>6} | "
            f"{r['total_pnl']:>+14,.0f} {r['n_trades']:>7,} {r['win_rate']:>5.1%} "
            f"{r['avg_pnl']:>+10,.0f} {r['avg_margin']:>10,.0f} "
            f"{r['max_dd_pct']:>6.1f}% {r['return_pct']:>+8.1f}%"
        )

    # --- 3b. ルール分布 (Max5) ---
    print(f"\n=== ルール分布 (Max 5 positions) ===")
    max5 = [r for r in results if r["max_positions"] == 5]
    print(f"{'方法':>12} | {'B1':>8} {'B2':>8} {'B3':>8} {'B4':>8} | {'合計':>8}")
    print("-" * 65)
    for r in max5:
        rc = r["rule_counts"]
        total = sum(rc.values())
        print(
            f"{r['method']:>12} | "
            f"{rc.get('B1',0):>8,} {rc.get('B2',0):>8,} "
            f"{rc.get('B3',0):>8,} {rc.get('B4',0):>8,} | {total:>8,}"
        )

    # --- 3c. 年別PnL比較 (Max5) ---
    print(f"\n=== 年別PnL比較 (Max 5 positions) ===")
    years = sorted(set().union(*[r["yearly_pnl"].keys() for r in max5]))

    header = f"{'年':>6}"
    for r in max5:
        header += f" | {r['method']:>12}"
    print(header)
    print("-" * (8 + 15 * len(max5)))

    method_wins = {r["method"]: 0 for r in max5}
    for year in years:
        line = f"{year:>6}"
        year_pnls = {}
        for r in max5:
            pnl = r["yearly_pnl"].get(year, 0)
            year_pnls[r["method"]] = pnl
            line += f" | {pnl:>+12,.0f}"
        print(line)
        # 年別勝者
        best_method = max(year_pnls, key=year_pnls.get)
        method_wins[best_method] = method_wins.get(best_method, 0) + 1

    print(f"\n年別勝利回数:")
    for method, wins in sorted(method_wins.items(), key=lambda x: -x[1]):
        print(f"  {method}: {wins}年")

    # --- 3d. 年平均リターン (Max5) ---
    print(f"\n=== 年平均リターン (Max 5 positions) ===")
    for r in max5:
        n_years = len(r["yearly_pnl"])
        avg_annual = r["total_pnl"] / n_years if n_years > 0 else 0
        avg_annual_pct = avg_annual / INITIAL_CAPITAL * 100
        print(f"  {r['method']:>12}: 年平均 {avg_annual:>+10,.0f}円 ({avg_annual_pct:>+5.1f}%/年)")

    # --- 3e. Max positions 感度分析 ---
    print(f"\n=== ポジション上限 感度分析 (累計PnL) ===")
    header = f"{'方法':>12}"
    for mp in POSITION_LIMITS:
        header += f" | {'Max'+str(mp):>12}"
    print(header)
    print("-" * (14 + 15 * len(POSITION_LIMITS)))

    method_names = [m[0] for m in methods]
    for mn in method_names:
        line = f"{mn:>12}"
        for mp in POSITION_LIMITS:
            r = [x for x in results if x["method"] == mn and x["max_positions"] == mp][0]
            line += f" | {r['total_pnl']:>+12,.0f}"
        print(line)

    # --- 3f. 結果保存 ---
    summary_rows = []
    for r in results:
        summary_rows.append({
            "method": r["method"],
            "max_positions": r["max_positions"],
            "total_pnl": r["total_pnl"],
            "n_trades": r["n_trades"],
            "win_rate": r["win_rate"],
            "avg_pnl": r["avg_pnl"],
            "avg_margin": r["avg_margin"],
            "max_dd_pct": r["max_dd_pct"],
            "return_pct": r["return_pct"],
            "n_skipped_pos": r["n_skipped_pos"],
            "n_skipped_cap": r["n_skipped_cap"],
        })
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(REPORT_DIR / "simulation_summary_topix.csv", index=False)
    print(f"\n保存: {REPORT_DIR / 'simulation_summary_topix.csv'}")
    print(f"Done in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
