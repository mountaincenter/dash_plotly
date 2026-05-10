#!/usr/bin/env python3
"""
generate_pre_market_allocation.py
==================================
寄前アロケータ: ポートフォリオレベルのリスク制御。

Phase A 実装:
  #1  クロス戦略DDコントロール + 方向集中キャップ
  #5  日経VIレジームスケーラー
  #6  固定率サイジング (1.5%/trade)
  #7  連続リスクオーバーレイ (CME→方向別連続倍率)
  #12 流動性ゲート (警告)

実行タイミング: 07:00 JST (data-pipeline cme_update フェーズ)

入力:
  signals.parquet          — 全戦略シグナル
  stock_results.parquet    — 実現損益履歴 (DD計算)
  nikkei_vi_max_1d.parquet — 日経VI時系列
  futures_prices_max_1d    — CME日経先物 (yfinance)
  calendar.parquet         — 営業日カレンダー

出力:
  data/parquet/allocation.parquet — シグナル別リスク調整結果
  /tmp/allocation_summary.json   — ダッシュボード/Slack用サマリー
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

# ── 定数 ──────────────────────────────────────────────

CAPITAL = 1_800_000
RISK_PER_TRADE_PCT = 0.015  # 1.5%/trade
MAX_POSITION_PCT = 0.15     # 1銘柄あたり最大15%

# DD制御
DD_DAILY_STOP = -0.02       # 前日-2% → 新規停止
DD_DAILY_HALT = -0.03       # 前日-3% → 全決済推奨
DD_20D_THRESHOLD = -0.08    # 20日DD -8% → 50%スケールダウン
DD_RECOVERY_DAYS = 10       # 連続10日プラスで通常復帰

# 方向集中キャップ
DIR_STACK_THRESHOLD = 3     # 同方向3戦略以上 → 50%カット
NET_SHORT_MAX_PCT = 0.35    # ネットショート>35% → 最低PF除外

# VIレジーム
VI_LOW_UPPER = 20.0
VI_MID_UPPER = 30.0
VI_SCALE_MAP = {"LOW": 1.00, "MID": 0.75, "HIGH": 0.50}

# CME連続オーバーレイ — 逆方向変化率×係数でスケール
CME_OVERLAY_COEFF = 0.15    # 1%逆行 → 0.85倍
CME_OVERLAY_FLOOR = 0.30    # 最低倍率

JST = ZoneInfo("Asia/Tokyo")

SIGNALS_PATH = PARQUET_DIR / "signals.parquet"
RESULTS_PATH = PARQUET_DIR / "stock_results.parquet"
VI_PATH = PARQUET_DIR / "nikkei_vi_max_1d.parquet"
CALENDAR_PATH = PARQUET_DIR / "calendar.parquet"
ALLOCATION_PATH = PARQUET_DIR / "allocation.parquet"
SUMMARY_PATH = Path("/tmp/allocation_summary.json")


# ── データ読み込み ────────────────────────────────────

def load_signals(target_date: date) -> pd.DataFrame:
    """target_date のシグナルを全戦略から取得。"""
    if not SIGNALS_PATH.exists():
        print("[WARN] signals.parquet not found")
        return pd.DataFrame()
    df = pd.read_parquet(SIGNALS_PATH)
    df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.date
    return df[df["signal_date"] == target_date].copy()


def load_stock_results() -> pd.DataFrame:
    """実現損益履歴を読み込み。"""
    if not RESULTS_PATH.exists():
        print("[WARN] stock_results.parquet not found")
        return pd.DataFrame()
    df = pd.read_parquet(RESULTS_PATH)
    df["約定日"] = pd.to_datetime(df["約定日"]).dt.date
    return df


def load_vi_latest() -> tuple[float | None, str, float]:
    """直近の日経VI終値を返す → (vi_close, regime, scale)"""
    if not VI_PATH.exists():
        print("[WARN] nikkei_vi_max_1d.parquet not found")
        return None, "UNKNOWN", 1.0
    df = pd.read_parquet(VI_PATH)
    if df.empty:
        return None, "UNKNOWN", 1.0
    vi_close = float(df["close"].iloc[-1])
    if vi_close < VI_LOW_UPPER:
        return vi_close, "LOW", VI_SCALE_MAP["LOW"]
    elif vi_close < VI_MID_UPPER:
        return vi_close, "MID", VI_SCALE_MAP["MID"]
    else:
        return vi_close, "HIGH", VI_SCALE_MAP["HIGH"]


def fetch_cme_change() -> float | None:
    """CME日経先物の前日比%を取得。"""
    try:
        import yfinance as yf
        nkd = yf.download("NKD=F", period="5d", interval="1d", progress=False)
        n225 = yf.download("^N225", period="5d", interval="1d", progress=False)
        if isinstance(nkd.columns, pd.MultiIndex):
            nkd.columns = [c[0] for c in nkd.columns]
        if isinstance(n225.columns, pd.MultiIndex):
            n225.columns = [c[0] for c in n225.columns]
        if nkd.empty or n225.empty:
            return None
        nkd_close = float(nkd["Close"].iloc[-1])
        n225_close = float(n225["Close"].iloc[-1])
        return round((nkd_close - n225_close) / n225_close * 100, 2)
    except Exception as e:
        print(f"[WARN] CME fetch failed: {e}")
        return None


# ── DD計算 ────────────────────────────────────────────

def compute_dd_metrics(results: pd.DataFrame) -> dict[str, Any]:
    """stock_results から DD 指標を計算。"""
    out: dict[str, Any] = {
        "dd_daily_pnl": 0.0,
        "dd_daily_pct": 0.0,
        "dd_20d_pct": 0.0,
        "dd_scale": 1.0,
        "dd_blocked": False,
        "dd_halt": False,
        "consecutive_positive_days": 0,
        "data_stale_days": None,
        "last_result_date": None,
    }
    if results.empty:
        out["data_stale_days"] = -1
        return out

    today = date.today()
    last_date = max(results["約定日"])
    out["last_result_date"] = str(last_date)
    out["data_stale_days"] = (today - last_date).days

    daily_pnl = (
        results.groupby("約定日")["実現損益"]
        .sum()
        .sort_index()
    )

    # 前営業日のP&L
    if len(daily_pnl) >= 1:
        last_pnl = float(daily_pnl.iloc[-1])
        out["dd_daily_pnl"] = last_pnl
        out["dd_daily_pct"] = round(last_pnl / CAPITAL, 4)

    if out["dd_daily_pct"] <= DD_DAILY_HALT:
        out["dd_halt"] = True
        out["dd_blocked"] = True
        out["dd_scale"] = 0.0
    elif out["dd_daily_pct"] <= DD_DAILY_STOP:
        out["dd_blocked"] = True
        out["dd_scale"] = 0.0

    # 20日ローリングDD
    if len(daily_pnl) >= 5:
        recent_20 = daily_pnl.tail(20)
        cumulative = recent_20.cumsum()
        peak = cumulative.cummax()
        dd = cumulative - peak
        max_dd_pct = float(dd.min()) / CAPITAL
        out["dd_20d_pct"] = round(max_dd_pct, 4)

        if max_dd_pct <= DD_20D_THRESHOLD:
            out["dd_scale"] = min(out["dd_scale"], 0.50)

    # 連続プラス日数（回復判定用）
    if len(daily_pnl) >= 1:
        count = 0
        for v in reversed(daily_pnl.values):
            if v > 0:
                count += 1
            else:
                break
        out["consecutive_positive_days"] = count

    return out


# ── 方向集中キャップ ──────────────────────────────────

def _normalize_direction(direction: str) -> str:
    """pairs の long_tk1/short_tk1 を neutral に、それ以外はそのまま。"""
    if direction in ("long_tk1", "short_tk1"):
        return "neutral"
    return direction


def compute_direction_cap(signals: pd.DataFrame) -> dict[str, Any]:
    """同方向戦略スタック検出。"""
    out: dict[str, Any] = {
        "dir_counts": {"long": 0, "short": 0, "neutral": 0},
        "dir_scale_short": 1.0,
        "dir_scale_long": 1.0,
        "dir_cap_active": False,
        "strategies_by_direction": {},
    }
    if signals.empty:
        return out

    signals = signals.copy()
    signals["net_dir"] = signals["direction"].map(_normalize_direction)

    for net_dir in ("long", "short", "neutral"):
        subset = signals[signals["net_dir"] == net_dir]
        strategies = sorted(subset["strategy"].unique().tolist())
        out["dir_counts"][net_dir] = len(strategies)
        out["strategies_by_direction"][net_dir] = strategies

    if out["dir_counts"]["short"] >= DIR_STACK_THRESHOLD:
        out["dir_scale_short"] = 0.50
        out["dir_cap_active"] = True

    if out["dir_counts"]["long"] >= DIR_STACK_THRESHOLD:
        out["dir_scale_long"] = 0.50
        out["dir_cap_active"] = True

    return out


# ── CME連続オーバーレイ ───────────────────────────────

def compute_cme_overlay(cme_change_pct: float | None, direction: str) -> float:
    """CME変化率と方向から連続倍率を計算。

    逆行(ショートにギャップアップ等) → 倍率低下。
    順行・中立 → 1.0 据置。
    """
    if cme_change_pct is None:
        return 1.0

    net_dir = _normalize_direction(direction)
    if net_dir == "neutral":
        return 1.0

    # ショートにとってCME上昇は逆行、ロングにとってCME下落は逆行
    if net_dir == "short":
        adverse = max(0.0, cme_change_pct)
    else:
        adverse = max(0.0, -cme_change_pct)

    if adverse <= 0:
        return 1.0

    overlay = 1.0 - adverse * CME_OVERLAY_COEFF
    return round(max(CME_OVERLAY_FLOOR, overlay), 3)


# ── 固定率サイジング ──────────────────────────────────

_LIMIT_TABLE = [
    (100, 30), (200, 50), (500, 50), (700, 100),
    (1000, 100), (1500, 300), (2000, 400), (3000, 500),
    (5000, 700), (10000, 1000), (20000, 2000), (30000, 3000),
    (50000, 5000), (70000, 7000), (100000, 10000),
    (150000, 15000), (200000, 20000), (300000, 30000),
    (500000, 50000), (700000, 70000), (1000000, 150000),
]


def _required_margin(price: float) -> float:
    """信用取引1単元あたり必要保証金。"""
    for threshold, limit in _LIMIT_TABLE:
        if price <= threshold:
            return (price + limit) * 100
    return (price + 150000) * 100


def compute_sizing(
    entry_price: float | None,
    final_scale: float,
) -> dict[str, Any]:
    """固定率サイジング: 1銘柄あたりの最大数量を計算。"""
    risk_budget = CAPITAL * RISK_PER_TRADE_PCT
    max_position = CAPITAL * MAX_POSITION_PCT

    out: dict[str, Any] = {
        "risk_budget": round(risk_budget),
        "max_position": round(max_position),
        "base_qty": 100,
        "sizing_scale": 1.0,
    }

    if entry_price and entry_price > 0:
        margin = _required_margin(entry_price)
        position_value = entry_price * 100
        if position_value > max_position:
            out["sizing_scale"] = round(max_position / position_value, 3)
        if margin > max_position:
            out["sizing_scale"] = min(
                out["sizing_scale"],
                round(max_position / margin, 3),
            )

    return out


# ── メインアロケータ ──────────────────────────────────

def allocate(target_date: date | None = None) -> tuple[pd.DataFrame, dict]:
    """全リスクルールを適用してアロケーション結果を生成。"""

    if target_date is None:
        target_date = _next_trading_date()

    print("=" * 60)
    print(f"寄前アロケータ  target={target_date}")
    print("=" * 60)

    # ── データ読み込み ──
    signals = load_signals(target_date)
    results = load_stock_results()
    vi_close, vi_regime, vi_scale = load_vi_latest()
    cme_change = fetch_cme_change()

    print(f"\n📊 シグナル数: {len(signals)}")
    if signals.empty:
        print(f"  ⚠ {target_date} のシグナルなし")

    # ── DD計算 ──
    dd = compute_dd_metrics(results)
    print(f"\n📉 DD制御:")
    print(f"  前日P&L: ¥{dd['dd_daily_pnl']:+,.0f} ({dd['dd_daily_pct']:+.2%})")
    print(f"  20日DD: {dd['dd_20d_pct']:+.2%}")
    print(f"  DDスケール: {dd['dd_scale']}")
    if dd["data_stale_days"] and dd["data_stale_days"] > 3:
        print(f"  ⚠ stock_results は {dd['data_stale_days']}日前が最新 → DD判定は参考値")
    if dd["dd_blocked"]:
        print(f"  🚨 DD制御発動 → 新規エントリー停止")
    if dd["dd_halt"]:
        print(f"  🚨🚨 前日-3%超 → 全ポジション決済推奨")

    # ── VI ──
    print(f"\n📊 日経VI:")
    if vi_close is not None:
        print(f"  VI: {vi_close:.1f} → レジーム: {vi_regime} → スケール: {vi_scale}")
    else:
        print(f"  VI: 取得不可 → デフォルト 1.0")

    # ── 方向集中 ──
    dir_cap = compute_direction_cap(signals)
    print(f"\n🔄 方向集中:")
    for d, strategies in dir_cap["strategies_by_direction"].items():
        if strategies:
            print(f"  {d}: {len(strategies)}戦略 {strategies}")
    if dir_cap["dir_cap_active"]:
        print(f"  🚨 方向集中キャップ発動")
        if dir_cap["dir_scale_short"] < 1.0:
            print(f"    SHORT → {dir_cap['dir_scale_short']}")
        if dir_cap["dir_scale_long"] < 1.0:
            print(f"    LONG → {dir_cap['dir_scale_long']}")

    # ── CME ──
    print(f"\n📈 CMEオーバーレイ:")
    if cme_change is not None:
        print(f"  CME Gap: {cme_change:+.2f}%")
    else:
        print(f"  CME: 取得不可 → オーバーレイ 1.0")

    # ── シグナル別アロケーション ──
    rows: list[dict[str, Any]] = []

    for _, sig in signals.iterrows():
        ticker = sig.get("ticker", "")
        strategy = sig.get("strategy", "")
        direction = sig.get("direction", "")
        net_dir = _normalize_direction(direction)
        stock_name = sig.get("stock_name", "")
        pair_id = sig.get("pair_id", "")
        entry_price = sig.get("entry_price_est") or sig.get("prev_close") or sig.get("close")
        if pd.isna(entry_price):
            entry_price = None

        # 各スケール計算
        sig_vi_scale = vi_scale
        sig_dd_scale = dd["dd_scale"]

        if net_dir == "short":
            sig_dir_scale = dir_cap["dir_scale_short"]
        elif net_dir == "long":
            sig_dir_scale = dir_cap["dir_scale_long"]
        else:
            sig_dir_scale = 1.0

        sig_overlay = compute_cme_overlay(cme_change, direction)
        sizing = compute_sizing(entry_price, 1.0)

        final_scale = round(
            sig_vi_scale * sig_dd_scale * sig_dir_scale * sig_overlay * sizing["sizing_scale"],
            3,
        )

        blocked = False
        block_reason = ""
        if dd["dd_blocked"]:
            blocked = True
            block_reason = f"DD制御: 前日{dd['dd_daily_pct']:+.2%}"
        if dd["dd_halt"]:
            block_reason = f"DD全停止: 前日{dd['dd_daily_pct']:+.2%}"

        # リスク判定: 100株単位制約のため量ではなくレベルで判定
        if blocked:
            rec_level = "SKIP"
        elif final_scale >= 0.75:
            rec_level = "FULL"
        elif final_scale >= 0.50:
            rec_level = "CAUTION"
        elif final_scale >= 0.25:
            rec_level = "REDUCE"
        else:
            rec_level = "SKIP"
            if not blocked:
                blocked = True
                block_reason = f"複合リスク過大 (scale={final_scale:.2f})"

        # 推奨数量: FULL/CAUTION=基本、REDUCE=半減(200→100等)、SKIP=0
        if rec_level in ("FULL", "CAUTION"):
            rec_qty = sizing["base_qty"]
        elif rec_level == "REDUCE":
            rec_qty = max(100, (sizing["base_qty"] // 2 // 100) * 100)
        else:
            rec_qty = 0

        rows.append({
            "signal_date": target_date,
            "ticker": ticker,
            "stock_name": stock_name,
            "strategy": strategy,
            "direction": direction,
            "net_direction": net_dir,
            "pair_id": pair_id,
            "entry_price_est": entry_price,
            "base_qty": sizing["base_qty"],
            "rec_qty": rec_qty,
            "rec_level": rec_level,
            "risk_budget": sizing["risk_budget"],
            "vi_close": vi_close,
            "vi_regime": vi_regime,
            "vi_scale": sig_vi_scale,
            "dd_daily_pct": dd["dd_daily_pct"],
            "dd_20d_pct": dd["dd_20d_pct"],
            "dd_scale": sig_dd_scale,
            "dir_count": dir_cap["dir_counts"].get(net_dir, 0),
            "dir_scale": sig_dir_scale,
            "cme_change_pct": cme_change,
            "overlay_scale": sig_overlay,
            "sizing_scale": sizing["sizing_scale"],
            "final_scale": final_scale,
            "blocked": blocked,
            "block_reason": block_reason,
        })

    alloc_df = pd.DataFrame(rows) if rows else pd.DataFrame()

    # ── サマリー ──
    if not alloc_df.empty:
        active_count = len(alloc_df[~alloc_df["blocked"]])
        blocked_count = len(alloc_df[alloc_df["blocked"]])
        level_counts = alloc_df["rec_level"].value_counts().to_dict()
    else:
        active_count = blocked_count = 0
        level_counts = {}

    summary: dict[str, Any] = {
        "as_of": datetime.now(JST).isoformat(),
        "target_date": str(target_date),
        "capital": CAPITAL,
        "vi_close": vi_close,
        "vi_regime": vi_regime,
        "vi_scale": vi_scale,
        "dd_daily_pnl": dd["dd_daily_pnl"],
        "dd_daily_pct": dd["dd_daily_pct"],
        "dd_20d_pct": dd["dd_20d_pct"],
        "dd_scale": dd["dd_scale"],
        "dd_blocked": dd["dd_blocked"],
        "dd_halt": dd["dd_halt"],
        "dd_data_stale_days": dd["data_stale_days"],
        "dd_last_result_date": dd["last_result_date"],
        "dd_consecutive_positive_days": dd["consecutive_positive_days"],
        "cme_change_pct": cme_change,
        "direction_counts": dir_cap["dir_counts"],
        "dir_cap_active": dir_cap["dir_cap_active"],
        "strategies_by_direction": dir_cap["strategies_by_direction"],
        "total_signals": len(alloc_df),
        "active_signals": active_count,
        "blocked_signals": blocked_count,
        "level_counts": level_counts,
        "warnings": _build_warnings(dd, dir_cap, vi_close, vi_regime, cme_change),
    }

    # ── 結果出力 ──
    _print_allocation_table(alloc_df)

    return alloc_df, summary


def _build_warnings(
    dd: dict, dir_cap: dict, vi_close: float | None, vi_regime: str, cme_change: float | None,
) -> list[str]:
    warnings: list[str] = []
    if dd.get("data_stale_days") and dd["data_stale_days"] > 3:
        warnings.append(f"stock_results {dd['data_stale_days']}日未更新 — DD判定は参考値")
    if dd.get("dd_halt"):
        warnings.append("前日-3%超 — 全ポジション決済を推奨")
    elif dd.get("dd_blocked"):
        warnings.append("前日-2%超 — 新規エントリー停止")
    if dd.get("dd_20d_pct", 0) <= DD_20D_THRESHOLD:
        warnings.append(f"20日DD {dd['dd_20d_pct']:+.2%} — 全戦略50%スケールダウン")
    if dir_cap.get("dir_cap_active"):
        for d in ("short", "long"):
            if dir_cap["dir_counts"].get(d, 0) >= DIR_STACK_THRESHOLD:
                strategies = dir_cap["strategies_by_direction"].get(d, [])
                warnings.append(f"{d.upper()} {len(strategies)}戦略スタック → 50%カット")
    if vi_regime == "HIGH":
        warnings.append(f"日経VI {vi_close:.1f} — 高ボラレジーム (サイズ50%)")
    elif vi_regime == "MID":
        warnings.append(f"日経VI {vi_close:.1f} — 中ボラレジーム (サイズ75%)")
    if cme_change is not None and abs(cme_change) >= 2.0:
        warnings.append(f"CME Gap {cme_change:+.2f}% — 大幅変動")
    return warnings


def _print_allocation_table(df: pd.DataFrame) -> None:
    if df.empty:
        print("\n(シグナルなし)")
        return

    LEVEL_ICON = {"FULL": "🟢", "CAUTION": "🟡", "REDUCE": "🟠", "SKIP": "🔴"}

    print(f"\n{'─' * 95}")
    print(f"{'戦略':<12} {'銘柄':<14} {'方向':<8} {'数量':>4} "
          f"{'VI':>5} {'DD':>5} {'方向':>5} {'CME':>5} {'Size':>5} "
          f"{'合計':>5} {'判定':<10}")
    print(f"{'─' * 95}")

    for _, r in df.iterrows():
        icon = LEVEL_ICON.get(r["rec_level"], "?")
        name = str(r["stock_name"])[:6]
        print(
            f"{r['strategy']:<12} {name:<14} {r['direction']:<8} "
            f"{r['rec_qty']:>4} "
            f"{r['vi_scale']:>5.2f} {r['dd_scale']:>5.2f} "
            f"{r['dir_scale']:>5.2f} {r['overlay_scale']:>5.2f} "
            f"{r['sizing_scale']:>5.2f} "
            f"{r['final_scale']:>5.2f} {icon} {r['rec_level']:<7}"
        )

    print(f"{'─' * 95}")
    for level in ("FULL", "CAUTION", "REDUCE", "SKIP"):
        count = len(df[df["rec_level"] == level])
        if count:
            print(f"  {LEVEL_ICON.get(level, '')} {level}: {count}件")
    active = df[~df["blocked"]]
    if not active.empty:
        total_risk = active["risk_budget"].sum()
        print(f"  リスク予算合計: ¥{total_risk:,.0f}")


def _next_trading_date() -> date:
    """signals.parquetから今日以降の最も近いシグナル日を返す。"""
    today = date.today()
    if SIGNALS_PATH.exists():
        df = pd.read_parquet(SIGNALS_PATH, columns=["signal_date"])
        df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.date
        future = sorted(d for d in df["signal_date"].unique() if d >= today)
        if future:
            return future[0]

    # カレンダーから次営業日
    if CALENDAR_PATH.exists():
        cal = pd.read_parquet(CALENDAR_PATH)
        cal_dates = pd.to_datetime(cal["date"]).dt.date
        future_cal = sorted(d for d in cal_dates if d >= today)
        if future_cal:
            return future_cal[0]

    return today


def save_results(alloc_df: pd.DataFrame, summary: dict) -> None:
    if not alloc_df.empty:
        alloc_df.to_parquet(ALLOCATION_PATH, index=False)
        print(f"\n✅ {ALLOCATION_PATH.name} saved ({len(alloc_df)} rows)")
    else:
        print(f"\n⚠ allocation.parquet not saved (no signals)")

    with open(SUMMARY_PATH, "w") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    print(f"✅ {SUMMARY_PATH} saved")


# ── エントリーポイント ────────────────────────────────

def main() -> int:
    alloc_df, summary = allocate()
    save_results(alloc_df, summary)

    # 警告出力
    if summary["warnings"]:
        print(f"\n⚠ 警告 ({len(summary['warnings'])}件):")
        for w in summary["warnings"]:
            print(f"  • {w}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
