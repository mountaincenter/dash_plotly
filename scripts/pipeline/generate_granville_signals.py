#!/usr/bin/env python3
"""
generate_granville_signals.py
グランビル B1-B4 シグナル生成（TOPIX 約1,660銘柄）

IMPLEMENTATION.md §3-§5 準拠:
  B1: prev_below AND above AND sma_up
  B2: sma_up AND dev ∈ [-5%, 0%] AND up_day AND below
  B3: sma_up AND above AND dev ∈ [0%, +3%] AND (prev_dev > dev) AND up_day
  B4: (dev < -8%) AND up_day

  up_day = Close > prev_close（前日比陽線。Close > Open ではない）
  優先順位: B4 > B1 > B3 > B2
  同一ルール内: 到着順（RSI sortは有害、禁止）

Exit（§6）:
  20日高値: High[t] >= max(High[t-19:t+1])
  MAX_HOLD: ルール別（B4=3, B1=30, B3=30, B2=30）2026-04-17再検証結果
  SLなし

出力 (2026-04-17 統合):
  data/parquet/signals.parquet    -- 全戦略シグナル統合 (granville + bearish + pairs)
  data/parquet/positions.parquet  -- granville + bearish のみ (pairs は 1日完結運用で追跡対象外)
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.nikkei_vi import fetch_nikkei_vi
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

load_dotenv_cascade()

GRANVILLE_DIR = PARQUET_DIR / "granville"
PRICES_PATH = GRANVILLE_DIR / "prices_topix.parquet"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
META_FALLBACK = PARQUET_DIR / "meta.parquet"
CSV_DIR = ROOT / "data" / "csv"
CREDIT_CSV = CSV_DIR / "credit_capacity.csv"

# 2026-04-17 統合: signals.parquet / positions.parquet (top-level, 全戦略統合)
SIGNALS_PATH = PARQUET_DIR / "signals.parquet"
POSITIONS_PATH = PARQUET_DIR / "positions.parquet"

RULE_PRIORITY = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}
# priority_rank は表示用 (1=最優先)。RULE_PRIORITY とは +1 オフセット
RULE_PRIORITY_RANK = {"B4": 1, "B1": 2, "B3": 3, "B2": 4}
# 2026-04-17 再検証 (Opus 4.7 + Codex, 27年フル期間, N=649K):
# - B1/B2/B3: 全期間 capital_eff で MH30 が最適 (旧 MH15 比 +30-50%)
# - B4: 逆張り性質で MH3 が最適 (PF 2.44-4.81 vs MH15 の PF 1.3-1.6)
RULE_MAX_HOLD = {"B4": 3, "B1": 30, "B3": 30, "B2": 30}
HIGH20D_PRE_ENTRY = 4  # 20日高値判定にエントリー前4日を含める（k=4最適、Codex検証済み）

# B1-B3ロングフィルター定義 (2026-04-17 再検証, 期間別 capital_eff 最適):
# H1: VI≥30 + B1 → hold 3d (全期間 PF 1.26, 5Y PF 1.85)
# H2: N225>SMA20 + CME flat(-0.5~0.5%) + B3 → hold 9d (全期間 PF 1.21, 3Y PF 1.72, 現行維持)
# H3: N225 ret20<-5% + B1 → hold 8d (10Y PF 1.86, 5Y PF 1.87)
INDEX_PATH = PARQUET_DIR / "index_prices_max_1d.parquet"
FUTURES_PATH = PARQUET_DIR / "futures_prices_max_1d.parquet"
VI_PATH = PARQUET_DIR / "nikkei_vi_max_1d.parquet"


def _get_max_hold(rule: str) -> int:
    """ルール別MAX_HOLD"""
    return RULE_MAX_HOLD.get(rule, 15)


def _load_market_regime(latest_date: pd.Timestamp) -> dict:
    """N225レジーム、CMEギャッ��、VIを取得"""
    regime = {"n225_above_sma20": None, "n225_ret20": None, "cme_gap": None, "vi": None}

    # N225
    try:
        idx = pd.read_parquet(INDEX_PATH)
        idx["date"] = pd.to_datetime(idx["date"])
        n225 = idx[idx["ticker"] == "^N225"][["date", "Close"]].dropna(subset=["date", "Close"]).sort_values("date")
        n225["sma20"] = n225["Close"].rolling(20).mean()
        n225["ret20"] = n225["Close"].pct_change(20) * 100
        row = n225[n225["date"] <= latest_date].iloc[-1]
        regime["n225_above_sma20"] = bool(row["Close"] > row["sma20"])
        regime["n225_ret20"] = round(float(row["ret20"]), 2) if pd.notna(row["ret20"]) else None
        n225_prev_close = float(n225[n225["date"] < latest_date].iloc[-1]["Close"]) if len(n225[n225["date"] < latest_date]) > 0 else None
    except Exception as e:
        print(f"  [WARN] N225 regime load failed: {e}")
        n225_prev_close = None

    # CMEギャップ: NKD終値 vs N225前日終値
    try:
        fut = pd.read_parquet(FUTURES_PATH)
        fut["date"] = pd.to_datetime(fut["date"])
        nkd = fut[fut["ticker"] == "NKD=F"][["date", "Close"]].sort_values("date")
        nkd_row = nkd[nkd["date"] <= latest_date].iloc[-1]
        if n225_prev_close and n225_prev_close > 0:
            regime["cme_gap"] = round((float(nkd_row["Close"]) / n225_prev_close - 1) * 100, 2)
    except Exception as e:
        print(f"  [WARN] CME gap load failed: {e}")

    # VI: 楽天証券当日ライブ値（fail-fast: 判断に直結するため）
    vi_data = fetch_nikkei_vi()
    regime["vi"] = round(float(vi_data["close"]), 1)
    regime["vi_prev_close"] = float(vi_data["prev_close"])

    return regime


def _classify_long_grade(rule: str, regime: dict) -> tuple[str, int, float] | None:
    """B1-B3シグナルにロングフィルターを適用。(grade, hold_days, expected_pf)を返す"""
    vi = regime.get("vi")
    n225_above = regime.get("n225_above_sma20")
    n225_ret20 = regime.get("n225_ret20")
    cme_gap = regime.get("cme_gap")

    # H1: VI≥30 + B1 → hold 3d (2026-04-17 再検証 全期間 PF 1.26, 5Y PF 1.85)
    if rule == "B1" and vi is not None and vi >= 30:
        return ("H1", 3, 1.85)

    # H3: N225 ret20<-5% + B1 → hold 8d (2026-04-17 再検証 10Y PF 1.86, 5Y PF 1.87)
    if rule == "B1" and n225_ret20 is not None and n225_ret20 < -5:
        return ("H3", 8, 1.87)

    # H2: N225>SMA20 + CME flat + B3 → hold 9d (2026-04-17 再検証 3Y PF 1.72, 現行維持)
    if rule == "B3" and n225_above and cme_gap is not None and -0.5 <= cme_gap <= 0.5:
        return ("H2", 9, 1.72)

    return None


def _load_capital() -> float:
    """信用余力を読み込み"""
    if CREDIT_CSV.exists():
        try:
            cc = pd.read_csv(CREDIT_CSV)
            if not cc.empty:
                return float(cc.iloc[-1]["available_margin"])
        except Exception:
            pass
    return 4_650_000  # §7: デフォルト465万


def load_prices() -> pd.DataFrame:
    """TOPIX価格データを読み込み、テクニカル指標を計算（§3準拠）"""
    print("[1/4] Loading prices...")
    ps = pd.read_parquet(PRICES_PATH)
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)
    print(f"  {len(ps):,} rows, {ps['ticker'].nunique()} tickers")

    g = ps.groupby("ticker")
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["prev_close"] = g["Close"].shift(1)
    ps["prev_sma20"] = g["sma20"].shift(1)
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)

    # ATR(10): ポジション管理用
    ps["prev_close_tr"] = g["Close"].shift(1)
    ps["tr"] = np.maximum(
        ps["High"] - ps["Low"],
        np.maximum(abs(ps["High"] - ps["prev_close_tr"]), abs(ps["Low"] - ps["prev_close_tr"]))
    )
    ps["atr10"] = g["tr"].transform(lambda x: x.rolling(10, min_periods=10).mean())
    ps["atr10_pct"] = ps["atr10"] / ps["Close"] * 100
    ps["ret5d"] = g["Close"].pct_change(5) * 100

    # 急騰フィルター用: SMA60/100の上方乖離（直近60日max）
    ps["sma60"] = g["Close"].transform(lambda x: x.rolling(60, min_periods=60).mean())
    ps["sma100"] = g["Close"].transform(lambda x: x.rolling(100, min_periods=100).mean())
    ps["dev60"] = (ps["Close"] - ps["sma60"]) / ps["sma60"] * 100
    ps["dev100"] = (ps["Close"] - ps["sma100"]) / ps["sma100"] * 100
    ps["max_up20"] = g["dev_from_sma20"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up60"] = g["dev60"].transform(lambda x: x.rolling(60, min_periods=1).max())
    ps["max_up100"] = g["dev100"].transform(lambda x: x.rolling(60, min_periods=1).max())

    # §3: up_day = Close > prev_close（前日比陽線）
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["above"] = ps["Close"] > ps["sma20"]
    ps["below"] = ps["Close"] < ps["sma20"]
    ps["prev_below"] = ps["prev_close"] < ps["prev_sma20"]
    ps["up_day"] = ps["Close"] > ps["prev_close"]

    # HVB (HighVol+Bear) grade: 出来高急増+陰線の先行シグナル
    # OOS PF: Grade A(HVB先行)=1.46 vs B(なし)=1.10, MaxDD 1/5
    ps["vol_ma20"] = g["Volume"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["vol_ratio"] = ps["Volume"] / ps["vol_ma20"]
    ps["day_ret_oc"] = (ps["Close"] / ps["Open"] - 1) * 100
    ps["_hvb"] = (ps["vol_ratio"] > 1.5) & (ps["day_ret_oc"] < 0)
    ps["hvb_recent_5d"] = g["_hvb"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).max()
    ).fillna(0).astype(bool)

    ps = ps.dropna(subset=["sma20"])
    return ps


def load_meta() -> pd.DataFrame:
    if META_PATH.exists():
        m = pd.read_parquet(META_PATH)
    elif META_FALLBACK.exists():
        m = pd.read_parquet(META_FALLBACK)
    else:
        return pd.DataFrame(columns=["ticker", "stock_name", "sectors"])
    if "sectors" in m.columns:
        m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    return m


def detect_signals(df: pd.DataFrame) -> pd.DataFrame:
    """§4: B1-B4 シグナル検出"""
    df = df.copy()
    dev = df["dev_from_sma20"]
    sma_up = df["sma20_up"]

    df["B1"] = df["prev_below"] & df["above"] & sma_up
    df["B2"] = sma_up & dev.between(-5, 0) & df["up_day"] & df["below"]
    df["B3"] = sma_up & df["above"] & dev.between(0, 3) & (df["prev_dev"] > dev) & df["up_day"]
    df["B4"] = (dev < -15) & df["up_day"]

    return df


def assign_rule(row: pd.Series) -> str:
    """§5: B4 > B1 > B3 > B2"""
    if row["B4"]:
        return "B4"
    if row["B1"]:
        return "B1"
    if row["B3"]:
        return "B3"
    return "B2"


def generate_signals(ps: pd.DataFrame, regime: dict | None = None) -> pd.DataFrame:
    """最新日のシグナルを生成"""
    ps = detect_signals(ps)
    meta = load_meta()

    latest_date = ps["date"].max()
    if regime is None:
        regime = _load_market_regime(latest_date)
    print(f"\n[2/4] Signal detection for {latest_date.date()}...")

    latest = ps[ps["date"] == latest_date].copy()

    for r in ["B1", "B2", "B3", "B4"]:
        print(f"  {r}: {latest[r].sum()} signals")

    # B4急騰フィルター: 直近60日でSMA20+15%超 or SMA60+20%超 or SMA100+30%超を除外
    b4_mask = latest["B4"].copy()
    if b4_mask.any():
        surge_filter = (
            (latest["max_up20"] >= 15) |
            (latest["max_up60"] >= 20) |
            (latest["max_up100"] >= 30)
        )
        b4_filtered = b4_mask & ~surge_filter
        filtered_count = b4_mask.sum() - b4_filtered.sum()
        latest["B4"] = b4_filtered
        print(f"  B4 surge filter: {filtered_count} excluded")

    # VI30-40GU除外: VI 30-40帯 AND 前日比上昇 → B4除外（Codex検証済み、PF+8%）
    # 判断に直結するため楽天証券ライブ値を fail-fast 参照（regimeでfetch済み）
    b4_mask2 = latest["B4"].copy()
    if b4_mask2.any():
        vi_close = float(regime["vi"])
        vi_prev = float(regime["vi_prev_close"])
        vi_chg = (vi_close - vi_prev) / vi_prev * 100
        if 30 <= vi_close <= 40 and vi_chg > 0:
            gu_count = b4_mask2.sum()
            latest["B4"] = False
            print(f"  B4 VI30-40GU filter: {gu_count} excluded (VI={vi_close:.1f}, chg={vi_chg:+.1f}%)")
        else:
            print(f"  B4 VI30-40GU filter: not triggered (VI={vi_close:.1f}, chg={vi_chg:+.1f}%)")

    sig_mask = latest["B1"] | latest["B2"] | latest["B3"] | latest["B4"]
    signals = latest[sig_mask].copy()
    print(f"  Total after filter: {len(signals)}")

    if signals.empty:
        return pd.DataFrame()

    signals["rule"] = signals.apply(assign_rule, axis=1)

    if not meta.empty:
        signals = signals.merge(
            meta[["ticker", "stock_name", "sectors"]].drop_duplicates(subset="ticker"),
            on="ticker", how="left",
        )
    else:
        signals["stock_name"] = ""
        signals["sectors"] = ""

    out = pd.DataFrame({
        # 共通カラム (3戦略統合スキーマ)
        "signal_date": signals["date"],
        "ticker": signals["ticker"],
        "stock_name": signals.get("stock_name", pd.Series("", index=signals.index)),
        "sector": signals.get("sectors", pd.Series("", index=signals.index)),
        "strategy": "granville",
        "direction": "long",
        "pair_id": "",
        "close": signals["Close"].round(1),
        "open": signals["Open"].round(1),
        "entry_price_est": signals["Close"].round(1),
        "prev_close": signals["prev_close"].round(1),
        "sma20": signals["sma20"].round(2),
        "dev_from_sma20": signals["dev_from_sma20"].round(3),
        # Granville 固有
        "rule": signals["rule"],
        "sma20_slope": signals["sma20_slope"].round(4),
        "atr10_pct": signals["atr10_pct"].round(2),
        "ret5d": signals["ret5d"].round(2),
        "hvb_grade": signals["hvb_recent_5d"].map({True: "A", False: "B"}),
        "hvb_recent_5d": signals["hvb_recent_5d"].astype(bool),
    })
    # max_hold / priority_rank (ルール由来、H-filter override は後段で適用)
    out["max_hold"] = out["rule"].map(RULE_MAX_HOLD).astype("int32")
    out["priority_rank"] = out["rule"].map(RULE_PRIORITY_RANK).astype("int32")

    # §5: ルール優先 → B4は乖離深い順（検証済み: Spearman r=-0.064, p<0.001）
    out["_priority"] = out["rule"].map(RULE_PRIORITY)
    out = out.sort_values(["_priority", "dev_from_sma20"], ascending=[True, True]).drop(columns=["_priority"]).reset_index(drop=True)

    return out


def generate_positions(ps: pd.DataFrame, latest_date: pd.Timestamp) -> pd.DataFrame:
    """§6: 過去シグナルから保有ポジション + Exit候補を計算"""
    print("\n[3/4] Generating positions...")

    capital = _load_capital()
    max_max_hold = max(RULE_MAX_HOLD.values())
    print(f"  Capital: ¥{capital:,.0f} → MAX_HOLD: {RULE_MAX_HOLD}")

    cutoff = latest_date - pd.Timedelta(days=max_max_hold * 2)
    recent = ps[ps["date"] >= cutoff].copy()
    recent = detect_signals(recent)

    sig_mask = recent["B1"] | recent["B2"] | recent["B3"] | recent["B4"]
    sigs = recent[sig_mask].copy()

    if sigs.empty:
        print("  No recent signals")
        return pd.DataFrame()

    sigs["rule"] = sigs.apply(assign_rule, axis=1)

    # メタデータ結合（stock_name）
    meta = load_meta()
    if not meta.empty:
        sigs = sigs.merge(
            meta[["ticker", "stock_name"]].drop_duplicates(subset="ticker"),
            on="ticker", how="left",
        )
        sigs["stock_name"] = sigs["stock_name"].fillna("")
    else:
        sigs["stock_name"] = ""

    # ticker別に事前グループ化（高速化）
    ticker_groups = {tk: gdf.sort_values("date").reset_index(drop=True)
                     for tk, gdf in ps.groupby("ticker")}

    rows: list[dict] = []
    for _, sig in sigs.iterrows():
        tk_all = ticker_groups.get(sig["ticker"])
        if tk_all is None:
            continue
        # エントリー日（シグナル翌営業日）のインデックスを特定
        entry_mask = tk_all["date"] > sig["date"]
        if not entry_mask.any():
            continue
        entry_iloc = entry_mask.idxmax()  # 最初のTrueのインデックス

        # §5: エントリーはシグナル翌営業日の寄付(Open)
        ep = float(tk_all.iloc[entry_iloc]["Open"])
        if pd.isna(ep) or ep <= 0:
            continue
        e_date = tk_all.iloc[entry_iloc]["date"]

        max_hold = _get_max_hold(sig["rule"])
        exited = False
        exit_type = ""
        exit_price = 0.0
        hold_end = min(entry_iloc + max_hold, len(tk_all))

        for day in range(entry_iloc, hold_end):
            row = tk_all.iloc[day]
            hold_day = day - entry_iloc

            # §6: 直近高値更新Exit — エントリー前k日を含む窓で判定
            # 発火したら翌営業日寄付で決済（ユーザーが執行）
            if hold_day > 0:
                w_start = max(entry_iloc - HIGH20D_PRE_ENTRY, day - 19)
                w_start = max(0, w_start)
                window_highs = tk_all.iloc[w_start:day + 1]["High"]
                high_20d = float(window_highs.max())
                if float(row["High"]) >= high_20d:
                    exit_price = float(row["Close"])  # 参考値（実約定は翌寄付）
                    if row["date"] == latest_date:
                        exit_type = "high_update"
                    exited = True
                    break

            # §6: MAX_HOLD到達 → 翌営業日寄付で決済
            if hold_day >= max_hold - 1:
                exit_price = float(row["Close"])  # 参考値（実約定は翌寄付）
                if row["date"] == latest_date:
                    exit_type = "max_hold"
                exited = True
                break

        cur_day = min(entry_iloc + min(hold_end - entry_iloc, max_hold) - 1, len(tk_all) - 1)
        cur = tk_all.iloc[cur_day]
        cp = float(cur["Close"])
        hold_days = cur_day - entry_iloc + 1

        # エントリー前k日を含むrolling高値（Exit判定と同じ窓定義）
        h_start = max(entry_iloc - HIGH20D_PRE_ENTRY, cur_day - 19)
        h_start = max(0, h_start)
        entry_high = float(tk_all.iloc[h_start:cur_day + 1]["High"].max())
        trigger_price = round(entry_high, 1) if not pd.isna(entry_high) else 0.0

        # ATR(10): 直近の平均true range
        atr10 = 0.0
        if "atr10" in cur.index and not pd.isna(cur.get("atr10")):
            atr10 = round(float(cur["atr10"]), 1)

        base = {
            # 共通カラム (3戦略統合スキーマ)
            "ticker": sig["ticker"],
            "stock_name": sig.get("stock_name", ""),
            "strategy": "granville",
            "direction": "long",
            "pair_id": "",
            "entry_date": e_date,
            "entry_price": round(ep, 1),
            "current_price": round(cp, 1),
            "pct": round((cp / ep - 1) * 100, 2),
            "pnl": int((cp - ep) * 100),
            "hold_days": hold_days,
            "max_hold": max_hold,
            "as_of": latest_date,
            # Granville 固有
            "rule": sig["rule"],
            "trigger_price": trigger_price,
            "atr10": atr10,
            "hvb_grade": "A" if sig.get("hvb_recent_5d", False) else "B",
        }

        if not exited:
            rows.append({**base, "status": "open", "exit_type": ""})
        elif exit_type:
            rows.append({**base, "status": "exit", "exit_type": exit_type})

    result = pd.DataFrame(rows)
    if not result.empty:
        open_n = (result["status"] == "open").sum()
        exit_n = (result["status"] == "exit").sum()
        print(f"  {open_n} open, {exit_n} exit candidates")
    else:
        print("  No active positions")

    return result


def main() -> int:
    print("=" * 60)
    print("Generate Granville Signals (IMPLEMENTATION.md §3-§6)")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    if not PRICES_PATH.exists():
        print(f"[ERROR] Price data not found: {PRICES_PATH}")
        print("  Run fetch_prices.py first")
        return 1

    GRANVILLE_DIR.mkdir(parents=True, exist_ok=True)

    ps = load_prices()
    latest_date = ps["date"].max()
    regime = _load_market_regime(latest_date)
    out = generate_signals(ps, regime)

    if out.empty:
        print("\n[INFO] No signals today")
    else:
        print(f"\n  {len(out)} signals generated")
        for _, row in out.iterrows():
            print(f"    [{row['rule']}] {row['ticker']} {row['stock_name']} "
                  f"¥{row['close']:,.0f} ({row['dev_from_sma20']:+.1f}%)")

    date_str = latest_date.strftime("%Y-%m-%d")

    # B1-B3 ロングフィルター判定 (H1/H2/H3) を signals に merge
    print(f"\n  Market regime: N225>SMA20={regime['n225_above_sma20']}, "
          f"ret20={regime['n225_ret20']}, CME gap={regime['cme_gap']}, VI={regime['vi']}")

    if not out.empty:
        long_grade_col, hold_days_col, expected_pf_col = [], [], []
        for _, sig in out.iterrows():
            if sig["rule"] in ("B1", "B2", "B3"):
                grade = _classify_long_grade(sig["rule"], regime)
                if grade:
                    long_grade_col.append(grade[0])
                    hold_days_col.append(grade[1])
                    expected_pf_col.append(grade[2])
                    continue
            long_grade_col.append("")
            hold_days_col.append(pd.NA)
            expected_pf_col.append(pd.NA)
        out["long_grade"] = long_grade_col
        out["hold_days"] = pd.array(hold_days_col, dtype="Int32")
        out["expected_pf"] = pd.array(expected_pf_col, dtype="Float32")
    else:
        out = pd.DataFrame(columns=[
            "signal_date", "ticker", "stock_name", "sector",
            "strategy", "direction", "pair_id",
            "close", "open", "entry_price_est", "prev_close", "sma20", "dev_from_sma20",
            "rule", "sma20_slope", "atr10_pct", "ret5d",
            "hvb_grade", "hvb_recent_5d", "max_hold", "priority_rank",
            "long_grade", "hold_days", "expected_pf",
        ])

    # 統合 signals.parquet に merge (strategy="granville" 行のみ差し替え)
    if SIGNALS_PATH.exists():
        existing_sigs = pd.read_parquet(SIGNALS_PATH)
        other_sigs = (existing_sigs[existing_sigs["strategy"] != "granville"]
                      if "strategy" in existing_sigs.columns else existing_sigs)
        merged_sigs = pd.concat([out, other_sigs], ignore_index=True) if len(other_sigs) else out
    else:
        merged_sigs = out
    SIGNALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    # atomic write: tmp → rename で中間状態が S3/読み手に見えない
    tmp_sigs = SIGNALS_PATH.parent / f"{SIGNALS_PATH.name}.tmp"
    merged_sigs.to_parquet(tmp_sigs, index=False)
    tmp_sigs.replace(SIGNALS_PATH)
    print(f"\n[OK] Saved: {SIGNALS_PATH.name} ({len(out)} rows, strategy=granville / total={len(merged_sigs)})")
    grade_counts = (out[out["long_grade"] != ""]["long_grade"].value_counts().to_dict()
                    if not out.empty and "long_grade" in out.columns else {})
    if grade_counts:
        print(f"     long_grade: {grade_counts}")

    # ポジション計算 → 統合 positions.parquet
    ps = detect_signals(ps)
    positions = generate_positions(ps, latest_date)
    if positions.empty:
        positions = pd.DataFrame(columns=[
            "ticker", "stock_name", "strategy", "direction", "pair_id",
            "entry_date", "entry_price", "current_price",
            "pct", "pnl", "hold_days", "max_hold", "as_of",
            "rule", "trigger_price", "atr10", "hvb_grade",
            "status", "exit_type",
        ])
    # 統合 positions.parquet に merge (strategy="granville" 行のみ差し替え)
    if POSITIONS_PATH.exists():
        existing_pos = pd.read_parquet(POSITIONS_PATH)
        other_pos = (existing_pos[existing_pos["strategy"] != "granville"]
                     if "strategy" in existing_pos.columns else existing_pos)
        merged_pos = pd.concat([positions, other_pos], ignore_index=True) if len(other_pos) else positions
    else:
        merged_pos = positions
    POSITIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_pos = POSITIONS_PATH.parent / f"{POSITIONS_PATH.name}.tmp"
    merged_pos.to_parquet(tmp_pos, index=False)
    tmp_pos.replace(POSITIONS_PATH)
    open_n = (positions["status"] == "open").sum() if not positions.empty else 0
    exit_n = (positions["status"] == "exit").sum() if not positions.empty else 0
    print(f"[OK] Saved: {POSITIONS_PATH.name} ({len(positions)} rows granville, open={open_n}, exit={exit_n} / total={len(merged_pos)})")

    # S3アップロード
    print("\n[4/4] Uploading to S3...")
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, SIGNALS_PATH, "signals.parquet")
            upload_file(cfg, POSITIONS_PATH, "positions.parquet")
        else:
            print("  [INFO] S3 bucket not configured")
    except Exception as e:
        print(f"  [WARN] S3 upload failed: {e}")

    print(f"\n{'=' * 60}")
    print(f"Date: {date_str}")
    print(f"Signals: {len(out)}")
    for rule in ["B4", "B1", "B3", "B2"]:
        n = (out["rule"] == rule).sum() if not out.empty else 0
        print(f"  {rule}: {n}")
    print(f"Long-filter: {grade_counts}")
    print(f"Positions: {len(positions)} (open={open_n}, exit={exit_n})")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
