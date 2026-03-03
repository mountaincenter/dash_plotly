#!/usr/bin/env python3
"""
generate_granville_signals.py
グランビルIFDロング戦略: 翌日エントリー候補シグナル生成

毎営業日16:45実行。prices_max_1d.parquet等から
sig_A(押し目買い) / sig_B(SMA支持反発) / B4(深い逆張り) を検出し、
フィルター適用後に granville_ifd_signals.parquet を出力。

=== レジーム分岐 ===
Uptrend (N225 > SMA20):
  - sig_A + sig_B（従来ルール）
  - フィルター: uptrend + CI拡大 + 悪セクター除外 + ¥2万未満

Downtrend (N225 < SMA20):
  - B4 + sig_A（乖離反発系）→ 12年+3,175万, 勝率68.6%
  - フィルター: 悪セクター除外 + ¥2万未満（uptrend不要）

=== Tier ===
T1: B4（SMA20下降 + 乖離<-8%）→ 勝率70%, 平均+4,420円
T2: sigA deep（乖離-5%~-8%）
T3: sigA moderate（乖離-3%~-5%）
T4: sigB（SMA支持反発, uptrend時のみ）

=== IFDパラメータ ===
SL: -3%（IFD逆指値）、保有: 7営業日引け成行売り
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

load_dotenv_cascade()
OUTPUT_PATH = PARQUET_DIR / "granville_ifd_signals.parquet"
POSITIONS_PATH = PARQUET_DIR / "granville_ifd_positions.parquet"
S3_POSITIONS_KEY = "granville_ifd_positions.parquet"

BAD_SECTORS = ["医薬品", "輸送用機器", "小売業", "その他製品", "陸運業", "サービス業"]


CI_PARQUET = PARQUET_DIR / "macro" / "estat_ci_index.parquet"


def load_ci_leading() -> pd.DataFrame:
    """ローカルparquetからCI先行指数を読み込み"""
    if not CI_PARQUET.exists():
        print(f"  ⚠️ {CI_PARQUET} が存在しない、CI先行指数スキップ")
        return pd.DataFrame(columns=["date", "ci_leading", "ci_leading_chg3m"])

    ci = pd.read_parquet(CI_PARQUET)
    ci["date"] = pd.to_datetime(ci["date"])
    ci = ci[["date", "leading"]].rename(columns={"leading": "ci_leading"}).sort_values("date").reset_index(drop=True)
    ci["ci_leading_chg3m"] = ci["ci_leading"].diff(3)
    return ci


def load_data() -> pd.DataFrame:
    """価格・指数・CI・メタデータを統合して返す"""
    # メタ
    m = pd.read_parquet(PARQUET_DIR / "meta.parquet")
    m["sectors"] = m["sectors"].str.replace("･", "・", regex=False)
    tickers = m["ticker"].tolist()

    # 株価
    p = pd.read_parquet(PARQUET_DIR / "prices_max_1d.parquet")
    ps = p[p["ticker"].isin(tickers)].copy()
    ps["date"] = pd.to_datetime(ps["date"])
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    # 日経225
    idx = pd.read_parquet(PARQUET_DIR / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"]).rename(columns={"Close": "nk225_close"})
    nk["nk225_sma20"] = nk["nk225_close"].rolling(20).mean()
    nk["market_uptrend"] = nk["nk225_close"] > nk["nk225_sma20"]

    # CI先行指数（e-Stat API）
    ci = load_ci_leading()

    # 日次ベースでCI先行指数をforward fill
    daily = pd.DataFrame({"date": ps["date"].drop_duplicates().sort_values()})
    daily = daily.merge(ci, on="date", how="left").sort_values("date").ffill()

    # テクニカル指標
    g = ps.groupby("ticker")
    ps["sma5"] = g["Close"].transform(lambda x: x.rolling(5).mean())
    ps["sma20"] = g["Close"].transform(lambda x: x.rolling(20).mean())
    ps["sma20_slope"] = g["sma20"].transform(lambda x: x.diff(3))
    ps["sma20_up"] = ps["sma20_slope"] > 0
    ps["dev_from_sma20"] = (ps["Close"] - ps["sma20"]) / ps["sma20"] * 100
    ps["prev_dev"] = g["dev_from_sma20"].shift(1)
    ps["prev_close"] = g["Close"].shift(1)
    ps = ps.dropna(subset=["sma20"])

    # マージ
    ps = ps.merge(nk[["date", "market_uptrend", "nk225_close", "nk225_sma20"]], on="date", how="left")
    ps = ps.merge(daily, on="date", how="left")
    ps["macro_ci_expand"] = ps["ci_leading_chg3m"] > 0
    ps = ps.merge(m[["ticker", "sectors", "stock_name"]], on="ticker", how="left")

    return ps


def detect_signals(df: pd.DataFrame) -> pd.DataFrame:
    """sig_A(押し目買い) / sig_B(SMA支持反発) / B4(深い逆張り) を検出"""
    df = df.copy()
    dev = df["dev_from_sma20"]
    sma_up = df["sma20_up"]
    above = df["Close"] > df["sma20"]

    # B4: SMA20下降中 + 乖離 < -8%（深い逆張り、downtrend主力）
    df["sig_B4"] = (~sma_up) & (dev < -8)

    # sig_A: 乖離-8%〜-3%、終値上昇
    df["sig_A"] = (dev.between(-8, -3)) & (df["Close"] > df["prev_close"])

    # sig_B: SMA上昇中、終値>SMA、乖離0-2%、前日乖離≤0.5%、終値上昇
    df["sig_B"] = (
        sma_up & above
        & (dev.between(0, 2)) & (df["prev_dev"] <= 0.5)
        & (df["Close"] > df["prev_close"])
    )

    return df


def generate_signals() -> pd.DataFrame:
    """最新日のシグナルを生成し、フィルター適用後のDataFrameを返す"""
    print("[INFO] Loading data...")
    ps = load_data()
    ps = detect_signals(ps)

    # 最新日のみ抽出
    latest_date = ps["date"].max()
    print(f"[INFO] Latest date: {latest_date.date()}")

    latest = ps[ps["date"] == latest_date].copy()

    # シグナル検出
    sig_mask = latest["sig_A"] | latest["sig_B"]
    signals = latest[sig_mask].copy()
    print(f"[INFO] Raw signals: {len(signals)} (sig_A={latest['sig_A'].sum()}, sig_B={latest['sig_B'].sum()})")

    if signals.empty:
        print("[INFO] No signals detected")
        return pd.DataFrame()

    # フィルター適用
    before = len(signals)
    signals = signals[signals["market_uptrend"] == True]
    print(f"[INFO] After uptrend filter: {len(signals)} (removed {before - len(signals)})")

    before = len(signals)
    signals = signals[signals["macro_ci_expand"] == True]
    print(f"[INFO] After CI expand filter: {len(signals)} (removed {before - len(signals)})")

    before = len(signals)
    signals = signals[~signals["sectors"].isin(BAD_SECTORS)]
    print(f"[INFO] After bad sectors filter: {len(signals)} (removed {before - len(signals)})")

    before = len(signals)
    signals = signals[signals["Close"] < 20000]
    print(f"[INFO] After price < ¥20,000 filter: {len(signals)} (removed {before - len(signals)})")

    if signals.empty:
        print("[INFO] All signals filtered out")
        return pd.DataFrame()

    # シグナルタイプ決定
    def signal_type(row):
        a, b = row["sig_A"], row["sig_B"]
        if a and b:
            return "A+B"
        return "A" if a else "B"

    signals["signal_type"] = signals.apply(signal_type, axis=1)

    # 出力カラム構成
    out = pd.DataFrame({
        "signal_date": signals["date"],
        "ticker": signals["ticker"],
        "stock_name": signals["stock_name"],
        "sector": signals["sectors"],
        "signal_type": signals["signal_type"],
        "close": signals["Close"],
        "sma20": signals["sma20"].round(2),
        "dev_from_sma20": signals["dev_from_sma20"].round(3),
        "sma20_slope": signals["sma20_slope"].round(4),
        "entry_price_est": signals["Close"],
        "sl_price": (signals["Close"] * 0.97).round(1),
        "market_uptrend": signals["market_uptrend"],
        "ci_expand": signals["macro_ci_expand"],
    })

    out = out.sort_values("dev_from_sma20").reset_index(drop=True)
    return out


def generate_positions(ps: pd.DataFrame) -> None:
    """過去120日のシグナルから保有ポジション+本日イグジットを前処理"""
    print("\n[INFO] Generating positions...")

    # ps は load_data() + detect_signals() 済みのDataFrame
    latest = ps["date"].max()

    # N225 uptrend マップ
    idx = pd.read_parquet(PARQUET_DIR / "index_prices_max_1d.parquet")
    nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
    nk["date"] = pd.to_datetime(nk["date"])
    nk = nk.sort_values("date").dropna(subset=["Close"])
    nk["sma20"] = nk["Close"].rolling(20).mean()
    nk["uptrend"] = nk["Close"] > nk["sma20"]
    uptrend_map = dict(zip(nk["date"].values, nk["uptrend"].values))

    # CI先行指数（e-Stat API）
    ci_ok = True
    try:
        ci = load_ci_leading()
        if not ci.empty:
            ci_ok = bool(ci.dropna(subset=["ci_leading_chg3m"]).iloc[-1]["ci_leading_chg3m"] > 0)
    except Exception:
        pass

    # 過去120日のシグナル
    cutoff = latest - pd.Timedelta(days=120)
    r = ps[ps["date"] >= cutoff].copy()

    # 再検出（detect_signals済みなので sig_A/sig_B 列がある）
    sigs = r[r["sig_A"] | r["sig_B"]].copy()

    # フィルター
    sigs = sigs[sigs["date"].map(lambda d: uptrend_map.get(np.datetime64(d), False))]
    if not ci_ok:
        sigs = sigs.iloc[0:0]
    sigs = sigs[~sigs["sectors"].isin(BAD_SECTORS)]
    sigs = sigs[sigs["Close"] < 20000]

    if sigs.empty:
        print("[INFO] No active signals in past 120 days")
        empty = pd.DataFrame(columns=[
            "status", "ticker", "stock_name", "signal_type",
            "entry_date", "entry_price", "current_price",
            "pct", "pnl", "sl_price", "hold_days", "exit_type", "as_of",
        ])
        empty.to_parquet(POSITIONS_PATH, index=False)
        _upload_positions()
        return

    sigs["sig_type"] = sigs.apply(
        lambda x: "A+B" if x["sig_A"] and x["sig_B"] else ("A" if x["sig_A"] else "B"), axis=1
    )

    # 前方シミュレーション
    rows = []
    for _, sig in sigs.iterrows():
        tk = ps[(ps["ticker"] == sig["ticker"]) & (ps["date"] > sig["date"])].sort_values("date")
        if tk.empty:
            continue

        ep = float(tk.iloc[0]["Open"])
        if pd.isna(ep) or ep <= 0:
            continue
        e_date = tk.iloc[0]["date"]
        sl = ep * 0.97
        st = sig["sig_type"]
        exited = False
        exit_today = None

        for i in range(min(len(tk), 60)):
            row = tk.iloc[i]
            if float(row["Low"]) <= sl:
                if row["date"] == latest:
                    exit_today = "SL"
                exited = True
                break
            if i == 0:
                continue
            cv, s5, s20 = float(row["Close"]), float(row["sma5"]), float(row["sma20"])

            # A: SMA20回帰
            if st in ("A", "A+B") and cv >= s20:
                if row["date"] == latest:
                    exit_today = "SMA20_touch"
                exited = True
                break

            # DC
            prev = tk.iloc[i - 1]
            if float(prev["sma5"]) >= float(prev["sma20"]) and s5 < s20:
                if row["date"] == latest:
                    exit_today = "dead_cross"
                exited = True
                break

            # 7日経過マイナスなら翌朝損切り
            if i == 6 and cv < ep:
                if row["date"] == latest:
                    exit_today = "time_cut"
                exited = True
                break

            if i >= 59:
                exited = True
                break

        if not exited:
            cur = tk.iloc[-1]
            cp = float(cur["Close"])
            rows.append({
                "status": "open",
                "ticker": sig["ticker"],
                "stock_name": sig.get("stock_name", ""),
                "signal_type": st,
                "entry_date": e_date,
                "entry_price": round(ep, 1),
                "current_price": round(cp, 1),
                "pct": round((cp / ep - 1) * 100, 2),
                "pnl": int((cp - ep) * 100),
                "sl_price": round(sl, 1),
                "hold_days": len(tk),
                "exit_type": "",
                "as_of": latest,
            })
        elif exit_today:
            if exit_today == "SL":
                # IFD注文: ザラ場中に約定済み → ポジションから除外（トレード一覧に計上済み）
                pass
            else:
                # dead_cross/SMA20_touch/time_cut: 翌朝Open売り → イグジットシグナル
                cur = tk.iloc[-1]
                cp = float(cur["Close"])
                rows.append({
                    "status": "exit",
                    "ticker": sig["ticker"],
                    "stock_name": sig.get("stock_name", ""),
                    "signal_type": st,
                    "entry_date": e_date,
                    "entry_price": round(ep, 1),
                    "current_price": round(cp, 1),
                    "pct": round((cp / ep - 1) * 100, 2),
                    "pnl": int((cp - ep) * 100),
                    "sl_price": 0,
                    "hold_days": len(tk),
                    "exit_type": exit_today,
                    "as_of": latest,
                })

    result = pd.DataFrame(rows)
    result.to_parquet(POSITIONS_PATH, index=False)
    open_count = (result["status"] == "open").sum()
    exit_count = (result["status"] == "exit").sum()
    print(f"[OK] Positions: {open_count} open, {exit_count} exits → {POSITIONS_PATH}")
    _upload_positions()


def _upload_positions() -> None:
    """ポジションparquetをS3にアップロード"""
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, POSITIONS_PATH, S3_POSITIONS_KEY)
            print(f"[OK] Uploaded to s3://{cfg.bucket}/{S3_POSITIONS_KEY}")
    except Exception as e:
        print(f"[WARN] S3 upload failed: {e}")


def main() -> int:
    print("=" * 60)
    print("Generate Granville IFD Signals")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    try:
        # load_data + detect_signals を一度だけ実行し、シグナル生成とポジション計算で共有
        print("[INFO] Loading data...")
        ps = load_data()
        ps = detect_signals(ps)

        # --- シグナル生成 ---
        latest_date = ps["date"].max()
        print(f"[INFO] Latest date: {latest_date.date()}")
        latest = ps[ps["date"] == latest_date].copy()

        # レジーム判定
        is_uptrend = bool(latest["market_uptrend"].iloc[0]) if not latest.empty else False
        regime = "Uptrend" if is_uptrend else "Downtrend"
        print(f"[INFO] Regime: {regime}")

        if is_uptrend:
            # Uptrend: sig_A + sig_B（従来ルール）
            sig_mask = latest["sig_A"] | latest["sig_B"]
        else:
            # Downtrend: B4 + sig_A（乖離反発系、uptrend不要）
            sig_mask = latest["sig_B4"] | latest["sig_A"]

        raw_signals = latest[sig_mask].copy()
        print(f"[INFO] Raw signals: {len(raw_signals)} "
              f"(B4={latest['sig_B4'].sum()}, sig_A={latest['sig_A'].sum()}, sig_B={latest['sig_B'].sum()})")

        signals = raw_signals.copy()
        if not signals.empty:
            if is_uptrend:
                # Uptrend: uptrend + CI フィルター必須
                before = len(signals)
                signals = signals[signals["market_uptrend"] == True]
                print(f"[INFO] After uptrend filter: {len(signals)} (removed {before - len(signals)})")
                before = len(signals)
                signals = signals[signals["macro_ci_expand"] == True]
                print(f"[INFO] After CI expand filter: {len(signals)} (removed {before - len(signals)})")
            else:
                # Downtrend: uptrend/CIフィルター不要（B4+sigAはdowntrendが主力）
                print(f"[INFO] Downtrend mode: skipping uptrend/CI filters")

            # 共通フィルター
            before = len(signals)
            signals = signals[~signals["sectors"].isin(BAD_SECTORS)]
            print(f"[INFO] After bad sectors filter: {len(signals)} (removed {before - len(signals)})")
            before = len(signals)
            signals = signals[signals["Close"] < 20000]
            print(f"[INFO] After price < ¥20,000 filter: {len(signals)} (removed {before - len(signals)})")

        if signals.empty:
            print("[INFO] No signals to save (empty result)")
            out = pd.DataFrame(columns=[
                "signal_date", "ticker", "stock_name", "sector", "signal_type", "tier",
                "close", "sma20", "dev_from_sma20", "sma20_slope",
                "entry_price_est", "sl_price", "market_uptrend", "ci_expand",
            ])
        else:
            def signal_type(row):
                if row.get("sig_B4", False):
                    return "B4"
                a, b = row["sig_A"], row["sig_B"]
                if a and b:
                    return "A+B"
                return "A" if a else "B"

            def assign_tier(row):
                if row.get("sig_B4", False):
                    return "T1"  # B4: 勝率70%, 平均+4,420円
                dev = row["dev_from_sma20"]
                if row["sig_A"]:
                    if dev <= -5:
                        return "T2"  # sigA deep
                    return "T3"  # sigA moderate
                return "T4"  # sigB

            signals["signal_type"] = signals.apply(signal_type, axis=1)
            signals["tier"] = signals.apply(assign_tier, axis=1)

            out = pd.DataFrame({
                "signal_date": signals["date"],
                "ticker": signals["ticker"],
                "stock_name": signals["stock_name"],
                "sector": signals["sectors"],
                "signal_type": signals["signal_type"],
                "tier": signals["tier"],
                "close": signals["Close"],
                "sma20": signals["sma20"].round(2),
                "dev_from_sma20": signals["dev_from_sma20"].round(3),
                "sma20_slope": signals["sma20_slope"].round(4),
                "entry_price_est": signals["Close"],
                "sl_price": (signals["Close"] * 0.97).round(1),
                "market_uptrend": signals["market_uptrend"],
                "ci_expand": signals["macro_ci_expand"],
            })
            out = out.sort_values(["tier", "dev_from_sma20"]).reset_index(drop=True)

        # --- 既存シグナルに追記（YAMLでS3→ローカル済み）---
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)

        existing = pd.DataFrame()
        if OUTPUT_PATH.exists():
            try:
                existing = pd.read_parquet(OUTPUT_PATH)
                existing["signal_date"] = pd.to_datetime(existing["signal_date"])
                print(f"[INFO] Existing signals: {len(existing)} rows")
            except Exception:
                pass

        if not out.empty:
            out["signal_date"] = pd.to_datetime(out["signal_date"])

        if not existing.empty and not out.empty:
            existing = existing[existing["signal_date"] != latest_date]
            merged = pd.concat([existing, out], ignore_index=True)
        elif not existing.empty:
            merged = existing
        else:
            merged = out

        # 30日超のシグナルを削除
        cutoff_30d = pd.Timestamp.now() - pd.Timedelta(days=30)
        before_purge = len(merged)
        merged = merged[merged["signal_date"] >= cutoff_30d].reset_index(drop=True)
        purged = before_purge - len(merged)
        if purged > 0:
            print(f"[INFO] Purged {purged} old signals (>30 days)")

        merged.to_parquet(OUTPUT_PATH, index=False)
        print(f"[OK] Saved {len(merged)} signals ({len(out)} new) to {OUTPUT_PATH}")

        print(f"\n{'=' * 60}")
        if len(out) > 0:
            print(f"Today: {len(out)} candidates")
            for _, row in out.iterrows():
                print(f"  {row['ticker']} {row['stock_name']} "
                      f"[{row['signal_type']}] ¥{row['close']:,.0f} "
                      f"SL ¥{row['sl_price']:,.0f} "
                      f"({row['dev_from_sma20']:+.1f}%)")
        else:
            print("No signals today")
        print(f"Total: {len(merged)} signals (rolling 30d)")
        print("=" * 60)

        # --- ポジション前処理 ---
        generate_positions(ps)

        return 0

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
