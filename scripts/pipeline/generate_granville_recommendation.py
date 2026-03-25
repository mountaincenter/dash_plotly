#!/usr/bin/env python3
"""
generate_granville_recommendation.py
Granville推奨銘柄リスト生成（IMPLEMENTATION.md §5, §7, §8 準拠）

1. 当日シグナルを読み込み
2. B4 > B1 > B3 > B2 優先順位
3. 同一ルール内: バックテスト統計ベースのスコア降順
4. 既存保有銘柄を除外
5. 証拠金計算: upper_limit(prev_close) × 100株
6. 15%証拠金上限（株価フィルター）: 証拠金 > 資金の15% → スキップ
7. 残余力不足 → スキップ
8. 出力: recommendations_YYYY-MM-DD.parquet
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
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3cfg import load_s3_config
from common_cfg.s3io import upload_file

load_dotenv_cascade()

GRANVILLE_DIR = PARQUET_DIR / "granville"
PRICES_PATH = GRANVILLE_DIR / "prices_topix.parquet"

MARGIN_LIMIT_PCT = 0.15  # §7: 証拠金上限15%（株価フィルター）
RULE_PRIORITY = {"B4": 0, "B1": 1, "B3": 2, "B2": 3}
RULE_MAX_HOLD = {"B4": 19, "B1": 13, "B3": 14, "B2": 15}

# §7: 証拠金テーブル
_LIMIT_TABLE = [
    (100, 30), (200, 50), (500, 80), (700, 100),
    (1000, 150), (1500, 300), (2000, 400), (3000, 500),
    (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000),
    (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000),
    (100000, 15000), (150000, 30000), (200000, 40000), (300000, 50000),
    (500000, 70000), (700000, 100000), (1000000, 150000),
]


# バックテスト2Y (63,727trades) クロス分析に基づくスコアリング
# B4最強条件: 乖離深(-15%超) × ATR高(>6%) × RSI30-40 × ret5d<-10% → WR93%/+7%
# B4最弱条件: 乖離浅(-10~-8%) × ATR低(<4%) → WR79%/+0.9%
# B1: ATR高いとリターン増(+4.58%)だが勝率低下(75%)。乖離>3%&ATR>5%でavg+3.8%
_SECTOR_BONUS = {
    "銀行業": 10, "証券・商品先物取引業": 8, "証券･商品先物取引業": 8,
    "不動産業": 5, "卸売業": 5, "電気機器": 5,
    "その他金融業": 5, "化学": 4, "輸送用機器": 4,
    "非鉄金属": 4, "鉄鋼": 4, "金属製品": 4, "機械": 4,
    "小売業": -5, "陸運業": -3, "食料品": -3,
}


def compute_rank_score(row: pd.Series, rule: str) -> float:
    """バックテスト統計ベースのランクスコア（0-100）"""
    score = 50.0

    dev = abs(float(row.get("dev_from_sma20", 0)))
    atr_pct = float(row.get("atr10_pct", 3.0))
    vol_ratio = float(row.get("vol_ratio", 1.0))
    rsi14 = float(row.get("rsi14", 50.0))
    ret5d = float(row.get("ret5d", 0.0))
    sector = str(row.get("sector", ""))
    price = float(row.get("close", 1000))

    if rule == "B4":
        # 乖離深度 (WR: <-15%=90%, -15~-12%=90%, -12~-10%=87%, -10~-8%=87%)
        if dev >= 20:
            score += 25
        elif dev >= 15:
            score += 20
        elif dev >= 12:
            score += 15
        elif dev >= 10:
            score += 10
        # ATR% — 最重要因子 (WR: >7%=94.2%, 5-7%=89.6%, 4-5%=84.4%, <3%=80.9%)
        if atr_pct >= 7:
            score += 20
        elif atr_pct >= 5:
            score += 12
        elif atr_pct >= 4:
            score += 5
        elif atr_pct < 3:
            score -= 10
        # RSI14 (30-40がスイートスポット: WR91.4%/+4.32%)
        if 30 <= rsi14 < 40:
            score += 8
        elif rsi14 < 30:
            score += 3  # 売られすぎだが30-40より劣る
        elif rsi14 >= 50:
            score -= 5
        # ret5d — 直近の下落モメンタム (<-10%: WR91.3%/+5.54%)
        if ret5d < -10:
            score += 10
        elif ret5d < -5:
            score += 5
        elif ret5d > 0:
            score -= 3

    elif rule == "B1":
        # B1: ATRがリターンに直結 (>7%: avg+4.58%, <2%: avg+0.85%)
        # ただし勝率はATR低い方が高い (ATR<2%: 84%, >7%: 75%)
        # → リスク調整後はATR4-5%がベスト (WR81.4%/+2.06%)
        if atr_pct >= 5:
            score += 15  # ハイリスク・ハイリターン
        elif atr_pct >= 4:
            score += 10  # ベストバランス
        elif atr_pct >= 3:
            score += 5
        elif atr_pct < 2:
            score -= 3
        # 乖離>3%&ATR>5%の組み合わせ (avg+3.8%)
        if dev > 3 and atr_pct >= 5:
            score += 8
        # RSI: 30-40帯が若干良い (WR82.1%)、>70はリスク
        if 30 <= rsi14 < 40:
            score += 5
        elif rsi14 >= 70:
            score -= 8

    else:
        # B2/B3: 差が小さい。ATR中心
        if atr_pct >= 5:
            score += 10
        elif atr_pct >= 3:
            score += 5
        elif atr_pct < 2:
            score -= 5

    # 出来高比率 (B4: 1.5-2x=WR91.3%, <0.5x=WR82.7%)
    if 1.0 <= vol_ratio <= 2.0:
        score += 8
    elif 0.5 <= vol_ratio < 1.0:
        score += 3
    elif vol_ratio > 3.0:
        score -= 3
    elif vol_ratio < 0.5:
        score -= 5

    # セクター (B4: 銀行WR98.4%, 小売WR80.5%)
    score += _SECTOR_BONUS.get(sector, 0)

    return round(max(0, score), 1)


# バックテスト2Y実績: (rule, score_band) → avg return %
_EXPECTED_RETURN = {
    ("B4", "green"): 4.88, ("B4", "yellow"): 2.05, ("B4", "gray"): 1.19,
    ("B1", "green"): 3.90, ("B1", "yellow"): 1.29, ("B1", "gray"): 0.90,
    ("B3", "green"): 1.06, ("B3", "yellow"): 1.06, ("B3", "gray"): 0.78,
    ("B2", "green"): 1.48, ("B2", "yellow"): 1.48, ("B2", "gray"): 0.97,
}


def compute_expected_profit(rule: str, rank_score: float, entry_price: float) -> int:
    """100株あたりの期待利益額（円）"""
    band = "green" if rank_score >= 80 else "yellow" if rank_score >= 60 else "gray"
    avg_ret = _EXPECTED_RETURN.get((rule, band), 1.0)
    return int(entry_price * avg_ret / 100 * 100)


def _upper_limit(price: float) -> float:
    for threshold, limit in _LIMIT_TABLE:
        if price < threshold:
            return price + limit
    return price + 150000


def _required_margin(prev_close: float) -> float:
    """§7: upper_limit(prev_close) × 100株"""
    return _upper_limit(prev_close) * 100


def _get_max_hold(rule: str) -> int:
    return RULE_MAX_HOLD.get(rule, 15)


def load_available_margin() -> float:
    """credit_status.parquetから現金保証金(信用)を取得"""
    cs_path = PARQUET_DIR / "credit_status.parquet"
    if cs_path.exists():
        try:
            cs = pd.read_parquet(cs_path)
            row = cs[cs["asset"].str.contains("信用", na=False)]
            if not row.empty:
                val = float(row["value"].iloc[0])
                if val > 0:
                    return val
        except Exception:
            pass
    # S3フォールバック
    try:
        from common_cfg.s3io import download_file
        from common_cfg.s3cfg import load_s3_config
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            download_file(cfg, "credit_status.parquet", cs_path)
            cs = pd.read_parquet(cs_path)
            row = cs[cs["asset"].str.contains("信用", na=False)]
            if not row.empty:
                return float(row["value"].iloc[0])
    except Exception:
        pass
    print(f"  [WARN] credit_status.parquet not found, using default 4,650,000")
    return 4_650_000


def load_hold_stocks() -> set[str]:
    """hold_stocks.parquetから保有銘柄ticker一覧を取得"""
    hs_path = PARQUET_DIR / "hold_stocks.parquet"
    if hs_path.exists():
        try:
            df = pd.read_parquet(hs_path)
            if not df.empty and "ticker" in df.columns:
                return {t if ".T" in str(t) else f"{t}.T" for t in df["ticker"]}
        except Exception:
            pass
    # S3フォールバック
    try:
        from common_cfg.s3io import download_file
        from common_cfg.s3cfg import load_s3_config
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            download_file(cfg, "hold_stocks.parquet", hs_path)
            df = pd.read_parquet(hs_path)
            if not df.empty and "ticker" in df.columns:
                return {t if ".T" in str(t) else f"{t}.T" for t in df["ticker"]}
    except Exception:
        pass
    return set()


def main() -> int:
    print("=" * 60)
    print("Generate Granville Recommendations (IMPLEMENTATION.md §5,§7,§8)")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 60)

    signal_files = sorted(GRANVILLE_DIR.glob("signals_*.parquet"))
    if not signal_files:
        print("[ERROR] No signal files found")
        return 1

    signal_path = signal_files[-1]
    print(f"[1/3] Loading signals: {signal_path.name}")
    signals = pd.read_parquet(signal_path)

    if signals.empty:
        print("[INFO] No signals to recommend")
        return 0

    date_str = signal_path.stem.replace("signals_", "")

    available = load_available_margin()
    hold_tickers = load_hold_stocks()
    print(f"  Available margin: ¥{available:,.0f}")
    print(f"  MAX_HOLD: {RULE_MAX_HOLD}")
    print(f"  Existing positions: {len(hold_tickers)}")

    # §8: 既存保有銘柄を除外
    before = len(signals)
    signals = signals[~signals["ticker"].isin(hold_tickers)].copy()
    if before > len(signals):
        print(f"  Excluded {before - len(signals)} held stocks")

    if signals.empty:
        print("[INFO] No new recommendations (all held)")
        return 0

    # 特徴量計算（スコアリング用）
    print("\n[2/5] Computing features for scoring...")
    ps = pd.read_parquet(PRICES_PATH)
    ps["date"] = pd.to_datetime(ps["date"])
    sig_date = pd.to_datetime(signals["signal_date"].iloc[0])
    # 直近60日分あれば十分
    ps = ps[ps["date"] >= sig_date - pd.Timedelta(days=90)].copy()
    ps = ps.sort_values(["ticker", "date"]).reset_index(drop=True)

    g = ps.groupby("ticker")
    ps["vol20"] = g["Volume"].transform(lambda x: x.rolling(20, min_periods=20).mean())
    ps["vol_ratio"] = ps["Volume"] / ps["vol20"]
    ps["prev_close_tr"] = g["Close"].shift(1)
    ps["tr"] = np.maximum(
        ps["High"] - ps["Low"],
        np.maximum(abs(ps["High"] - ps["prev_close_tr"]), abs(ps["Low"] - ps["prev_close_tr"])),
    )
    ps["atr10"] = g["tr"].transform(lambda x: x.rolling(10, min_periods=10).mean())
    ps["atr10_pct"] = ps["atr10"] / ps["Close"] * 100

    # RSI14
    avg_gain = g["Close"].transform(
        lambda x: x.diff().where(x.diff() > 0, 0).rolling(14, min_periods=14).mean(),
    )
    avg_loss = g["Close"].transform(
        lambda x: (-x.diff()).where(x.diff() < 0, 0).rolling(14, min_periods=14).mean(),
    )
    rs = avg_gain / avg_loss.replace(0, np.nan)
    ps["rsi14"] = 100 - 100 / (1 + rs)

    # ret5d
    ps["ret5d"] = g["Close"].transform(lambda x: x.pct_change(5) * 100)

    # シグナル日の特徴量をマージ
    latest = ps[ps["date"] == sig_date][["ticker", "vol_ratio", "atr10_pct", "rsi14", "ret5d"]].copy()
    signals = signals.merge(latest, on="ticker", how="left")
    signals["vol_ratio"] = signals["vol_ratio"].fillna(1.0)
    signals["atr10_pct"] = signals["atr10_pct"].fillna(3.0)
    signals["rsi14"] = signals["rsi14"].fillna(50.0)
    signals["ret5d"] = signals["ret5d"].fillna(0.0)

    # スコア計算
    print("\n[3/5] Scoring...")
    signals["rank_score"] = signals.apply(
        lambda r: compute_rank_score(r, r["rule"]), axis=1,
    )
    # 期待利益額（100株あたり）
    signals["expected_profit"] = signals.apply(
        lambda r: compute_expected_profit(r["rule"], r["rank_score"], float(r["close"])),
        axis=1,
    )

    # ルール優先 → 同一ルール内はスコア降順
    signals["_priority"] = signals["rule"].map(RULE_PRIORITY)
    signals = signals.sort_values(["_priority", "rank_score"], ascending=[True, False])

    # §7: 証拠金計算（prev_closeベース）
    print("\n[4/5] Computing margins (prev_close based)...")
    if "prev_close" in signals.columns:
        signals["margin"] = signals["prev_close"].apply(_required_margin)
    else:
        # prev_closeが無い場合はentry_price_estで代替
        print("  [WARN] prev_close not in signals, using entry_price_est")
        signals["margin"] = signals["entry_price_est"].apply(_required_margin)

    # §8: 資金制約フィルタリング
    print("\n[5/5] Applying constraints...")
    recommended: list[dict] = []
    remaining = available

    for _, row in signals.iterrows():
        margin = float(row["margin"])

        # §7: 証拠金上限15%チェック（株価フィルター）
        if margin > available * MARGIN_LIMIT_PCT:
            continue

        # 残余力不足
        if margin > remaining:
            continue

        remaining -= margin
        rec = {
            "signal_date": row["signal_date"],
            "ticker": row["ticker"],
            "stock_name": row.get("stock_name", ""),
            "sector": row.get("sector", ""),
            "rule": row["rule"],
            "rank_score": float(row.get("rank_score", 50)),
            "close": float(row["close"]),
            "entry_price_est": float(row["entry_price_est"]),
            "prev_close": float(row.get("prev_close", row["entry_price_est"])),
            "sma20": float(row["sma20"]),
            "dev_from_sma20": float(row["dev_from_sma20"]),
            "atr10_pct": round(float(row.get("atr10_pct", 0)), 2),
            "vol_ratio": round(float(row.get("vol_ratio", 0)), 2),
            "rsi14": round(float(row.get("rsi14", 0)), 1),
            "ret5d": round(float(row.get("ret5d", 0)), 2),
            "expected_profit": int(row.get("expected_profit", 0)),
            "margin": int(margin),
            "margin_pct": round(margin / available * 100, 1),
            "max_hold": _get_max_hold(row["rule"]),
        }
        recommended.append(rec)

    result = pd.DataFrame(recommended)
    if result.empty:
        print("[INFO] No recommendations (all filtered by constraints)")
        return 0

    rec_path = GRANVILLE_DIR / f"recommendations_{date_str}.parquet"
    result.to_parquet(rec_path, index=False)
    print(f"\n[OK] {len(result)} recommendations → {rec_path.name}")

    total_margin = result["margin"].sum()
    print(f"  Total margin: ¥{total_margin:,.0f} / ¥{available:,.0f} "
          f"({total_margin / available * 100:.1f}%)")

    for rule in ["B4", "B1", "B3", "B2"]:
        rdf = result[result["rule"] == rule]
        if not rdf.empty:
            print(f"  {rule}: {len(rdf)} stocks, margin ¥{rdf['margin'].sum():,.0f}")

    print(f"\nRecommendations:")
    for _, row in result.iterrows():
        print(f"  [{row['rule']}] {row['ticker']} {row['stock_name']} "
              f"Score={row['rank_score']:.0f} 期待¥{row['expected_profit']:,} "
              f"¥{row['entry_price_est']:,.0f} ATR={row['atr10_pct']:.1f}% "
              f"margin=¥{row['margin']:,.0f}")

    # S3アップロード
    try:
        cfg = load_s3_config()
        if cfg and cfg.bucket:
            upload_file(cfg, rec_path, f"granville/recommendations_{date_str}.parquet")
    except Exception as e:
        print(f"[WARN] S3 upload failed: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
