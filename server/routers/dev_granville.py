# server/routers/dev_granville.py
"""
グランビル戦略 API（B1-B4, TOPIX 1,660銘柄）
/api/dev/granville/* - 推奨銘柄・シグナル・ポジション・ステータス・統計
"""
from fastapi import APIRouter
from pathlib import Path
import pandas as pd
from datetime import datetime
from typing import Optional
import sys
import os

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

router = APIRouter()

GRANVILLE_DIR = PARQUET_DIR / "granville"
META_PATH = PARQUET_DIR / "meta_jquants.parquet"
META_FALLBACK = PARQUET_DIR / "meta.parquet"
CSV_DIR = ROOT / "data" / "csv"

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", os.getenv("DATA_BUCKET", "stock-api-data"))
_S3_PREFIX_RAW = os.getenv("PARQUET_PREFIX", "parquet")
S3_PREFIX = _S3_PREFIX_RAW.strip("/") if _S3_PREFIX_RAW else "parquet"
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# キャッシュ
_cache: dict[str, tuple[datetime, object]] = {}
CACHE_TTL = 120


def _cached(key: str) -> Optional[object]:
    if key in _cache:
        ts, data = _cache[key]
        if (datetime.now() - ts).total_seconds() < CACHE_TTL:
            return data
    return None


def _set_cache(key: str, data: object) -> None:
    _cache[key] = (datetime.now(), data)


def _latest_file(prefix: str) -> Optional[Path]:
    """granvilleディレクトリから最新の日付ファイルを取得"""
    files = sorted(GRANVILLE_DIR.glob(f"{prefix}_*.parquet"))
    return files[-1] if files else None


def _load_latest(prefix: str) -> pd.DataFrame:
    """最新ファイルを読み込み（キャッシュ付き）"""
    cached = _cached(prefix)
    if cached is not None:
        return cached

    path = _latest_file(prefix)
    if path is None:
        # S3フォールバック
        try:
            import boto3
            s3 = boto3.client("s3", region_name=AWS_REGION)
            resp = s3.list_objects_v2(
                Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/granville/{prefix}_",
            )
            if "Contents" in resp:
                keys = sorted([o["Key"] for o in resp["Contents"]])
                if keys:
                    local = GRANVILLE_DIR / Path(keys[-1]).name
                    GRANVILLE_DIR.mkdir(parents=True, exist_ok=True)
                    s3.download_file(S3_BUCKET, keys[-1], str(local))
                    path = local
        except Exception:
            pass

    if path is None:
        return pd.DataFrame()

    df = pd.read_parquet(path)
    _set_cache(prefix, df)
    return df


def _safe_int(v) -> int:
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0


def _safe_float(v, decimals: int = 1) -> float:
    try:
        return round(float(v), decimals)
    except (ValueError, TypeError):
        return 0.0


# ==========================================
# エンドポイント
# ==========================================

@router.get("/api/dev/granville/recommendations")
async def get_recommendations():
    """当日推奨銘柄（B4>B1>B3>B2、到着順、15%証拠金上限済み）"""
    df = _load_latest("recommendations")
    if df.empty:
        return {"recommendations": [], "count": 0, "date": None}

    date_str = None
    if "signal_date" in df.columns:
        date_str = pd.to_datetime(df["signal_date"].iloc[0]).strftime("%Y-%m-%d")

    recs = []
    for _, r in df.iterrows():
        rec = {
            "ticker": r.get("ticker", ""),
            "stock_name": r.get("stock_name", ""),
            "sector": r.get("sector", ""),
            "rule": r.get("rule", ""),
            "rank_score": _safe_float(r.get("rank_score", 0)),
            "close": _safe_float(r.get("close", 0)),
            "entry_price_est": _safe_float(r.get("entry_price_est", 0)),
            "prev_close": _safe_float(r.get("prev_close", 0)),
            "sma20": _safe_float(r.get("sma20", 0), 2),
            "dev_from_sma20": _safe_float(r.get("dev_from_sma20", 0), 3),
            "atr10_pct": _safe_float(r.get("atr10_pct", 0), 2),
            "vol_ratio": _safe_float(r.get("vol_ratio", 0), 2),
            "expected_profit": _safe_int(r.get("expected_profit", 0)),
            "margin": _safe_int(r.get("margin", 0)),
            "margin_pct": _safe_float(r.get("margin_pct", 0)),
            "max_hold": _safe_int(r.get("max_hold", 0)),
        }
        recs.append(rec)

    total_margin = sum(r["margin"] for r in recs)

    return {
        "recommendations": recs,
        "count": len(recs),
        "total_margin": total_margin,
        "date": date_str,
    }


@router.get("/api/dev/granville/signals")
async def get_signals():
    """当日全シグナル（フィルター前）"""
    df = _load_latest("signals")
    if df.empty:
        return {"signals": [], "count": 0, "signal_date": None}

    date_str = None
    if "signal_date" in df.columns:
        date_str = pd.to_datetime(df["signal_date"].iloc[0]).strftime("%Y-%m-%d")

    signals = []
    for _, r in df.iterrows():
        sig = {
            "ticker": r.get("ticker", ""),
            "stock_name": r.get("stock_name", ""),
            "sector": r.get("sector", ""),
            "rule": r.get("rule", ""),
            "close": _safe_float(r.get("close", 0)),
            "sma20": _safe_float(r.get("sma20", 0), 2),
            "dev_from_sma20": _safe_float(r.get("dev_from_sma20", 0), 3),
            "sma20_slope": _safe_float(r.get("sma20_slope", 0), 4),
            "entry_price_est": _safe_float(r.get("entry_price_est", 0)),
            "prev_close": _safe_float(r.get("prev_close", 0)),
        }
        signals.append(sig)

    return {
        "signals": signals,
        "count": len(signals),
        "signal_date": date_str,
    }


@router.get("/api/dev/granville/positions")
async def get_positions():
    """保有ポジション（MarketSpeed実データ）+ Exit候補（シグナル算出）"""
    # 実保有ポジション（hold_stocks.parquet）
    hold_df = _load_hold_stocks()
    positions = []
    as_of = None

    if not hold_df.empty:
        if "as_of" in hold_df.columns:
            as_of = str(hold_df["as_of"].iloc[0])[:10]

        # 20日高値計算用に株価データを読み込み
        prices_path = PARQUET_DIR / "prices_max_1d.parquet"
        prices_df = None
        if prices_path.exists():
            try:
                prices_df = pd.read_parquet(prices_path)
                prices_df["date"] = pd.to_datetime(prices_df["date"])
            except Exception:
                pass

        # prices_max_1d.parquetにない保有銘柄をyfinanceで補完
        hold_tickers = [str(r.get("ticker", "")) for _, r in hold_df.iterrows()]
        hold_tickers_t = [t if ".T" in t else f"{t}.T" for t in hold_tickers]
        missing = [t for t in hold_tickers_t if prices_df is None or prices_df[prices_df["ticker"] == t].empty]
        if missing:
            try:
                import yfinance as yf
                yf_tickers = [t.replace("_", ".") for t in missing]
                df_yf = yf.download(yf_tickers, period="30d", interval="1d", progress=False, threads=True)
                if not df_yf.empty:
                    rows_yf = []
                    if isinstance(df_yf.columns, pd.MultiIndex):
                        for i, t_yf in enumerate(yf_tickers):
                            try:
                                sub = df_yf.xs(t_yf, level=1, axis=1).dropna(subset=["Close"]).copy()
                                sub["ticker"] = missing[i]
                                sub["date"] = sub.index
                                sub = sub.reset_index(drop=True)
                                rows_yf.append(sub[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]])
                            except Exception:
                                pass
                    else:
                        df_yf = df_yf.dropna(subset=["Close"])
                        df_yf["ticker"] = missing[0]
                        df_yf["date"] = df_yf.index
                        df_yf = df_yf.reset_index(drop=True)
                        rows_yf.append(df_yf[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]])
                    if rows_yf:
                        extra = pd.concat(rows_yf, ignore_index=True)
                        extra["date"] = pd.to_datetime(extra["date"]).dt.tz_localize(None)
                        if prices_df is not None:
                            prices_df = pd.concat([prices_df, extra], ignore_index=True)
                        else:
                            prices_df = extra
            except Exception:
                pass

        today = pd.Timestamp.now().normalize()

        for _, r in hold_df.iterrows():
            cost = _safe_float(r.get("cost_total", 0))
            mv = _safe_float(r.get("market_value", 0))
            qty = _safe_int(r.get("quantity", 100))
            entry_price = round(cost / qty) if qty > 0 else 0
            current_price = _safe_float(r.get("current_price", 0))
            pnl = _safe_float(r.get("unrealized_pnl", 0))
            pct = _safe_float(r.get("unrealized_pct", 0), 2)
            direction = str(r.get("direction", ""))
            ticker = str(r.get("ticker", ""))

            # 建日: 弁済期限(expiry_date)から6ヶ月逆算
            entry_date = ""
            hold_days = 0
            expiry = r.get("expiry_date", r.get("deadline", ""))
            if expiry and str(expiry) not in ("", "nan", "NaT"):
                try:
                    exp_dt = pd.to_datetime(str(expiry))
                    entry_dt = exp_dt - pd.DateOffset(months=6)
                    entry_date = entry_dt.strftime("%Y-%m-%d")
                    # 営業日ベースの保有日数
                    bdays = pd.bdate_range(entry_dt, today)
                    hold_days = max(0, len(bdays) - 1)
                except Exception:
                    pass

            # 20日高値
            high_20d = 0.0
            # ticker形式の正規化: "4151" → "4151.T"
            price_ticker = ticker if ".T" in ticker else f"{ticker}.T"
            if prices_df is not None and price_ticker:
                tk_df = prices_df[prices_df["ticker"] == price_ticker].sort_values("date")
                if not tk_df.empty:
                    high_20d = float(tk_df["High"].tail(20).max())

            # 買建: exit指値=20日高値, 売建: exit指値=20日安値
            if direction == "売建" and prices_df is not None and price_ticker:
                tk_df = prices_df[prices_df["ticker"] == price_ticker].sort_values("date")
                if not tk_df.empty:
                    low_20d = float(tk_df["Low"].tail(20).min())
                    gap = _safe_int(round(current_price - low_20d)) if low_20d > 0 else 0
                    high_20d = low_20d  # 売建はexit基準を安値にする
                    gap_to_high = -gap
                else:
                    gap_to_high = 0
            else:
                gap_to_high = _safe_int(round(high_20d - current_price)) if high_20d > 0 else 0

            positions.append({
                "ticker": ticker,
                "stock_name": str(r.get("stock_name", "")),
                "rule": "",
                "direction": direction,
                "margin_type": str(r.get("margin_type", "")),
                "deadline": str(r.get("deadline", "")),
                "entry_date": entry_date,
                "entry_price": entry_price,
                "current_price": current_price,
                "quantity": qty,
                "cost_total": _safe_int(cost),
                "market_value": _safe_int(mv),
                "high_20d": high_20d,
                "atr10": 0,
                "gap_to_high": gap_to_high,
                "unrealized_pct": pct,
                "unrealized_yen": _safe_int(pnl),
                "hold_days": hold_days,
                "max_hold": 0,
                "remaining_days": 0,
                "exit_type": "",
            })

    # Exit候補（hold_stocksの実際の建日に紐づくもののみ）
    exits = []
    calc_df = _load_latest("positions")

    # hold_stocksのticker→建日マップ（弁済期限から6ヶ月逆算）
    hold_entry_map = {}  # {ticker_t: entry_date}
    if not hold_df.empty and "ticker" in hold_df.columns:
        for _, hr in hold_df.iterrows():
            tk = str(hr.get("ticker", ""))
            tk_t = tk if ".T" in tk else f"{tk}.T"
            expiry = hr.get("expiry_date", hr.get("deadline", ""))
            if expiry and str(expiry) not in ("", "nan", "NaT"):
                try:
                    entry_dt = pd.to_datetime(str(expiry)) - pd.DateOffset(months=6)
                    hold_entry_map[tk_t] = entry_dt.normalize()
                except Exception:
                    hold_entry_map[tk_t] = None
            else:
                hold_entry_map[tk_t] = None

    if not calc_df.empty and "status" in calc_df.columns and hold_entry_map:
        # hold_stocksにある銘柄のpositionsを取得（exit/open問わず）
        hold_tickers_t = set(hold_entry_map.keys())
        held_pos = calc_df[calc_df["ticker"].isin(hold_tickers_t)].copy()

        if not held_pos.empty:
            # 各hold銘柄について、実際の建日に最も近いpositionを選択
            if "stock_name" not in held_pos.columns:
                for p_path in [META_PATH, META_FALLBACK]:
                    if p_path.exists():
                        meta = pd.read_parquet(p_path, columns=["ticker", "stock_name"])
                        name_map = dict(zip(meta["ticker"], meta["stock_name"]))
                        held_pos["stock_name"] = held_pos["ticker"].map(name_map).fillna("")
                        break
                else:
                    held_pos["stock_name"] = ""

            for tk_t, hold_entry_dt in hold_entry_map.items():
                tk_pos = held_pos[held_pos["ticker"] == tk_t]
                if tk_pos.empty:
                    continue

                # 建日と一致するentry_dateのpositionのみ（granvilleエントリーのみ）
                if hold_entry_dt is not None and "entry_date" in tk_pos.columns:
                    tk_pos = tk_pos.copy()
                    tk_pos["_entry_dt"] = pd.to_datetime(tk_pos["entry_date"]).dt.normalize()
                    matched = tk_pos[tk_pos["_entry_dt"] == hold_entry_dt]
                    if matched.empty:
                        continue  # granvilleシグナルと建日が一致しない → 非granvilleポジション
                    best = matched.iloc[-1]
                    # exit条件に達したもののみ表示
                    if best.get("status") != "exit":
                        continue
                else:
                    continue

                cp = _safe_float(best.get("current_price", 0))
                high_20d = _safe_float(best.get("high_20d", 0))
                exits.append({
                    "ticker": best.get("ticker", ""),
                    "stock_name": best.get("stock_name", ""),
                    "rule": best.get("rule", ""),
                    "direction": "",
                    "margin_type": "",
                    "deadline": "",
                    "entry_date": pd.to_datetime(best["entry_date"]).strftime("%Y-%m-%d") if "entry_date" in best else "",
                    "entry_price": _safe_float(best.get("entry_price", 0)),
                    "current_price": cp,
                    "quantity": 100,
                    "cost_total": 0,
                    "market_value": 0,
                    "high_20d": high_20d,
                    "atr10": _safe_float(best.get("atr10", 0)),
                    "gap_to_high": _safe_int(round(high_20d - cp)) if high_20d > 0 else 0,
                    "unrealized_pct": _safe_float(best.get("pct", 0), 2),
                    "unrealized_yen": _safe_int(best.get("pnl", 0)),
                    "hold_days": _safe_int(best.get("hold_days", 0)),
                    "max_hold": _safe_int(best.get("max_hold", 0)),
                    "remaining_days": max(0, _safe_int(best.get("max_hold", 0)) - _safe_int(best.get("hold_days", 0))),
                    "exit_type": best.get("exit_type", "") if best.get("status") == "exit" else "",
                    "status": best.get("status", ""),
                })

    return {"positions": positions, "exits": exits, "as_of": as_of}


def _load_credit_status() -> dict:
    """現金保証金と信用余力を取得（credit_status.parquet → CSV フォールバック）"""
    cash_margin = 0
    # credit_status.parquet（generate_stock_results_html.py が生成）
    cs_path = PARQUET_DIR / "credit_status.parquet"
    if cs_path.exists():
        try:
            cs = pd.read_parquet(cs_path)
            row = cs[cs["asset"].str.contains("信用", na=False)]
            if not row.empty:
                cash_margin = int(row["value"].iloc[0])
        except Exception:
            pass

    # フォールバック: CSV
    if cash_margin == 0:
        credit_csv = CSV_DIR / "credit_capacity.csv"
        if credit_csv.exists():
            try:
                cc = pd.read_csv(credit_csv)
                if "available_margin" in cc.columns and not cc.empty:
                    cash_margin = int(float(cc.iloc[-1]["available_margin"]))
                elif not cc.empty:
                    # MarketSpeed形式: 現金保証金(信用) の行
                    for _, r in cc.iterrows():
                        if "信用" in str(r.iloc[0]):
                            cash_margin = int(float(str(r.iloc[1]).replace(",", "")))
                            break
            except Exception:
                pass

    if cash_margin == 0:
        cash_margin = 4_650_000  # デフォルト

    # 信用余力 = 現金保証金 / 委託保証金率(30%) - 既存ポジション評価額
    position_value = 0
    hs_path = PARQUET_DIR / "hold_stocks.parquet"
    if hs_path.exists():
        try:
            hs = pd.read_parquet(hs_path)
            if "market_value" in hs.columns:
                position_value = int(hs["market_value"].abs().sum())
        except Exception:
            pass

    credit_capacity = int(cash_margin / 0.3) - position_value

    return {
        "cash_margin": cash_margin,
        "credit_capacity": max(0, credit_capacity),
        "position_value": position_value,
    }


def _load_hold_stocks() -> pd.DataFrame:
    """hold_stocks.parquet から実保有ポジションを取得"""
    hs_path = PARQUET_DIR / "hold_stocks.parquet"
    if hs_path.exists():
        try:
            return pd.read_parquet(hs_path)
        except Exception:
            pass
    # S3フォールバック
    try:
        import boto3
        s3 = boto3.client("s3", region_name=AWS_REGION)
        local = PARQUET_DIR / "hold_stocks.parquet"
        s3.download_file(S3_BUCKET, f"{S3_PREFIX}/hold_stocks.parquet", str(local))
        return pd.read_parquet(local)
    except Exception:
        return pd.DataFrame()


@router.get("/api/dev/granville/status")
async def get_status():
    """現金保証金・信用余力・ポジション数・シグナル数"""
    credit = _load_credit_status()

    # シグナル数
    signals_df = _load_latest("signals")
    signal_count = len(signals_df)
    signal_date = None
    if not signals_df.empty and "signal_date" in signals_df.columns:
        signal_date = pd.to_datetime(signals_df["signal_date"].iloc[0]).strftime("%Y-%m-%d")

    # 実保有ポジション数
    hold_df = _load_hold_stocks()
    hold_count = len(hold_df)

    # 算出ポジション（Exit候補用）
    pos_df = _load_latest("positions")
    exit_count = 0
    if not pos_df.empty and "status" in pos_df.columns:
        exit_count = int((pos_df["status"] == "exit").sum())

    # 推奨数
    rec_df = _load_latest("recommendations")
    rec_count = len(rec_df)
    total_margin_used = 0
    if not rec_df.empty and "margin" in rec_df.columns:
        total_margin_used = int(rec_df["margin"].sum())

    # ルール別内訳
    rule_breakdown = {}
    for rule in ["B4", "B1", "B3", "B2"]:
        if not signals_df.empty and "rule" in signals_df.columns:
            rule_breakdown[rule] = int((signals_df["rule"] == rule).sum())
        else:
            rule_breakdown[rule] = 0

    return {
        "cash_margin": credit["cash_margin"],
        "credit_capacity": credit["credit_capacity"],
        "position_value": credit["position_value"],
        "total_margin_used": total_margin_used,
        "signal_count": signal_count,
        "signal_date": signal_date,
        "open_positions": hold_count,
        "exit_candidates": exit_count,
        "recommendation_count": rec_count,
        "rule_breakdown": rule_breakdown,
    }


@router.get("/api/dev/granville/stats")
async def get_stats():
    """ルール別勝率、月別PnL（B1-B4バックテストアーカイブから）"""
    archive_path = PARQUET_DIR / "backtest" / "granville_b1b4_archive.parquet"
    if not archive_path.exists():
        # S3フォールバック
        try:
            import boto3
            s3 = boto3.client("s3", region_name=AWS_REGION)
            s3_key = f"{S3_PREFIX}/backtest/granville_b1b4_archive.parquet"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(S3_BUCKET, s3_key, str(archive_path))
        except Exception:
            return {"by_rule": {}, "monthly": [], "total_trades": 0}

    if not archive_path.exists():
        return {"by_rule": {}, "monthly": [], "total_trades": 0}

    cached = _cached("stats_archive")
    if cached is not None:
        df = cached
    else:
        df = pd.read_parquet(archive_path)
        _set_cache("stats_archive", df)

    if df.empty:
        return {"by_rule": {}, "monthly": [], "total_trades": 0}

    df["ret_pct"] = pd.to_numeric(df["ret_pct"], errors="coerce")
    df["pnl_yen"] = pd.to_numeric(df["pnl_yen"], errors="coerce").fillna(0).astype(int)
    for col in ["entry_date", "exit_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    # B1-B4 ルール別統計
    by_rule = {}
    for rule in ["B4", "B1", "B3", "B2"]:
        gdf = df[df["rule"] == rule]
        n = len(gdf)
        if n == 0:
            by_rule[rule] = {"count": 0, "win_rate": 0.0, "total_pnl": 0, "avg_pnl": 0, "avg_pct": 0.0, "exit_20d_high": 0, "exit_max_hold": 0}
            continue
        wins = int((gdf["ret_pct"] > 0).sum())
        h20 = int((gdf["exit_type"] == "20d_high").sum()) if "exit_type" in gdf.columns else 0
        mh = int((gdf["exit_type"] == "max_hold").sum()) if "exit_type" in gdf.columns else 0
        by_rule[rule] = {
            "count": n,
            "win_rate": _safe_float(wins / n * 100),
            "total_pnl": _safe_int(gdf["pnl_yen"].sum()),
            "avg_pnl": _safe_int(gdf["pnl_yen"].mean()),
            "avg_pct": _safe_float(gdf["ret_pct"].mean(), 2),
            "exit_20d_high": h20,
            "exit_max_hold": mh,
        }

    # 月別統計
    monthly = []
    if "entry_date" in df.columns:
        df["month"] = df["entry_date"].dt.strftime("%Y-%m")
        for month, mdf in df.groupby("month"):
            n = len(mdf)
            wins = int((mdf["ret_pct"] > 0).sum())
            monthly.append({
                "month": month,
                "count": n,
                "pnl": _safe_int(mdf["pnl_yen"].sum()),
                "win_rate": _safe_float(wins / n * 100),
            })
        monthly.sort(key=lambda x: x["month"], reverse=True)

    return {
        "by_rule": by_rule,
        "monthly": monthly[:24],
        "total_trades": len(df),
    }
