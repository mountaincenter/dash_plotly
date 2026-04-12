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
_IS_LOCAL = PARQUET_DIR.exists()


# キャッシュ
_cache: dict[str, tuple[datetime, object]] = {}
CACHE_TTL = 120


@router.post("/api/dev/granville/refresh")
async def refresh_cache():
    """キャッシュクリア+S3から再ダウンロード+即時反映。"""
    from datetime import datetime
    _cache.clear()

    # staging S3から最新データを強制ダウンロード
    refreshed = []
    for f in ["hold_stocks.parquet", "orders.parquet", "credit_status.parquet",
              "nikkei_vi_max_1d.parquet", "index_prices_max_1d.parquet",
              "futures_prices_max_1d.parquet"]:
        local = PARQUET_DIR / f
        if _s3_download(f, local):
            refreshed.append(f)

    # hold_stocks件数
    hold_count = 0
    hold_path = PARQUET_DIR / "hold_stocks.parquet"
    if hold_path.exists():
        try:
            hold_count = len(pd.read_parquet(hold_path))
        except Exception:
            pass

    return {
        "status": "success",
        "message": "Cache refreshed",
        "hold_stocks": hold_count,
        "refreshed_files": refreshed,
        "updated_at": datetime.now().isoformat(),
    }


def _s3_download(s3_key: str, local_path: Path) -> bool:
    """S3からファイルをダウンロード。ディレクトリ作成含む。"""
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        import boto3
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.download_file(S3_BUCKET, f"{S3_PREFIX}/{s3_key}", str(local_path))
        return True
    except Exception as e:
        print(f"[S3] download failed: {S3_BUCKET}/{S3_PREFIX}/{s3_key} → {e}")
        return False


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


@router.get("/api/dev/granville/long-recommendations")
async def get_long_recommendations():
    """B1-B3ロング推奨（H1/H2/H3フィルター通過分）"""
    df = _load_latest("long_recommendations")
    if df.empty:
        return {"long_recommendations": [], "count": 0, "date": None, "regime": {}}

    date_str = None
    if "signal_date" in df.columns:
        date_str = pd.to_datetime(df["signal_date"].iloc[0]).strftime("%Y-%m-%d")

    recs = []
    for _, r in df.iterrows():
        recs.append({
            "ticker": r.get("ticker", ""),
            "stock_name": r.get("stock_name", ""),
            "sector": r.get("sector", ""),
            "rule": r.get("rule", ""),
            "long_grade": r.get("long_grade", ""),
            "hold_days": _safe_int(r.get("hold_days", 0)),
            "close": _safe_float(r.get("close", 0)),
            "entry_price_est": _safe_float(r.get("entry_price_est", 0)),
            "sma20": _safe_float(r.get("sma20", 0), 2),
            "dev_from_sma20": _safe_float(r.get("dev_from_sma20", 0), 3),
            "atr10_pct": _safe_float(r.get("atr10_pct", 0), 2),
            "expected_pf": _safe_float(r.get("expected_pf", 0), 2),
        })

    # レジーム情報も返す
    regime = _get_current_regime()

    return {
        "long_recommendations": recs,
        "count": len(recs),
        "date": date_str,
        "regime": regime,
    }


def _read_parquet_by_env(filename: str) -> pd.DataFrame:
    """環境フラグに応じてparquetを完全分離で読み込む（フォールバック禁止）。"""
    if _IS_LOCAL:
        return pd.read_parquet(PARQUET_DIR / filename)

    import boto3
    import io

    s3 = boto3.client("s3", region_name=AWS_REGION)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}/{filename}")
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _get_current_regime() -> dict:
    """現在の市場レジーム情報を取得"""
    regime = {"n225_above_sma20": None, "n225_ret20": None, "cme_gap": None, "vi": None,
              "n225_close": None, "n225_sma20": None}

    try:
        idx = _read_parquet_by_env("index_prices_max_1d.parquet")
        idx["date"] = pd.to_datetime(idx["date"])
        n225 = idx[idx["ticker"] == "^N225"][["date", "Close"]].sort_values("date").dropna(subset=["Close"])
        n225["sma20"] = n225["Close"].rolling(20).mean()
        n225["ret20"] = n225["Close"].pct_change(20) * 100
        latest = n225.dropna(subset=["sma20"]).iloc[-1]
        regime["n225_above_sma20"] = bool(latest["Close"] > latest["sma20"])
        regime["n225_ret20"] = round(float(latest["ret20"]), 2) if pd.notna(latest["ret20"]) else None
        regime["n225_close"] = round(float(latest["Close"]), 0)
        regime["n225_sma20"] = round(float(latest["sma20"]), 0)
    except Exception:
        pass

    try:
        vi_df = _read_parquet_by_env("nikkei_vi_max_1d.parquet")
        vi_df["date"] = pd.to_datetime(vi_df["date"])
        regime["vi"] = round(float(vi_df.sort_values("date").iloc[-1]["close"]), 1)
    except Exception:
        pass

    try:
        fut = _read_parquet_by_env("futures_prices_max_1d.parquet")
        fut["date"] = pd.to_datetime(fut["date"])
        nkd = fut[fut["ticker"] == "NKD=F"].sort_values("date")
        idx = _read_parquet_by_env("index_prices_max_1d.parquet")
        idx["date"] = pd.to_datetime(idx["date"])
        n225 = idx[idx["ticker"] == "^N225"].sort_values("date")
        if len(nkd) >= 1 and len(n225) >= 2:
            nkd_close = float(nkd.iloc[-1]["Close"])
            n225_prev = float(n225.iloc[-2]["Close"])
            regime["cme_gap"] = round((nkd_close / n225_prev - 1) * 100, 2)
    except Exception:
        pass

    return regime


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

            # エントリー後rolling高値（trigger_price: これを超えたら翌朝売り）
            trigger_price = 0.0
            price_ticker = ticker if ".T" in ticker else f"{ticker}.T"
            if prices_df is not None and price_ticker and entry_date:
                tk_df = prices_df[prices_df["ticker"] == price_ticker].sort_values("date")
                if not tk_df.empty:
                    entry_dt = pd.to_datetime(entry_date)
                    # エントリー日以降のHigh
                    after_entry = tk_df[tk_df["date"] >= entry_dt]
                    if not after_entry.empty:
                        trigger_price = float(after_entry["High"].max())

            # 買建: trigger_price超えで翌朝売り
            # 売建: 20日安値割れで翌朝買い戻し
            if direction == "売建" and prices_df is not None and price_ticker:
                tk_df = prices_df[prices_df["ticker"] == price_ticker].sort_values("date")
                if not tk_df.empty:
                    low_20d = float(tk_df["Low"].tail(20).min())
                    trigger_price = low_20d
                    gap_to_high = _safe_int(round(current_price - low_20d)) if low_20d > 0 else 0
                else:
                    gap_to_high = 0
            else:
                gap_to_high = _safe_int(round(trigger_price - current_price)) if trigger_price > 0 else 0

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
                "high_20d": trigger_price,
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
                trigger = _safe_float(best.get("trigger_price", best.get("high_20d", 0)))
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
                    "high_20d": trigger,
                    "atr10": _safe_float(best.get("atr10", 0)),
                    "gap_to_high": _safe_int(round(trigger - cp)) if trigger > 0 else 0,
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
    """現金保証金と信用余力を取得（S3優先）"""
    cash_margin = 0

    # S3からcredit_status.parquetを取得
    cs_path = PARQUET_DIR / "credit_status.parquet"
    _s3_download("credit_status.parquet", cs_path)

    if cs_path.exists():
        try:
            cs = pd.read_parquet(cs_path)
            row = cs[cs["asset"].str.contains("信用", na=False)]
            if not row.empty:
                cash_margin = int(row["value"].iloc[0])
        except Exception:
            pass

    if cash_margin == 0:
        cash_margin = 4_650_000  # デフォルト

    # 信用余力 = (現金保証金 - 必要保証金) / 委託保証金率(30%)
    position_value = 0
    hold_df = _load_hold_stocks()
    if not hold_df.empty and "market_value" in hold_df.columns:
        position_value = int(hold_df["market_value"].abs().sum())

    # 必要保証金 = 建玉金額合計 * 30%
    required_margin = 0
    if not hold_df.empty and "cost_total" in hold_df.columns:
        required_margin = int(hold_df["cost_total"].abs().sum() * 0.3)

    credit_capacity = int((cash_margin - required_margin) / 0.3)

    return {
        "cash_margin": cash_margin,
        "credit_capacity": max(0, credit_capacity),
        "position_value": position_value,
    }


def _load_hold_stocks() -> pd.DataFrame:
    """hold_stocks.parquet をS3から取得"""
    local = PARQUET_DIR / "hold_stocks.parquet"
    _s3_download("hold_stocks.parquet", local)
    if local.exists():
        try:
            return pd.read_parquet(local)
        except Exception:
            pass
    return pd.DataFrame()


def _compute_triggers() -> dict:
    """戦略トリガー: CMEギャップ、SMA乖離、VIX"""
    import numpy as np
    result = {
        "cme_gap": None, "cme_signal": "unknown", "cme_close": None,
        "nk_close": None, "sma20_60_gap": None, "sma_signal": "unknown",
        "vix": None, "vix_signal": "unknown",
        "strategy": "待機",
    }

    try:
        # N225 SMA（パイプラインのparquetから）
        n225_path = PARQUET_DIR / "index_prices_max_1d.parquet"
        if n225_path.exists():
            idx_df = pd.read_parquet(n225_path)
            nk = idx_df[idx_df["ticker"] == "^N225"].copy()
            if not nk.empty:
                nk = nk.sort_values("date")
                nk["sma20"] = nk["Close"].rolling(20).mean()
                nk["sma60"] = nk["Close"].rolling(60).mean()
                latest_nk = nk.iloc[-1]
                result["nk_close"] = round(float(latest_nk["Close"]), 0)
                if not np.isnan(latest_nk["sma20"]) and not np.isnan(latest_nk["sma60"]):
                    gap = (latest_nk["sma20"] / latest_nk["sma60"] - 1) * 100
                    result["sma20_60_gap"] = round(gap, 2)
                    if gap <= -3:
                        result["sma_signal"] = "green"  # B4最強帯
                    elif gap <= 0:
                        result["sma_signal"] = "yellow"  # DC済み
                    elif gap <= 3:
                        result["sma_signal"] = "red"  # 現在帯、期待値マイナス
                    else:
                        result["sma_signal"] = "red"

        # CME 22時ギャップ（パイプラインのparquetから）
        nkd_path = PARQUET_DIR / "futures_prices_max_1d.parquet"
        if nkd_path.exists() and result["nk_close"]:
            fut_df = pd.read_parquet(nkd_path)
            nkd_df = fut_df[fut_df["ticker"] == "NKD=F"].copy()
            if not nkd_df.empty:
                nkd_df = nkd_df.sort_values("date")
                cme_close = float(nkd_df["Close"].iloc[-1])
                gap = (cme_close / result["nk_close"] - 1) * 100
                result["cme_close"] = round(cme_close, 0)
                result["cme_gap"] = round(gap, 2)
                if gap <= -2:
                    result["cme_signal"] = "green"
                elif gap <= -0.5:
                    result["cme_signal"] = "yellow"
                elif gap >= 1:
                    result["cme_signal"] = "green"
                elif abs(gap) < 0.5:
                    result["cme_signal"] = "red"
                else:
                    result["cme_signal"] = "yellow"

        # VIX（パイプラインのparquetから）
        # production pipelineがindex_prices_max_1dに^VIXを含む
        if n225_path.exists():
            idx_df = pd.read_parquet(n225_path) if "idx_df" not in dir() else idx_df
            vix_df = idx_df[idx_df["ticker"] == "^VIX"].copy()
            if not vix_df.empty:
                vix_df = vix_df.sort_values("date")
                v = float(vix_df["Close"].iloc[-1])
                result["vix"] = round(v, 2)
                if v <= 20:
                    result["vix_signal"] = "green"
                elif v <= 30:
                    result["vix_signal"] = "yellow"
                else:
                    result["vix_signal"] = "red"

        # 総合判定
        signals = [result["cme_signal"], result["sma_signal"], result["vix_signal"]]
        greens = signals.count("green")
        reds = signals.count("red")
        if greens >= 2:
            result["strategy"] = "積極"
        elif reds >= 2:
            result["strategy"] = "消極"
        elif result["cme_signal"] == "green":
            result["strategy"] = "限定エントリー"
        else:
            result["strategy"] = "待機"

    except Exception as e:
        print(f"[triggers] Error: {e}")

    return result


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

    # トリガー情報（CMEギャップ、SMA、VIX）
    triggers = _compute_triggers()

    # ロング推奨
    long_df = _load_latest("long_recommendations")
    long_count = len(long_df)
    long_grades = {}
    if not long_df.empty and "long_grade" in long_df.columns:
        long_grades = long_df["long_grade"].value_counts().to_dict()

    regime = _get_current_regime()

    return {
        "triggers": triggers,
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
        "long_recommendation_count": long_count,
        "long_grades": long_grades,
        "regime": regime,
    }


@router.get("/api/dev/granville/b4_entry")
async def get_b4_entry():
    """B4エントリー判定: VI + good_count(乖離+ATR+ret5d)で上位3件"""
    import numpy as np

    # 市場環境データ取得（シグナル有無に関わらず常に取得）
    vi_val = None
    try:
        vi_df = _read_parquet_by_env("nikkei_vi_max_1d.parquet")
        vi_df["date"] = pd.to_datetime(vi_df["date"])
        vi_df = vi_df.sort_values("date")
        vi_val = float(vi_df["close"].iloc[-1])
    except Exception:
        pass

    cme_gap = None
    n225_chg = None
    try:
        idx_df = _read_parquet_by_env("index_prices_max_1d.parquet")
        n225_df = idx_df[idx_df["ticker"] == "^N225"].copy()
        if not n225_df.empty:
            n225_df = n225_df.sort_values("date").tail(5)
            if len(n225_df) >= 2:
                n225_chg = round((float(n225_df["Close"].iloc[-1]) / float(n225_df["Close"].iloc[-2]) - 1) * 100, 2)
            fut_df = _read_parquet_by_env("futures_prices_max_1d.parquet")
            nkd_df = fut_df[fut_df["ticker"] == "NKD=F"].copy()
            if not nkd_df.empty:
                nkd_df = nkd_df.sort_values("date").tail(5)
                nkd_close = float(nkd_df["Close"].iloc[-1])
                n225_close = float(n225_df["Close"].iloc[-1])
                if n225_close > 0:
                    cme_gap = round((nkd_close - n225_close) / n225_close * 100, 2)
    except Exception:
        pass

    _base_env = {
        "vi": vi_val, "cme_gap": cme_gap, "n225_chg": n225_chg,
        "excluded_rules": [], "weekday": None,
        "total_b4_signals": 0, "candidates": [], "selected": [],
        "selected_cost": 0, "budget_remaining": 0, "date": None,
    }

    # 今日のシグナル（B4のみ）
    signals_df = _load_latest("signals")
    if signals_df.empty:
        return {**_base_env, "decision": "no_signal"}

    b4 = signals_df[signals_df["rule"] == "B4"].copy() if "rule" in signals_df.columns else pd.DataFrame()
    if b4.empty:
        date_str = pd.to_datetime(signals_df["signal_date"].iloc[0]).strftime("%Y-%m-%d") if "signal_date" in signals_df.columns else None
        return {**_base_env, "decision": "no_b4", "date": date_str}

    date_str = pd.to_datetime(b4["signal_date"].iloc[0]).strftime("%Y-%m-%d") if "signal_date" in b4.columns else None

    # 除外ルール判定（3ルール）
    excluded_rules = []
    if vi_val and cme_gap is not None:
        if (vi_val >= 30) and (vi_val < 40) and (cme_gap >= -1) and (cme_gap < 1):
            excluded_rules.append("VI30-40×膠着")
        if (vi_val >= 30) and (vi_val < 40) and (cme_gap >= 1):
            excluded_rules.append("VI30-40×GU")
    if n225_chg is not None and n225_chg < -3:
        excluded_rules.append("N225<-3%")

    from scripts.lib.price_limit import calc_max_cost_100

    # 候補生成（乖離深い順）
    candidates = []
    for _, r in b4.iterrows():
        tk = r.get("ticker", "")
        dev = float(r.get("dev_from_sma20", 0))
        atr = float(r.get("atr10_pct", 0))
        ret5d = float(r.get("ret5d", 0))
        close_price = float(r.get("close", r.get("entry_price_est", 0)))
        cost = calc_max_cost_100(close_price)

        candidates.append({
            "ticker": tk,
            "stock_name": r.get("stock_name", ""),
            "sector": r.get("sector", ""),
            "close": _safe_float(r.get("close", 0)),
            "entry_price_est": _safe_float(r.get("entry_price_est", r.get("close", 0))),
            "dev_from_sma20": _safe_float(dev, 2),
            "atr_pct": _safe_float(atr, 2),
            "ret5d": _safe_float(ret5d, 2),
            "max_cost": cost,
            "expected_pf": 2.79,  # B4全期間PF（急騰フィルター付き）
        })

    # 乖離深い順（検証済み）
    candidates.sort(key=lambda x: x["dev_from_sma20"])

    # 余力の許す限り選定（件数無制限）
    budget = 3_000_000  # デフォルト余力
    selected = []
    for c in candidates:
        if budget >= c["max_cost"]:
            budget -= c["max_cost"]
            selected.append(c)

    # 判定
    if excluded_rules:
        decision = "excluded"
    elif not selected:
        decision = "no_candidate"
    elif vi_val and vi_val >= 40:
        decision = "strong_entry"
    elif vi_val and vi_val >= 30:
        decision = "entry"
    elif vi_val and vi_val >= 25:
        decision = "consider"
    else:
        decision = "wait"

    # シグナル日の曜日（金曜はPF1.89で有意に弱い）
    weekday = None
    weekday_warning = False  # 曜日エッジなし（日単位検定で全曜日ns）
    if date_str:
        wd = pd.to_datetime(date_str).weekday()
        weekday = ["月","火","水","木","金"][wd]
        # weekday_warning廃止: 曜日エッジなし

    return {
        "decision": decision,
        "vi": _safe_float(vi_val, 2) if vi_val else None,
        "cme_gap": cme_gap,
        "n225_chg": n225_chg,
        "excluded_rules": excluded_rules,
        "weekday": weekday,
        "weekday_warning": weekday_warning,
        "total_b4_signals": len(b4),
        "candidates": candidates[:10],
        "selected": selected,
        "selected_cost": sum(s["max_cost"] for s in selected),
        "budget_remaining": budget,
        "date": date_str,
    }


@router.get("/api/dev/granville/stats")
async def get_stats(rule: Optional[str] = None):
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
    rule_filter = rule  # パラメータを退避
    by_rule = {}
    for r in ["B4", "B1", "B3", "B2"]:
        gdf = df[df["rule"] == r]
        n = len(gdf)
        if n == 0:
            by_rule[r] = {"count": 0, "win_rate": 0.0, "total_pnl": 0, "avg_pnl": 0, "avg_pct": 0.0, "exit_high_update": 0, "exit_max_hold": 0}
            continue
        wins = int((gdf["ret_pct"] > 0).sum())
        h20 = int((gdf["exit_type"].isin(["20d_high", "high_update"])).sum()) if "exit_type" in gdf.columns else 0
        mh = int((gdf["exit_type"] == "max_hold").sum()) if "exit_type" in gdf.columns else 0
        by_rule[r] = {
            "count": n,
            "win_rate": _safe_float(wins / n * 100),
            "total_pnl": _safe_int(gdf["pnl_yen"].sum()),
            "avg_pnl": _safe_int(gdf["pnl_yen"].mean()),
            "avg_pct": _safe_float(gdf["ret_pct"].mean(), 2),
            "exit_high_update": h20,
            "exit_max_hold": mh,
        }

    # ルールフィルター
    monthly_df = df.copy()
    if rule_filter and "rule" in monthly_df.columns:
        monthly_df = monthly_df[monthly_df["rule"] == rule_filter]

    # 月別統計
    monthly = []
    if "entry_date" in monthly_df.columns and not monthly_df.empty:
        monthly_df["month"] = monthly_df["entry_date"].dt.strftime("%Y-%m")
        for month, mdf in monthly_df.groupby("month"):
            n = len(mdf)
            wins = int((mdf["ret_pct"] > 0).sum())
            monthly.append({
                "month": month,
                "count": n,
                "pnl": _safe_int(mdf["pnl_yen"].sum()),
                "win_rate": _safe_float(wins / n * 100),
            })
        monthly.sort(key=lambda x: x["month"])

    # フィルター後の全体統計
    filtered_total = len(monthly_df)
    filtered_pnl = _safe_int(monthly_df["pnl_yen"].sum()) if not monthly_df.empty else 0
    filtered_wr = _safe_float((monthly_df["ret_pct"] > 0).sum() / filtered_total * 100) if filtered_total > 0 else 0
    gp = monthly_df[monthly_df["pnl_yen"] > 0]["pnl_yen"].sum() if not monthly_df.empty else 0
    gl = abs(monthly_df[monthly_df["pnl_yen"] < 0]["pnl_yen"].sum()) if not monthly_df.empty else 0
    filtered_pf = _safe_float(gp / gl, 2) if gl > 0 else 0

    return {
        "by_rule": by_rule,
        "monthly": monthly,
        "total_trades": filtered_total,
        "total_pnl": filtered_pnl,
        "win_rate": filtered_wr,
        "pf": filtered_pf,
        "rule_filter": rule_filter,
    }
