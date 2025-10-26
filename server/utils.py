# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import re
import time
import unicodedata
from functools import cache
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

# ==============================
# 環境変数（S3設定）
# ==============================
def _get_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else None

_S3_BUCKET = _get_env("DATA_BUCKET")
_S3_PREFIX_RAW = _get_env("PARQUET_PREFIX")
_S3_PREFIX = (_S3_PREFIX_RAW.strip("/") if _S3_PREFIX_RAW else "parquet")
_AWS_REGION = _get_env("AWS_REGION")
_AWS_PROFILE = _get_env("AWS_PROFILE")
_AWS_ENDPOINT = _get_env("AWS_ENDPOINT_URL")

# ---- 既存ローカルパス（フォールバック用） ----
PARQUET_DIR = Path(__file__).resolve().parent.parent / "data" / "parquet"

MASTER_META_PATH = PARQUET_DIR / "meta.parquet"
ALL_STOCKS_PATH = PARQUET_DIR / "all_stocks.parquet"
PRICES_1D_PATH = PARQUET_DIR / "prices_max_1d.parquet"
TECH_SNAPSHOT_PATH = PARQUET_DIR / "tech_snapshot_1d.parquet"

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return default

_DATA_CACHE_SECONDS = max(0, _env_int("DATA_CACHE_SECONDS", 300))

def _s3_key(default_name: str) -> str:
    return f"{_S3_PREFIX}/{default_name}" if _S3_PREFIX else default_name

def _env_s3_key(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = _get_env(name)
        if value:
            return value.lstrip("/")
    if default:
        return _s3_key(default)
    return None

_S3_MASTER_META_KEY = _env_s3_key("MASTER_META_KEY", "META_KEY", "CORE30_META_KEY", default=MASTER_META_PATH.name)
_S3_ALL_STOCKS_KEY = _env_s3_key("ALL_STOCKS_KEY", default=ALL_STOCKS_PATH.name)
_S3_PRICES_1D_KEY = _env_s3_key("PRICES_MAX_1D_KEY", "PRICES_1D_KEY", "CORE30_PRICES_KEY", default=PRICES_1D_PATH.name)
_S3_TECH_SNAPSHOT_KEY = _env_s3_key("TECH_SNAPSHOT_KEY", "CORE30_TECH_SNAPSHOT_KEY", default=TECH_SNAPSHOT_PATH.name)

# ==============================
# S3 / Local 読み込みヘルパ
# ==============================
def _read_parquet_local(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(str(path), engine="pyarrow")
    except Exception:
        return None

def _read_parquet_s3(bucket: Optional[str], key: Optional[str]) -> Optional[pd.DataFrame]:
    if not bucket or not key:
        return None
    try:
        import boto3
        from io import BytesIO

        session_kwargs = {}
        if _AWS_PROFILE:
            session_kwargs["profile_name"] = _AWS_PROFILE
        session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()

        client_kwargs = {}
        if _AWS_REGION:
            client_kwargs["region_name"] = _AWS_REGION
        if _AWS_ENDPOINT:
            client_kwargs["endpoint_url"] = _AWS_ENDPOINT

        s3 = session.client("s3", **client_kwargs)
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        return pd.read_parquet(BytesIO(data), engine="pyarrow")
    except Exception as e:
        print(f"!!! S3 READ ERROR: Failed to read s3://{bucket}/{key}. Error: {e}")
        return None

# ==============================
# DataFrame キャッシュ
# ==============================
class _DataFrameCache:
    def __init__(self, ttl_seconds: int):
        self._ttl_seconds = max(0, ttl_seconds)
        self._store: Dict[Tuple, Tuple[Optional[float], float, Optional[pd.DataFrame]]] = {}

    def get(
        self,
        key: Tuple,
        loader: Callable[[], Optional[pd.DataFrame]],
        *,
        local_path: Optional[Path] = None,
    ) -> Optional[pd.DataFrame]:
        if self._ttl_seconds == 0:
            return loader()

        now = time.time()
        mtime: Optional[float] = None
        if local_path is not None:
            try:
                mtime = local_path.stat().st_mtime
            except FileNotFoundError:
                mtime = None

        cached = self._store.get(key)
        if cached:
            cached_mtime, expires_at, df = cached
            if (mtime is None or mtime == cached_mtime) and now < expires_at:
                return df

        df = loader()
        if df is None:
            # Avoid caching failures so next call can retry immediately.
            self._store.pop(key, None)
            return None

        expires_at = now + self._ttl_seconds
        self._store[key] = (mtime, expires_at, df)
        return df

_df_cache = _DataFrameCache(_DATA_CACHE_SECONDS)

# ==============================
# 既存ユーティリティ
# ==============================
_ALLOWED = re.compile(r"[^A-Za-z0-9._-]")

def _secure_filename(filename: str) -> str:
    if not isinstance(filename, str):
        filename = str(filename or "")
    filename = filename.replace("/", "_").replace("\\", "_")
    filename = unicodedata.normalize("NFKC", filename)
    filename = _ALLOWED.sub("_", filename)
    filename = filename.lstrip(".")
    filename = re.sub(r"_+", "_", filename)
    filename = filename.strip(" ._")
    if not filename:
        filename = "file"
    return filename[:255]

def to_ticker(code: str) -> str:
    s = str(code).strip()
    return s if s.endswith(".T") else f"{s}.T"

# ==============================
# 公開API向けローダ
# ==============================
@cache
def _resolve_tag(tag: Optional[str]) -> Optional[str]:
    if not tag:
        return None
    tag_norm = str(tag).strip()
    if not tag_norm:
        return None
    lut = {
        "core30": "TOPIX_CORE30",
        "topix": "TOPIX_CORE30",
        "topix_core30": "TOPIX_CORE30",
        "topixcore30": "TOPIX_CORE30",
        "policy": "政策銘柄",
        "policy_stock": "政策銘柄",
        "政策": "政策銘柄",
        "政策銘柄": "政策銘柄",
        "topix_core30_upper": "TOPIX_CORE30",
        "scalping_entry": "SCALPING_ENTRY",
        "scalping_active": "SCALPING_ACTIVE",
        "scalping": "SCALPING_ENTRY",  # デフォルトはEntry
        "grok": "GROK",
        "grok_trending": "GROK",
    }
    key = tag_norm.lower()
    return lut.get(key, tag_norm)


@cache
def load_master_meta(tag: Optional[str] = None) -> List[Dict]:
    # all_stocks.parquet を優先（スキャルピング銘柄含む統合ファイル）
    df = _read_parquet_local(ALL_STOCKS_PATH)
    if (df is None or df.empty) and _S3_BUCKET and _S3_ALL_STOCKS_KEY:
        df = _read_parquet_s3(_S3_BUCKET, _S3_ALL_STOCKS_KEY)

    # フォールバック: meta.parquet（旧形式）
    if df is None or df.empty:
        df = _read_parquet_local(MASTER_META_PATH)
        if (df is None or df.empty) and _S3_BUCKET and _S3_MASTER_META_KEY:
            df = _read_parquet_s3(_S3_BUCKET, _S3_MASTER_META_KEY)

    if df is None or df.empty:
        return []

    # 旧スキーマ（tag1, tag2, tag3）から新スキーマ（categories, tags）への変換
    if "tag1" in df.columns and "categories" not in df.columns:
        # tag1 を categories 配列に変換
        def build_categories(row):
            cats = []
            for col in ["tag1", "tag2", "tag3"]:
                if col in row and pd.notna(row[col]) and row[col]:
                    cats.append(str(row[col]))
            return cats if cats else None

        df["categories"] = df.apply(build_categories, axis=1)
        df["tags"] = None  # 旧スキーマにはtagsがない

    # 新スキーマ: categories, tags (配列)
    cols = ["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries", "categories", "tags"]
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[cols].copy()

    # tagフィルタリング: categoriesの配列に含まれるかチェック
    resolved_tag = _resolve_tag(tag)
    if resolved_tag:
        def contains_tag(cats):
            if cats is None:
                return False
            # numpy.ndarray or list をサポート
            try:
                return resolved_tag in cats
            except (TypeError, ValueError):
                return False
        df = df[df["categories"].apply(contains_tag)]

    # ソート: categories配列の最初の要素でソート
    def get_first_category(x):
        if x is None:
            return ""
        try:
            return x[0] if len(x) > 0 else ""
        except (TypeError, IndexError):
            return ""
    df["_sort_key"] = df["categories"].apply(get_first_category)
    df = df.sort_values(["_sort_key", "code"], kind="mergesort")
    df = df.drop(columns=["_sort_key"])

    # numpy.ndarray を list に変換（JSON serialization対応）
    for col in ["categories", "tags"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: list(x) if x is not None and hasattr(x, '__iter__') and not isinstance(x, str) else x)

    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")

def read_prices_1d_df() -> Optional[pd.DataFrame]:
    def _load() -> Optional[pd.DataFrame]:
        df = _read_parquet_local(PRICES_1D_PATH)
        if (df is None or df.empty) and _S3_BUCKET and _S3_PRICES_1D_KEY:
            df = _read_parquet_s3(_S3_BUCKET, _S3_PRICES_1D_KEY)
        return df

    return _df_cache.get(("prices_1d",), _load, local_path=PRICES_1D_PATH)

def read_prices_df(period: str, interval: str) -> Optional[pd.DataFrame]:
    filename = f"prices_{period}_{interval}.parquet"
    local_path = PARQUET_DIR / filename
    def _load() -> Optional[pd.DataFrame]:
        df = _read_parquet_local(local_path)
        if (df is None or df.empty) and _S3_BUCKET:
            s3_key = _s3_key(filename)
            df = _read_parquet_s3(_S3_BUCKET, s3_key)
        return df

    return _df_cache.get(("prices", period, interval), _load, local_path=local_path)

def read_tech_snapshot_df() -> Optional[pd.DataFrame]:
    """事前計算されたテクニカル指標スナップショットを読み込む"""
    def _load() -> Optional[pd.DataFrame]:
        df = _read_parquet_local(TECH_SNAPSHOT_PATH)
        if (df is None or df.empty) and _S3_BUCKET and _S3_TECH_SNAPSHOT_KEY:
            df = _read_parquet_s3(_S3_BUCKET, _S3_TECH_SNAPSHOT_KEY)

        if df is None or df.empty:
            return df

        # JSON文字列の列を辞書に変換
        for col in ["values", "votes", "overall"]:
            if col in df.columns and isinstance(df[col].iloc[0], str):
                df[col] = df[col].apply(json.loads)
        return df

    return _df_cache.get(("tech_snapshot",), _load, local_path=TECH_SNAPSHOT_PATH)

def load_scalping_meta(category: str) -> List[Dict]:
    """スキャルピング銘柄メタデータを読み込む

    Args:
        category: "entry", "active"

    Returns:
        meta.parquet互換の辞書リスト
    """
    valid_categories = ("entry", "active")
    if category not in valid_categories:
        return []

    filename = f"scalping_{category}.parquet"
    local_path = PARQUET_DIR / filename

    df = _read_parquet_local(local_path)
    if (df is None or df.empty) and _S3_BUCKET:
        s3_key = _s3_key(filename)
        df = _read_parquet_s3(_S3_BUCKET, s3_key)

    if df is None or df.empty:
        return []

    # meta.parquet互換スキーマに変換
    # code を ticker から抽出 (例: "7203.T" -> "7203")
    df["code"] = df["ticker"].str.replace(".T", "", regex=False)

    # categories を配列で追加
    category_name = f"SCALPING_{category.upper()}"
    df["categories"] = [[category_name]] * len(df)

    # tags は既に配列形式（J-Quants生成時に設定済み）
    # tags が文字列の場合は配列に変換
    if "tags" in df.columns:
        def normalize_tags(x):
            if isinstance(x, list):
                return x
            if hasattr(x, '__iter__') and not isinstance(x, str):
                # numpy.ndarray などの iterable
                return list(x)
            if pd.isna(x) or x is None or x == "":
                return []
            return [x]
        df["tags"] = df["tags"].apply(normalize_tags)
    else:
        df["tags"] = [[] for _ in range(len(df))]

    # series, topixnewindexseries が存在しない場合のみ None で埋める
    # （既存のデータがあれば保持する）

    # meta.parquet と同じカラム順序
    cols = ["ticker", "code", "stock_name", "market", "sectors",
            "series", "topixnewindexseries", "categories", "tags"]

    # 存在しないカラムは None で埋める
    for col in cols:
        if col not in df.columns:
            df[col] = None

    result = df[cols].copy()

    # numpy.ndarray を list に変換（JSON serialization対応）
    for col in ["categories", "tags"]:
        if col in result.columns:
            result[col] = result[col].apply(
                lambda x: list(x) if x is not None and hasattr(x, '__iter__') and not isinstance(x, str) else x
            )

    result = result.where(pd.notna(result), None)
    return result.to_dict(orient="records")

GROK_TRENDING_PATH = PARQUET_DIR / "grok_trending.parquet"

@cache
def load_all_stocks(tag: Optional[str] = None) -> List[Dict]:
    """all_stocks.parquetから全銘柄を読み込む（meta + scalping + grok統合版）

    Args:
        tag: categoriesでフィルタリング（例: "TOPIX_CORE30", "高市銘柄", "SCALPING_ENTRY", "SCALPING_ACTIVE", "GROK"）

    Returns:
        全銘柄の辞書リスト（meta.parquet互換）
    """
    # GROK銘柄の場合は、grok_trending.parquetから直接読み込む
    resolved_tag = _resolve_tag(tag)
    if resolved_tag == "GROK":
        grok_df = _read_parquet_local(GROK_TRENDING_PATH)
        if (grok_df is None or grok_df.empty) and _S3_BUCKET:
            s3_key = _s3_key("grok_trending.parquet")
            grok_df = _read_parquet_s3(_S3_BUCKET, s3_key)

        if grok_df is not None and not grok_df.empty:
            # 必要なカラムを確認
            cols = ["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries", "categories", "tags", "selection_score"]
            for col in cols:
                if col not in grok_df.columns:
                    grok_df[col] = pd.NA

            # selection_scoreで降順ソート
            if "selection_score" in grok_df.columns:
                grok_df = grok_df.sort_values("selection_score", ascending=False)

            # numpy.ndarray を list に変換（JSON serialization対応）
            for col in ["categories", "tags"]:
                if col in grok_df.columns:
                    grok_df[col] = grok_df[col].apply(lambda x: list(x) if x is not None and hasattr(x, '__iter__') and not isinstance(x, str) else x)

            # selection_scoreをfloatに変換してJSONシリアライズ可能にする
            if "selection_score" in grok_df.columns:
                grok_df["selection_score"] = grok_df["selection_score"].apply(lambda x: float(x) if pd.notna(x) else None)

            grok_df = grok_df.where(pd.notna(grok_df), None)
            return grok_df[cols].to_dict(orient="records")

    df = _read_parquet_local(ALL_STOCKS_PATH)
    if (df is None or df.empty) and _S3_BUCKET and _S3_ALL_STOCKS_KEY:
        df = _read_parquet_s3(_S3_BUCKET, _S3_ALL_STOCKS_KEY)

    # フォールバック: all_stocks.parquetがない場合はmeta.parquetとscalping_*.parquetから読み込む
    if df is None or df.empty:
        print("[WARN] all_stocks.parquet not found. Falling back to meta.parquet + scalping_*.parquet")
        # meta.parquetから読み込み
        meta = load_master_meta(tag=None)
        # scalping_*.parquetから読み込み
        scalping_entry = load_scalping_meta("entry")
        scalping_active = load_scalping_meta("active")

        # マージ
        all_stocks = meta + scalping_entry + scalping_active

        # tagフィルタリング
        if tag:
            if resolved_tag:
                all_stocks = [s for s in all_stocks if resolved_tag in (s.get("categories") or [])]

        return all_stocks

    # all_stocks.parquetから読み込み成功
    # 必要なカラムを確認
    cols = ["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries", "categories", "tags"]
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[cols].copy()

    # tagフィルタリング: categoriesの配列に含まれるかチェック
    if resolved_tag:
        def contains_tag(cats):
            if cats is None:
                return False
            try:
                return resolved_tag in cats
            except (TypeError, ValueError):
                return False
        df = df[df["categories"].apply(contains_tag)]

    # ソート: categories配列の最初の要素でソート
    def get_first_category(x):
        if x is None:
            return ""
        try:
            return x[0] if len(x) > 0 else ""
        except (TypeError, IndexError):
            return ""
    df["_sort_key"] = df["categories"].apply(get_first_category)
    df = df.sort_values(["_sort_key", "code"], kind="mergesort")
    df = df.drop(columns=["_sort_key"])

    # numpy.ndarray を list に変換（JSON serialization対応）
    for col in ["categories", "tags"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: list(x) if x is not None and hasattr(x, '__iter__') and not isinstance(x, str) else x)

    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")

def normalize_prices(df: pd.DataFrame) -> pd.DataFrame:
    need = {"date", "Open", "High", "Low", "Close", "ticker"}
    if not need.issubset(df.columns):
        return pd.DataFrame()
    keep = ["date", "Open", "High", "Low", "Close", "ticker"]
    if "Volume" in df.columns:
        keep.append("Volume")
    out = df[keep].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    out = out[out["date"].notna()].copy()
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out["ticker"] = out["ticker"].astype("string")
    return out

def to_json_records(df: pd.DataFrame) -> List[Dict]:
    g = df.copy()
    g["date"] = g["date"].dt.strftime("%Y-%m-%d")
    g = g.sort_values(["ticker", "date"]).reset_index(drop=True)
    return g.to_dict(orient="records")

# ==============================
# メタデータとデータ統合
# ==============================
def merge_price_data_into_meta(meta_list: List[Dict]) -> List[Dict]:
    """メタデータに価格・パフォーマンスデータをマージ

    Args:
        meta_list: メタデータの辞書リスト（ticker, code, stock_name, ...を含む）

    Returns:
        価格データがマージされた辞書リスト（Row[]型互換）
    """
    if not meta_list:
        return []

    # スナップショット取得（prices_max_1d から最新2日分）
    prices_df = read_prices_1d_df()
    snapshot_data = {}

    if prices_df is not None and not prices_df.empty:
        # 各tickerの最新2日分を取得
        for ticker in set(m["ticker"] for m in meta_list if m.get("ticker")):
            ticker_df = prices_df[prices_df["ticker"] == ticker].sort_values("date", ascending=False).head(2)
            if len(ticker_df) >= 1:
                latest = ticker_df.iloc[0]
                prev = ticker_df.iloc[1] if len(ticker_df) >= 2 else None

                close = float(latest["Close"]) if pd.notna(latest["Close"]) else None
                prev_close = float(prev["Close"]) if prev is not None and pd.notna(prev["Close"]) else None
                diff = (close - prev_close) if (close is not None and prev_close is not None) else None

                snapshot_data[ticker] = {
                    "date": latest["date"].strftime("%Y-%m-%d") if pd.notna(latest["date"]) else None,
                    "close": close,
                    "prevClose": prev_close,
                    "diff": diff,
                    "volume": float(latest["Volume"]) if pd.notna(latest.get("Volume")) else None,
                }

    # メタデータとマージ
    result = []
    for meta in meta_list:
        ticker = meta.get("ticker")
        row = {**meta}  # メタデータをコピー

        # スナップショットデータを追加
        if ticker and ticker in snapshot_data:
            snap = snapshot_data[ticker]
            row.update({
                "date": snap["date"],
                "close": snap["close"],
                "prevClose": snap["prevClose"],
                "diff": snap["diff"],
                "pct_diff": (snap["diff"] / snap["prevClose"] * 100) if (snap["diff"] is not None and snap["prevClose"] is not None and snap["prevClose"] != 0) else None,
                "volume": snap["volume"],
            })
        else:
            # データがない場合はnullで埋める
            row.update({
                "date": None,
                "close": None,
                "prevClose": None,
                "diff": None,
                "pct_diff": None,
                "volume": None,
            })

        result.append(row)

    return result

def _calculate_perf_for_enriched(prices_df: pd.DataFrame, tickers: List[str]) -> Dict[str, Dict]:
    """パフォーマンス指標を計算（enriched用）"""
    if prices_df is None or prices_df.empty:
        return {}

    df = prices_df[prices_df["ticker"].isin(tickers)].copy()
    if df.empty:
        return {}

    windows = ["5d", "1mo", "3mo", "ytd", "1y", "3y", "5y", "all"]
    days_map = {
        "5d": 7,
        "1mo": 30,
        "3mo": 90,
        "1y": 365,
        "3y": 365 * 3,
        "5y": 365 * 5,
    }

    def pct_return(last_close: float, base_close: Optional[float]) -> Optional[float]:
        if (
            last_close is None
            or base_close is None
            or pd.isna(last_close)
            or pd.isna(base_close)
            or base_close == 0
        ):
            return None
        return float((float(last_close) / float(base_close) - 1.0) * 100.0)

    perf_map = {}
    for ticker, grp in df.sort_values(["ticker", "date"]).groupby("ticker"):
        g = grp[["date", "Close"]].dropna(subset=["Close"])
        if g.empty:
            continue
        last_row = g.iloc[-1]
        last_date = last_row["date"]
        last_close = float(last_row["Close"])

        def base_close_before_or_on(target_dt: pd.Timestamp) -> Optional[float]:
            sel = g[g["date"] <= target_dt]
            if sel.empty:
                return None
            return float(sel.iloc[-1]["Close"])

        perf = {}
        for w in windows:
            if w == "ytd":
                start_of_year = pd.Timestamp(year=last_date.year, month=1, day=1)
                base = base_close_before_or_on(start_of_year)
                perf[f"r_{w}"] = pct_return(last_close, base)
            elif w == "all":
                base = float(g.iloc[0]["Close"])
                perf[f"r_{w}"] = pct_return(last_close, base)
            else:
                days = days_map.get(w)
                if not days:
                    perf[f"r_{w}"] = None
                else:
                    target = last_date - pd.Timedelta(days=days)
                    base = base_close_before_or_on(target)
                    perf[f"r_{w}"] = pct_return(last_close, base)

        perf_map[str(ticker)] = perf

    return perf_map


def enrich_stocks_with_all_data(meta_list: List[Dict]) -> List[Dict]:
    """メタデータに価格・パフォーマンス・テクニカルデータを統合

    Args:
        meta_list: メタデータの辞書リスト

    Returns:
        全データが統合された辞書リスト（Row[]型完全互換）
    """
    if not meta_list:
        return []

    # 1. 価格データをマージ
    enriched = merge_price_data_into_meta(meta_list)

    # 2. テクニカル評価データを取得
    tech_snapshot_df = read_tech_snapshot_df()
    tech_rating_map = {}
    if tech_snapshot_df is not None and not tech_snapshot_df.empty:
        for _, row in tech_snapshot_df.iterrows():
            ticker = str(row["ticker"])
            values = row.get("values", {}) or {}
            votes = row.get("votes", {}) or {}
            overall = row.get("overall", {}) or {}

            date_value = row.get("date")
            if pd.notna(date_value):
                if hasattr(date_value, 'strftime'):
                    date_str = date_value.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_value)
            else:
                date_str = None

            tech_rating_map[ticker] = {
                "date": date_str,
                # 数値データ
                "rsi14": values.get("rsi14"),
                "macd_hist": values.get("macd_hist"),
                "bb_percent_b": values.get("percent_b"),  # フロントエンドはbb_percent_bを期待
                "sma25_dev_pct": values.get("sma25_dev_pct"),
                # 評価データ
                "overall_rating": overall.get("label"),
                "tech_rating": votes.get("rsi14", {}).get("label") or votes.get("macd_hist", {}).get("label"),  # テクニカル総合
                "ma_rating": votes.get("ma", {}).get("label"),
                "ichimoku_rating": votes.get("ichimoku", {}).get("label"),
            }

    # 3. TR, ATR14, vol_ma10, パフォーマンスを価格データから計算
    prices_df = read_prices_1d_df()
    if prices_df is not None and not prices_df.empty:
        # TR, ATR14 を計算（routers/prices.py の _add_volatility_columns と同じロジック）
        df = prices_df.copy()
        df = df.sort_values(["ticker", "date"])
        df["prevClose"] = df.groupby("ticker")["Close"].shift(1)

        # True Range (TR)
        hl = df["High"] - df["Low"]
        hp = (df["High"] - df["prevClose"]).abs()
        lp = (df["Low"] - df["prevClose"]).abs()
        df["tr"] = pd.concat([hl, hp, lp], axis=1).max(axis=1)

        # ATR(14): EMA(TR, span=14)
        df["atr14"] = (
            df.groupby("ticker", group_keys=False)["tr"]
            .apply(lambda s: s.ewm(span=14, adjust=False).mean())
        )

        # %表記
        with pd.option_context("mode.use_inf_as_na", True):
            df["tr_pct"] = (df["tr"] / df["prevClose"] * 100.0).where(df["prevClose"] > 0)
            df["atr14_pct"] = (df["atr14"] / df["Close"] * 100.0).where(df["Close"] > 0)

        # vol_ma10
        if "Volume" in df.columns:
            df["vol_ma10"] = (
                df.groupby("ticker")["Volume"]
                .rolling(window=10, min_periods=1)
                .mean()
                .reset_index(level=0, drop=True)
            )

        # 各tickerの最新値を取得
        tech_map = {}
        for ticker, grp in df.groupby("ticker"):
            latest = grp.iloc[-1]
            tech_map[str(ticker)] = {
                "tr": float(latest["tr"]) if pd.notna(latest.get("tr")) else None,
                "tr_pct": float(latest["tr_pct"]) if pd.notna(latest.get("tr_pct")) else None,
                "atr14": float(latest["atr14"]) if pd.notna(latest.get("atr14")) else None,
                "atr14_pct": float(latest["atr14_pct"]) if pd.notna(latest.get("atr14_pct")) else None,
                "vol_ma10": float(latest["vol_ma10"]) if pd.notna(latest.get("vol_ma10")) else None,
            }

        # 4. パフォーマンスデータを計算
        tickers = [m["ticker"] for m in meta_list if m.get("ticker")]
        perf_map = _calculate_perf_for_enriched(prices_df, tickers)

        # enriched にテクニカル、パフォーマンス、評価データをマージ
        for stock in enriched:
            ticker = stock.get("ticker")

            # TR, ATR14, vol_ma10
            if ticker and ticker in tech_map:
                tech = tech_map[ticker]
                stock.update(tech)
            else:
                # テクニカルデータがない場合はnullで埋める
                stock.update({
                    "tr": None,
                    "tr_pct": None,
                    "atr14": None,
                    "atr14_pct": None,
                    "vol_ma10": None,
                })

            # パフォーマンスデータをマージ
            if ticker and ticker in perf_map:
                perf = perf_map[ticker]
                stock.update(perf)
            else:
                # パフォーマンスデータがない場合はnullで埋める
                stock.update({
                    "r_5d": None,
                    "r_1mo": None,
                    "r_3mo": None,
                    "r_ytd": None,
                    "r_1y": None,
                    "r_3y": None,
                    "r_5y": None,
                    "r_all": None,
                })

            # テクニカル評価データをマージ
            if ticker and ticker in tech_rating_map:
                rating = tech_rating_map[ticker]
                stock.update(rating)
            else:
                # テクニカル評価がない場合はnullで埋める
                stock.update({
                    "rsi14": None,
                    "macd_hist": None,
                    "bb_percent_b": None,
                    "sma25_dev_pct": None,
                    "overall_rating": None,
                    "tech_rating": None,
                    "ma_rating": None,
                    "ichimoku_rating": None,
                })
    else:
        # 価格データがない場合は全てnullで埋める
        for stock in enriched:
            ticker = stock.get("ticker")

            stock.update({
                "tr": None,
                "tr_pct": None,
                "atr14": None,
                "atr14_pct": None,
                "vol_ma10": None,
                "r_5d": None,
                "r_1mo": None,
                "r_3mo": None,
                "r_ytd": None,
                "r_1y": None,
                "r_3y": None,
                "r_5y": None,
                "r_all": None,
            })

            # テクニカル評価データはtech_snapshotから取得可能
            if ticker and ticker in tech_rating_map:
                rating = tech_rating_map[ticker]
                stock.update(rating)
            else:
                stock.update({
                    "rsi14": None,
                    "macd_hist": None,
                    "bb_percent_b": None,
                    "sma25_dev_pct": None,
                    "overall_rating": None,
                    "tech_rating": None,
                    "ma_rating": None,
                    "ichimoku_rating": None,
                })

    return enriched
