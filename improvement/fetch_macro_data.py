#!/usr/bin/env python3
"""
マクロ経済データ取得 → parquet化
ソース: e-Stat API, BOJ (日銀時系列統計), EDINET API

出力先: improvement/data/macro/
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from io import StringIO

import requests
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade
load_dotenv_cascade()

OUT_DIR = Path(__file__).resolve().parent / "data" / "macro"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ESTAT_API_KEY = os.getenv("ESTAT_API_KEY")
ESTAT_BASE = "https://api.e-stat.go.jp/rest/3.0/app/json"


# ========== e-Stat ==========

def fetch_estat(stats_data_id: str, params: dict | None = None) -> list[dict]:
    """e-Stat APIからデータ取得"""
    p = {
        "appId": ESTAT_API_KEY,
        "statsDataId": stats_data_id,
        "limit": 100000,
    }
    if params:
        p.update(params)

    resp = requests.get(f"{ESTAT_BASE}/getStatsData", params=p, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]


def estat_ci_index() -> pd.DataFrame:
    """景気動向指数 CI（先行・一致・遅行）"""
    print("  [e-Stat] 景気動向指数 CI...")
    values = fetch_estat("0003446461", {"cdTab": "100"})  # CI指数

    cat_map = {"100": "leading", "110": "coincident", "120": "lagging"}
    rows = []
    for v in values:
        tc = v["@time"]
        year, month = int(tc[:4]), int(tc[8:10])
        cat = cat_map.get(v["@cat01"])
        if not cat:
            continue
        val = v.get("$")
        if val == "-" or val is None:
            continue
        rows.append({"year": year, "month": month, "indicator": cat, "value": float(val)})

    df = pd.DataFrame(rows)
    # ピボット
    piv = df.pivot_table(index=["year", "month"], columns="indicator", values="value").reset_index()
    piv["date"] = pd.to_datetime(piv["year"].astype(str) + "-" + piv["month"].astype(str).str.zfill(2) + "-01")
    piv = piv.sort_values("date").reset_index(drop=True)
    return piv


def estat_di_index() -> pd.DataFrame:
    """景気動向指数 DI（先行・一致・遅行）"""
    print("  [e-Stat] 景気動向指数 DI...")
    values = fetch_estat("0003446461", {"cdTab": "120"})  # DI指数

    cat_map = {"100": "di_leading", "110": "di_coincident", "120": "di_lagging"}
    rows = []
    for v in values:
        tc = v["@time"]
        year, month = int(tc[:4]), int(tc[8:10])
        cat = cat_map.get(v["@cat01"])
        if not cat:
            continue
        val = v.get("$")
        if val == "-" or val is None:
            continue
        rows.append({"year": year, "month": month, "indicator": cat, "value": float(val)})

    df = pd.DataFrame(rows)
    piv = df.pivot_table(index=["year", "month"], columns="indicator", values="value").reset_index()
    piv["date"] = pd.to_datetime(piv["year"].astype(str) + "-" + piv["month"].astype(str).str.zfill(2) + "-01")
    piv = piv.sort_values("date").reset_index(drop=True)
    return piv


def estat_machinery_orders() -> pd.DataFrame:
    """機械受注（船舶・電力除く民需、季調系列）"""
    print("  [e-Stat] 機械受注...")
    values = fetch_estat("0003355222", {
        "cdCat01": "160",   # 民需(船舶・電力除く)
        "cdCat02": "100",   # 季調系列
    })

    rows = []
    for v in values:
        tc = v["@time"]
        year, month = int(tc[:4]), int(tc[8:10])
        val = v.get("$")
        if val == "-" or val is None:
            continue
        rows.append({
            "year": year, "month": month,
            "machinery_orders_m": float(val),  # 百万円
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01")
    df = df.sort_values("date").reset_index(drop=True)
    return df


# ========== FRED (BOJ代替) ==========
# BOJ APIはCSV取得が困難なため、FRED（セントルイス連邦準備銀行）の
# 公開CSVを使用。日本のマクロ金融データを月次で取得。

FRED_SERIES = {
    # 長期金利（10年国債利回り）
    "IRLTLT01JPM156N": "jgb_10y_yield",
    # 短期金利（インターバンク3ヶ月）
    "IR3TIB01JPM156N": "interbank_3m_rate",
    # マネタリーベース
    "BOGMBASEJPM052N": "monetary_base",
    # CPI（消費者物価指数）
    "JPNCPIALLMINMEI": "cpi",
    # 鉱工業生産指数
    "JPNPROINDMISMEI": "industrial_production",
    # 失業率
    "LRHUTTTTJPM156S": "unemployment_rate",
    # M2マネーサプライ
    "MYAGM2JPM189N": "m2_money_supply",
}

FRED_CSV_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv"


def fetch_fred_series(series_id: str, col_name: str) -> pd.DataFrame:
    """FRED公開CSVから1系列を取得"""
    url = f"{FRED_CSV_BASE}?id={series_id}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text))
    # "." は欠損値
    df = df[df[series_id] != "."].copy()
    df["date"] = pd.to_datetime(df["observation_date"])
    df[col_name] = pd.to_numeric(df[series_id], errors="coerce")
    return df[["date", col_name]].dropna()


def fetch_fred_japan_macro() -> pd.DataFrame:
    """FRED経由で日本のマクロ金融データを一括取得 → 月次テーブル"""
    print("  [FRED] 日本マクロ金融データ取得中...")

    merged = None
    for series_id, col_name in FRED_SERIES.items():
        try:
            df = fetch_fred_series(series_id, col_name)
            print(f"    {col_name}: {len(df)}行 ({df['date'].min().date()} ~ {df['date'].max().date()})")
            if merged is None:
                merged = df
            else:
                merged = merged.merge(df, on="date", how="outer")
            time.sleep(0.3)
        except Exception as e:
            print(f"    {col_name}: ERROR - {e}")

    if merged is not None:
        merged = merged.sort_values("date").reset_index(drop=True)

    return merged


def fetch_fred_usdjpy() -> pd.DataFrame:
    """FRED: USD/JPY為替レート（月次）"""
    print("  [FRED] USD/JPY 為替レート...")
    try:
        df = fetch_fred_series("EXJPUS", "usdjpy")
        print(f"    {len(df)}行 ({df['date'].min().date()} ~ {df['date'].max().date()})")
        return df
    except Exception as e:
        print(f"    ERROR: {e}")
        return pd.DataFrame()


# ========== EDINET ==========

def fetch_edinet_filings_for_meta(days: int = 30) -> pd.DataFrame:
    """EDINET: 直近N日間のmeta銘柄の開示情報"""
    print(f"  [EDINET] 直近{days}日の開示情報...")

    EDINET_API_KEY = os.getenv("EDINET_API_KEY")
    if not EDINET_API_KEY:
        print("    EDINET_API_KEY未設定、スキップ")
        return pd.DataFrame()

    meta = pd.read_parquet(ROOT / "data" / "parquet" / "meta.parquet")
    sec_codes = set(t.replace(".T", "") for t in meta["ticker"].tolist())

    from datetime import datetime, timedelta

    all_docs = []
    today = datetime.now()

    for i in range(days):
        target = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            resp = requests.get(
                "https://api.edinet-fsa.go.jp/api/v2/documents.json",
                params={"date": target, "type": 2, "Subscription-Key": EDINET_API_KEY},
                timeout=30,
            )
            data = resp.json()
            results = data.get("results", [])

            for doc in results:
                sc = doc.get("secCode", "")
                if sc and sc[:4] in sec_codes:
                    all_docs.append({
                        "date": target,
                        "ticker": sc[:4] + ".T",
                        "secCode": sc,
                        "docID": doc.get("docID"),
                        "filerName": doc.get("filerName"),
                        "docDescription": doc.get("docDescription"),
                        "docTypeCode": doc.get("docTypeCode"),
                        "submitDateTime": doc.get("submitDateTime"),
                    })

            if i % 10 == 0:
                print(f"    {target}: {len(results)}件中{sum(1 for d in results if d.get('secCode','')[:4] in sec_codes)}件マッチ")

            time.sleep(0.5)  # レート制限
        except Exception as e:
            print(f"    {target}: ERROR - {e}")

    df = pd.DataFrame(all_docs)
    return df


# ========== メイン ==========

def main():
    print("=" * 60)
    print("マクロ経済データ取得 → parquet化")
    print(f"出力先: {OUT_DIR}")
    print("=" * 60)

    # --- e-Stat ---
    print("\n[1/6] e-Stat: 景気動向指数 CI")
    try:
        ci = estat_ci_index()
        ci.to_parquet(OUT_DIR / "estat_ci_index.parquet", index=False)
        print(f"  → {len(ci)}行 ({ci['date'].min().date()} ~ {ci['date'].max().date()})")
    except Exception as e:
        print(f"  ERROR: {e}")

    time.sleep(1)

    print("\n[2/6] e-Stat: 景気動向指数 DI")
    try:
        di = estat_di_index()
        di.to_parquet(OUT_DIR / "estat_di_index.parquet", index=False)
        print(f"  → {len(di)}行 ({di['date'].min().date()} ~ {di['date'].max().date()})")
    except Exception as e:
        print(f"  ERROR: {e}")

    time.sleep(1)

    print("\n[3/6] e-Stat: 機械受注")
    try:
        mo = estat_machinery_orders()
        mo.to_parquet(OUT_DIR / "estat_machinery_orders.parquet", index=False)
        print(f"  → {len(mo)}行 ({mo['date'].min().date()} ~ {mo['date'].max().date()})")
    except Exception as e:
        print(f"  ERROR: {e}")

    time.sleep(1)

    print("\n[4/6] EDINET: meta銘柄の開示情報（直近30日）")
    try:
        filings = fetch_edinet_filings_for_meta(days=30)
        if not filings.empty:
            filings.to_parquet(OUT_DIR / "edinet_meta_filings.parquet", index=False)
            print(f"  → {len(filings)}件")
            print(f"  docTypeCode別:")
            for dtc, cnt in filings["docTypeCode"].value_counts().items():
                print(f"    {dtc}: {cnt}件")
        else:
            print("  → 0件")
    except Exception as e:
        print(f"  ERROR: {e}")

    # --- FRED (BOJ代替) ---
    time.sleep(1)

    print("\n[5/6] FRED: 日本マクロ金融データ（BOJ代替）")
    try:
        macro = fetch_fred_japan_macro()
        if macro is not None and not macro.empty:
            macro.to_parquet(OUT_DIR / "fred_japan_macro.parquet", index=False)
            print(f"  → {len(macro)}行 ({macro['date'].min().date()} ~ {macro['date'].max().date()})")
            print(f"  カラム: {[c for c in macro.columns if c != 'date']}")
        else:
            print("  → 0行")
    except Exception as e:
        print(f"  ERROR: {e}")

    time.sleep(1)

    print("\n[6/6] FRED: USD/JPY 為替レート")
    try:
        fx = fetch_fred_usdjpy()
        if not fx.empty:
            fx.to_parquet(OUT_DIR / "fred_usdjpy.parquet", index=False)
            print(f"  → {len(fx)}行 ({fx['date'].min().date()} ~ {fx['date'].max().date()})")
        else:
            print("  → 0行")
    except Exception as e:
        print(f"  ERROR: {e}")

    # --- サマリー ---
    print("\n" + "=" * 60)
    print("完了。生成ファイル:")
    for f in sorted(OUT_DIR.glob("*.parquet")):
        size = f.stat().st_size
        print(f"  {f.name} ({size:,} bytes)")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
