#!/usr/bin/env python3
"""
決算開示データ取得: J-Quants fins/summary + EDINET 書類一覧
パイプライン用の earnings_disclosure.parquet を生成する。
"""
from __future__ import annotations

import os
import sys
import subprocess
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "data" / "parquet" / "earnings_disclosure.parquet"

sys.path.insert(0, str(ROOT))
from common_cfg.env import load_dotenv_cascade
load_dotenv_cascade()

EDINET_API_KEY = os.getenv("EDINET_API_KEY")
EDINET_BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"


def fetch_jquants_fins_summary(date: str) -> list[dict]:
    """jquants CLI v2 で fins/summary を取得"""
    cmd = ["jquants", "--output", "json", "fins", "summary", "--date", date]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return []


def fetch_edinet_documents(date: str) -> list[dict]:
    """EDINET 書類一覧API で決算短信(docTypeCode=140)を取得"""
    if not EDINET_API_KEY:
        print(f"  [WARN] EDINET_API_KEY 未設定、スキップ")
        return []
    url = f"{EDINET_BASE_URL}/documents.json"
    params = {"date": date, "type": 2, "Subscription-Key": EDINET_API_KEY}
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        print(f"  [WARN] EDINET {date}: HTTP {resp.status_code}")
        return []
    data = resp.json()
    results = data.get("results", [])
    return [d for d in results if d.get("docTypeCode") == "140"]


def main() -> int:
    today = datetime.now()
    # 直近5営業日分を取得（土日を考慮して7日遡る）
    all_jq: list[dict] = []
    all_edinet: list[dict] = []

    for i in range(7):
        target = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        print(f"[{target}]")

        jq = fetch_jquants_fins_summary(target)
        if jq:
            print(f"  J-Quants: {len(jq)}件")
            for item in jq:
                all_jq.append({
                    "disc_date": item.get("DiscDate", ""),
                    "disc_time": item.get("DiscTime", ""),
                    "code": str(item.get("Code", ""))[:4],
                    "source": "jquants",
                })
        else:
            print(f"  J-Quants: 0件")

        ed = fetch_edinet_documents(target)
        if ed:
            print(f"  EDINET:   {len(ed)}件")
            for doc in ed:
                sec_code = str(doc.get("secCode", ""))[:4]
                if sec_code:
                    all_edinet.append({
                        "disc_date": target,
                        "disc_time": doc.get("submitDateTime", ""),
                        "code": sec_code,
                        "filer_name": doc.get("filerName", ""),
                        "doc_description": doc.get("docDescription", ""),
                        "source": "edinet",
                    })
        else:
            print(f"  EDINET:   0件")

    # マージしてparquet保存
    jq_df = pd.DataFrame(all_jq) if all_jq else pd.DataFrame(columns=["disc_date", "disc_time", "code", "source"])
    ed_df = pd.DataFrame(all_edinet) if all_edinet else pd.DataFrame(columns=["disc_date", "disc_time", "code", "source"])

    # 共通カラムで結合
    if not ed_df.empty:
        ed_df = ed_df[["disc_date", "disc_time", "code", "source"]]
    merged = pd.concat([jq_df, ed_df], ignore_index=True).drop_duplicates(subset=["disc_date", "code"])

    merged.to_parquet(OUTPUT_PATH, index=False)
    print(f"\n[結果] {len(merged)}件 → {OUTPUT_PATH}")
    print(f"  J-Quants: {len(jq_df)}件, EDINET: {len(ed_df)}件")
    print(f"  日付範囲: {merged['disc_date'].min()} 〜 {merged['disc_date'].max()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
