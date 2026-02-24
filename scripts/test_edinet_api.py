#!/usr/bin/env python3
"""
EDINET API テストスクリプト
grok_trending.parquet の銘柄で書類一覧APIを叩く
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

import requests
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.env import load_dotenv_cascade

load_dotenv_cascade()

EDINET_API_KEY = os.getenv("EDINET_API_KEY")
BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"


def fetch_document_list(date: str) -> dict:
    """書類一覧API (type=2: 書類一覧+メタデータ)"""
    url = f"{BASE_URL}/documents.json"
    params = {
        "date": date,
        "type": 2,
        "Subscription-Key": EDINET_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main():
    if not EDINET_API_KEY:
        print("[ERROR] EDINET_API_KEY が設定されていません")
        return 1

    print(f"[INFO] EDINET_API_KEY: {EDINET_API_KEY[:8]}...")

    # grok_trending から証券コード取得
    gt = pd.read_parquet(ROOT / "data" / "parquet" / "grok_trending.parquet")
    tickers = gt["ticker"].tolist()
    # .T を除去して4桁コードに
    sec_codes = [t.replace(".T", "") for t in tickers]
    print(f"[INFO] grok_trending銘柄数: {len(sec_codes)}")
    print(f"[INFO] 証券コード例: {sec_codes[:5]}")

    # 直近の営業日で書類一覧を取得（今日 → 過去5日さかのぼる）
    today = datetime.now()
    for i in range(5):
        target_date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        print(f"\n{'='*50}")
        print(f"[TEST] 書類一覧API: date={target_date}")
        print(f"{'='*50}")

        try:
            data = fetch_document_list(target_date)
        except Exception as e:
            print(f"[ERROR] {e}")
            continue

        meta = data.get("metadata", {})
        print(f"  status: {meta.get('status')}")
        print(f"  message: {meta.get('message')}")

        results = data.get("results", [])
        print(f"  書類総数: {len(results)}")

        if not results:
            continue

        # grok_trending 銘柄に該当する書類を検索
        matched = []
        for doc in results:
            doc_sec = doc.get("secCode", "")
            if doc_sec and doc_sec[:4] in sec_codes:
                matched.append(doc)

        print(f"  grok銘柄の書類: {len(matched)}件")

        for doc in matched[:5]:
            print(f"\n  --- {doc.get('filerName')} ({doc.get('secCode')}) ---")
            print(f"  docID: {doc.get('docID')}")
            print(f"  種別: {doc.get('docDescription')}")
            print(f"  提出日時: {doc.get('submitDateTime')}")
            print(f"  docTypeCode: {doc.get('docTypeCode')}")
            print(f"  XBRL: {doc.get('xbrlFlag')}")
            print(f"  PDF: {doc.get('pdfFlag')}")
            print(f"  CSV: {doc.get('csvFlag')}")

        # 最初にヒットした日で終了
        if len(results) > 0:
            break

    # 全書類のdocTypeCode別集計
    if results:
        print(f"\n{'='*50}")
        print(f"[集計] docTypeCode 別")
        print(f"{'='*50}")
        type_counts: dict[str, int] = {}
        for doc in results:
            dtc = doc.get("docTypeCode", "unknown")
            type_counts[dtc] = type_counts.get(dtc, 0) + 1
        for dtc, cnt in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"  {dtc}: {cnt}件")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
