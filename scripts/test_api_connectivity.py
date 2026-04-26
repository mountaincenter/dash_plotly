#!/usr/bin/env python3
"""
J-Quants fins/summary + EDINET 書類一覧 API 疎通テスト
GHA workflow_dispatch から実行。各APIに軽量リクエストを送り、レスポンスを検証する。
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

import requests


def send_slack(results: list[dict[str, str | bool]]) -> None:
    webhook = os.getenv("SLACK_TEST_WEBHOOK_URL")
    if not webhook:
        print("[SKIP] SLACK_TEST_WEBHOOK_URL 未設定、Slack送信スキップ")
        return

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "API 疎通テスト結果"}},
        {"type": "divider"},
    ]
    for r in results:
        icon = ":white_check_mark:" if r["ok"] else ":x:"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"{icon} *{r['name']}*\n{r['detail']}"},
        })

    all_ok = all(r["ok"] for r in results)
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"{'全テスト通過' if all_ok else 'テスト失敗あり'}"}],
    })

    resp = requests.post(webhook, json={"blocks": blocks}, timeout=10)
    if resp.status_code == 200:
        print("[OK] Slack送信完了")
    else:
        print(f"[WARN] Slack送信失敗: {resp.status_code}")


def test_jquants_fins_summary() -> tuple[bool, str]:
    """jquants CLI v2: fins summary で DiscDate 取得テスト"""
    print("=" * 50)
    print("[J-Quants] fins/summary テスト (CLI v2)")
    print("=" * 50)

    today = datetime.now()
    for i in range(7):
        target = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        cmd = ["jquants", "--output", "json", "fins", "summary", "--date", target]
        print(f"  実行: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"  {target}: 失敗 ({result.stderr.strip()[:100]})")
            continue

        import json
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            print(f"  {target}: JSON解析失敗")
            continue

        if not data:
            print(f"  {target}: データなし")
            continue

        print(f"\n[OK] {target}: {len(data)}件の決算開示")
        for item in data[:3]:
            code = item.get("Code", "?")
            disc_date = item.get("DiscDate", "?")
            disc_time = item.get("DiscTime", "?")
            print(f"  {code} DiscDate={disc_date} DiscTime={disc_time}")
        if len(data) > 3:
            print(f"  ... 他 {len(data) - 3}件")

        detail = f"{target}: {len(data)}件の決算開示 (DiscDate取得OK)"
        return True, detail

    return False, "直近7日間でデータ取得できず"


def test_edinet_document_list() -> tuple[bool, str]:
    """EDINET 書類一覧API: 直近営業日の提出書類を取得"""
    print("\n" + "=" * 50)
    print("[EDINET] 書類一覧API テスト")
    print("=" * 50)

    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        print("[FAIL] EDINET_API_KEY が未設定")
        return False, "EDINET_API_KEY 未設定"
    print(f"[OK] EDINET_API_KEY: {api_key[:8]}...")

    today = datetime.now()
    for i in range(7):
        target = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        url = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
        params = {"date": target, "type": 2, "Subscription-Key": api_key}
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  {target}: HTTP {resp.status_code}")
            continue

        data = resp.json()
        meta = data.get("metadata", {})
        results = data.get("results", [])

        status = str(meta.get("status", ""))
        if status != "200":
            print(f"  {target}: status={status} {meta.get('message')}")
            continue

        if not results:
            continue

        type_counts: dict[str, int] = {}
        for doc in results:
            dtc = doc.get("docTypeCode", "?")
            type_counts[dtc] = type_counts.get(dtc, 0) + 1

        kessan_count = type_counts.get("140", 0)

        print(f"\n[OK] {target}: 書類総数 {len(results)}件")
        print(f"  決算短信(140): {kessan_count}件")
        for dtc, cnt in sorted(type_counts.items(), key=lambda x: -x[1])[:5]:
            print(f"  docTypeCode {dtc}: {cnt}件")

        if kessan_count > 0:
            kessan_docs = [d for d in results if d.get("docTypeCode") == "140"]
            print(f"\n  決算短信サンプル:")
            for doc in kessan_docs[:3]:
                print(f"    {doc.get('filerName')} ({doc.get('secCode', '----')[:4]})")
                print(f"      {doc.get('docDescription')}")

        detail = f"{target}: 書類{len(results)}件 (決算短信{kessan_count}件)"
        return True, detail

    return False, "直近7日間でデータ取得できず"


def main() -> int:
    results: list[dict[str, str | bool]] = []

    jq_ok, jq_detail = test_jquants_fins_summary()
    results.append({"name": "J-Quants fins/summary", "ok": jq_ok, "detail": jq_detail})

    ed_ok, ed_detail = test_edinet_document_list()
    results.append({"name": "EDINET 書類一覧", "ok": ed_ok, "detail": ed_detail})

    print("\n" + "=" * 50)
    print("[結果]")
    print(f"  J-Quants fins/summary: {'OK' if jq_ok else 'FAIL'}")
    print(f"  EDINET 書類一覧:       {'OK' if ed_ok else 'FAIL'}")
    print("=" * 50)

    send_slack(results)

    return 0 if (jq_ok and ed_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
