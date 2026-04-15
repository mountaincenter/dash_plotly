#!/usr/bin/env python3
"""
format_jquants_slack.py
jquants-cliで市場データを取得し、Slack通知用テキストを生成・送信する。

Usage:
    python3 scripts/format_jquants_slack.py                # 最新営業日
    python3 scripts/format_jquants_slack.py --date 2026-04-15
    python3 scripts/format_jquants_slack.py --dry-run      # Slack送信なし

環境変数:
    SLACK_WEBHOOK_URL: Slack Incoming Webhook URL
    JQUANTS_API_KEY: J-Quants API key
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import requests


def _jquants(*args: str) -> list[dict]:
    """jquants-cli を呼び出してJSONを返す"""
    cmd = ["jquants", "-o", "json"] + list(args)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        print(f"  [WARN] jquants timeout: {' '.join(args)}", file=sys.stderr)
        return []
    if result.returncode != 0 or not result.stdout.strip():
        print(f"  [WARN] jquants {' '.join(args)}: {result.stderr[:200]}", file=sys.stderr)
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        print(f"  [WARN] jquants JSON parse error: {e}", file=sys.stderr)
        return []


def _master_map() -> dict[str, dict]:
    data = _jquants("eq", "master")
    return {d["Code"]: d for d in data}


def _detect_date() -> str:
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    # 土日は金曜に戻す
    wd = now.weekday()
    if wd == 5:
        now -= timedelta(days=1)
    elif wd == 6:
        now -= timedelta(days=2)
    return now.strftime("%Y-%m-%d")


def build_message(date: str) -> str:
    """jquantsデータを取得してSlackメッセージを構築"""
    master = _master_map()
    lines = [f"*J-Quants Market Data* ({date})\n"]

    # --- 売買代金TOP10 ---
    daily = _jquants("eq", "daily", "--date", date)
    if daily:
        with_va = sorted(
            [d for d in daily if d.get("Va") and float(d["Va"]) > 0],
            key=lambda x: float(x["Va"]),
            reverse=True,
        )[:10]
        lines.append("*売買代金TOP10:*")
        for i, d in enumerate(with_va, 1):
            code = d["Code"]
            name = master.get(code, {}).get("CoName", "")[:10]
            va = float(d["Va"]) / 1e9
            c = float(d["C"]) if d.get("C") else 0
            o = float(d["O"]) if d.get("O") else 0
            pct = ((c - o) / o * 100) if o > 0 else 0
            sign = "+" if pct >= 0 else ""
            lines.append(f"  {i:2d}. {code} {name}  {va:,.0f}億  {sign}{pct:.1f}% (日中)")
        lines.append("")

    # --- 市場別騰落数 ---
    if daily:
        # 前営業日
        import pandas as pd
        td = pd.Timestamp(date)
        prev_data = None
        for i in range(1, 11):
            candidate = (td - pd.Timedelta(days=i)).strftime("%Y-%m-%d")
            prev_data = _jquants("eq", "daily", "--date", candidate)
            if prev_data:
                break

        if prev_data:
            prev_close = {d["Code"]: float(d["C"]) for d in prev_data if d.get("C") is not None}
            mkt_map = {"プライム": "Prime", "スタンダード": "Std", "グロース": "Growth"}
            counts = {v: {"adv": 0, "dec": 0, "unch": 0} for v in mkt_map.values()}
            counts["Total"] = {"adv": 0, "dec": 0, "unch": 0}

            for d in daily:
                code = d["Code"]
                if d.get("C") is None or code not in prev_close:
                    continue
                curr_c = float(d["C"])
                if curr_c > prev_close[code]:
                    direction = "adv"
                elif curr_c < prev_close[code]:
                    direction = "dec"
                else:
                    direction = "unch"
                counts["Total"][direction] += 1
                mkt = mkt_map.get(master.get(code, {}).get("MktNm", ""))
                if mkt:
                    counts[mkt][direction] += 1

            lines.append("*騰落数 (↑/↓/→):*")
            for mkt in ["Total", "Prime", "Std", "Growth"]:
                c = counts[mkt]
                lines.append(f"  {mkt:6s}: {c['adv']:>4d} / {c['dec']:>4d} / {c['unch']:>4d}")
            lines.append("")

    # --- 投資部門別 ---
    import pandas as pd
    td = pd.Timestamp(date)
    inv = _jquants("eq", "investor-types", "--section", "TSEPrime",
                   "--from", (td - pd.Timedelta(days=14)).strftime("%Y-%m-%d"),
                   "--to", date)
    if inv:
        inv.sort(key=lambda x: x.get("StDate", ""))
        latest = inv[-1]
        frgn = float(latest.get("FrgnBal", 0)) / 1e9
        ind = float(latest.get("IndBal", 0)) / 1e9
        period = f"{latest.get('StDate', '')}~{latest.get('EnDate', '')}"
        lines.append(f"*投資部門別* ({period}):")
        lines.append(f"  外国人: {frgn:+,.1f}億円")
        lines.append(f"  個人:   {ind:+,.1f}億円")
        lines.append("")

    # --- 信用残 ---
    margin = _jquants("mkt", "margin-alert", "--date", date)
    if margin:
        total_long = sum(float(d.get("LongOut", 0)) for d in margin)
        total_short = sum(float(d.get("ShrtOut", 0)) for d in margin)
        sl = total_long / total_short if total_short > 0 else 0
        lines.append(f"*信用残* (日々公表 {len(margin)}銘柄):")
        lines.append(f"  信用倍率: {sl:.2f}")
        lines.append("")

    return "\n".join(lines)


def send_slack(text: str, webhook_url: str) -> bool:
    payload = {"text": text}
    resp = requests.post(webhook_url, json=payload, timeout=10)
    return resp.status_code == 200


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Print only, don't send to Slack")
    args = parser.parse_args()

    date = args.date or _detect_date()
    print(f"Date: {date}")

    msg = build_message(date)
    print(msg)

    if args.dry_run:
        print("\n[DRY RUN] Slack送信スキップ")
        return 0

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("[ERROR] SLACK_WEBHOOK_URL not set", file=sys.stderr)
        return 1

    if send_slack(msg, webhook_url):
        print("[OK] Slack送信完了")
        return 0
    else:
        print("[ERROR] Slack送信失敗", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
