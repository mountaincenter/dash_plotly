#!/usr/bin/env python3
"""
test_grok_today.py
Grokが認識している「今日の日付」を確認

実行方法:
    python3 scripts/pipeline/test_grok_today.py
"""

import sys
import json
import requests
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import dotenv_values

ENV_XAI_PATH = ROOT / ".env.xai"


def main() -> int:
    """メイン処理"""
    print("=" * 60)
    print("Grokが認識している今日の日付を確認")
    print("=" * 60)

    # API Key読み込み
    if not ENV_XAI_PATH.exists():
        print(f"[ERROR] .env.xai not found: {ENV_XAI_PATH}")
        return 1

    config = dotenv_values(ENV_XAI_PATH)
    api_key = config.get("XAI_API_KEY")

    if not api_key:
        print("[ERROR] XAI_API_KEY not found in .env.xai")
        return 1

    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "grok-2-latest",
        "messages": [
            {
                "role": "user",
                "content": "今日は何年何月何日ですか？YYYY-MM-DD形式で答えてください。"
            }
        ],
        "search": True,
        "temperature": 0.1
    }

    print("[INFO] Sending request to Grok...")
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()

    result = response.json()

    print("\n[Grokの回答]")
    content = result['choices'][0]['message']['content']
    print(content)

    print("\n[システム認識]")
    from datetime import datetime
    print(f"実際の今日: {datetime.now().strftime('%Y-%m-%d')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
