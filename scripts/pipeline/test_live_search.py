#!/usr/bin/env python3
"""
test_live_search.py
xAI Live Search機能のテスト（search=trueパラメータ使用）

実行方法:
    python3 scripts/pipeline/test_live_search.py
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


def load_xai_api_key() -> str:
    """Load XAI_API_KEY from .env.xai"""
    if not ENV_XAI_PATH.exists():
        raise FileNotFoundError(f".env.xai not found: {ENV_XAI_PATH}")

    config = dotenv_values(ENV_XAI_PATH)
    api_key = config.get("XAI_API_KEY")

    if not api_key:
        raise ValueError("XAI_API_KEY not found in .env.xai")

    return api_key


def main() -> int:
    """メイン処理"""
    print("=" * 60)
    print("xAI Live Search Test: 2025-10-31 日経平均終値")
    print("=" * 60)

    try:
        api_key = load_xai_api_key()
        print(f"[OK] API Key loaded\n")

        # xAI APIに直接HTTPリクエスト
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
                    "content": "2025年10月31日の日経平均株価の終値を調べてください。東証公式サイトまたは日経新聞から正確な数値を取得し、出典URLも教えてください。"
                }
            ],
            "search": True,  # Live Search有効化
            "temperature": 0.1
        }

        print("[INFO] Sending request with search=True...")
        print(f"[INFO] Endpoint: {url}")
        print(f"[INFO] Payload: {json.dumps(payload, ensure_ascii=False, indent=2)}\n")

        response = requests.post(url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()

        result = response.json()

        print("=" * 60)
        print("Response")
        print("=" * 60)

        print(f"\nStatus Code: {response.status_code}")
        print(f"Model: {result.get('model', 'N/A')}")

        if 'choices' in result and len(result['choices']) > 0:
            choice = result['choices'][0]
            print(f"Finish Reason: {choice.get('finish_reason', 'N/A')}")

            content = choice.get('message', {}).get('content', '')
            print(f"\n[Content]")
            print(content)

        if 'usage' in result:
            usage = result['usage']
            print(f"\n[Usage]")
            print(f"  Input tokens: {usage.get('prompt_tokens', 0)}")
            print(f"  Output tokens: {usage.get('completion_tokens', 0)}")
            print(f"  Total tokens: {usage.get('total_tokens', 0)}")

        print("\n" + "=" * 60)

        # 実際の終値と比較
        print("\n[検証]")
        print("実際の2025年10月31日の日経平均終値: 52,411円（yfinance）")
        print("Grokの回答と一致するか確認してください")

        return 0

    except requests.exceptions.HTTPError as e:
        print(f"\n[ERROR] HTTP Error: {e}")
        print(f"Response: {e.response.text if e.response else 'N/A'}")
        return 1
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
