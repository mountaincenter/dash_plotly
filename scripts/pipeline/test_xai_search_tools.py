#!/usr/bin/env python3
"""
test_xai_search_tools.py
xAI Search Tools（公式ドキュメント準拠）のテスト

公式ドキュメント:
    https://docs.x.ai/docs/guides/tools/search-tools

実行方法:
    python3 scripts/pipeline/test_xai_search_tools.py
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


def test_web_search(api_key: str) -> dict:
    """
    Test 1: Web Search Tool

    2025年10月31日の日経平均終値を取得
    """
    print("\n" + "=" * 60)
    print("TEST 1: Web Search - 日経平均終値取得")
    print("=" * 60)

    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    # grok-4-fast-reasoning でツール機能をテスト
    payload = {
        "model": "grok-4-fast-reasoning",
        "messages": [
            {
                "role": "system",
                "content": "あなたは金融データの専門家です。正確な数値とソースを提供してください。"
            },
            {
                "role": "user",
                "content": "2025年10月31日の日経平均株価の終値を調べてください。正確な数値と出典URLを教えてください。"
            }
        ],
        "search": True,  # Live Search有効化
        "temperature": 0.1
    }

    print("[INFO] Sending request with web_search tool...")
    print(f"[DEBUG] Payload:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n")

    response = requests.post(url, headers=headers, json=payload, timeout=120)

    if response.status_code != 200:
        print(f"[ERROR] Status: {response.status_code}")
        print(f"[ERROR] Response: {response.text}")
        try:
            error_json = response.json()
            print(f"[ERROR] JSON: {json.dumps(error_json, indent=2, ensure_ascii=False)}")
        except:
            pass

    response.raise_for_status()

    result = response.json()

    print("\n[Response]")
    print(f"Status: {response.status_code}")
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
        print(f"  Prompt tokens: {usage.get('prompt_tokens', 0)}")
        print(f"  Completion tokens: {usage.get('completion_tokens', 0)}")
        print(f"  Total tokens: {usage.get('total_tokens', 0)}")

    return result


def test_x_search(api_key: str) -> dict:
    """
    Test 2: X Search Tool

    銘柄3914（JIG-SAW）の過去24時間のバズを検索
    """
    print("\n" + "=" * 60)
    print("TEST 2: X Search - 銘柄3914 JIG-SAWのバズ検索")
    print("=" * 60)

    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    # grok-4-fast-reasoning でX検索をテスト
    payload = {
        "model": "grok-4-fast-reasoning",
        "messages": [
            {
                "role": "system",
                "content": "あなたはX（Twitter）のトレンド分析の専門家です。投稿数とセンチメントを正確に分析してください。"
            },
            {
                "role": "user",
                "content": "銘柄コード3914（JIG-SAW）について、過去24時間のX（Twitter）での言及を調べてください。投稿数、主なトピック、センチメント（ポジティブ/ネガティブ）を教えてください。"
            }
        ],
        "search": True,  # Live Search有効化
        "temperature": 0.1
    }

    print("[INFO] Sending request with x_search tool...")
    print(f"[DEBUG] Payload:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n")

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()

    result = response.json()

    print("\n[Response]")
    print(f"Status: {response.status_code}")
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
        print(f"  Prompt tokens: {usage.get('prompt_tokens', 0)}")
        print(f"  Completion tokens: {usage.get('completion_tokens', 0)}")
        print(f"  Total tokens: {usage.get('total_tokens', 0)}")

    return result


def main() -> int:
    """メイン処理"""
    print("=" * 60)
    print("xAI Search Tools Test (Official Documentation)")
    print("=" * 60)

    try:
        api_key = load_xai_api_key()
        print("[OK] API Key loaded")

        # Test 1: Web Search
        web_result = test_web_search(api_key)

        # Test 2: X Search
        x_result = test_x_search(api_key)

        print("\n" + "=" * 60)
        print("All Tests Completed")
        print("=" * 60)

        print("\n[検証]")
        print("実際の2025年10月31日の日経平均終値: 52,411円")
        print("Grokの回答と一致するか確認してください")

        return 0

    except requests.exceptions.HTTPError as e:
        print(f"\n[ERROR] HTTP Error: {e}")
        if e.response:
            print(f"Response: {e.response.text}")
            try:
                error_json = e.response.json()
                print(f"Error JSON: {json.dumps(error_json, indent=2, ensure_ascii=False)}")
            except:
                pass
        return 1
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
