#!/usr/bin/env python3
"""
test_grok_tools.py
Grok API Function Calling (Web Search / X Search) の最小テスト

実行方法:
    python3 scripts/pipeline/test_grok_tools.py

Requirements:
    - .env.xai with XAI_API_KEY
    - openai library

出力:
    実際にWeb Search/X Searchが動作するか確認
"""

import sys
import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import httpx
from openai import OpenAI
from dotenv import dotenv_values

ENV_XAI_PATH = ROOT / ".env.xai"


def load_xai_api_key() -> str:
    """Load XAI_API_KEY from .env.xai"""
    if not ENV_XAI_PATH.exists():
        raise FileNotFoundError(
            f".env.xai not found: {ENV_XAI_PATH}\n"
            "Please create .env.xai with XAI_API_KEY=your_api_key"
        )

    config = dotenv_values(ENV_XAI_PATH)
    api_key = config.get("XAI_API_KEY")

    if not api_key:
        raise ValueError("XAI_API_KEY not found in .env.xai")

    return api_key


def test_web_search(api_key: str) -> None:
    """
    Test Web Search tool

    今日の日経平均終値を取得
    """
    print("\n" + "=" * 60)
    print("TEST 1: Web Search - 日経平均終値取得")
    print("=" * 60)

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
        http_client=httpx.Client(),
    )

    today = datetime.now().strftime("%Y年%m月%d日")

    messages = [
        {
            "role": "user",
            "content": f"{today}の日経平均株価の終値を調べてください。東証公式サイトまたは日経新聞から正確な数値を取得してください。"
        }
    ]

    # ツール定義
    tools = [
        {
            "type": "function",
            "function": {
                "name": "web_search",
                "description": "Search the web for information",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The search query"
                        }
                    },
                    "required": ["query"]
                }
            }
        }
    ]

    print(f"[INFO] Sending request to Grok API with Web Search tool...")

    response = client.chat.completions.create(
        model="grok-2-latest",
        messages=messages,
        tools=tools,
        tool_choice="auto",  # Grokが自動でツールを使うか判断
        temperature=0.1,
    )

    print(f"\n[RESPONSE]")
    print(f"Model: {response.model}")
    print(f"Finish reason: {response.choices[0].finish_reason}")

    # ツール呼び出しがあったか確認
    if response.choices[0].message.tool_calls:
        print(f"\n[TOOL CALLS] {len(response.choices[0].message.tool_calls)} call(s) detected:")
        for i, tool_call in enumerate(response.choices[0].message.tool_calls, 1):
            print(f"\n  Tool Call #{i}:")
            print(f"    Function: {tool_call.function.name}")
            print(f"    Arguments: {tool_call.function.arguments}")

    # 最終レスポンス
    content = response.choices[0].message.content
    print(f"\n[CONTENT]")
    print(content)

    return content


def test_x_search(api_key: str) -> None:
    """
    Test X Search tool

    特定銘柄のXバズを検索
    """
    print("\n" + "=" * 60)
    print("TEST 2: X Search - 銘柄3914 JIG-SAWのバズ検索")
    print("=" * 60)

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
        http_client=httpx.Client(),
    )

    messages = [
        {
            "role": "user",
            "content": "銘柄コード3914（JIG-SAW）について、過去24時間のX（Twitter）での言及を調べてください。投稿数とセンチメントを教えてください。"
        }
    ]

    # ツール定義
    tools = [
        {
            "type": "function",
            "function": {
                "name": "x_search",
                "description": "Search X (Twitter) posts",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The search query"
                        }
                    },
                    "required": ["query"]
                }
            }
        }
    ]

    print(f"[INFO] Sending request to Grok API with X Search tool...")

    response = client.chat.completions.create(
        model="grok-2-latest",
        messages=messages,
        tools=tools,
        tool_choice="auto",
        temperature=0.1,
    )

    print(f"\n[RESPONSE]")
    print(f"Model: {response.model}")
    print(f"Finish reason: {response.choices[0].finish_reason}")

    # ツール呼び出しがあったか確認
    if response.choices[0].message.tool_calls:
        print(f"\n[TOOL CALLS] {len(response.choices[0].message.tool_calls)} call(s) detected:")
        for i, tool_call in enumerate(response.choices[0].message.tool_calls, 1):
            print(f"\n  Tool Call #{i}:")
            print(f"    Function: {tool_call.function.name}")
            print(f"    Arguments: {tool_call.function.arguments}")

    # 最終レスポンス
    content = response.choices[0].message.content
    print(f"\n[CONTENT]")
    print(content)

    return content


def main() -> int:
    """メイン処理"""
    print("=" * 60)
    print("Grok API Function Calling Test")
    print("=" * 60)

    try:
        # API Key読み込み
        api_key = load_xai_api_key()
        print(f"[OK] XAI_API_KEY loaded from {ENV_XAI_PATH}")

        # Test 1: Web Search
        web_result = test_web_search(api_key)

        # Test 2: X Search
        x_result = test_x_search(api_key)

        print("\n" + "=" * 60)
        print("All tests completed!")
        print("=" * 60)
        print("\n[結論]")
        print("✅ Web Search: 実行された" if web_result else "❌ Web Search: 実行されなかった")
        print("✅ X Search: 実行された" if x_result else "❌ X Search: 実行されなかった")

        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
