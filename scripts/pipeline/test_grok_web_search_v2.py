#!/usr/bin/env python3
"""
test_grok_web_search_v2.py
xAI公式SDKでweb_searchツールを使用してリアルタイムデータ取得

実行方法:
    python3 scripts/pipeline/test_grok_web_search_v2.py

Requirements:
    - .env.xai with XAI_API_KEY
    - xai-sdk==1.2.0
"""

import sys
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import dotenv_values
from xai_sdk import Client

ENV_XAI_PATH = ROOT / ".env.xai"


def main() -> int:
    """メイン処理"""
    print("=" * 60)
    print("xAI SDK: web_searchツールで株価取得テスト")
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

    print("[OK] API Key loaded\n")

    # xAI公式SDKでクライアント初期化
    try:
        client = Client(api_key=api_key)
        print("[INFO] xAI Client initialized")
    except Exception as e:
        print(f"[ERROR] Failed to initialize client: {e}")
        return 1

    # ツール定義（xAI形式）
    tools = [
        {
            "type": "function",
            "function": {
                "name": "web_search",
                "description": "Search the web for real-time stock market data",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query for stock prices"
                        }
                    },
                    "required": ["query"]
                }
            }
        }
    ]

    # メッセージ構築
    messages = [
        {
            "role": "user",
            "content": "2025年10月31日の日経平均株価（Nikkei 225）の終値をweb_searchツールで検索してください。正確な数値を教えてください。"
        }
    ]

    print("[INFO] Sending request with web_search tool...")
    print(f"Query: {messages[0]['content']}\n")

    try:
        # チャット作成（ツール付き）
        response = client.chat.completions.create(
            model="grok-4-fast-reasoning",
            messages=messages,
            tools=tools,
            tool_choice="auto",  # 自動でツール選択
            temperature=0.1,
            max_tokens=1000
        )

        print("=" * 60)
        print("Response")
        print("=" * 60)

        # レスポンス解析
        choice = response.choices[0]
        print(f"\nFinish Reason: {choice.finish_reason}")

        # ツール呼び出しチェック
        if choice.message.tool_calls:
            print(f"\n[TOOL CALLS] {len(choice.message.tool_calls)} detected:")
            for i, tool_call in enumerate(choice.message.tool_calls, 1):
                print(f"\n  Tool Call #{i}:")
                print(f"    ID: {tool_call.id}")
                print(f"    Function: {tool_call.function.name}")
                print(f"    Arguments: {tool_call.function.arguments}")

            # ツール実行結果を追加して再リクエスト
            print("\n[INFO] Adding tool results and re-querying...")

            messages.append(choice.message)

            # 各ツール呼び出しに対する結果を追加
            for tool_call in choice.message.tool_calls:
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps({
                        "status": "executed",
                        "note": "xAI should execute this server-side"
                    })
                })

            # 2回目のリクエスト
            response2 = client.chat.completions.create(
                model="grok-4-fast-reasoning",
                messages=messages,
                tools=tools,
                temperature=0.1
            )

            print("\n[FINAL RESPONSE]")
            print(response2.choices[0].message.content)

            if hasattr(response2, 'usage'):
                print(f"\n[Usage - 2nd call]")
                print(f"  Prompt: {response2.usage.prompt_tokens}")
                print(f"  Completion: {response2.usage.completion_tokens}")
                print(f"  Total: {response2.usage.total_tokens}")

        else:
            print("\n[NO TOOL CALLS]")
            print(f"\n[Content]")
            print(choice.message.content)

        if hasattr(response, 'usage'):
            print(f"\n[Usage - 1st call]")
            print(f"  Prompt: {response.usage.prompt_tokens}")
            print(f"  Completion: {response.usage.completion_tokens}")
            print(f"  Total: {response.usage.total_tokens}")

        print("\n" + "=" * 60)
        print("\n[検証]")
        print("実際の2025年10月31日の日経平均終値: 52,411円")
        print("Grokの回答と一致するか確認してください")

        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
