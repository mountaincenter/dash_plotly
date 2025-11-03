#!/usr/bin/env python3
"""
test_web_search_only.py
Grok API Web Searchで2025-10-31の日経平均終値を取得

実行方法:
    python3 scripts/pipeline/test_web_search_only.py
"""

import sys
from pathlib import Path

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
        raise FileNotFoundError(f".env.xai not found: {ENV_XAI_PATH}")

    config = dotenv_values(ENV_XAI_PATH)
    api_key = config.get("XAI_API_KEY")

    if not api_key:
        raise ValueError("XAI_API_KEY not found in .env.xai")

    return api_key


def main() -> int:
    """メイン処理"""
    print("=" * 60)
    print("Web Search Test: 2025-10-31 日経平均終値")
    print("=" * 60)

    try:
        api_key = load_xai_api_key()
        print(f"[OK] API Key loaded\n")

        client = OpenAI(
            api_key=api_key,
            base_url="https://api.x.ai/v1",
            http_client=httpx.Client(),
        )

        # Web Search有効でリクエスト
        print("[INFO] Sending request to Grok with Web Search tool...")

        response = client.chat.completions.create(
            model="grok-2-latest",
            messages=[
                {
                    "role": "user",
                    "content": "2025年10月31日の日経平均株価の終値を調べてください。東証公式サイトまたは日経新聞から正確な数値を取得し、出典URLも教えてください。"
                }
            ],
            search=True,  # Live Search有効化
            temperature=0.1,
        )

        print("\n" + "=" * 60)
        print("Response")
        print("=" * 60)

        print(f"\nFinish Reason: {response.choices[0].finish_reason}")
        print(f"Model: {response.model}")

        # Tool callsを確認
        if response.choices[0].message.tool_calls:
            print("\n[Tool Calls Detected]")
            for i, tool_call in enumerate(response.choices[0].message.tool_calls, 1):
                print(f"\n  Tool Call #{i}:")
                print(f"    ID: {tool_call.id}")
                print(f"    Function: {tool_call.function.name}")
                print(f"    Arguments: {tool_call.function.arguments}")
        else:
            print("\n[No Tool Calls] Grokはツールを使わずに回答しました")

        # 最終コンテンツ
        content = response.choices[0].message.content
        print(f"\n[Content (1st response)]")
        print(content if content else "(empty - tool_calls only)")

        # Usage情報
        if hasattr(response, 'usage') and response.usage:
            print(f"\n[Usage (1st response)]")
            print(f"  Input tokens: {response.usage.prompt_tokens}")
            print(f"  Output tokens: {response.usage.completion_tokens}")
            print(f"  Total tokens: {response.usage.total_tokens}")

        # ツール実行後の2回目のリクエスト
        if response.choices[0].message.tool_calls:
            print("\n" + "=" * 60)
            print("2nd Request: Processing tool results")
            print("=" * 60)

            # messagesに1回目のレスポンスを追加
            messages = [
                {
                    "role": "user",
                    "content": "2025年10月31日の日経平均株価の終値を調べてください。東証公式サイトまたは日経新聞から正確な数値を取得し、出典URLも教えてください。"
                },
                response.choices[0].message  # 1回目のレスポンス（tool_calls含む）
            ]

            # ツール実行結果をシミュレート（実際はxAI側で自動実行される）
            for tool_call in response.choices[0].message.tool_calls:
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": f"Web search executed for query: {tool_call.function.arguments}. Please use the search results to answer the question."
                })

            # 2回目のAPI呼び出し
            print("\n[INFO] Sending 2nd request with tool results...")

            response2 = client.chat.completions.create(
                model="grok-2-latest",
                messages=messages,
                tools=[
                    {
                        "type": "function",
                        "function": {
                            "name": "web_search",
                            "description": "Search the web for information",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "query": {"type": "string", "description": "The search query"}
                                },
                                "required": ["query"]
                            }
                        }
                    }
                ],
                temperature=0.1,
            )

            print("\n" + "=" * 60)
            print("2nd Response (Final Answer)")
            print("=" * 60)

            print(f"\nFinish Reason: {response2.choices[0].finish_reason}")

            final_content = response2.choices[0].message.content
            print(f"\n[Final Content]")
            print(final_content)

            # Usage情報
            if hasattr(response2, 'usage') and response2.usage:
                print(f"\n[Usage (2nd response)]")
                print(f"  Input tokens: {response2.usage.prompt_tokens}")
                print(f"  Output tokens: {response2.usage.completion_tokens}")
                print(f"  Total tokens: {response2.usage.total_tokens}")

        print("\n" + "=" * 60)

        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
