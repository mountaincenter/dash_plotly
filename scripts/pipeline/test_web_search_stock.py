#!/usr/bin/env python3
"""
test_web_search_stock.py
xAI SDK 1.3.1でweb_search()を使って日経平均終値を取得

実行方法:
    python3 scripts/pipeline/test_web_search_stock.py
"""
import os
import sys
from pathlib import Path
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from xai_sdk import Client
from xai_sdk.chat import user
from xai_sdk.tools import web_search

# .env.xaiからAPIキー読み込み
ENV_XAI_PATH = ROOT / ".env.xai"
config = dotenv_values(ENV_XAI_PATH)
api_key = config.get("XAI_API_KEY")

if not api_key:
    raise ValueError("XAI_API_KEY not found in .env.xai")

print("=" * 60)
print("Web Search Test: 日経平均終値取得")
print("=" * 60)

client = Client(api_key=api_key)
chat = client.chat.create(
    model="grok-4-fast",
    tools=[
        web_search(),
    ],
)

# 2025年10月31日の日経平均終値を取得
chat.append(user("2025年10月31日の日経平均株価（Nikkei 225）の終値を調べてください。正確な数値と出典URLを教えてください。"))

is_thinking = True
for response, chunk in chat.stream():
    # ツール呼び出しを表示
    for tool_call in chunk.tool_calls:
        print(f"\nCalling tool: {tool_call.function.name} with arguments: {tool_call.function.arguments}")

    if response.usage.reasoning_tokens and is_thinking:
        print(f"\rThinking... ({response.usage.reasoning_tokens} tokens)", end="", flush=True)

    if chunk.content and is_thinking:
        print("\n\nFinal Response:")
        is_thinking = False

    if chunk.content and not is_thinking:
        print(chunk.content, end="", flush=True)

print("\n\n" + "=" * 60)
print("Citations:")
print("=" * 60)
for i, citation in enumerate(response.citations, 1):
    print(f"{i}. {citation}")

print("\n" + "=" * 60)
print("Usage:")
print("=" * 60)
print(f"Completion tokens: {response.usage.completion_tokens}")
print(f"Prompt tokens: {response.usage.prompt_tokens}")
print(f"Total tokens: {response.usage.total_tokens}")
print(f"Reasoning tokens: {response.usage.reasoning_tokens}")
print(f"Server-side tools used: {response.usage.server_side_tools_used}")
print(f"Tool usage count: {response.server_side_tool_usage}")

print("\n" + "=" * 60)
print("Server Side Tool Calls:")
print("=" * 60)
for tool_call in response.tool_calls:
    print(tool_call)

print("\n" + "=" * 60)
print("検証")
print("=" * 60)
print("実際の2025年10月31日の日経平均終値: 52,411円（yfinance）")
print("Grokの回答と一致するか確認してください")
