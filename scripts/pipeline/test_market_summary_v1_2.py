#!/usr/bin/env python3
"""
test_market_summary_v1_2.py
v1.2プロンプトでmarket summaryを生成テスト

実行方法:
    python3 scripts/pipeline/test_market_summary_v1_2.py
"""
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from xai_sdk import Client
from xai_sdk.chat import user, system
from xai_sdk.tools import web_search

# プロンプトインポート
sys.path.insert(0, str(ROOT / "data" / "prompts"))
from v1_2_market_summary import build_market_summary_prompt, get_prompt_metadata

# .env.xaiからAPIキー読み込み
ENV_XAI_PATH = ROOT / ".env.xai"
config = dotenv_values(ENV_XAI_PATH)
api_key = config.get("XAI_API_KEY")

if not api_key:
    raise ValueError("XAI_API_KEY not found in .env.xai")

print("=" * 60)
print("Market Summary v1.2 Test")
print("=" * 60)

# メタデータ表示
metadata = get_prompt_metadata()
print(f"\nPrompt Version: {metadata['version']}")
print(f"Model: {metadata['recommended_settings']['model']}")
print(f"Tools: {metadata['recommended_settings']['tools']}")
print(f"Expected Length: {metadata['expected_length']}")

# コンテキスト構築（2025-10-31をテスト）
context = {
    'execution_date': '2025-11-01',
    'latest_trading_day': '2025-10-31',
    'report_time': '16:00'
}

print(f"\nTarget Date: {context['latest_trading_day']}")
print(f"Execution Date: {context['execution_date']}")

# プロンプト構築
prompt_text = build_market_summary_prompt(context)
print(f"\nPrompt Length: {len(prompt_text)} chars")

# Grok API呼び出し
print("\n" + "=" * 60)
print("Calling Grok API...")
print("=" * 60)

client = Client(api_key=api_key)
chat = client.chat.create(
    model="grok-4-fast",
    tools=[web_search()],
)

# システムメッセージとユーザープロンプト
chat.append(system("あなたは経験豊富な日本株アナリストです。Web Searchツールを使用して正確な市場データを取得してください。"))
chat.append(user(prompt_text))

# ストリーミング処理
is_thinking = True
full_response = ""

for response, chunk in chat.stream():
    # ツール呼び出しを表示
    for tool_call in chunk.tool_calls:
        print(f"\n[Tool Call] {tool_call.function.name}")
        print(f"Arguments: {tool_call.function.arguments[:100]}...")

    # Thinking表示
    if response.usage.reasoning_tokens and is_thinking:
        print(f"\rThinking... ({response.usage.reasoning_tokens} tokens)", end="", flush=True)

    # コンテンツ出力開始
    if chunk.content and is_thinking:
        print("\n\n" + "=" * 60)
        print("Market Summary Report")
        print("=" * 60 + "\n")
        is_thinking = False

    # レスポンスを蓄積
    if chunk.content and not is_thinking:
        print(chunk.content, end="", flush=True)
        full_response += chunk.content

# 統計情報
print("\n\n" + "=" * 60)
print("Statistics")
print("=" * 60)

print(f"\nResponse Length: {len(full_response)} chars")
print(f"Target: 1000-1500 chars")
print(f"Match: {'✅' if 1000 <= len(full_response) <= 1500 else '❌'}")

print(f"\nCitations: {len(response.citations)}")
for i, citation in enumerate(response.citations[:5], 1):
    print(f"  {i}. {citation}")
if len(response.citations) > 5:
    print(f"  ... and {len(response.citations) - 5} more")

print(f"\nUsage:")
print(f"  Completion tokens: {response.usage.completion_tokens}")
print(f"  Prompt tokens: {response.usage.prompt_tokens}")
print(f"  Total tokens: {response.usage.total_tokens}")
print(f"  Reasoning tokens: {response.usage.reasoning_tokens}")
print(f"  Server-side tools used: {response.usage.server_side_tools_used}")

print(f"\nTool Usage Count:")
for tool, count in response.server_side_tool_usage.items():
    print(f"  {tool}: {count}")

print(f"\nTool Calls:")
for tool_call in response.tool_calls:
    print(f"  - {tool_call.function.name}")

# 保存
output_dir = ROOT / "data" / "test_output"
output_dir.mkdir(parents=True, exist_ok=True)

output_file = output_dir / f"market_summary_v1_2_{context['latest_trading_day']}.md"
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(full_response)

print(f"\n✅ Saved to: {output_file}")

print("\n" + "=" * 60)
print("Test Completed")
print("=" * 60)
