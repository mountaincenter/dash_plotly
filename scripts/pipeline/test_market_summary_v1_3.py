#!/usr/bin/env python3
"""
test_market_summary_v1_3.py
v1.3プロンプト（J-Quantsデータ統合版）でmarket summaryを生成テスト

実行方法:
    python3 scripts/pipeline/test_market_summary_v1_3.py
"""
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import dotenv_values
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from xai_sdk import Client
from xai_sdk.chat import user, system
from xai_sdk.tools import web_search
from common_cfg.paths import PARQUET_DIR

# プロンプトインポート
sys.path.insert(0, str(ROOT / "data" / "prompts"))
from v1_3_market_summary import build_market_summary_prompt, format_jquants_table

# .env.xaiからAPIキー読み込み
ENV_XAI_PATH = ROOT / ".env.xai"
config = dotenv_values(ENV_XAI_PATH)
api_key = config.get("XAI_API_KEY")

if not api_key:
    raise ValueError("XAI_API_KEY not found in .env.xai")

print("=" * 60)
print("Market Summary v1.3 Test (J-Quants Integration)")
print("=" * 60)


def calculate_change_pct(df: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
    """前日比を計算"""
    if df.empty:
        return df

    prev_date = target_date.date() - timedelta(days=1)
    df_copy = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date

    current = df_copy[df_copy['date'] == target_date.date()]
    previous = df_copy[df_copy['date'] <= prev_date].groupby('ticker').tail(1)

    if previous.empty:
        current['change_pct'] = float('nan')
        current['change'] = float('nan')
        return current

    merged = current.merge(
        previous[['ticker', 'close']],
        on='ticker',
        how='left',
        suffixes=('', '_prev')
    )

    merged['change'] = merged['close'] - merged['close_prev']
    merged['change_pct'] = ((merged['close'] - merged['close_prev']) / merged['close_prev'] * 100)

    return merged


def load_jquants_data(target_date: datetime) -> dict:
    """J-Quantsデータを読み込み"""
    result = {'date': target_date.strftime("%Y-%m-%d")}

    # TOPIX系指数
    topix_file = PARQUET_DIR / "topix_prices_max_1d.parquet"
    if topix_file.exists():
        df = pd.read_parquet(topix_file)
        df['date'] = pd.to_datetime(df['date'])
        result['topix'] = calculate_change_pct(df, target_date)
        print(f"[OK] Loaded TOPIX: {len(result['topix'])} indices")
    else:
        result['topix'] = pd.DataFrame()
        print(f"[WARN] TOPIX file not found")

    # 33業種別指数
    sectors_file = PARQUET_DIR / "sectors_prices_max_1d.parquet"
    if sectors_file.exists():
        df = pd.read_parquet(sectors_file)
        df['date'] = pd.to_datetime(df['date'])
        result['sectors'] = calculate_change_pct(df, target_date)
        print(f"[OK] Loaded Sectors: {len(result['sectors'])} sectors")
    else:
        result['sectors'] = pd.DataFrame()
        print(f"[WARN] Sectors file not found")

    # 17業種別指数
    series_file = PARQUET_DIR / "series_prices_max_1d.parquet"
    if series_file.exists():
        df = pd.read_parquet(series_file)
        df['date'] = pd.to_datetime(df['date'])
        result['series'] = calculate_change_pct(df, target_date)
        print(f"[OK] Loaded Series: {len(result['series'])} series")
    else:
        result['series'] = pd.DataFrame()
        print(f"[WARN] Series file not found")

    return result


# テスト対象日（2025-10-31）
target_date = datetime.strptime('2025-10-31', '%Y-%m-%d')
print(f"\nTarget Date: {target_date.strftime('%Y-%m-%d')}")

# J-Quantsデータ読み込み
print("\nLoading J-Quants data...")
jquants_data = load_jquants_data(target_date)

# データプレビュー
if not jquants_data['topix'].empty:
    print("\nTOPIX Preview:")
    print(jquants_data['topix'][['ticker', 'name', 'close', 'change_pct']].to_string(index=False))

if not jquants_data['sectors'].empty:
    print("\nSectors Top 5 by change_pct:")
    top5 = jquants_data['sectors'].sort_values('change_pct', ascending=False).head(5)
    print(top5[['ticker', 'name', 'close', 'change_pct']].to_string(index=False))

# コンテキスト構築
context = {
    'execution_date': target_date.strftime("%Y-%m-%d"),
    'latest_trading_day': target_date.strftime("%Y-%m-%d"),
    'report_time': '16:00',
    'jquants_topix': jquants_data['topix'],
    'jquants_sectors': jquants_data['sectors'],
    'jquants_series': jquants_data['series'],
}

# プロンプト構築
print("\nBuilding prompt...")
prompt_text = build_market_summary_prompt(context)
print(f"Prompt Length: {len(prompt_text)} chars")

# Grok API呼び出し
print("\n" + "=" * 60)
print("Calling Grok API...")
print("=" * 60)

client = Client(api_key=api_key)
chat = client.chat.create(
    model="grok-4-fast-reasoning",
    tools=[web_search()],
)

# システムメッセージとユーザープロンプト
chat.append(system("あなたは経験豊富な日本株アナリストです。提供されたJ-Quantsデータをそのまま使用し、Web Searchツールはニュース・トレンド取得のみに使用してください。"))
chat.append(user(prompt_text))

# ストリーミング処理
is_thinking = True
full_response = ""

for response, chunk in chat.stream():
    # ツール呼び出しを表示
    for tool_call in chunk.tool_calls:
        print(f"\n[Tool Call] {tool_call.function.name}")
        args_str = str(tool_call.function.arguments)
        print(f"Arguments: {args_str[:150]}...")

    # Thinking表示
    if response.usage.reasoning_tokens and is_thinking:
        print(f"\rThinking... ({response.usage.reasoning_tokens} tokens)", end="", flush=True)

    # コンテンツ出力開始
    if chunk.content and is_thinking:
        print("\n\n" + "=" * 60)
        print("Market Summary Report (v1.3)")
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

# [確認中]プレースホルダーの検出
placeholder_count = full_response.count('[確認中]')
print(f"\n[確認中] placeholders: {placeholder_count}")
if placeholder_count > 0:
    print("  ⚠️  v1.3 should eliminate most placeholders with J-Quants data")
else:
    print("  ✅ No placeholders detected!")

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

output_file = output_dir / f"market_summary_v1_3_{context['latest_trading_day']}.md"
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(full_response)

print(f"\n✅ Saved to: {output_file}")

print("\n" + "=" * 60)
print("Test Completed")
print("=" * 60)
