#!/usr/bin/env python3
"""
test_generate_grok_trending.py
v1.3 Zero Labelプロンプトを使ってテスト実行するためのスクリプト

実行方法:
    # 2025-11-06の銘柄を選定（2025-11-05 23時選定想定）
    python3 scripts/pipeline/test_generate_grok_trending.py --target-date 2025-11-06

    # 2025-11-05の銘柄を選定（2025-11-04 23時選定想定）
    python3 scripts/pipeline/test_generate_grok_trending.py --target-date 2025-11-05

出力:
    test_output/v1.3_grok_trending_20251106.parquet
    test_output/v1.3_grok_trending_20251105.parquet

備考:
    - .env.xai に XAI_API_KEY が必要
    - v1.3_zero_label プロンプトを使用（固定）
    - web_search() + x_search() tools 有効化
"""

from __future__ import annotations

import sys
import json
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from dotenv import dotenv_values
from xai_sdk import Client
from xai_sdk.chat import user, system
from xai_sdk.tools import web_search, x_search

from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_fetcher import JQuantsFetcher

# 出力先ディレクトリ
TEST_OUTPUT_DIR = ROOT / "test_output"
ENV_XAI_PATH = ROOT / ".env.xai"


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="Test Grok trending stocks with v1.3 Zero Label")
    parser.add_argument(
        "--target-date",
        type=str,
        required=True,
        help="Target date for stock selection (YYYY-MM-DD format). This is the trading day for which to select stocks."
    )
    return parser.parse_args()


def load_backtest_patterns() -> dict[str, Any]:
    """
    バックテストパターンを読み込み、Grokプロンプト用のコンテキストを生成
    （generate_grok_trending.py から流用）

    Returns:
        dict: バックテストパターン情報
    """
    patterns_file = PARQUET_DIR / "grok_backtest_patterns.parquet"
    top_performers_file = PARQUET_DIR / "grok_top_performers.parquet"
    worst_performers_file = PARQUET_DIR / "grok_worst_performers.parquet"

    backtest_context = {
        'has_data': False,
        'phase1_success_rate': 0,
        'phase1_avg_return': 0,
        'phase2_achievement_rate': 0,
        'success_categories': [],
        'failure_categories': [],
        'top_performers': [],
        'worst_performers': []
    }

    # パターンサマリーを読み込み
    if patterns_file.exists():
        try:
            df_patterns = pd.read_parquet(patterns_file)

            # Phase1成功パターン
            phase1_success = df_patterns[df_patterns['pattern_type'] == 'phase1_success']
            if len(phase1_success) > 0:
                row = phase1_success.iloc[0]
                backtest_context['has_data'] = True
                backtest_context['phase1_success_rate'] = row['rate']
                backtest_context['phase1_avg_return'] = row['avg_return']
                backtest_context['phase1_avg_sentiment'] = row['avg_sentiment']
                backtest_context['phase1_success_category'] = row['top_category']

            # Phase1失敗パターン
            phase1_failure = df_patterns[df_patterns['pattern_type'] == 'phase1_failure']
            if len(phase1_failure) > 0:
                row = phase1_failure.iloc[0]
                backtest_context['phase1_failure_rate'] = row['rate']
                backtest_context['phase1_avg_loss'] = row['avg_return']
                backtest_context['phase1_failure_category'] = row['top_category']

            # Phase2成功パターン
            phase2_success = df_patterns[df_patterns['pattern_type'] == 'phase2_success']
            if len(phase2_success) > 0:
                row = phase2_success.iloc[0]
                backtest_context['phase2_achievement_rate'] = row['rate']
                backtest_context['phase2_avg_return'] = row['avg_return']
                backtest_context['phase2_success_category'] = row['top_category']

            print(f"[OK] Loaded backtest patterns: Phase1勝率 {backtest_context['phase1_success_rate']:.1f}%")

        except Exception as e:
            print(f"[WARN] Failed to load backtest patterns: {e}")

    # Top成功銘柄を読み込み
    if top_performers_file.exists():
        try:
            df_top = pd.read_parquet(top_performers_file)
            for _, row in df_top.head(3).iterrows():
                backtest_context['top_performers'].append({
                    'ticker': row['ticker'],
                    'name': row.get('stock_name', 'N/A'),
                    'categories': row.get('categories', 'N/A'),
                    'return': row.get('morning_change_pct', 0)
                })
        except Exception as e:
            print(f"[WARN] Failed to load top performers: {e}")

    # Worst失敗銘柄を読み込み
    if worst_performers_file.exists():
        try:
            df_worst = pd.read_parquet(worst_performers_file)
            for _, row in df_worst.head(3).iterrows():
                backtest_context['worst_performers'].append({
                    'ticker': row['ticker'],
                    'name': row.get('stock_name', 'N/A'),
                    'categories': row.get('categories', 'N/A'),
                    'return': row.get('morning_change_pct', 0)
                })
        except Exception as e:
            print(f"[WARN] Failed to load worst performers: {e}")

    return backtest_context


def get_trading_context(target_date_str: str) -> dict[str, str]:
    """
    指定された日付を基準に取引コンテキストを生成

    target_date = 翌営業日（銘柄選定対象日）
    latest_trading_day = target_dateの前営業日（選定日の前営業日）
    execution_date = latest_trading_day（23時選定を想定）

    Args:
        target_date_str: 取引対象日（YYYY-MM-DD形式）

    Returns:
        dict: 実行日、最新営業日、翌営業日などの情報
    """
    fetcher = JQuantsFetcher()

    # target_dateをdatetimeに変換
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

    # target_dateの前営業日を取得
    # target_dateから過去10日間の営業日カレンダーを取得
    past_start = target_date - timedelta(days=10)

    params = {
        "from": str(past_start),
        "to": str(target_date)
    }

    response = fetcher.client.request("/markets/trading_calendar", params=params)
    calendar = pd.DataFrame(response["trading_calendar"])

    # 営業日のみフィルタ（HolidayDivision == "1"）
    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
    trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
    trading_days = trading_days.sort_values("Date")

    # target_dateより前の営業日 = 最新営業日（選定日の前営業日）
    past_trading_days = trading_days[trading_days["Date"] < target_date]

    if past_trading_days.empty:
        raise RuntimeError(f"No trading days found before {target_date_str}")

    latest_trading_day = past_trading_days.iloc[-1]["Date"]

    # target_dateが翌営業日
    next_trading_day = target_date

    # 実行日時は「latest_trading_day の 23時選定」を想定
    # 例: 2025-11-06の銘柄を選定 → 2025-11-05 23:00に実行
    execution_date = latest_trading_day

    return {
        "execution_date": execution_date.strftime("%Y年%m月%d日"),
        "latest_trading_day": latest_trading_day.strftime("%Y年%m月%d日"),
        "next_trading_day": next_trading_day.strftime("%Y年%m月%d日"),
        "latest_trading_day_raw": str(latest_trading_day),
        "next_trading_day_raw": str(next_trading_day),
    }


def build_grok_prompt(context: dict[str, str], backtest: dict[str, Any]) -> str:
    """
    v1.3 Zero Labelプロンプトを生成（固定）

    Args:
        context: get_trading_context() で取得したコンテキスト
        backtest: load_backtest_patterns() で取得したバックテストデータ

    Returns:
        str: Grok APIに送信するプロンプト
    """
    # v1.3 Zero Labelプロンプトを使用（固定）
    from data.prompts.v1_3_zero_label import build_grok_prompt as build_v13_prompt

    print(f"[INFO] Using prompt version: v1.3_zero_label (fixed)")
    return build_v13_prompt(context, backtest)


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


def query_grok(api_key: str, prompt: str) -> tuple[str, dict]:
    """
    Query Grok API via xAI SDK with web_search + x_search tools

    Args:
        api_key: xAI API key
        prompt: User prompt

    Returns:
        tuple: (response_content, tool_usage_stats)
    """
    print("[INFO] Querying Grok API with xAI SDK + web_search + x_search tools...")

    client = Client(api_key=api_key)

    # chat.create()でセッション作成
    chat = client.chat.create(
        model="grok-4-fast-reasoning",  # grok-4ファミリー（server-side tools対応）
        tools=[web_search(), x_search()],
    )

    # システムメッセージとユーザープロンプトを追加
    # v1.3ではカテゴリ・ラベル付けを厳しく禁止
    chat.append(system("あなたは日本株市場のデイトレード専門家です。銘柄選定の際は具体的な数値と根拠を示してください。web_searchツールとx_searchツールを積極的に使用して、一次情報に基づいた事実のみを出力してください。カテゴリやラベルは一切使用せず、事実のみを記述してください。"))
    chat.append(user(prompt))

    # ストリーミング処理
    full_response = ""
    tool_calls_count = 0

    for response, chunk in chat.stream():
        # ツール呼び出しをカウント
        if chunk.tool_calls:
            tool_calls_count += len(chunk.tool_calls)

        # レスポンスを蓄積
        if chunk.content:
            full_response += chunk.content

    print(f"[OK] Received response from Grok ({len(full_response)} chars)")

    # ツール使用統計を取得
    tool_stats = {
        "total_tool_calls": tool_calls_count,
        "usage": {
            "completion_tokens": response.usage.completion_tokens if hasattr(response.usage, 'completion_tokens') else 0,
            "prompt_tokens": response.usage.prompt_tokens if hasattr(response.usage, 'prompt_tokens') else 0,
            "total_tokens": response.usage.total_tokens if hasattr(response.usage, 'total_tokens') else 0,
        }
    }

    print(f"[INFO] Tool usage: {tool_calls_count} tool calls (web_search + x_search)")

    return full_response, tool_stats


def parse_grok_response(response: str) -> list[dict[str, Any]]:
    """Parse Grok's JSON response (extract first JSON array)"""
    print("[INFO] Parsing Grok response...")

    # JSONブロックを抽出
    if "```json" in response:
        json_str = response.split("```json")[1].split("```")[0].strip()
    elif "```" in response:
        json_str = response.split("```")[1].split("```")[0].strip()
    else:
        json_str = response.strip()

    # 複数のJSONオブジェクトが含まれている場合、最初のJSON配列のみを抽出
    # 例: [...]  {verification_summary} の場合、[...]部分のみ取得

    # 最後の"]"を探す（配列の終端）
    # ネストしたJSON配列の場合を考慮して、最後の"]"を探す
    last_array_end = json_str.rfind("]")

    # その後に"{" があるか確認（verification_summaryが続く場合）
    remaining = json_str[last_array_end + 1:].strip()
    if remaining and remaining.startswith("{"):
        # verification_summaryがある場合、配列の終端を再設定
        # 配列の最後の要素の"]"を見つける
        # 簡易的に、"}\n]"のパターンを探す
        pattern = "}\n]"
        array_end = json_str.find(pattern)
        if array_end != -1:
            first_json = json_str[:array_end + 3]  # "}\n]"を含める
        else:
            # パターンが見つからない場合は最後の"]"まで
            first_json = json_str[:last_array_end + 1]
    else:
        # verification_summaryがない場合
        first_json = json_str[:last_array_end + 1]

    try:
        data = json.loads(first_json)
        if isinstance(data, list):
            print(f"[OK] Parsed {len(data)} stocks from Grok response")
            return data
        else:
            raise ValueError(f"Expected JSON array, got {type(data)}")
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON: {e}")
        print(f"[DEBUG] Attempted to parse:\n{first_json[:500]}...")
        raise


def calculate_selection_score(item: dict[str, Any]) -> float:
    """選定時点でのスコアを計算"""
    sentiment_score = item.get("sentiment_score", 0.5)
    score = sentiment_score * 100

    policy_link = item.get("policy_link", "Low")
    policy_bonus = {"High": 30, "Med": 20, "Low": 10}
    score += policy_bonus.get(policy_link, 10)

    has_mention = item.get("has_mention", False)
    if has_mention:
        score += 50

    return score


def convert_to_all_stocks_schema(grok_data: list[dict], selected_date: str, selected_time: str, prompt_version: str) -> pd.DataFrame:
    """
    Convert Grok data to all_stocks.parquet compatible schema
    v1.3版: previous_day_change_pct, twitter_mentionsフィールドを追加
    """
    print("[INFO] Converting to all_stocks.parquet compatible schema...")
    print(f"[INFO] Using prompt version: {prompt_version}")

    rows = []
    for idx, item in enumerate(grok_data, 1):
        ticker_symbol = item.get("ticker_symbol", "")
        company_name = item.get("company_name", "")
        reason = item.get("reason", "")
        category = item.get("category", "")  # v1.3ではcategoryフィールドがないはず
        sentiment_score = item.get("sentiment_score", 0.5)
        policy_link = item.get("policy_link", "Low")
        has_mention = item.get("has_mention", False)
        mentioned_by = item.get("mentioned_by", "")
        previous_day_change_pct = item.get("previous_day_change_pct", None)  # v1.3新規フィールド
        twitter_mentions = item.get("twitter_mentions", None)  # v1.3新規フィールド

        # tickerは "1234.T" 形式、codeは "1234" 形式
        ticker = f"{ticker_symbol}.T" if not ticker_symbol.endswith(".T") else ticker_symbol
        code = ticker_symbol.replace(".T", "")

        # 選定スコアを計算
        selection_score = calculate_selection_score(item)

        row = {
            "ticker": ticker,
            "code": code,
            "stock_name": company_name,
            "market": None,
            "sectors": None,
            "series": None,
            "topixnewindexseries": None,
            "categories": ["GROK"],
            "tags": category,
            "reason": reason,
            "date": selected_date,
            "Close": None,
            "change_pct": None,
            "Volume": None,
            "vol_ratio": None,
            "atr14_pct": None,
            "rsi14": None,
            "score": None,
            "key_signal": None,
            "source": "grok",
            "selected_time": selected_time,
            "updated_at": datetime.now().isoformat(),
            "sentiment_score": sentiment_score,
            "policy_link": policy_link,
            "has_mention": has_mention,
            "mentioned_by": mentioned_by,
            "selection_score": selection_score,
            "prompt_version": prompt_version,
            "previous_day_change_pct": previous_day_change_pct,  # v1.3新規フィールド
            "twitter_mentions": twitter_mentions,  # v1.3新規フィールド
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # selection_scoreで降順ソート
    df = df.sort_values("selection_score", ascending=False).reset_index(drop=True)

    # ランクを再計算
    df["grok_rank"] = range(1, len(df) + 1)
    df["selection_rank"] = range(1, len(df) + 1)

    # tagsを再構築
    def build_tags(row):
        tags_list = []
        if row.get("tags"):
            tags_list.append(row["tags"])
        tags_list.append(f"{row['selection_score']:.1f}")
        if row["selection_rank"] <= 5:
            tags_list.append("⭐Top5")
        return ",".join(tags_list)

    df["tags"] = df.apply(build_tags, axis=1)

    print(f"[OK] Converted {len(df)} stocks to DataFrame")
    print(f"[INFO] Top 5 by selection_score:")
    for i, row in df.head(5).iterrows():
        print(f"  {row['selection_rank']}. {row['ticker']} - {row['stock_name']} (Score: {row['selection_score']:.1f})")

    return df


def main() -> int:
    """メイン処理（v1.3テスト実行版）"""
    print("=" * 60)
    print("Test Grok Trending Stocks with v1.3 Zero Label")
    print("=" * 60)

    # コマンドライン引数をパース
    args = parse_args()

    # プロンプトバージョンは v1.3_zero_label 固定
    prompt_version = "v1.3_zero_label"
    print(f"[INFO] Prompt version: {prompt_version} (fixed)")

    # 対象日と選定時刻
    target_date = args.target_date
    selected_time = "23:00"  # 23時選定固定
    print(f"[INFO] Target date: {target_date}")
    print(f"[INFO] Selected time: {selected_time} (fixed)")

    # 出力ディレクトリ作成
    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Get trading context
        print("[INFO] Fetching trading calendar from J-Quants...")
        context = get_trading_context(target_date)
        print(f"[OK] Execution date: {context['execution_date']}")
        print(f"[OK] Latest trading day: {context['latest_trading_day']}")
        print(f"[OK] Next trading day: {context['next_trading_day']}")
        print()

        # 2. Load backtest patterns
        print("[INFO] Loading backtest patterns...")
        backtest = load_backtest_patterns()
        if backtest.get('has_data'):
            print(f"[OK] Backtest data loaded (Phase1勝率: {backtest['phase1_success_rate']:.1f}%)")
        else:
            print("[INFO] No backtest data available yet")
        print()

        # 3. Build v1.3 prompt
        prompt = build_grok_prompt(context, backtest)
        print(f"[INFO] Built v1.3 Zero Label prompt ({len(prompt)} chars)")
        print()

        # 4. Load API key
        api_key = load_xai_api_key()
        print(f"[OK] Loaded XAI_API_KEY from {ENV_XAI_PATH}")
        print()

        # 5. Query Grok with xAI SDK + web_search + x_search
        response, tool_stats = query_grok(api_key, prompt)
        print()

        # 6. Parse response
        grok_data = parse_grok_response(response)
        print()

        # 7. Convert to DataFrame
        selected_date = context['next_trading_day_raw']
        df = convert_to_all_stocks_schema(grok_data, selected_date, selected_time, prompt_version)
        print()

        # 8. Preview
        print("[INFO] Preview (first 5 stocks):")
        print("-" * 80)
        for i, row in df.head(5).iterrows():
            print(f"{i+1}. {row['ticker']} - {row['stock_name']}")
            if row.get('tags'):
                print(f"   Tags: {row['tags']}")
            print(f"   前日騰落率: {row.get('previous_day_change_pct', 'N/A')}%")
            print(f"   Twitter言及: {row.get('twitter_mentions', 'N/A')}件")
            print(f"   Reason: {row['reason'][:80]}..." if len(row['reason']) > 80 else f"   Reason: {row['reason']}")
            print()

        # 9. Save to test_output
        # v1.3_grok_trending_YYYYMMDD.parquet 形式
        date_str = target_date.replace("-", "")
        output_parquet = TEST_OUTPUT_DIR / f"v1.3_grok_trending_{date_str}.parquet"

        # Parquet保存
        df.to_parquet(output_parquet, index=False)
        print(f"[OK] Saved: {output_parquet}")
        print(f"     Total stocks: {len(df)}")
        print(f"     Prompt version: {prompt_version}")

        print("\n" + "=" * 60)
        print(f"[OK] Generated {len(df)} Grok trending stocks (v1.3)")
        print(f"[OK] Output: {output_parquet}")
        print(f"[INFO] Tool usage: {tool_stats}")
        print("=" * 60)

        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
