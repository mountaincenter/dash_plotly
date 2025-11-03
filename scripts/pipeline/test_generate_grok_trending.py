#!/usr/bin/env python3
"""
test_generate_grok_trending.py
xAI SDK + web_search() + x_search() を使った「翌営業日デイトレ銘柄」選定テスト

実行方法:
    # v1_1_web_searchプロンプトでテスト実行
    PROMPT_VERSION=v1_1_web_search python3 scripts/pipeline/test_generate_grok_trending.py

出力:
    data/test_output/grok_trending_YYYYMMDD.parquet
    data/test_output/grok_trending_YYYYMMDD.json (デバッグ用)

備考:
    - .env.xai に XAI_API_KEY が必要
    - xAI SDK (xai_sdk) を使用
    - web_search() + x_search() tools 有効化
    - プロンプトバージョンは環境変数 PROMPT_VERSION で指定
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
TEST_OUTPUT_DIR = ROOT / "data" / "test_output"
ENV_XAI_PATH = ROOT / ".env.xai"


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="Test Grok trending stocks with xAI SDK")
    parser.add_argument(
        "--prompt-version",
        type=str,
        default=None,
        help="Prompt version (default: from PROMPT_VERSION env var or v1_1_web_search)"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date in YYYY-MM-DD format (default: today)"
    )
    parser.add_argument(
        "--time",
        type=str,
        default="16:00",
        help="Target time in HH:MM format (default: 16:00 for workflow simulation)"
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
                    'name': row.get('company_name', 'N/A'),
                    'category': row.get('category', 'N/A'),
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
                    'name': row.get('company_name', 'N/A'),
                    'category': row.get('category', 'N/A'),
                    'return': row.get('morning_change_pct', 0)
                })
        except Exception as e:
            print(f"[WARN] Failed to load worst performers: {e}")

    return backtest_context


def get_trading_context(target_date: str | None = None) -> dict[str, str]:
    """
    営業日カレンダーを参照して取引コンテキストを生成
    （generate_grok_trending.py から流用）

    Args:
        target_date: 対象日（YYYY-MM-DD形式、Noneの場合は今日）

    Returns:
        dict: 実行日、最新営業日、翌営業日などの情報
    """
    fetcher = JQuantsFetcher()

    # 対象日を設定
    if target_date:
        today = datetime.strptime(target_date, "%Y-%m-%d").date()
    else:
        today = datetime.now().date()

    # 最新営業日を取得（target_date以前の最新営業日）
    params = {
        "from": str(today - timedelta(days=10)),
        "to": str(today)
    }
    response = fetcher.client.request("/markets/trading_calendar", params=params)
    calendar = pd.DataFrame(response["trading_calendar"])

    # HolidayDivisionカラムで営業日をフィルタ（"1"が営業日）
    trading_days = calendar[calendar["HolidayDivision"] == "1"]["Date"].tolist()

    if trading_days:
        latest_trading_day_str = max([d for d in trading_days if d <= str(today)])
        latest_trading_day = datetime.strptime(latest_trading_day_str, "%Y-%m-%d").date()
    else:
        # フォールバック
        latest_trading_day_str = fetcher.get_latest_trading_day()
        latest_trading_day = datetime.strptime(latest_trading_day_str, "%Y-%m-%d").date()

    # 営業日カレンダーから今後の営業日を取得
    future_end = today + timedelta(days=10)

    params = {
        "from": str(today),
        "to": str(future_end)
    }

    response = fetcher.client.request("/markets/trading_calendar", params=params)
    calendar = pd.DataFrame(response["trading_calendar"])

    # 営業日のみフィルタ
    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
    trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
    trading_days = trading_days.sort_values("Date")

    # 翌営業日
    future_trading_days = trading_days[trading_days["Date"] > latest_trading_day]

    if future_trading_days.empty:
        raise RuntimeError("No future trading days found in calendar")

    next_trading_day = future_trading_days.iloc[0]["Date"]

    return {
        "execution_date": datetime.now().strftime("%Y年%m月%d日"),
        "latest_trading_day": latest_trading_day.strftime("%Y年%m月%d日"),
        "next_trading_day": next_trading_day.strftime("%Y年%m月%d日"),
        "latest_trading_day_raw": str(latest_trading_day),
        "next_trading_day_raw": str(next_trading_day),
    }


def build_grok_prompt(context: dict[str, str], backtest: dict[str, Any], prompt_version: str) -> str:
    """
    Grokプロンプトを生成（バージョン管理対応）

    Args:
        context: get_trading_context() で取得したコンテキスト
        backtest: load_backtest_patterns() で取得したバックテストデータ
        prompt_version: プロンプトバージョン

    Returns:
        str: Grok APIに送信するプロンプト
    """
    import importlib

    try:
        # data.prompts モジュールから指定バージョンをインポート
        module_name = f"data.prompts.{prompt_version}"
        prompt_module = importlib.import_module(module_name)
        build_prompt_func = prompt_module.build_grok_prompt

        print(f"[INFO] Using prompt version: {prompt_version}")
        return build_prompt_func(context, backtest)

    except ImportError as e:
        print(f"[ERROR] Failed to import prompt module: {module_name}")
        print(f"[ERROR] {e}")
        raise


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
        model="grok-4-fast-reasoning",
        tools=[web_search(), x_search()],
    )

    # システムメッセージとユーザープロンプトを追加
    chat.append(system("あなたは日本株市場のデイトレード専門家です。銘柄選定の際は具体的な数値と根拠を示してください。web_searchツールとx_searchツールを積極的に使用して、一次情報に基づいた事実のみを出力してください。"))
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
    （generate_grok_trending.py から流用）
    """
    print("[INFO] Converting to all_stocks.parquet compatible schema...")
    print(f"[INFO] Using prompt version: {prompt_version}")

    rows = []
    for idx, item in enumerate(grok_data, 1):
        ticker_symbol = item.get("ticker_symbol", "")
        company_name = item.get("company_name", "")
        reason = item.get("reason", "")
        category = item.get("category", "")
        sentiment_score = item.get("sentiment_score", 0.5)
        policy_link = item.get("policy_link", "Low")
        has_mention = item.get("has_mention", False)
        mentioned_by = item.get("mentioned_by", "")

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
    """メイン処理"""
    print("=" * 60)
    print("Test Grok Trending Stocks (xAI SDK + web_search + x_search)")
    print("=" * 60)

    # コマンドライン引数をパース
    args = parse_args()

    # プロンプトバージョンを取得
    prompt_version = args.prompt_version or os.getenv("PROMPT_VERSION", "v1_1_web_search")
    print(f"[INFO] Prompt version: {prompt_version}")

    # 対象日と選定時刻
    target_date = args.date
    selected_time = args.time
    if target_date:
        print(f"[INFO] Target date: {target_date}")
    print(f"[INFO] Selected time: {selected_time}")

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

        # 3. Build dynamic prompt
        prompt = build_grok_prompt(context, backtest, prompt_version)
        print(f"[INFO] Built dynamic prompt ({len(prompt)} chars)")
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
            print(f"   Category: {row['tags']}")
            print(f"   Reason: {row['reason'][:100]}..." if len(row['reason']) > 100 else f"   Reason: {row['reason']}")
            print()

        # 9. Save to test_output
        # YYYYMMDDHH形式のファイル名（target_dateが指定されている場合はそれを使用）
        file_date = target_date if target_date else selected_date
        date_str = file_date.replace("-", "")
        time_str = selected_time.replace(":", "")
        output_parquet = TEST_OUTPUT_DIR / f"grok_trending_{date_str}{time_str}.parquet"
        output_json = TEST_OUTPUT_DIR / f"grok_trending_{date_str}{time_str}.json"

        # Parquet保存
        df.to_parquet(output_parquet, index=False)
        print(f"[OK] Saved: {output_parquet}")
        print(f"     Total stocks: {len(df)}")
        print(f"     Prompt version: {prompt_version}")

        # デバッグ用JSON保存
        debug_data = {
            "metadata": {
                "execution_date": context['execution_date'],
                "latest_trading_day": context['latest_trading_day'],
                "next_trading_day": context['next_trading_day'],
                "prompt_version": prompt_version,
                "selected_time": selected_time,
                "tool_usage": tool_stats
            },
            "stocks": grok_data,
            "raw_response": response
        }

        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(debug_data, f, ensure_ascii=False, indent=2)
        print(f"[OK] Saved debug JSON: {output_json}")

        print("\n" + "=" * 60)
        print(f"[OK] Generated {len(df)} Grok trending stocks (TEST)")
        print(f"[OK] Output: {output_parquet}")
        print(f"[OK] Debug JSON: {output_json}")
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
