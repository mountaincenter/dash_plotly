#!/usr/bin/env python3
"""
generate_grok_trending.py
xAI Grok APIを使って「翌営業日デイトレ銘柄」を選定

✨ xAI SDK + x_search() + web_search() ツール有効版（v1.1）

実行方法:
    # パイプライン実行（23時更新、デフォルトで v1_1_web_search プロンプト使用）
    python3 scripts/pipeline/generate_grok_trending.py

    # プロンプトバージョン指定実行
    PROMPT_VERSION=v1_0_baseline python3 scripts/pipeline/generate_grok_trending.py

    # 手動実行（クリーンアップして新規作成）
    python3 scripts/pipeline/generate_grok_trending.py --cleanup

出力:
    data/parquet/grok_trending.parquet

動作仕様:
    - 毎日23時（JST）に実行（土日祝含む）
    - 10〜15銘柄を選定し、フロントエンドでTop5のみ表示
    - 古いデータを削除して新規作成（1日1回更新）
    - xAI SDK の Client を使用
    - x_search() ツールで X(Twitter) からリアルタイム情報取得
    - web_search() ツールで IR・ニュースを検証
    - ストリーミング処理でツール呼び出しをカウント

備考:
    - .env.xai に XAI_API_KEY が必要
    - xAI SDK (xai_sdk) が必要: pip install xai-sdk
    - all_stocks.parquet と統合できるスキーマ
    - categories: ["GROK"]
    - tags: Grokが返したcategoryをそのまま格納
    - selected_time: "23:00" 固定
    - デフォルトプロンプト: v1_1_web_search (環境変数で変更可能)
"""

from __future__ import annotations

import sys
import os
import io
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

import boto3

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

GROK_TRENDING_PATH = PARQUET_DIR / "grok_trending.parquet"
ENV_XAI_PATH = ROOT / ".env.xai"

# 取引制限データのパス（複数候補）
MARGIN_CODE_MASTER_PATHS = [
    PARQUET_DIR / "margin_code_master.parquet",
    ROOT / "improvement" / "data" / "margin_code_master.parquet",
]
JSF_RESTRICTION_PATHS = [
    PARQUET_DIR / "jsf_seigenichiran.csv",
    ROOT / "improvement" / "data" / "jsf_seigenichiran.csv",
]


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="Generate Grok trending stocks")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Force cleanup of existing data before generating new stocks (for manual execution)"
    )
    return parser.parse_args()


def load_backtest_patterns() -> dict[str, Any]:
    """
    バックテストパターンを読み込み、Grokプロンプト用のコンテキストを生成

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
            # 最近3銘柄のみ抽出
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
            # 最近3銘柄のみ抽出
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


def get_trading_context() -> dict[str, str]:
    """
    営業日カレンダーを参照して取引コンテキストを生成

    営業日の夜にcron実行される想定：
    - 金曜夜 → 翌営業日は月曜
    - 月曜夜 → 翌営業日は火曜
    - 祝日前は翌営業日まで飛ぶ

    Returns:
        dict: 実行日、最新営業日、翌営業日などの情報
    """
    fetcher = JQuantsFetcher()

    # 最新営業日を取得（J-Quants営業日カレンダーから）
    latest_trading_day_str = fetcher.get_latest_trading_day()
    latest_trading_day = datetime.strptime(latest_trading_day_str, "%Y-%m-%d").date()

    # 営業日カレンダーから今後の営業日を取得
    # 翌営業日を取得するため、今日から+10日の範囲で営業日を取得
    today = datetime.now().date()
    future_end = today + timedelta(days=10)

    params = {
        "from": str(today),
        "to": str(future_end)
    }

    # v2: /markets/calendar（v1は/markets/trading_calendar）
    response = fetcher.client.request("/markets/calendar", params=params)
    calendar = pd.DataFrame(response["data"])

    # 営業日のみフィルタ（v2: HolDiv == "1"、v1はHolidayDivision）
    trading_days = calendar[calendar["HolDiv"] == "1"].copy()
    trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
    trading_days = trading_days.sort_values("Date")

    # 最新営業日より後の営業日 = 翌営業日
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


# プロンプト生成関数は data/prompts/ モジュールからimport
# 環境変数でバージョンを切り替え可能
def build_grok_prompt(context: dict[str, str], backtest: dict[str, Any]) -> str:
    """
    Grokプロンプトを生成（バージョン管理対応）

    環境変数 PROMPT_VERSION でバージョンを指定可能
    例: PROMPT_VERSION=v1_0_baseline python3 generate_grok_trending.py

    Args:
        context: get_trading_context() で取得したコンテキスト
        backtest: load_backtest_patterns() で取得したバックテストデータ

    Returns:
        str: Grok APIに送信するプロンプト
    """
    import os
    import importlib

    # 環境変数からバージョンを取得（デフォルトは v1_0_baseline）
    prompt_version = os.getenv("PROMPT_VERSION", "v1_0_baseline")

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
        print(f"[INFO] Falling back to v1_1_web_search")

        # フォールバック: v1_1_web_search を使用
        from data.prompts.v1_1_web_search import build_grok_prompt as fallback_prompt
        return fallback_prompt(context, backtest)


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
    print(f"[INFO] Token usage: {tool_stats['usage']['total_tokens']} tokens (prompt: {tool_stats['usage']['prompt_tokens']}, completion: {tool_stats['usage']['completion_tokens']})")

    return full_response, tool_stats


def repair_json(json_str: str) -> str:
    """
    Grok APIが返す壊れたJSONを修復する

    よくあるエラー:
    1. オブジェクト間のカンマ欠落: } { → }, {
    2. 配列要素間のカンマ欠落: } \n  { → }, {
    3. 末尾カンマ: ,] → ]
    """
    import re

    repaired = json_str

    # 1. オブジェクト間のカンマ欠落を修復: }\n  { → },\n  {
    # パターン: } の後に空白・改行があり { が続く場合
    repaired = re.sub(r'\}\s*\n\s*\{', '},\n  {', repaired)

    # 2. 直接隣接: }{ → },{
    repaired = re.sub(r'\}\s*\{', '}, {', repaired)

    # 3. 末尾カンマを除去: ,] → ]
    repaired = re.sub(r',\s*\]', ']', repaired)

    # 4. 末尾カンマを除去: ,} → }
    repaired = re.sub(r',\s*\}', '}', repaired)

    return repaired


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
    last_array_end = json_str.rfind("]")

    # その後に"{" があるか確認（verification_summaryが続く場合）
    remaining = json_str[last_array_end + 1:].strip()
    if remaining and remaining.startswith("{"):
        # verification_summaryがある場合、配列の終端を再設定
        # "}\n]"のパターンを探す
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

    # まず通常のパースを試行
    try:
        data = json.loads(first_json)
        if isinstance(data, list):
            print(f"[OK] Parsed {len(data)} stocks from Grok response")
            return data
        else:
            raise ValueError(f"Expected JSON array, got {type(data)}")
    except json.JSONDecodeError as e:
        print(f"[WARN] Initial JSON parse failed: {e}")
        print("[INFO] Attempting JSON repair...")

        # JSON修復を試行
        repaired_json = repair_json(first_json)

        try:
            data = json.loads(repaired_json)
            if isinstance(data, list):
                print(f"[OK] Parsed {len(data)} stocks after JSON repair")
                return data
            else:
                raise ValueError(f"Expected JSON array, got {type(data)}")
        except json.JSONDecodeError as e2:
            print(f"[ERROR] JSON repair also failed: {e2}")
            print(f"[DEBUG] Original JSON:\n{first_json[:500]}...")
            print(f"[DEBUG] Repaired JSON:\n{repaired_json[:500]}...")
            raise


def calculate_selection_score(item: dict[str, Any]) -> float:
    """
    選定時点でのスコアを計算

    Args:
        item: Grok APIレスポンスの1銘柄分のデータ

    Returns:
        float: 選定スコア (0-200点)
    """
    # sentiment_score があれば使用、なければ0.5をデフォルト
    sentiment_score = item.get("sentiment_score", 0.5)
    score = sentiment_score * 100  # ベーススコア (0-100)

    # policy_link があれば加点
    policy_link = item.get("policy_link", "Low")
    policy_bonus = {"High": 30, "Med": 20, "Low": 10}
    score += policy_bonus.get(policy_link, 10)

    # has_mention (プレミアムユーザー言及) があれば加点
    has_mention = item.get("has_mention", False)
    if has_mention:
        score += 50

    return score


def convert_to_all_stocks_schema(grok_data: list[dict], selected_date: str, selected_time: str, prompt_version: str) -> pd.DataFrame:
    """
    Convert Grok data to all_stocks.parquet compatible schema

    all_stocks.parquet schema:
        ticker, code, stock_name, market, sectors, series, topixnewindexseries,
        categories, tags, date, Close, price_diff, Volume, vol_ratio,
        atr14_pct, rsi9, score, key_signal

    + Grok拡張スキーマ:
        reason, source, selected_time, updated_at, sentiment_score,
        policy_link, has_mention, mentioned_by, selection_score, prompt_version
    """
    print("[INFO] Converting to all_stocks.parquet compatible schema...")
    print(f"[INFO] Using prompt version: {prompt_version}")

    rows = []
    for idx, item in enumerate(grok_data, 1):
        ticker_symbol = item.get("ticker_symbol", "")
        stock_name_val = item.get("stock_name", item.get("company_name", ""))
        reason = item.get("reason", "")
        categories_val = item.get("categories", item.get("category", ""))
        sentiment_score = item.get("sentiment_score", 0.5)
        policy_link = item.get("policy_link", "Low")
        has_mention = item.get("has_mention", False)
        mentioned_by = item.get("mentioned_by", "")
        # Normalize mentioned_by to string (Grok may return list or string)
        if isinstance(mentioned_by, list):
            mentioned_by = ", ".join(str(m) for m in mentioned_by)
        elif mentioned_by is None:
            mentioned_by = ""

        # tickerは "1234.T" 形式、codeは "1234" 形式
        ticker = f"{ticker_symbol}.T" if not ticker_symbol.endswith(".T") else ticker_symbol
        code = ticker_symbol.replace(".T", "")

        # 選定スコアを計算
        selection_score = calculate_selection_score(item)

        row = {
            "ticker": ticker,
            "code": code,
            "stock_name": stock_name_val,
            "market": None,  # Grokからは取得できない
            "sectors": None,
            "series": None,
            "topixnewindexseries": None,
            "categories": ["GROK"],  # 固定値（配列形式）
            "tags": categories_val,  # Grokのcategoriesをtagsに格納
            "reason": reason,  # 新規カラム: Grokの選定理由
            "date": selected_date,
            "Close": None,
            "price_diff": None,
            "Volume": None,
            "vol_ratio": None,
            "atr14_pct": None,
            "rsi9": None,
            "score": None,
            "key_signal": None,
            "source": "grok",  # 新規カラム: データソース
            "selected_time": selected_time,  # 新規カラム: 選定時刻（16:00 or 26:00）
            "updated_at": datetime.now().isoformat(),  # 新規カラム: 更新日時
            "sentiment_score": sentiment_score,  # センチメントスコア
            "policy_link": policy_link,  # 政策リンク強度
            "has_mention": has_mention,  # プレミアムユーザー言及フラグ
            "mentioned_by": mentioned_by,  # 言及者名
            "selection_score": selection_score,  # 選定時点スコア
            "prompt_version": prompt_version,  # 新規カラム: プロンプトバージョン
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # selection_scoreで降順ソート（Top5を上位に）
    df = df.sort_values("selection_score", ascending=False).reset_index(drop=True)

    # ランクを再計算（スコア順）
    df["grok_rank"] = range(1, len(df) + 1)  # スコア順のランク
    df["selection_rank"] = range(1, len(df) + 1)

    # tagsを再構築（カテゴリ、スコア、Top5バッジ）
    def build_tags(row):
        tags_list = []
        # [0] カテゴリ
        if row.get("tags"):
            tags_list.append(row["tags"])
        # [1] スコア（数字のみの文字列）
        tags_list.append(f"{row['selection_score']:.1f}")
        # [2] Top5バッジ（該当時のみ）
        if row["selection_rank"] <= 5:
            tags_list.append("⭐Top5")
        return ",".join(tags_list)  # カンマ区切りの文字列に変換

    df["tags"] = df.apply(build_tags, axis=1)

    print(f"[OK] Converted {len(df)} stocks to DataFrame")
    print(f"[INFO] Top 5 by selection_score:")
    for i, row in df.head(5).iterrows():
        print(f"  {row['selection_rank']}. {row['ticker']} - {row['stock_name']} (Score: {row['selection_score']:.1f})")

    return df


def filter_and_enrich_with_meta_jquants(df: pd.DataFrame) -> pd.DataFrame:
    """
    meta_jquants.parquetを使って銘柄をフィルタリング・エンリッチ

    1. meta_jquantsに存在しない銘柄を除外（ETF等）
    2. stock_nameを正式名称で上書き
    3. market, sectors等のメタ情報を付与

    Args:
        df: Grok APIから変換されたDataFrame

    Returns:
        フィルタリング・エンリッチされたDataFrame
    """
    import boto3
    import io
    from botocore.exceptions import ClientError

    if df.empty:
        return df

    # meta_jquants.parquetを読み込み（S3から）
    try:
        bucket = os.getenv('S3_BUCKET', 'stock-api-data')
        key = 'parquet/meta_jquants.parquet'

        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        meta_df = pd.read_parquet(io.BytesIO(response['Body'].read()))

        print(f"[OK] Loaded meta_jquants.parquet from S3: {len(meta_df)} stocks")

    except ClientError as e:
        print(f"[WARN] Failed to load meta_jquants.parquet from S3: {e}")
        print("[WARN] Skipping meta_jquants filtering")
        return df
    except Exception as e:
        print(f"[WARN] Failed to parse meta_jquants.parquet: {e}")
        print("[WARN] Skipping meta_jquants filtering")
        return df

    # meta_jquantsに存在する銘柄のセットを作成
    valid_tickers = set(meta_df['ticker'].tolist())

    # フィルタリング前の銘柄数
    before_count = len(df)

    # meta_jquantsに存在しない銘柄を除外
    excluded_tickers = df[~df['ticker'].isin(valid_tickers)]['ticker'].tolist()
    df_filtered = df[df['ticker'].isin(valid_tickers)].copy()

    if excluded_tickers:
        print(f"[INFO] Excluded {len(excluded_tickers)} stocks not in meta_jquants:")
        for ticker in excluded_tickers:
            original_name = df[df['ticker'] == ticker]['stock_name'].iloc[0]
            print(f"       - {ticker}: {original_name}")

    # stock_nameを正式名称で上書き & メタ情報付与
    meta_dict = meta_df.set_index('ticker').to_dict('index')

    corrected_names = []
    for idx, row in df_filtered.iterrows():
        ticker = row['ticker']
        if ticker in meta_dict:
            meta = meta_dict[ticker]
            original_name = row['stock_name']
            correct_name = meta.get('stock_name', original_name)

            # stock_nameを上書き
            df_filtered.at[idx, 'stock_name'] = correct_name

            # メタ情報を付与
            df_filtered.at[idx, 'market'] = meta.get('market')
            df_filtered.at[idx, 'sectors'] = meta.get('sectors')
            df_filtered.at[idx, 'series'] = meta.get('series')
            df_filtered.at[idx, 'topixnewindexseries'] = meta.get('topixnewindexseries')

            if original_name != correct_name:
                corrected_names.append((ticker, original_name, correct_name))

    if corrected_names:
        print(f"[INFO] Corrected {len(corrected_names)} stock names:")
        for ticker, orig, correct in corrected_names:
            print(f"       - {ticker}: '{orig}' → '{correct}'")

    # grok_rank を再計算（フィルタリング後の順位）
    df_filtered = df_filtered.reset_index(drop=True)
    df_filtered['grok_rank'] = range(1, len(df_filtered) + 1)
    df_filtered['selection_rank'] = range(1, len(df_filtered) + 1)

    # tagsを再構築（Top5バッジ更新）
    def rebuild_tags(row):
        tags_list = []
        # 元のカテゴリを保持（tagsの最初の要素）
        original_tags = row.get('tags', '')
        if original_tags and ',' in str(original_tags):
            tags_list.append(str(original_tags).split(',')[0])
        elif original_tags:
            tags_list.append(str(original_tags))
        # スコア
        tags_list.append(f"{row['selection_score']:.1f}")
        # Top5バッジ
        if row['selection_rank'] <= 5:
            tags_list.append("⭐Top5")
        return ",".join(tags_list)

    df_filtered['tags'] = df_filtered.apply(rebuild_tags, axis=1)

    after_count = len(df_filtered)
    print(f"[OK] Filtered: {before_count} → {after_count} stocks")

    return df_filtered


def get_jst_date() -> str:
    """
    JST基準の日付を取得（YYYY-MM-DD形式）
    """
    from datetime import timezone, timedelta
    jst = timezone(timedelta(hours=9))
    now_jst = datetime.now(jst)
    return now_jst.strftime("%Y-%m-%d")


def download_manifest_from_s3() -> dict:
    """
    S3から manifest.json をダウンロード

    Returns:
        manifest.json の内容（辞書）
        ダウンロード失敗時は空の辞書
    """
    import os
    import boto3
    import json
    from botocore.exceptions import ClientError

    try:
        bucket = os.getenv('S3_BUCKET', 'stock-api-data')
        key = 'parquet/manifest.json'

        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        manifest_data = response['Body'].read().decode('utf-8')
        manifest = json.loads(manifest_data)

        print(f"[OK] Downloaded manifest.json from S3: s3://{bucket}/{key}")
        return manifest

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"[WARN] manifest.json not found in S3")
        else:
            print(f"[WARN] Failed to download manifest.json from S3: {e}")
        return {}
    except Exception as e:
        print(f"[WARN] Failed to parse manifest.json: {e}")
        return {}


def add_trading_restrictions(df: pd.DataFrame) -> pd.DataFrame:
    """
    取引制限カラムを追加（margin_code, jsf_restricted, is_shortable）

    Args:
        df: grok_trending DataFrame

    Returns:
        取引制限カラムを追加したDataFrame

    Raises:
        FileNotFoundError: 必須ファイルが見つからない場合
    """
    if df.empty:
        return df

    # MarginCodeマスター読み込み
    margin_path = None
    for path in MARGIN_CODE_MASTER_PATHS:
        if path.exists():
            margin_path = path
            break

    if not margin_path:
        raise FileNotFoundError(
            f"[ERROR] MarginCode master not found. "
            f"Checked paths: {[str(p) for p in MARGIN_CODE_MASTER_PATHS]}. "
            f"Please ensure margin_code_master.parquet is available."
        )

    margin_df = pd.read_parquet(margin_path)
    margin_code_map = dict(zip(margin_df['ticker'], margin_df['margin_code']))
    margin_name_map = dict(zip(margin_df['ticker'], margin_df['margin_code_name']))
    print(f"[INFO] MarginCode loaded: {len(margin_code_map)} stocks from {margin_path.name}")

    # 日証金制限データ読み込み
    jsf_path = None
    for path in JSF_RESTRICTION_PATHS:
        if path.exists():
            jsf_path = path
            break

    if not jsf_path:
        raise FileNotFoundError(
            f"[ERROR] JSF restriction file not found. "
            f"Checked paths: {[str(p) for p in JSF_RESTRICTION_PATHS]}. "
            f"Please ensure jsf_seigenichiran.csv is available."
        )

    try:
        jsf = pd.read_csv(jsf_path, skiprows=4)
        jsf_stop_codes = set(jsf[jsf['実施措置'] == '申込停止']['銘柄コード'].astype(str))
        print(f"[INFO] JSF restrictions loaded: {len(jsf_stop_codes)} stocks from {jsf_path.name}")
    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to parse JSF CSV: {e}")

    # カラム追加
    df = df.copy()
    df['margin_code'] = df['ticker'].map(margin_code_map).fillna('2')
    df['margin_code_name'] = df['ticker'].map(margin_name_map).fillna('貸借')
    df['jsf_restricted'] = df['ticker'].str.replace('.T', '', regex=False).isin(jsf_stop_codes)
    df['is_shortable'] = (df['margin_code'] == '2') & (~df['jsf_restricted'])

    # サマリー表示
    print(f"[INFO] Trading restrictions added:")
    print(f"       Shortable: {df['is_shortable'].sum()}/{len(df)}")
    print(f"       JSF restricted: {df['jsf_restricted'].sum()}")

    return df


def add_day_trade_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    grok_day_trade_list.parquet から信用区分（shortable, day_trade, ng, day_trade_available_shares）をJOIN

    Args:
        df: grok_trending DataFrame

    Returns:
        信用区分カラムを追加したDataFrame
    """
    import os
    import boto3
    from io import BytesIO

    if df.empty:
        return df

    # grok_day_trade_list.parquet を読み込み（ローカル優先、なければS3）
    local_path = PARQUET_DIR / "grok_day_trade_list.parquet"
    day_trade_df = None

    if local_path.exists():
        day_trade_df = pd.read_parquet(local_path)
        print(f"[INFO] Day trade list loaded from local: {len(day_trade_df)} stocks")
    else:
        try:
            s3_client = boto3.client('s3')
            bucket = os.getenv('S3_BUCKET', 'stock-api-data')
            key = 'parquet/grok_day_trade_list.parquet'

            response = s3_client.get_object(Bucket=bucket, Key=key)
            day_trade_df = pd.read_parquet(BytesIO(response['Body'].read()))
            print(f"[INFO] Day trade list loaded from S3: {len(day_trade_df)} stocks")
        except Exception as e:
            print(f"[WARN] Failed to load grok_day_trade_list.parquet: {e}")
            # デフォルト値で初期化
            df['shortable'] = False
            df['day_trade'] = False
            df['ng'] = False
            df['day_trade_available_shares'] = None
            return df

    # tickerでJOIN用のマップを作成
    dtl_map = day_trade_df.set_index('ticker')[['shortable', 'day_trade', 'ng', 'day_trade_available_shares']].to_dict('index')

    # カラム追加（既存銘柄はマスタから、新規銘柄はデフォルト値）
    df = df.copy()
    df['shortable'] = df['ticker'].apply(lambda t: dtl_map.get(t, {}).get('shortable', False))
    df['day_trade'] = df['ticker'].apply(lambda t: dtl_map.get(t, {}).get('day_trade', False))
    df['ng'] = df['ticker'].apply(lambda t: dtl_map.get(t, {}).get('ng', False))
    df['day_trade_available_shares'] = df['ticker'].apply(lambda t: dtl_map.get(t, {}).get('day_trade_available_shares', None))
    # 売り残・買い残は手入力用に初期化（日付×銘柄で管理）
    df['margin_sell_balance'] = None
    df['margin_buy_balance'] = None

    # サマリー表示
    print(f"[INFO] Day trade flags added:")
    print(f"       Shortable: {df['shortable'].sum()}/{len(df)}")
    print(f"       Day trade: {df['day_trade'].sum()}/{len(df)}")
    print(f"       NG: {df['ng'].sum()}/{len(df)}")
    unchecked = len(df) - df['shortable'].sum() - df['day_trade'].sum() - df['ng'].sum()
    print(f"       Unchecked (new): {unchecked}/{len(df)}")

    return df


def save_grok_trending(df: pd.DataFrame, selected_time: str, should_merge: bool = False) -> None:
    """
    Save to grok_trending.parquet

    Args:
        df: 新規データ
        selected_time: "16:00" or "26:00"
        should_merge: True なら既存データとマージ（26時更新用）、False なら新規作成
    """
    import os
    import boto3
    from botocore.exceptions import ClientError

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # 26時更新時: S3から既存ファイルをダウンロードしてマージ
    if should_merge:
        # ローカルにファイルがない場合、S3からダウンロード
        if not GROK_TRENDING_PATH.exists():
            print("[INFO] Local file not found, attempting to download from S3...")
            try:
                s3_client = boto3.client('s3')
                bucket = os.getenv('S3_BUCKET', 'stock-api-data')
                key = 'parquet/grok_trending.parquet'

                # S3からダウンロード
                s3_client.download_file(bucket, key, str(GROK_TRENDING_PATH))
                print(f"[OK] Downloaded from S3: s3://{bucket}/{key}")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"[WARN] File not found in S3: s3://{bucket}/{key}")
                    print(f"[WARN] Will create new file instead of merging")
                else:
                    print(f"[WARN] Failed to download from S3: {e}")
                    print(f"[WARN] Will create new file instead of merging")
            except Exception as e:
                print(f"[WARN] Failed to download from S3: {e}")
                print(f"[WARN] Will create new file instead of merging")

    # weekday計算（date列から曜日を取得: 0=月, 1=火, ..., 6=日）
    if 'date' in df.columns:
        df['weekday'] = pd.to_datetime(df['date']).dt.dayofweek

    # 取引制限カラムを追加
    df = add_trading_restrictions(df)

    # 信用区分カラムを追加（grok_day_trade_list.parquetからJOIN）
    df = add_day_trade_flags(df)

    if should_merge and GROK_TRENDING_PATH.exists():
        # 既存データとマージ（26時更新時）
        try:
            existing_df = pd.read_parquet(GROK_TRENDING_PATH)
            print(f"[INFO] Loaded existing data: {len(existing_df)} stocks")

            # 同じselected_timeのデータを削除（上書き）
            if "selected_time" in existing_df.columns:
                existing_df = existing_df[existing_df["selected_time"] != selected_time]
                print(f"[INFO] Removed old {selected_time} data, remaining: {len(existing_df)} stocks")

            # 既存データにも取引制限カラムがない場合は追加
            if 'margin_code' not in existing_df.columns:
                existing_df = add_trading_restrictions(existing_df)

            # 既存データにも信用区分カラムがない場合は追加
            if 'shortable' not in existing_df.columns:
                existing_df = add_day_trade_flags(existing_df)

            # 新データと結合
            df_merged = pd.concat([existing_df, df], ignore_index=True)
            print(f"[INFO] Merged with new data: {len(df_merged)} stocks")

            # 保存
            df_merged.to_parquet(GROK_TRENDING_PATH, index=False)
            print(f"[OK] Saved: {GROK_TRENDING_PATH}")
            print(f"     Total stocks: {len(df_merged)}")
            print(f"     Breakdown by selected_time:")
            for time, count in df_merged["selected_time"].value_counts().items():
                print(f"       {time}: {count} stocks")

        except Exception as e:
            print(f"[WARN] Failed to merge with existing data: {e}, creating new file")
            df.to_parquet(GROK_TRENDING_PATH, index=False)
            print(f"[OK] Saved: {GROK_TRENDING_PATH}")
            print(f"     Total stocks: {len(df)}")
            print(f"     Selected time: {selected_time}")
    else:
        # 新規作成（16時更新時またはクリーンアップ指定時）
        df.to_parquet(GROK_TRENDING_PATH, index=False)
        print(f"[OK] Saved: {GROK_TRENDING_PATH}")
        print(f"     Total stocks: {len(df)}")
        print(f"     Selected time: {selected_time}")


def main() -> int:
    """メイン処理（パイプライン統合版）"""
    import os

    print("=" * 60)
    print("Generate Grok Trending Stocks (xAI API)")
    print("=" * 60)

    # コマンドライン引数をパース
    args = parse_args()

    # プロンプトバージョンを取得（環境変数から）
    # デフォルトは v1_1_web_search（x_search + web_search ツール使用版）
    prompt_version = os.getenv("PROMPT_VERSION", "v1_1_web_search")
    print(f"[INFO] Prompt version: {prompt_version}")

    # 固定で23時更新（selected_time は23:00固定）
    selected_time = "23:00"
    print(f"[INFO] Update time: {selected_time} (fixed)")

    # 常にクリーンアップ（1日1回更新）
    should_cleanup = True
    should_merge = False
    print("[INFO] Mode: CLEANUP (daily 23:00 update)")

    # クリーンアップ実行
    if should_cleanup:
        if GROK_TRENDING_PATH.exists():
            GROK_TRENDING_PATH.unlink()
            print("[INFO] Removed old grok_trending.parquet")

    print()

    try:
        # 1. Get trading context (営業日カレンダーから)
        print("[INFO] Fetching trading calendar from J-Quants...")
        context = get_trading_context()
        print(f"[OK] Execution date: {context['execution_date']}")
        print(f"[OK] Latest trading day: {context['latest_trading_day']}")
        print(f"[OK] Next trading day: {context['next_trading_day']}")
        print()

        # 2. Load backtest patterns (バックテストフィードバック)
        print("[INFO] Loading backtest patterns...")
        backtest = load_backtest_patterns()
        if backtest.get('has_data'):
            print(f"[OK] Backtest data loaded (Phase1勝率: {backtest['phase1_success_rate']:.1f}%)")
        else:
            print("[INFO] No backtest data available yet")
        print()

        # 3. Build dynamic prompt (with backtest feedback)
        prompt = build_grok_prompt(context, backtest)
        print(f"[INFO] Built dynamic prompt ({len(prompt)} chars)")
        print()

        # 3. Load API key
        api_key = load_xai_api_key()
        print(f"[OK] Loaded XAI_API_KEY from {ENV_XAI_PATH}")
        print()

        # 4. Query Grok with xAI SDK + web_search + x_search
        response, tool_stats = query_grok(api_key, prompt)
        print()

        # 5. Parse response
        grok_data = parse_grok_response(response)
        print()

        # 6. Convert to DataFrame (prompt_versionを追加)
        selected_date = context['next_trading_day_raw']
        df = convert_to_all_stocks_schema(grok_data, selected_date, selected_time, prompt_version)
        print()

        # 7. Filter and enrich with meta_jquants (ETF除外、stock_name正式名称化)
        print("[INFO] Filtering and enriching with meta_jquants...")
        df = filter_and_enrich_with_meta_jquants(df)
        print()

        # 8. Preview
        print("[INFO] Preview (first 5 stocks):")
        print("-" * 80)
        for i, row in df.head(5).iterrows():
            print(f"{i+1}. {row['ticker']} - {row['stock_name']}")
            print(f"   Category: {row['tags']}")
            print(f"   Reason: {row['reason'][:80]}..." if len(row['reason']) > 80 else f"   Reason: {row['reason']}")
            print()

        # 9. Save
        save_grok_trending(df, selected_time, should_merge=should_merge)

        print("\n" + "=" * 60)
        print(f"[OK] Generated {len(df)} Grok trending stocks")
        print(f"[OK] Saved: {GROK_TRENDING_PATH}")
        print("=" * 60)

        return 0

    except FileNotFoundError as e:
        # .env.xai が存在しない場合は空のファイルを作成して終了
        if ".env.xai" in str(e):
            print(f"\n[WARN] {e}")
            print("[WARN] Creating empty grok_trending.parquet (XAI API key not configured)")

            # 空のDataFrameを作成
            empty_df = pd.DataFrame(columns=[
                "ticker", "code", "stock_name", "market", "sectors", "series",
                "topixnewindexseries", "categories", "tags", "reason",
                "date", "Close", "price_diff", "Volume", "vol_ratio",
                "atr14_pct", "rsi9", "weekday", "score", "key_signal",
                "source", "selected_time", "updated_at"
            ])

            PARQUET_DIR.mkdir(parents=True, exist_ok=True)
            empty_df.to_parquet(GROK_TRENDING_PATH, index=False)
            print(f"[OK] Saved empty: {GROK_TRENDING_PATH}")

            return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

        # エラー時も空のファイルを作成（パイプライン継続のため）
        print("[WARN] Creating empty grok_trending.parquet due to error")
        empty_df = pd.DataFrame(columns=[
            "ticker", "code", "stock_name", "market", "sectors", "series",
            "topixnewindexseries", "categories", "tags", "reason",
            "date", "Close", "price_diff", "Volume", "vol_ratio",
            "atr14_pct", "rsi9", "weekday", "score", "key_signal",
            "source", "selected_time", "updated_at"
        ])

        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        empty_df.to_parquet(GROK_TRENDING_PATH, index=False)
        print(f"[OK] Saved empty: {GROK_TRENDING_PATH}")

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
