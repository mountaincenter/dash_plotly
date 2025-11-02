#!/usr/bin/env python3
"""
generate_grok_trending.py
xAI Grok APIを使って「翌営業日デイトレ銘柄」を選定

実行方法:
    # パイプライン実行（23時更新）
    python3 scripts/pipeline/generate_grok_trending.py

    # 手動実行（クリーンアップして新規作成）
    python3 scripts/pipeline/generate_grok_trending.py --cleanup

出力:
    data/parquet/grok_trending.parquet

動作仕様:
    - 毎日23時（JST）に実行（土日祝含む）
    - 10銘柄を選定し、フロントエンドでTop5のみ表示
    - 古いデータを削除して新規作成（1日1回更新）

備考:
    - .env.xai に XAI_API_KEY が必要
    - all_stocks.parquet と統合できるスキーマ
    - categories: ["GROK"]
    - tags: Grokが返したcategoryをそのまま格納
    - selected_time: "23:00" 固定
"""

from __future__ import annotations

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from openai import OpenAI
from dotenv import dotenv_values
from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_fetcher import JQuantsFetcher

GROK_TRENDING_PATH = PARQUET_DIR / "grok_trending.parquet"
ENV_XAI_PATH = ROOT / ".env.xai"


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
            # 最近3銘柄のみ抽出
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

    response = fetcher.client.request("/markets/trading_calendar", params=params)
    calendar = pd.DataFrame(response["trading_calendar"])

    # 営業日のみフィルタ（HolidayDivision == "1"）
    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
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
        print(f"[INFO] Falling back to v1_0_baseline")

        # フォールバック: v1_0_baseline を使用
        from data.prompts.v1_0_baseline import build_grok_prompt as fallback_prompt
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


def query_grok(api_key: str, prompt: str) -> str:
    """Query Grok API via OpenAI client"""
    print("[INFO] Querying Grok API...")

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
    )

    response = client.chat.completions.create(
        model="grok-4-fast-reasoning",
        messages=[
            {"role": "system", "content": "あなたは日本株市場のデイトレード専門家です。銘柄選定の際は具体的な数値と根拠を示してください。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=4000,
    )

    content = response.choices[0].message.content
    print(f"[OK] Received response from Grok ({len(content)} chars)")
    return content


def parse_grok_response(response: str) -> list[dict[str, Any]]:
    """Parse Grok's JSON response"""
    print("[INFO] Parsing Grok response...")

    # JSONブロックを抽出（```json ... ``` や ``` ... ``` で囲まれている場合）
    if "```json" in response:
        json_str = response.split("```json")[1].split("```")[0].strip()
    elif "```" in response:
        json_str = response.split("```")[1].split("```")[0].strip()
    else:
        json_str = response.strip()

    # JSON配列のパース
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON: {e}")
        print(f"[DEBUG] Response content:\n{response}")
        raise

    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array, got {type(data)}")

    print(f"[OK] Parsed {len(data)} stocks from Grok response")
    return data


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
        categories, tags, date, Close, change_pct, Volume, vol_ratio,
        atr14_pct, rsi14, score, key_signal

    + Grok拡張スキーマ:
        reason, source, selected_time, updated_at, sentiment_score,
        policy_link, has_mention, mentioned_by, selection_score, prompt_version
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
            "market": None,  # Grokからは取得できない
            "sectors": None,
            "series": None,
            "topixnewindexseries": None,
            "categories": ["GROK"],  # 固定値（配列形式）
            "tags": category,  # Grokのcategoryをtagsに格納
            "reason": reason,  # 新規カラム: Grokの選定理由
            "date": selected_date,
            "Close": None,
            "change_pct": None,
            "Volume": None,
            "vol_ratio": None,
            "atr14_pct": None,
            "rsi14": None,
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

    if should_merge and GROK_TRENDING_PATH.exists():
        # 既存データとマージ（26時更新時）
        try:
            existing_df = pd.read_parquet(GROK_TRENDING_PATH)
            print(f"[INFO] Loaded existing data: {len(existing_df)} stocks")

            # 同じselected_timeのデータを削除（上書き）
            if "selected_time" in existing_df.columns:
                existing_df = existing_df[existing_df["selected_time"] != selected_time]
                print(f"[INFO] Removed old {selected_time} data, remaining: {len(existing_df)} stocks")

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
    prompt_version = os.getenv("PROMPT_VERSION", "v1_0_baseline")
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

        # 4. Query Grok
        response = query_grok(api_key, prompt)
        print()

        # 5. Parse response
        grok_data = parse_grok_response(response)
        print()

        # 6. Convert to DataFrame (prompt_versionを追加)
        selected_date = context['next_trading_day_raw']
        df = convert_to_all_stocks_schema(grok_data, selected_date, selected_time, prompt_version)
        print()

        # 7. Preview
        print("[INFO] Preview (first 5 stocks):")
        print("-" * 80)
        for i, row in df.head(5).iterrows():
            print(f"{i+1}. {row['ticker']} - {row['stock_name']}")
            print(f"   Category: {row['tags']}")
            print(f"   Reason: {row['reason'][:80]}..." if len(row['reason']) > 80 else f"   Reason: {row['reason']}")
            print()

        # 8. Save
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
                "date", "Close", "change_pct", "Volume", "vol_ratio",
                "atr14_pct", "rsi14", "score", "key_signal",
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
            "date", "Close", "change_pct", "Volume", "vol_ratio",
            "atr14_pct", "rsi14", "score", "key_signal",
            "source", "selected_time", "updated_at"
        ])

        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        empty_df.to_parquet(GROK_TRENDING_PATH, index=False)
        print(f"[OK] Saved empty: {GROK_TRENDING_PATH}")

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
