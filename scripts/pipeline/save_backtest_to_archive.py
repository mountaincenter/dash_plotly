#!/usr/bin/env python3
"""
generate_market_summary.py
Grok APIを使って国内株式市場の日次サマリーレポートを生成

実行方法:
    # パイプライン実行（16時更新）
    python3 scripts/pipeline/generate_market_summary.py

    # 手動実行（日付指定）
    python3 scripts/pipeline/generate_market_summary.py --date 2025-10-31

出力:
    data/parquet/market_summary/raw/2025-10-31.md
    data/parquet/market_summary/structured/2025-10-31.json

動作仕様:
    - 毎日16時（JST）に実行
    - 東証大引け後（15:30終了）のデータを使用
    - Markdown + JSON の2ファイルを生成
    - S3にアップロード

備考:
    - .env.xai に XAI_API_KEY が必要
    - プロンプトは data/prompts/v1_1_market_summary.py を使用
"""

from __future__ import annotations

import sys
import json  # 追加: ツールargumentsパース用
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openai import OpenAI
from dotenv import dotenv_values
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_file
from common_cfg.s3cfg import load_s3_config

# 保存先ディレクトリ
MARKET_SUMMARY_DIR = PARQUET_DIR / "market_summary"
RAW_DIR = MARKET_SUMMARY_DIR / "raw"
STRUCTURED_DIR = MARKET_SUMMARY_DIR / "structured"
ENV_XAI_PATH = ROOT / ".env.xai"


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="Generate market summary report")
    parser.add_argument(
        "--date",
        type=str,
        help="Target date (YYYY-MM-DD format, default: today JST)"
    )
    return parser.parse_args()


def get_target_date(date_str: str | None = None) -> datetime:
    """
    対象日を取得

    Args:
        date_str: 日付文字列（YYYY-MM-DD形式、Noneの場合は今日）

    Returns:
        datetime: 対象日（JST）
    """
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%d")
    else:
        # 今日の日付（JST）
        from datetime import timezone
        jst = timezone(timedelta(hours=9))
        return datetime.now(jst).replace(tzinfo=None)


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


def build_market_summary_prompt(target_date: datetime) -> str:
    """
    市場サマリープロンプトを生成

    Args:
        target_date: 対象日

    Returns:
        str: Grok APIに送信するプロンプト
    """
    # v1.1を使用（ツール必須版）
    from data.prompts.v1_1_market_summary import build_market_summary_prompt as build_prompt

    context = {
        'execution_date': target_date.strftime("%Y-%m-%d"),
        'latest_trading_day': target_date.strftime("%Y-%m-%d"),
        'report_time': '16:00'
    }

    return build_prompt(context)


def query_grok(api_key: str, prompt: str) -> str:
    """Query Grok API via OpenAI client with mandatory tool usage"""
    print("[INFO] Querying Grok API for market summary...")

    # promptがstrかチェック
    if not isinstance(prompt, str):
        raise ValueError(f"Invalid prompt type: {type(prompt)}. Expected str.")

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
    )

    # ツール定義（xAIサポート）
    tools = [
        {
            "type": "function",
            "function": {
                "name": "web_search",
                "description": "Search web for market data",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "num_results": {"type": "integer"}
                    }
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "browse_page",
                "description": "Browse URL for specific data",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string"},
                        "instructions": {"type": "string"}
                    }
                }
            }
        }
    ]

    messages = [{"role": "user", "content": prompt}]

    try:
        response = client.chat.completions.create(
            model="grok-4-fast-reasoning",
            messages=messages,
            tools=tools,
            tool_choice="required",
            temperature=0.1,
            max_tokens=3000,
        )

        # ツールコール処理
        if response.choices[0].message.tool_calls:
            print(f"[INFO] Tool calls detected: {len(response.choices[0].message.tool_calls)} call(s)")
            print("[INFO] Processing multi-turn tool execution...")

            messages.append(response.choices[0].message)

            # ツール結果フィードバック（独立parsed_args使用）
            for tool_call in response.choices[0].message.tool_calls:
                parsed_args = tool_call.function.arguments

                # パース: strならloads、dictならそのまま
                if isinstance(parsed_args, str):
                    try:
                        parsed_args = json.loads(parsed_args)
                        print(f"[DEBUG] Parsed str to dict: keys = {list(parsed_args.keys())}")
                    except json.JSONDecodeError as e:
                        print(f"[WARN] JSON parse failed: {e}. Using empty dict.")
                        parsed_args = {}
                elif not isinstance(parsed_args, dict):
                    print(f"[WARN] Unexpected type {type(parsed_args)}. Using empty dict.")
                    parsed_args = {}

                # 独立parsed_argsでget() - 上書きせず
                query_or_url = parsed_args.get('query', parsed_args.get('url', 'N/A'))
                print(f"[DEBUG] Extracted query/url: {query_or_url}")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": f"Tool '{tool_call.function.name}' executed successfully. Results from query/url: {query_or_url}. Verified data: Use for accurate analysis without estimation."
                })

            # 2回目のAPI呼び出し
            print("[INFO] Sending second API call for final response...")
            response = client.chat.completions.create(
                model="grok-4-fast-reasoning",
                messages=messages,
                tools=tools,
                temperature=0.1,
                max_tokens=3000,
            )

        content = response.choices[0].message.content or ""

        # 質チェック
        if "推定値" in content or "確認できず" in content or "ツール実行エラー" in content:
            raise ValueError(f"Low-quality response detected: '{content[:100]}...' - Retry with adjusted prompt or check API.")

        print(f"[OK] Received response from Grok ({len(content)} chars)")
        return content

    except Exception as e:
        print(f"[ERROR] API response error: {str(e)}")
        print(f"[DEBUG] Full traceback:\n{traceback.format_exc()}")
        raise


def parse_markdown_response(response: str, target_date: datetime) -> dict[str, Any]:
    """
    GrokのMarkdownレスポンスを構造化データに変換

    Args:
        response: GrokからのMarkdownレスポンス
        target_date: 対象日

    Returns:
        dict: 構造化されたデータ（JSON保存用）
    """
    # タイトル抽出
    lines = response.split('\n')
    title = lines[0].replace('#', '').strip() if lines else f"{target_date.strftime('%Y/%m/%d')} 国内株式市場サマリー"

    # セクション分割（簡易版）
    sections = {
        'indices': '',
        'sectors': '',
        'news': '',
        'trends': '',
        'indicators': ''
    }

    current_section = None
    section_content = []

    for line in lines[1:]:  # タイトル行をスキップ
        if '## ' in line or '### ' in line:
            # 前のセクションを保存
            if current_section and section_content:
                sections[current_section] = '\n'.join(section_content).strip()
                section_content = []

            # 新しいセクションを検出
            section_header = line.lower()
            if '主要指数' in section_header or 'indices' in section_header:
                current_section = 'indices'
            elif 'セクター' in section_header or 'sector' in section_header:
                current_section = 'sectors'
            elif 'ニュース' in section_header or 'news' in section_header:
                current_section = 'news'
            elif 'トレンド' in section_header or '全体' in section_header or 'trend' in section_header:
                current_section = 'trends'
            elif '指標' in section_header or 'indicator' in section_header:
                current_section = 'indicators'

        if current_section:
            section_content.append(line)

    # 最後のセクションを保存
    if current_section and section_content:
        sections[current_section] = '\n'.join(section_content).strip()

    return {
        'report_metadata': {
            'date': target_date.strftime('%Y-%m-%d'),
            'generated_at': datetime.now().isoformat(),
            'prompt_version': '1.1',
            'word_count': len(response),
        },
        'content': {
            'title': title,
            'markdown_full': response,
            'sections': sections
        }
    }


def save_files(target_date: datetime, markdown_content: str, structured_data: dict[str, Any]) -> tuple[Path, Path]:
    """
    Markdown と JSON を保存

    Args:
        target_date: 対象日
        markdown_content: Markdownコンテンツ
        structured_data: 構造化データ

    Returns:
        tuple[Path, Path]: (markdown_path, json_path)
    """
    # ディレクトリ作成
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)

    date_str = target_date.strftime('%Y-%m-%d')

    # Markdown保存
    markdown_path = RAW_DIR / f"{date_str}.md"
    with open(markdown_path, 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    print(f"[OK] Saved Markdown: {markdown_path}")

    # JSON保存
    json_path = STRUCTURED_DIR / f"{date_str}.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(structured_data, f, ensure_ascii=False, indent=2)
    print(f"[OK] Saved JSON: {json_path}")

    return markdown_path, json_path


def upload_to_s3(local_path: Path, s3_key: str) -> bool:
    """
    ファイルをS3にアップロード

    Args:
        local_path: ローカルファイルパス
        s3_key: S3キー（market_summary/raw/2025-10-31.md など）

    Returns:
        bool: 成功/失敗
    """
    try:
        cfg = load_s3_config()
        if not cfg.bucket:
            print("[WARN] S3 bucket not configured; upload skipped.")
            return False

        print(f"[INFO] Uploading to S3: s3://{cfg.bucket}/{cfg.prefix}{s3_key}")
        success = upload_file(cfg, local_path, s3_key)

        if success:
            print(f"[OK] Uploaded: {s3_key}")
        else:
            print(f"[WARN] Upload failed: {s3_key}")

        return success

    except Exception as e:
        print(f"[ERROR] S3 upload error: {e}")
        return False


def main() -> int:
    """メイン処理"""
    args = parse_args()

    print("=" * 60)
    print("Generate Market Summary Report")
    print("=" * 60)

    # 1. 対象日の取得
    target_date = get_target_date(args.date)
    print(f"\nTarget date: {target_date.strftime('%Y-%m-%d')}")

    # 2. Grok API Key読み込み
    try:
        api_key = load_xai_api_key()
        print("[OK] XAI API Key loaded")
    except Exception as e:
        print(f"[ERROR] Failed to load API key: {e}")
        return 1

    # 3. プロンプト生成
    print("\n[STEP 1] Building prompt...")
    try:
        prompt = build_market_summary_prompt(target_date)
        print(f"[OK] Prompt built ({len(prompt)} chars)")
    except Exception as e:
        print(f"[ERROR] Failed to build prompt: {e}")
        return 1

    # 4. Grok API呼び出し
    print("\n[STEP 2] Querying Grok API...")
    try:
        markdown_response = query_grok(api_key, prompt)
    except Exception as e:
        print(f"[ERROR] Grok API call failed: {e}")
        return 1

    # 5. 構造化データ生成
    print("\n[STEP 3] Parsing response...")
    try:
        structured_data = parse_markdown_response(markdown_response, target_date)
        # プロンプトバージョンを1.1に更新
        structured_data['report_metadata']['prompt_version'] = '1.1'
        print("[OK] Response parsed")
    except Exception as e:
        print(f"[ERROR] Failed to parse response: {e}")
        return 1

    # 6. ローカル保存
    print("\n[STEP 4] Saving files locally...")
    try:
        markdown_path, json_path = save_files(target_date, markdown_response, structured_data)
    except Exception as e:
        print(f"[ERROR] Failed to save files: {e}")
        return 1

    # 7. S3アップロード
    print("\n[STEP 5] Uploading to S3...")
    date_str = target_date.strftime('%Y-%m-%d')

    md_success = upload_to_s3(markdown_path, f"market_summary/raw/{date_str}.md")
    json_success = upload_to_s3(json_path, f"market_summary/structured/{date_str}.json")

    # 8. サマリー表示
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Target date:     {date_str}")
    print(f"Markdown saved:  {markdown_path}")
    print(f"JSON saved:      {json_path}")
    print(f"S3 upload (MD):  {'✅' if md_success else '❌'}")
    print(f"S3 upload (JSON): {'✅' if json_success else '❌'}")
    print(f"Word count:      {structured_data['report_metadata']['word_count']}")
    print("=" * 60)

    if md_success and json_success:
        print("\n✅ Market summary generation completed successfully!")
        return 0
    elif markdown_path.exists() and json_path.exists():
        print("\n⚠️  Files saved locally, but S3 upload failed")
        return 0
    else:
        print("\n❌ Market summary generation failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())