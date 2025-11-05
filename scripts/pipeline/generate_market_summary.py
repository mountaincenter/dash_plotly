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
    - J-Quants APIからTOPIX/業種別指数データを取得
    - Markdown + JSON の2ファイルを生成
    - S3にアップロード

備考:
    - .env.xai に XAI_API_KEY が必要
    - プロンプトは data/prompts/v1_3_market_summary.py を使用
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
from dotenv import dotenv_values
from xai_sdk import Client
from xai_sdk.chat import user, system
from xai_sdk.tools import web_search

from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_file
from common_cfg.s3cfg import load_s3_config

# 保存先ディレクトリ
MARKET_SUMMARY_DIR = PARQUET_DIR / "market_summary"
RAW_DIR = MARKET_SUMMARY_DIR / "raw"
STRUCTURED_DIR = MARKET_SUMMARY_DIR / "structured"
ENV_XAI_PATH = ROOT / ".env.xai"

# プロンプトインポート
sys.path.insert(0, str(ROOT / "data" / "prompts"))
from v1_3_market_summary import build_market_summary_prompt, format_jquants_table


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


def calculate_change_pct(df: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
    """
    前日比を計算

    Args:
        df: DataFrame with date, ticker, close columns
        target_date: 対象日

    Returns:
        DataFrame with change_pct column added
    """
    if df.empty:
        return df

    # 前日データを取得
    prev_date = target_date.date() - timedelta(days=1)

    # 全データから前日と当日のデータを抽出
    df_copy = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date

    current = df_copy[df_copy['date'] == target_date.date()]
    previous = df_copy[df_copy['date'] <= prev_date].groupby('ticker').tail(1)

    if previous.empty:
        # 前日データがない場合はchange_pctをNaNに
        current['change_pct'] = float('nan')
        current['change'] = float('nan')
        return current

    # マージして前日比計算
    merged = current.merge(
        previous[['ticker', 'close']],
        on='ticker',
        how='left',
        suffixes=('', '_prev')
    )

    merged['change'] = merged['close'] - merged['close_prev']
    merged['change_pct'] = ((merged['close'] - merged['close_prev']) / merged['close_prev'] * 100)

    return merged


def load_jquants_indices_data(target_date: datetime) -> dict[str, Any]:
    """
    J-QuantsのTOPIX/業種別指数データを読み込み（前日比付き）

    Args:
        target_date: 対象日

    Returns:
        dict: {
            'topix': DataFrame,
            'sectors': DataFrame,
            'series': DataFrame,
            'date': str
        }
    """
    date_str = target_date.strftime("%Y-%m-%d")
    result = {'date': date_str}

    # TOPIX系指数
    topix_file = PARQUET_DIR / "topix_prices_max_1d.parquet"
    if topix_file.exists():
        df = pd.read_parquet(topix_file)
        df['date'] = pd.to_datetime(df['date'])
        result['topix'] = calculate_change_pct(df, target_date)
        print(f"  [OK] Loaded TOPIX: {len(result['topix'])} indices")
    else:
        result['topix'] = pd.DataFrame()
        print(f"  [WARN] TOPIX file not found")

    # 33業種別指数
    sectors_file = PARQUET_DIR / "sectors_prices_max_1d.parquet"
    if sectors_file.exists():
        df = pd.read_parquet(sectors_file)
        df['date'] = pd.to_datetime(df['date'])
        result['sectors'] = calculate_change_pct(df, target_date)
        print(f"  [OK] Loaded Sectors: {len(result['sectors'])} sectors")
    else:
        result['sectors'] = pd.DataFrame()
        print(f"  [WARN] Sectors file not found")

    # 17業種別指数
    series_file = PARQUET_DIR / "series_prices_max_1d.parquet"
    if series_file.exists():
        df = pd.read_parquet(series_file)
        df['date'] = pd.to_datetime(df['date'])
        result['series'] = calculate_change_pct(df, target_date)
        print(f"  [OK] Loaded Series: {len(result['series'])} series")
    else:
        result['series'] = pd.DataFrame()
        print(f"  [WARN] Series file not found")

    return result


def build_prompt(target_date: datetime) -> str:
    """
    市場サマリープロンプトを生成（J-Quantsデータ付き）

    Args:
        target_date: 対象日

    Returns:
        str: Grok APIに送信するプロンプト
    """
    # J-Quantsデータを読み込み
    jquants_data = load_jquants_indices_data(target_date)

    # v1.3プロンプト構築
    context = {
        'execution_date': target_date.strftime("%Y-%m-%d"),
        'latest_trading_day': target_date.strftime("%Y-%m-%d"),
        'report_time': '16:00',
        'jquants_topix': jquants_data['topix'],
        'jquants_sectors': jquants_data['sectors'],
        'jquants_series': jquants_data['series'],
    }

    return build_market_summary_prompt(context)


def query_grok(api_key: str, prompt: str) -> tuple[str, dict]:
    """
    Query Grok API via xai_sdk with web_search tool

    Args:
        api_key: XAI API Key
        prompt: プロンプト

    Returns:
        tuple: (response_text, metadata)
    """
    print("  [INFO] Querying Grok API for market summary...")

    client = Client(api_key=api_key)
    chat = client.chat.create(
        model="grok-4-fast-reasoning",
        tools=[web_search()],
    )

    # システムメッセージとユーザープロンプト
    chat.append(system("あなたは経験豊富な日本株アナリストです。提供されたJ-Quantsデータをそのまま使用し、Web Searchツールはニュース・トレンド取得のみに使用してください。"))
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

    # メタデータ収集
    metadata = {
        'usage': {
            'completion_tokens': response.usage.completion_tokens,
            'prompt_tokens': response.usage.prompt_tokens,
            'total_tokens': response.usage.total_tokens,
            'reasoning_tokens': response.usage.reasoning_tokens,
        },
        'citations': list(response.citations),
        'tool_calls_count': tool_calls_count,
        'server_side_tool_usage': dict(response.server_side_tool_usage),
    }

    # 品質チェック: [確認中]プレースホルダー検出
    placeholder_count = full_response.count('[確認中]')
    if placeholder_count > 0:
        print(f"  [WARN] Found {placeholder_count} [確認中] placeholders in response")

    # 推定・推測の検出
    bad_words = ['推定', '推測', '一般知識で補完']
    for word in bad_words:
        if word in full_response:
            print(f"  [WARN] Found prohibited word '{word}' in response")

    print(f"  [OK] Received response from Grok ({len(full_response)} chars)")
    print(f"  [OK] Tool calls: {tool_calls_count}, Citations: {len(metadata['citations'])}")

    return full_response, metadata


def parse_markdown_response(response: str, target_date: datetime, metadata: dict) -> dict[str, Any]:
    """
    GrokのMarkdownレスポンスを構造化データに変換

    Args:
        response: GrokからのMarkdownレスポンス
        target_date: 対象日
        metadata: APIメタデータ

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
        # Only detect level 2 headers (##) as section boundaries
        # Level 3 headers (###) are subsections and should stay in the same section
        if line.startswith('## '):
            # 前のセクションを保存
            if current_section and section_content:
                content_text = '\n'.join(section_content).strip()
                sections[current_section] = content_text
                section_content = []

            # 新しいセクションを検出
            section_header = line.lower()

            # 出典セクションに到達したら終了（既にクリーンアップ済みのため）
            if '出典' in section_header:
                break

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

            # セクションヘッダー自体は追加しない（二重表示を防ぐ）
            continue

        if current_section:
            section_content.append(line)

    # 最後のセクションを保存
    if current_section and section_content:
        content_text = '\n'.join(section_content).strip()
        sections[current_section] = content_text

    return {
        'report_metadata': {
            'date': target_date.strftime('%Y-%m-%d'),
            'generated_at': datetime.now().isoformat(),
            'prompt_version': '1.3',
            'word_count': len(response),
            'placeholder_count': response.count('[確認中]'),
            'usage': metadata['usage'],
            'citations_count': len(metadata['citations']),
            'tool_calls_count': metadata['tool_calls_count'],
        },
        'content': {
            'title': title,
            'markdown_full': response,
            'sections': sections
        },
        'citations': metadata['citations'][:10],  # 上位10件のみ保存
    }


def cleanup_citations(markdown_content: str) -> str:
    """
    出典表記をクリーンアップ

    1. （出典: URL, URL）形式から全URLを抽出
    2. 表内の生URLを抽出（Markdownテーブルの | URL | 形式）
    3. URLの末尾から不要な文字（）。, 等）を削除
    4. 本文中の（出典: URL）を [出典N] に置き換え
    5. 表内の生URLを [出典N] に置き換え
    6. 末尾に同じURLを [出典#,#,#] URL 形式にまとめた一覧を追加

    Args:
        markdown_content: 元のMarkdownコンテンツ

    Returns:
        str: クリーンアップ済みコンテンツ
    """
    import re

    # URLをクリーンアップして番号を付与
    url_to_numbers = {}
    citation_counter = 0
    replacements = []

    # 1. （出典: URL, URL）のパターンを抽出
    citation_pattern = r'（出典:\s*([^）]+)）'
    matches = list(re.finditer(citation_pattern, markdown_content))

    for match in matches:
        urls_text = match.group(1)
        # カンマやスペースで分割してURL抽出
        urls = [url.strip() for url in re.split(r'[,\s]+', urls_text) if url.strip().startswith('http')]

        # この出典グループの番号を収集
        citation_numbers = []

        for url in urls:
            # URLから末尾の不要な文字を削除
            clean_url = re.sub(r'[/,)。、\s→]+$', '', url)

            citation_counter += 1
            citation_numbers.append(str(citation_counter))

            if clean_url not in url_to_numbers:
                url_to_numbers[clean_url] = []
            url_to_numbers[clean_url].append(str(citation_counter))

        # 置き換え文字列を生成
        if len(citation_numbers) == 1:
            replacement = f"[出典{citation_numbers[0]}]"
        else:
            replacement = f"[出典{','.join(citation_numbers)}]"

        replacements.append((match.group(0), replacement))

    # 2. 表内の生URLを抽出（| URL | 形式）
    # Markdownテーブルのセル内にあるURLを検出
    table_url_pattern = r'\|\s*(https?://[^\s|]+)\s*\|'
    table_url_matches = list(re.finditer(table_url_pattern, markdown_content))

    for match in table_url_matches:
        url = match.group(1).strip()
        # URLから末尾の不要な文字を削除
        clean_url = re.sub(r'[/,)。、\s→]+$', '', url)

        citation_counter += 1
        if clean_url not in url_to_numbers:
            url_to_numbers[clean_url] = []
        url_to_numbers[clean_url].append(str(citation_counter))

        # 置き換え文字列を生成
        replacement = f"| [出典{citation_counter}] |"
        replacements.append((match.group(0), replacement))

    # 置き換え実行
    result = markdown_content
    for original, replacement in replacements:
        result = result.replace(original, replacement, 1)

    # まとめた出典を末尾に追加
    if url_to_numbers:
        result += "\n\n---\n\n## 出典\n\n"
        # 最初の出現順でソート
        for url, numbers in sorted(url_to_numbers.items(), key=lambda x: int(x[1][0])):
            if len(numbers) == 1:
                result += f"[出典{numbers[0]}] {url}\n"
            else:
                numbers_str = ','.join(numbers)
                result += f"[出典{numbers_str}] {url}\n"

    return result


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

    # Markdown保存（既にクリーンアップ済み）
    markdown_path = RAW_DIR / f"{date_str}.md"
    with open(markdown_path, 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    print(f"  [OK] Saved Markdown: {markdown_path}")

    # JSON保存
    json_path = STRUCTURED_DIR / f"{date_str}.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(structured_data, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Saved JSON: {json_path}")

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
            print("  [WARN] S3 bucket not configured; upload skipped.")
            return False

        print(f"  [INFO] Uploading to S3: s3://{cfg.bucket}/{cfg.prefix}{s3_key}")
        success = upload_file(cfg, local_path, s3_key)

        if success:
            print(f"  [OK] Uploaded: {s3_key}")
        else:
            print(f"  [WARN] Upload failed: {s3_key}")

        return success

    except Exception as e:
        print(f"  [ERROR] S3 upload error: {e}")
        return False


def main() -> int:
    """メイン処理"""
    args = parse_args()

    print("=" * 60)
    print("Generate Market Summary Report (v1.3)")
    print("=" * 60)

    # 1. 対象日の取得
    target_date = get_target_date(args.date)
    print(f"\nTarget date: {target_date.strftime('%Y-%m-%d')}")

    # 2. Grok API Key読み込み
    try:
        api_key = load_xai_api_key()
        print("  [OK] XAI API Key loaded")
    except Exception as e:
        print(f"  [ERROR] Failed to load API key: {e}")
        return 1

    # 3. プロンプト生成（J-Quantsデータ読み込み含む）
    print("\n[STEP 1] Building prompt with J-Quants data...")
    try:
        prompt = build_prompt(target_date)
        print(f"  [OK] Prompt built ({len(prompt)} chars)")
    except Exception as e:
        print(f"  [ERROR] Failed to build prompt: {e}")
        return 1

    # 4. Grok API呼び出し
    print("\n[STEP 2] Querying Grok API...")
    try:
        markdown_response, metadata = query_grok(api_key, prompt)
    except Exception as e:
        print(f"  [ERROR] Grok API call failed: {e}")
        return 1

    # 4.5. 出典のクリーンアップ（セクション分割前に実行）
    print("\n[STEP 2.5] Cleaning citations...")
    try:
        markdown_response = cleanup_citations(markdown_response)
        print(f"  [OK] Citations cleaned")
    except Exception as e:
        print(f"  [ERROR] Failed to clean citations: {e}")
        return 1

    # 5. 構造化データ生成
    print("\n[STEP 3] Parsing response...")
    try:
        structured_data = parse_markdown_response(markdown_response, target_date, metadata)
        print(f"  [OK] Response parsed")
    except Exception as e:
        print(f"  [ERROR] Failed to parse response: {e}")
        return 1

    # 6. ローカル保存
    print("\n[STEP 4] Saving files locally...")
    try:
        markdown_path, json_path = save_files(target_date, markdown_response, structured_data)
    except Exception as e:
        print(f"  [ERROR] Failed to save files: {e}")
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
    print(f"Target date:        {date_str}")
    print(f"Markdown saved:     {markdown_path}")
    print(f"JSON saved:         {json_path}")
    print(f"S3 upload (MD):     {'✅' if md_success else '❌'}")
    print(f"S3 upload (JSON):   {'✅' if json_success else '❌'}")
    print(f"Word count:         {structured_data['report_metadata']['word_count']}")
    print(f"[確認中] count:     {structured_data['report_metadata']['placeholder_count']}")
    print(f"Citations:          {structured_data['report_metadata']['citations_count']}")
    print(f"Tool calls:         {structured_data['report_metadata']['tool_calls_count']}")
    print("=" * 60)

    if markdown_path.exists() and json_path.exists():
        print("\n✅ Market summary generation completed successfully!")
        return 0
    else:
        print("\n❌ Market summary generation failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
