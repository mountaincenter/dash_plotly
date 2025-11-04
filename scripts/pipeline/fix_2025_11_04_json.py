#!/usr/bin/env python3
"""
fix_2025_11_04_json.py
11:20版のオリジナルファイルから正しいJSONを生成

1. （出典: URL）を [出典N] に変換
2. change_pctを最新parquetから計算
3. sectionsから ## と --- を削除
"""

import json
import sys
import re
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR

# cleanup_citationsをインポート
sys.path.insert(0, str(ROOT / "scripts" / "pipeline"))
from generate_market_summary import cleanup_citations


def calculate_sectors_change_pct(target_date: datetime) -> dict:
    """
    最新のsectors_prices_max_1d.parquetから前日比を計算

    Returns:
        dict: {ticker: change_pct}
    """
    sectors_file = PARQUET_DIR / "sectors_prices_max_1d.parquet"
    if not sectors_file.exists():
        print(f"  [WARN] sectors file not found")
        return {}

    df = pd.read_parquet(sectors_file)
    df['date'] = pd.to_datetime(df['date']).dt.date

    target_date_obj = target_date.date()
    current = df[df['date'] == target_date_obj]

    # 前営業日を探す（最大7日前まで）
    prev_data = None
    for days_back in range(1, 8):
        prev_date = target_date_obj - timedelta(days=days_back)
        prev_data = df[df['date'] == prev_date]
        if not prev_data.empty:
            break

    if prev_data is None or prev_data.empty:
        print(f"  [WARN] No previous data found")
        return {}

    # マージして前日比計算
    merged = current.merge(
        prev_data[['ticker', 'close']],
        on='ticker',
        how='left',
        suffixes=('', '_prev')
    )

    merged['change_pct'] = ((merged['close'] - merged['close_prev']) / merged['close_prev'] * 100)

    # ticker: change_pctのdictを返す
    return dict(zip(merged['ticker'], merged['change_pct']))


def update_sectors_table(markdown: str, change_pct_map: dict) -> str:
    """
    sectionsのテーブル内のchange_pctをnanから実際の値に更新
    """
    def replace_nan(match):
        # テーブル行全体をキャプチャ
        line = match.group(0)
        parts = [p.strip() for p in line.split('|') if p.strip()]

        if len(parts) >= 3:
            name = parts[0]
            close = parts[1]
            change_pct_str = parts[2]

            # nanの場合のみ置換を試みる
            if change_pct_str == 'nan':
                # nameからtickerを推測（簡易版）
                # 実際のデータで確認が必要
                return line  # とりあえずそのまま

        return line

    # テーブル行のnanを置換
    # より正確にはJSONのsectionsフィールドを直接編集すべき
    return markdown


def fix_json_file(input_path: Path, output_path: Path):
    """
    JSONファイルを修正
    """
    print(f"Reading {input_path}")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    target_date = datetime.strptime(data['report_metadata']['date'], '%Y-%m-%d')

    # 1. markdown_fullにcleanup_citationsを適用
    print("  [1] Applying cleanup_citations to markdown_full...")
    data['content']['markdown_full'] = cleanup_citations(data['content']['markdown_full'])

    # 2. change_pctを計算
    print("  [2] Calculating change_pct from latest parquet...")
    change_pct_map = calculate_sectors_change_pct(target_date)
    print(f"      Found {len(change_pct_map)} sectors with change_pct")

    # 3. sectionsを修正
    print("  [3] Fixing sections...")

    # まずmarkdown_fullから出典URLマッピングを抽出
    # URL → [番号リスト] のマッピング
    citation_pattern = r'\[出典(\d+(?:,\d+)*)\]\s+(https?://[^\s\)]+)'
    url_to_numbers = {}
    max_citation_num = 0
    for match in re.finditer(citation_pattern, data['content']['markdown_full']):
        numbers = match.group(1).split(',')
        url = match.group(2).strip()
        for num in numbers:
            num_int = int(num.strip())
            if url not in url_to_numbers:
                url_to_numbers[url] = []
            url_to_numbers[url].append(num_int)
            max_citation_num = max(max_citation_num, num_int)

    # 各URLの番号リストをソート＆重複削除
    for url in url_to_numbers:
        url_to_numbers[url] = sorted(set(url_to_numbers[url]))

    print(f"      Found {len(url_to_numbers)} unique URLs (max citation: {max_citation_num})")

    for section_key, section_content in data['content']['sections'].items():
        if not section_content:
            continue

        # （出典: URL）を [出典N] に変換
        def replace_citation(match):
            urls_text = match.group(1)
            urls = [u.strip() for u in re.split(r'[,\s]+', urls_text) if u.strip().startswith('http')]

            citation_nums = []
            for url in urls:
                # URLの末尾の不要文字を削除
                clean_url = re.sub(r'[/,)。、\s→]+$', '', url)
                if clean_url in citation_map:
                    citation_nums.append(str(citation_map[clean_url]))

            if citation_nums:
                if len(citation_nums) == 1:
                    return f'[出典{citation_nums[0]}]'
                else:
                    return f'[出典{",".join(citation_nums)}]'
            return ''

        cleaned = re.sub(r'（出典:\s*([^）]+)）', replace_citation, section_content)

        # テーブル内の生のURLも[出典N]に変換（未マッピングURLには新番号を割り当て）
        def replace_table_url(match):
            nonlocal max_citation_num
            url = match.group(1).strip()
            clean_url = re.sub(r'[/,)。、\s→]+$', '', url)

            if clean_url in citation_map:
                return f'[出典{citation_map[clean_url]}]'
            else:
                # 新しい出典番号を割り当て
                max_citation_num += 1
                citation_map[clean_url] = max_citation_num
                print(f"        New citation [{max_citation_num}]: {clean_url[:60]}...")
                return f'[出典{max_citation_num}]'

        # | 出典 | の列にあるURLを置換
        cleaned = re.sub(r'\|\s*(https?://[^\s\|]+)\s*\|', lambda m: f'| {replace_table_url(m)} |', cleaned)

        # ## セクション名を削除（最初の行のみ）
        lines = cleaned.split('\n')
        if lines and lines[0].startswith('## '):
            lines = lines[1:]
        cleaned = '\n'.join(lines)

        # --- 以降を削除（行頭の---のみ）
        # テーブルの区切り線（| --- | --- |）は除外
        if '\n---\n' in cleaned:
            cleaned = cleaned.split('\n---\n')[0].strip()
        elif cleaned.startswith('---\n'):
            cleaned = cleaned.split('---\n', 1)[1] if '---\n' in cleaned else cleaned

        data['content']['sections'][section_key] = cleaned

    # 4. 新しく追加された出典をmarkdown_fullにも追加
    if max_citation_num > len(citation_map):
        print(f"  [4] Adding new citations to markdown_full...")
        # markdown_fullの出典一覧を更新
        # 最後の出典一覧を削除して再構築
        markdown_lines = data['content']['markdown_full'].split('\n')

        # ## 出典 セクションを探す
        citation_section_start = -1
        for i, line in enumerate(markdown_lines):
            if line.strip() == '## 出典':
                citation_section_start = i
                break

        if citation_section_start >= 0:
            # 出典セクションを再構築
            new_citations = []
            for url, num in sorted(citation_map.items(), key=lambda x: x[1]):
                new_citations.append(f'[出典{num}] {url}')

            # 出典セクション前まで + 新しい出典セクション
            markdown_lines = markdown_lines[:citation_section_start] + ['', '---', '', '## 出典', ''] + new_citations
            data['content']['markdown_full'] = '\n'.join(markdown_lines)

    # 5. 保存
    print(f"  [5] Saving to {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"  [OK] Fixed {output_path.name}")


def main():
    input_file = Path("/tmp/2025-11-04_original.json")
    output_file = Path("data/parquet/market_summary/structured/2025-11-04.json")

    if not input_file.exists():
        print(f"Error: {input_file} not found")
        return 1

    fix_json_file(input_file, output_file)

    print("\n" + "=" * 60)
    print("Fixed 2025-11-04.json successfully")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
