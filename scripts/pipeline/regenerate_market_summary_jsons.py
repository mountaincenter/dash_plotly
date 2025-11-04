#!/usr/bin/env python3
"""
regenerate_market_summary_jsons.py
既存のraw markdownファイルから structured JSONを再生成

理由:
cleanup_citations関数の修正により、本文中に [出典N] を残す必要があるため、
既存のJSONファイルを再生成する必要がある。
"""

import json
from pathlib import Path
from datetime import datetime
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# プロンプトインポート（cleanup_citations関数を使用）
sys.path.insert(0, str(ROOT / "scripts" / "pipeline"))
from generate_market_summary import cleanup_citations

MARKET_SUMMARY_DIR = ROOT / "data" / "parquet" / "market_summary"
RAW_DIR = MARKET_SUMMARY_DIR / "raw"
STRUCTURED_DIR = MARKET_SUMMARY_DIR / "structured"


def parse_markdown_to_sections(markdown_content: str) -> dict:
    """
    Markdownコンテンツをセクションに分割

    generate_market_summary.py の parse_markdown_response から抽出
    """
    lines = markdown_content.split('\n')

    # タイトル抽出
    title = lines[0].replace('#', '').strip() if lines else "市場サマリー"

    # セクション分割
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
        if line.startswith('## '):
            # 前のセクションを保存
            if current_section and section_content:
                # 出典セクション（---以降）を除去
                content_text = '\n'.join(section_content).strip()
                if '---' in content_text:
                    content_text = content_text.split('---')[0].strip()
                sections[current_section] = content_text
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

            # セクションヘッダー自体は追加しない
            continue

        if current_section:
            section_content.append(line)

    # 最後のセクションを保存
    if current_section and section_content:
        content_text = '\n'.join(section_content).strip()
        if '---' in content_text:
            content_text = content_text.split('---')[0].strip()
        sections[current_section] = content_text

    return {
        'title': title,
        'sections': sections
    }


def regenerate_json(md_path: Path, json_path: Path) -> bool:
    """
    単一のMarkdownファイルからJSONを再生成

    Returns:
        bool: 成功/失敗
    """
    try:
        # 既存のJSONを読み込み（メタデータを保持）
        with open(json_path, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)

        # Markdownを読み込み
        with open(md_path, 'r', encoding='utf-8') as f:
            raw_markdown = f.read()

        # cleanup_citationsを適用
        cleaned_markdown = cleanup_citations(raw_markdown)

        # セクションに分割
        parsed = parse_markdown_to_sections(cleaned_markdown)

        # 既存のメタデータを保持しつつ、contentを更新
        existing_data['content']['title'] = parsed['title']
        existing_data['content']['markdown_full'] = cleaned_markdown
        existing_data['content']['sections'] = parsed['sections']

        # JSONを保存
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)

        print(f"  [REGENERATED] {json_path.name}")
        return True

    except Exception as e:
        print(f"  [ERROR] {json_path.name}: {e}")
        return False


def main():
    """全てのJSONファイルを再生成"""
    print("=" * 60)
    print("Regenerate Market Summary JSONs")
    print("=" * 60)

    # 既存のJSONファイルを取得
    json_files = sorted(STRUCTURED_DIR.glob("*.json"))

    if not json_files:
        print("No JSON files found")
        return 1

    success_count = 0
    for json_path in json_files:
        # 対応するMarkdownファイルを取得
        date_str = json_path.stem
        md_path = RAW_DIR / f"{date_str}.md"

        if not md_path.exists():
            print(f"  [SKIP] {json_path.name} (no corresponding .md file)")
            continue

        if regenerate_json(md_path, json_path):
            success_count += 1

    print("\n" + "=" * 60)
    print(f"Regenerated {success_count}/{len(json_files)} files")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
