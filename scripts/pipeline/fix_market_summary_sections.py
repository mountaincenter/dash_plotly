#!/usr/bin/env python3
"""
fix_market_summary_sections.py
既存のmarket_summary JSONファイルからセクションヘッダーと出典を除去

問題:
1. 各セクションに `## セクション名` が含まれている（二重表示）
2. 各セクションに `---` 以降の出典が含まれている

修正:
- 各セクションの冒頭から `## セクション名` を削除
- 各セクションから `---` 以降を削除
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STRUCTURED_DIR = ROOT / "data" / "parquet" / "market_summary" / "structured"


def cleanup_section(content: str) -> str:
    """
    セクションコンテンツをクリーンアップ

    1. 冒頭の ## セクション名 を削除
    2. --- 以降の出典部分を削除
    """
    if not content:
        return content

    lines = content.split('\n')
    cleaned_lines = []

    for i, line in enumerate(lines):
        # 冒頭の ## セクション名 をスキップ
        if i == 0 and line.startswith('## '):
            continue

        # --- 以降は全て削除
        if line.strip() == '---':
            break

        cleaned_lines.append(line)

    return '\n'.join(cleaned_lines).strip()


def fix_json_file(json_path: Path) -> bool:
    """
    単一のJSONファイルを修正

    Returns:
        bool: 修正が適用された場合True
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    modified = False

    # sectionsを修正
    if 'content' in data and 'sections' in data['content']:
        for section_key, section_content in data['content']['sections'].items():
            if section_content:
                cleaned = cleanup_section(section_content)
                if cleaned != section_content:
                    data['content']['sections'][section_key] = cleaned
                    modified = True

    # markdown_fullも修正（念のため）
    if 'content' in data and 'markdown_full' in data['content']:
        original = data['content']['markdown_full']
        # markdown_fullは全体なので、セクション個別の---は残してOK
        # ただし各セクションの重複ヘッダーは除去したい
        # → ここでは何もしない（既に cleanup_citations で処理済み）

    if modified:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  [FIXED] {json_path.name}")
        return True
    else:
        print(f"  [SKIP] {json_path.name} (no changes needed)")
        return False


def main():
    """全てのJSONファイルを修正"""
    print("=" * 60)
    print("Fix Market Summary Sections")
    print("=" * 60)

    json_files = sorted(STRUCTURED_DIR.glob("*.json"))

    if not json_files:
        print("No JSON files found")
        return 1

    fixed_count = 0
    for json_path in json_files:
        if fix_json_file(json_path):
            fixed_count += 1

    print("\n" + "=" * 60)
    print(f"Fixed {fixed_count}/{len(json_files)} files")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
