"""
既存のmarket summaryファイルからJ-Quants APIの記載を削除するスクリプト
"""

import json
import re
from pathlib import Path

# データディレクトリ
DATA_DIR = Path(__file__).parent.parent / "data" / "parquet" / "market_summary"

def remove_disclaimer(text: str) -> str:
    """
    テキストからJ-Quants API関連の文言を削除

    Args:
        text: 元のテキスト

    Returns:
        クリーンなテキスト
    """
    # 削除するパターン
    patterns = [
        r"※TOPIX系指数はJ-Quants Standard APIより取得\n*",
        r"※33業種別指数はJ-Quants Standard APIより取得\n*",
    ]

    result = text
    for pattern in patterns:
        result = re.sub(pattern, "", result)

    return result


def process_json_file(file_path: Path):
    """
    JSONファイルを処理して文言を削除

    Args:
        file_path: JSONファイルのパス
    """
    print(f"処理中: {file_path.name}")

    try:
        # JSONを読み込み
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        modified = False

        # content配下を処理
        if 'content' in data:
            # markdown_full を処理
            if 'markdown_full' in data['content']:
                original = data['content']['markdown_full']
                cleaned = remove_disclaimer(original)
                if original != cleaned:
                    data['content']['markdown_full'] = cleaned
                    modified = True
                    print(f"  - content.markdown_full を修正")

            # sections内のindices, sectorsを処理
            if 'sections' in data['content']:
                for section_key in ['indices', 'sectors']:
                    if section_key in data['content']['sections']:
                        original = data['content']['sections'][section_key]
                        cleaned = remove_disclaimer(original)
                        if original != cleaned:
                            data['content']['sections'][section_key] = cleaned
                            modified = True
                            print(f"  - content.sections.{section_key} を修正")

        # 修正があれば保存
        if modified:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"  ✓ {file_path.name} を更新しました")
        else:
            print(f"  - 変更なし")

    except Exception as e:
        print(f"  ✗ エラー: {e}")


def process_markdown_file(file_path: Path):
    """
    Markdownファイルを処理して文言を削除

    Args:
        file_path: Markdownファイルのパス
    """
    print(f"処理中: {file_path.name}")

    try:
        # Markdownを読み込み
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()

        # クリーン化
        cleaned_content = remove_disclaimer(original_content)

        if original_content != cleaned_content:
            # 保存
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(cleaned_content)
            print(f"  ✓ {file_path.name} を更新しました")
        else:
            print(f"  - 変更なし")

    except Exception as e:
        print(f"  ✗ エラー: {e}")


def main():
    """メイン処理"""
    print("=" * 80)
    print("J-Quants API記載削除スクリプト")
    print("=" * 80)

    # JSONファイルを処理
    json_dir = DATA_DIR / "structured"
    if json_dir.exists():
        print("\n[JSON ファイル]")
        for json_file in sorted(json_dir.glob("*.json")):
            process_json_file(json_file)

    # Markdownファイルを処理
    md_dir = DATA_DIR / "raw"
    if md_dir.exists():
        print("\n[Markdown ファイル]")
        for md_file in sorted(md_dir.glob("*.md")):
            process_markdown_file(md_file)

    print("\n" + "=" * 80)
    print("✓ 処理完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
