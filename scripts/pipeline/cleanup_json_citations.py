#!/usr/bin/env python3
"""
cleanup_json_citations.py
既存のstructured JSONファイルの出典をクリーンアップして再保存

実行方法:
    python3 scripts/pipeline/cleanup_json_citations.py
"""
import sys
import re
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR


def cleanup_citations(markdown_content: str) -> str:
    """
    出典表記をクリーンアップ

    1. （出典: URL, URL）形式から全URLを抽出
    2. URLの末尾から不要な文字（）。, 等）を削除
    3. 同じURLを [出典#,#,#] URL 形式にまとめる

    Args:
        markdown_content: 元のMarkdownコンテンツ

    Returns:
        str: クリーンアップ済みコンテンツ
    """
    # （出典: URL, URL）のパターンを抽出
    citation_pattern = r'（出典:\s*([^）]+)）'
    matches = re.findall(citation_pattern, markdown_content)

    if not matches:
        return markdown_content

    # 全URLを収集
    all_urls = []
    for match in matches:
        # カンマやスペースで分割してURL抽出
        urls = [url.strip() for url in re.split(r'[,\s]+', match) if url.strip().startswith('http')]
        all_urls.extend(urls)

    # URLをクリーンアップして番号を付与
    url_to_numbers = {}
    citation_number = 1

    for url in all_urls:
        # URLから末尾の不要な文字を削除（複数パターン対応）
        # 例: /,  ） → 。, 、 などを削除
        clean_url = re.sub(r'[/,)。、\s→]+$', '', url)

        if clean_url not in url_to_numbers:
            url_to_numbers[clean_url] = []
        url_to_numbers[clean_url].append(str(citation_number))
        citation_number += 1

    # 元の出典表記を削除
    result = re.sub(citation_pattern, '', markdown_content)

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


def main():
    print("=" * 60)
    print("Cleanup JSON Citations")
    print("=" * 60)

    structured_dir = PARQUET_DIR / "market_summary" / "structured"

    if not structured_dir.exists():
        print(f"[ERROR] Directory not found: {structured_dir}")
        return 1

    # 全jsonファイルを取得
    json_files = sorted(structured_dir.glob("*.json"))

    if not json_files:
        print("[WARN] No JSON files found")
        return 0

    print(f"\n[1] Found {len(json_files)} JSON files")

    cleaned_files = []

    for json_file in json_files:
        print(f"\n[2] Processing {json_file.name}...")

        # 読み込み
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 変更フラグ
        changed = False

        # content.markdown_full をクリーンアップ
        if 'content' in data:
            if 'markdown_full' in data['content']:
                original = data['content']['markdown_full']
                cleaned = cleanup_citations(original)
                if cleaned != original:
                    data['content']['markdown_full'] = cleaned
                    changed = True
                    print(f"  ✓ Cleaned markdown_full")

            # content.sections の各値をクリーンアップ
            if 'sections' in data['content']:
                for section_key, section_content in data['content']['sections'].items():
                    if section_content:
                        original = section_content
                        cleaned = cleanup_citations(original)
                        if cleaned != original:
                            data['content']['sections'][section_key] = cleaned
                            changed = True
                            print(f"  ✓ Cleaned section: {section_key}")

        # 変更があった場合のみ保存
        if changed:
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"  ✓ Saved: {json_file.name}")
            cleaned_files.append(json_file)
        else:
            print(f"  - No changes needed: {json_file.name}")

    if not cleaned_files:
        print("\n[OK] All files are already clean")
        return 0

    # manifest.json更新 & S3アップロード
    print(f"\n[3] Updating manifest and uploading to S3...")

    try:
        # update_manifest.pyを実行
        import subprocess
        result = subprocess.run(
            ["python3", str(ROOT / "scripts" / "pipeline" / "update_manifest.py")],
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode == 0:
            print("  ✓ Manifest updated and S3 upload completed")
        else:
            print(f"  ✗ Error: {result.stderr}")
            return 1

    except Exception as e:
        print(f"  ✗ Failed to update manifest: {e}")
        return 1

    print("\n" + "=" * 60)
    print(f"✅ Cleaned {len(cleaned_files)} files and uploaded to S3")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
