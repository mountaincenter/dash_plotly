#!/usr/bin/env python3
"""
古い静観→売りセクションを全て削除
"""
from pathlib import Path
import re

def main():
    # パス設定
    base_dir = Path(__file__).parent.parent.parent
    html_file = base_dir / 'improvement' / 'v2_1_0_comparison_report.html'

    # HTML読み込み
    print(f"HTML読み込み: {html_file}")
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 正規表現で全てのセクションを削除
    # パターン1: 旧スタイル - <div style...> から次の </div></div> まで
    pattern1 = r'<div style="margin: 30px 0; padding: 20px;[^>]+?>[\s\S]*?<h3[^>]*?>📊 静観→売り 銘柄の成績</h3>[\s\S]*?</table>\s*</div>\s*</div>'

    # パターン2: 新スタイル - <div class="summary-section"> から 詳細比較テーブル の直前まで
    pattern2 = r'<div class="summary-section">[\s\S]*?<h2[^>]*?>📊 静観→売り 銘柄の成績</h2>[\s\S]*?</table>\s*(?=\s*<h2[^>]*?>詳細比較テーブル</h2>)'

    original_length = len(content)

    # パターン1で削除
    content, count1 = re.subn(pattern1, '', content)
    print(f"パターン1（旧スタイル）: {count1}個削除")

    # パターン2で削除
    content, count2 = re.subn(pattern2, '', content)
    print(f"パターン2（新スタイル）: {count2}個削除")

    # まだ残っている場合は手動で確認
    if '📊 静観→売り 銘柄の成績' in content:
        print("⚠️ 警告: まだセクションが残っています")
        # 行番号を表示
        for i, line in enumerate(content.split('\n'), 1):
            if '📊 静観→売り 銘柄の成績' in line:
                print(f"  行 {i}: {line.strip()[:100]}")
    else:
        print("✅ 全ての古いセクションを削除しました")

    # 保存
    if original_length != len(content):
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"HTML保存: {html_file}")
        print(f"削減サイズ: {original_length - len(content)} bytes")
    else:
        print("変更なし")

if __name__ == '__main__':
    main()
