#!/usr/bin/env python3
"""
grok_9am_short_ifo.html を topix_selected.html のダークテーマスタイルに変換
- 日付を逆順に
- テーブルヘッダーの金額に背景色を追加
"""

import re
from pathlib import Path

IMPROVEMENT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = IMPROVEMENT_DIR / "output"

def main():
    input_path = OUTPUT_DIR / "grok_9am_short_ifo.html"
    output_path = OUTPUT_DIR / "grok_9am_short_ifo.html"

    with open(input_path, 'r', encoding='utf-8') as f:
        html = f.read()

    # 新しいダークテーマスタイル（topix_selected.htmlベース）
    new_style = '''    <style>
        :root {
            --bg-primary: #1a1a2e;
            --bg-secondary: #16213e;
            --bg-card: #0f3460;
            --text-primary: #e0e0e0;
            --text-secondary: #a0a0a0;
            --border-color: #2a4a6a;
            --positive: #4ade80;
            --negative: #f87171;
            --accent-blue: #60a5fa;
            --accent-orange: #fb923c;
            --accent-purple: #a78bfa;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Consolas', 'Monaco', 'Menlo', monospace;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { margin-bottom: 10px; color: var(--accent-blue); }
        h2 { margin: 30px 0 15px; color: var(--accent-purple); border-bottom: 1px solid var(--border-color); padding-bottom: 5px; }
        h3 { margin: 20px 0 10px; color: var(--text-secondary); }
        .meta { color: var(--text-secondary); margin-bottom: 20px; }
        .warning { background: rgba(251, 191, 36, 0.15); border: 1px solid #fbbf24; padding: 10px; border-radius: 5px; margin-bottom: 20px; color: #fbbf24; }
        .summary-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 30px; }
        .summary-section { background: var(--bg-secondary); border-radius: 8px; padding: 15px; border: 1px solid var(--border-color); }
        .summary-section h3 { margin-top: 0; color: var(--accent-blue); }
        .summary-section.afternoon h3 { color: var(--accent-orange); }
        .summary-cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
        .card { background: var(--bg-card); border-radius: 8px; padding: 12px; border: 1px solid var(--border-color); }
        .card-title { font-size: 0.85em; color: var(--text-secondary); }
        .card-value { font-size: 1.4em; font-weight: bold; }
        .card-value.positive { color: var(--positive); }
        .card-value.negative { color: var(--negative); }
        .total-box { background: linear-gradient(135deg, #065f46, #0f3460); color: white; padding: 20px; border-radius: 8px; text-align: center; margin-bottom: 30px; border: 1px solid var(--positive); }
        .total-box .label { font-size: 1em; opacity: 0.9; }
        .total-box .value { font-size: 2.5em; font-weight: bold; color: var(--positive); }
        table { width: 100%; border-collapse: collapse; margin-bottom: 20px; font-size: 0.85em; }
        th, td { padding: 6px 10px; text-align: left; border-bottom: 1px solid var(--border-color); }
        th { background: var(--bg-card); font-weight: 600; color: var(--text-secondary); }
        tr:hover { background: rgba(96, 165, 250, 0.1); }
        .number { text-align: right; }
        .positive { color: var(--positive); }
        .negative { color: var(--negative); }
        .date-section { margin-bottom: 30px; }
        .date-header { background: var(--bg-card); color: var(--text-primary); padding: 10px 15px; border-radius: 5px 5px 0 0; display: flex; justify-content: space-between; border: 1px solid var(--border-color); }
        .date-header .positive { background: rgba(74, 222, 128, 0.2); padding: 2px 8px; border-radius: 4px; }
        .date-header .negative { background: rgba(248, 113, 113, 0.2); padding: 2px 8px; border-radius: 4px; }
        .date-content { border: 1px solid var(--border-color); border-top: none; padding: 15px; background: var(--bg-secondary); }
        .session-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .session-box { }
        .session-box h4 { color: var(--accent-blue); margin-bottom: 10px; padding: 5px; background: rgba(96, 165, 250, 0.15); border-radius: 3px; }
        .session-box.afternoon h4 { color: var(--accent-orange); background: rgba(251, 146, 60, 0.15); }
        .badge { display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 0.75em; }
        .badge-profit { background: rgba(74, 222, 128, 0.15); color: var(--positive); }
        .badge-loss { background: rgba(248, 113, 113, 0.15); color: var(--negative); }
        .badge-timeout { background: rgba(160, 160, 160, 0.15); color: var(--text-secondary); }
        .strategy-box { background: var(--bg-secondary); border: 1px solid var(--accent-blue); padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        .strategy-box ul { margin-left: 20px; color: var(--text-secondary); }
    </style>'''

    # スタイルを置換
    html = re.sub(r'<style>.*?</style>', new_style, html, flags=re.DOTALL)

    # 日付セクションを抽出して逆順に
    date_section_pattern = r'(<div class="date-section">.*?</div>\s*</div>\s*</div>)'
    date_sections = re.findall(date_section_pattern, html, flags=re.DOTALL)

    if date_sections:
        # 日付を抽出してソート用のキーを取得
        date_with_section = []
        for section in date_sections:
            match = re.search(r'<strong>(\d{4}-\d{2}-\d{2})</strong>', section)
            if match:
                date_with_section.append((match.group(1), section))

        # 日付で逆順ソート（新しい日付が上）
        date_with_section.sort(key=lambda x: x[0], reverse=True)

        # 元のセクションを全て削除
        for section in date_sections:
            html = html.replace(section, '', 1)

        # 日付別詳細のh2の後に逆順で挿入
        h2_pattern = r'(<h2>日付別詳細</h2>\s*)'
        h2_match = re.search(h2_pattern, html)
        if h2_match:
            insert_pos = h2_match.end()
            reversed_sections = '\n\n'.join([s[1] for s in date_with_section])
            html = html[:insert_pos] + '\n\n' + reversed_sections + html[insert_pos:]

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ 変換完了: {output_path}")
    print("  - ダークテーマ適用")
    print("  - 日付を逆順（新しい日付が上）")
    print("  - ヘッダー金額に背景色追加")


if __name__ == "__main__":
    main()
