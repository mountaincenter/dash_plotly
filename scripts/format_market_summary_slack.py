#!/usr/bin/env python3
"""
format_market_summary_slack.py
市場サマリーJSONからSlack用の簡潔なテキストを生成

Usage:
    python3 scripts/format_market_summary_slack.py <json_file_path>
"""
import json
import sys
import re
from pathlib import Path


def format_market_summary(json_file: str) -> str:
    """
    市場サマリーJSONをSlack用に整形

    Args:
        json_file: 市場サマリーJSONファイルのパス

    Returns:
        Slack用にフォーマットされたテキスト
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        content = data.get('content', {})
        metadata = data.get('report_metadata', {})

        # タイトル
        date = metadata.get('date', 'N/A')
        word_count = metadata.get('word_count', 0)

        # 全体トレンドを抽出（簡潔に）
        trends = content.get('sections', {}).get('trends', '')
        # 最初の段落のみ抽出（箇条書き部分）
        trend_lines = []
        for line in trends.split('\n'):
            if line.startswith('- '):
                # URLを削除して簡潔に
                clean_line = re.sub(r'\[.*?\]\(.*?\)', '', line)
                clean_line = re.sub(r'http[s]?://\S+', '', clean_line)
                trend_lines.append(clean_line.strip())
            if len(trend_lines) >= 3:  # 最大3行
                break

        trends_summary = '\n'.join(trend_lines) if trend_lines else '（データ取得中）'

        # 注目ニュースのタイトルのみ抽出
        news = content.get('sections', {}).get('news', '')
        news_titles = []
        for line in news.split('\n'):
            if line.startswith('- **') and '**:' in line:
                # タイトル部分のみ抽出
                title_match = re.search(r'\*\*(.*?)\*\*:', line)
                if title_match:
                    news_titles.append(f'• {title_match.group(1)}')
            if len(news_titles) >= 3:  # 最大3つ
                break

        news_summary = '\n'.join(news_titles) if news_titles else '（データ取得中）'

        # Slack用テキスト作成（簡潔版）
        summary = f'''*📊 国内株式市場サマリー* ({date})

*市場動向:*
{trends_summary}

*注目ニュース:*
{news_summary}

_全{word_count}文字のレポート → `GET /market-summary/latest`_
'''
        return summary

    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python3 format_market_summary_slack.py <json_file_path>', file=sys.stderr)
        sys.exit(1)

    json_file = sys.argv[1]

    if not Path(json_file).exists():
        print(f'Error: File not found: {json_file}', file=sys.stderr)
        sys.exit(1)

    result = format_market_summary(json_file)
    print(result)
