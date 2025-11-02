#!/usr/bin/env python3
"""
simple_news_fetcher.py
xAI SDK (公式)で今日の日本株ニューストップ3をWeb Searchで自動取得・JSON出力

Usage:
    python3 scripts/pipeline/simple_news_fetcher.py

Requirements:
    - .env.xai with XAI_API_KEY
    - pip install xai-sdk

Output:
    JSON array with top 3 news items
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
from xai_sdk import Client
from xai_sdk.chat import user, system
from dotenv import dotenv_values

# Root directory
ROOT = Path(__file__).resolve().parents[2]
ENV_XAI_PATH = ROOT / ".env.xai"


def build_simple_news_prompt(today: str) -> str:
    """
    今日のニュース取得用プロンプトを構築

    Args:
        today: 対象日（YYYY-MM-DD形式）

    Returns:
        str: プロンプト文字列
    """
    return f"""
【タスク】
本日{today}の日本株関連ニューストップ3を取得し、JSON配列で出力してください。

- 焦点: 小型株/急騰/デイトレ関連のホットニュース（日経/かぶたんなど）
- 基準: 最新性（{today}または直近）、影響度（株価変動大）
- 出力: 各ニュースのタイトル、要約（50文字以内）、関連銘柄（1-2つ）、URL

【出力JSON（厳守）】
[
  {{
    "title": "メディシノバ急騰ニュース",
    "summary": "バイオIRで+26%ストップ高。",
    "related_tickers": ["4875"],
    "url": "https://example.com/news1"
  }},
  {{
    "title": "テーマ株連動",
    "summary": "防衛関連で小型株注目。",
    "related_tickers": ["6208", "7012"],
    "url": "https://example.com/news2"
  }},
  {{
    "title": "IR発表サプライズ",
    "summary": "業績上方修正で急騰。",
    "related_tickers": ["3031"],
    "url": "https://example.com/news3"
  }}
]

注意:
- JSONのみ出力（説明文不要）
- 実際の{today}のニュースを反映
- 関連銘柄は証券コード4桁のみ
"""


def main() -> int:
    """メイン処理"""
    print("=" * 60)
    print("Simple News Fetcher (xAI SDK)")
    print("=" * 60)

    # 1. API Key読み込み
    if not ENV_XAI_PATH.exists():
        print(f"[ERROR] .env.xai not found: {ENV_XAI_PATH}")
        print("[INFO] Please create .env.xai with XAI_API_KEY=your_api_key")
        return 1

    config = dotenv_values(ENV_XAI_PATH)
    api_key = config.get("XAI_API_KEY")

    if not api_key:
        print("[ERROR] XAI_API_KEY not found in .env.xai")
        return 1

    print("[OK] XAI_API_KEY loaded from .env.xai")

    # 2. 今日の日付取得
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"[INFO] Target date: {today}")

    # 3. プロンプト構築
    prompt_text = build_simple_news_prompt(today)
    print(f"[INFO] Built prompt ({len(prompt_text)} chars)")

    # 4. xAI SDK公式方法でGrok API呼び出し
    try:
        print("[INFO] Querying Grok API (xai-sdk official method)...")

        # Clientを初期化
        client = Client(api_key=api_key)

        # Chatを作成
        chat = client.chat.create(
            model="grok-3-mini-fast",
            messages=[
                system("あなたは日本株市場のニュースキュレーターです。JSON形式で正確に出力してください。"),
            ]
        )

        # ユーザーメッセージを追加
        chat.append(user(prompt_text))

        # レスポンス取得
        print("[INFO] Waiting for Grok response...")
        response = chat.sample()

        print("[OK] Received response from Grok")
        print(f"[DEBUG] Response type: {type(response)}")
        print(f"[DEBUG] Response content: {response.content[:200]}...")

        # 5. JSON解析
        content = response.content

        # JSONブロックを抽出（```json ... ``` で囲まれている場合）
        if "```json" in content:
            json_str = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            json_str = content.split("```")[1].split("```")[0].strip()
        else:
            json_str = content.strip()

        news_json = json.loads(json_str)

        # 6. 結果出力
        print("\n" + "=" * 60)
        print("Top 3 News Items")
        print("=" * 60)
        print(json.dumps(news_json, indent=2, ensure_ascii=False))
        print("=" * 60)

        return 0

    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON response: {e}")
        print(f"[DEBUG] Raw response: {content if 'content' in locals() else 'N/A'}")
        return 1

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
