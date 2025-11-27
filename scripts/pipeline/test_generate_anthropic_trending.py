#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Anthropic (Claude) API for stock selection
Target: 勝率50%超、金が増えること
"""

import sys
from pathlib import Path
import pandas as pd
from anthropic import Anthropic
import os
from datetime import datetime, timedelta
import argparse

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from scripts.lib.jquants_fetcher import JQuantsFetcher


def build_claude_prompt(execution_date: str, latest_trading_day: str, next_trading_day: str) -> str:
    """
    Build optimized prompt for Claude
    Goal: 勝率50%超、誇大宣伝を検出、実質的価値のあるIRを見極める
    """

    prompt = f"""あなたは日本株のデイトレード銘柄選定AIです。

## 目的
**明日（{next_trading_day}）寄り付きで買い、大引けで売って利益が出る銘柄を15銘柄選定してください。**

勝率50%超を目指します。誇大宣伝や材料の薄い銘柄は絶対に避けてください。

## 選定基準（優先順位順）

### 1. IRの実質的価値（最重要）
以下のIRは**高評価**：
- 業績上方修正（具体的数値あり、前回予想から+20%以上）
- 大型契約獲得（契約金額が年間売上の10%以上）
- 配当増配（前期比+30%以上）
- 自社株買い（発行済株式の5%以上）
- M&A・業務提携（大手企業との提携、具体的な売上見込みあり）

以下のIRは**低評価または除外**：
- 「検討中」「予定」「目指す」など曖昧な表現
- 具体的数値がない（「大幅増益見込み」など）
- プレスリリースのみで業績インパクト不明
- 過去に類似IRで株価が下落した企業
- IR頻度が高すぎる企業（月3回以上 = 誇大宣伝の可能性）

### 2. リスク要因（絶対除外）
以下は**必ず除外**：
- 前日終値変化率が**+15%以上**（急騰しすぎ、翌日反落リスク）
- 前日終値変化率が**-10%以下**（急落銘柄、さらなる下落リスク）
- 出来高が過去平均の**10倍以上**（仕手株・投機的売買）
- 株価が**200円未満**（ボラティリティ高すぎ）
- 時価総額が**50億円未満**（流動性リスク）

### 3. Twitter/SNS言及数
- **20-60件が最適**（適度な注目度）
- **80件以上は除外**（誇大宣伝・仕手筋の可能性）
- **5件未満も除外**（材料が弱い）

### 4. 市場環境との整合性
- 当日の日経平均・TOPIX動向を確認
- セクター全体のトレンドと整合性があるか
- 地政学リスク・為替変動の影響を考慮

## データソース
- TDnet（適時開示）: https://www.release.tdnet.info/
- 株探ニュース: https://kabutan.jp/news/
- 日経速報: https://www.nikkei.com/markets/
- Yahoo!ファイナンス: https://finance.yahoo.co.jp/

## 出力形式（JSON）

{{
  "stocks": [
    {{
      "ticker": "1234.T",
      "stock_name": "〇〇株式会社",
      "reason": "[web_search: 株探]2025-XX-XX 17:00配信、今期最終2倍上方修正IR発表（最終利益XX億円、前回予想YY億円から+50%）。[web_search: TDnet]決算短信で具体的数値確認。前日終値+3.2%、出来高1.5倍、Twitter言及35件。",
      "sentiment_score": 0.75,
      "selection_score": 88,
      "previous_day_change_pct": 3.2,
      "twitter_mentions": 35,
      "risk_flags": []
    }}
  ]
}}

## 重要な注意事項
1. **具体的数値がないIRは選ばない**
2. **「検討」「予定」は材料として弱い** → 選ばない
3. **前日急騰（+15%以上）は必ず除外**
4. **reasonには必ずweb_searchの具体的引用を含める**（配信元、日時、具体的数値）
5. **15銘柄に満たない場合、無理に埋めない**（質 > 量）

実行日: {execution_date}
対象取引日: {next_trading_day}

それでは、{latest_trading_day}に発表されたIRニュースを検索して、{next_trading_day}に利益が出る銘柄を選定してください。
"""

    return prompt


def parse_claude_response(response_text: str) -> pd.DataFrame:
    """Parse Claude API response to DataFrame"""
    import json
    import re

    # Extract JSON from response
    json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
    if not json_match:
        print("[ERROR] No JSON found in response")
        return pd.DataFrame()

    try:
        data = json.loads(json_match.group())
        stocks = data.get('stocks', [])

        if not stocks:
            print("[ERROR] No stocks in response")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(stocks)

        # Add metadata
        df['selected_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['prompt_version'] = 'anthropic_v1'

        print(f"[OK] Parsed {len(df)} stocks from Claude response")
        return df

    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON decode error: {e}")
        return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target-date', type=str, help='Target date (YYYY-MM-DD)')
    args = parser.parse_args()

    print("=" * 60)
    print("Test Anthropic (Claude) Trending Stocks")
    print("=" * 60)
    print()

    # Initialize
    fetcher = JQuantsFetcher()

    # Determine dates
    if args.target_date:
        target_date = datetime.strptime(args.target_date, "%Y-%m-%d").date()
        execution_date = target_date - timedelta(days=1)
    else:
        execution_date = datetime.now().date()
        target_date = execution_date + timedelta(days=1)

    print(f"[INFO] Execution date: {execution_date}")
    print(f"[INFO] Target date: {target_date}")
    print()

    # Get trading calendar
    print("[INFO] Fetching trading calendar...")
    params = {
        "from": str(execution_date - timedelta(days=5)),
        "to": str(target_date + timedelta(days=5))
    }
    response = fetcher.client.request("/markets/trading_calendar", params=params)
    calendar = pd.DataFrame(response["trading_calendar"])
    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
    trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
    trading_days = trading_days.sort_values("Date")

    # Latest trading day (execution date or before)
    past_days = trading_days[trading_days["Date"] <= execution_date]
    if past_days.empty:
        print("[ERROR] No trading day found")
        return
    latest_trading_day = str(past_days.iloc[-1]["Date"])

    # Next trading day (target date or after)
    future_days = trading_days[trading_days["Date"] >= target_date]
    if future_days.empty:
        print("[ERROR] No future trading day found")
        return
    next_trading_day = str(future_days.iloc[0]["Date"])

    print(f"[OK] Latest trading day: {latest_trading_day}")
    print(f"[OK] Next trading day: {next_trading_day}")
    print()

    # Load API key
    env_file = project_root / ".env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                if line.startswith("ANTHROPIC_API_KEY="):
                    os.environ["ANTHROPIC_API_KEY"] = line.split("=", 1)[1].strip()
                    break

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("[ERROR] ANTHROPIC_API_KEY not found in environment")
        return

    print("[OK] Loaded ANTHROPIC_API_KEY")
    print()

    # Build prompt
    prompt = build_claude_prompt(
        str(execution_date),
        latest_trading_day,
        next_trading_day
    )

    print(f"[INFO] Built prompt ({len(prompt)} chars)")
    print()

    # Call Claude API
    print("[INFO] Querying Claude API...")
    client = Anthropic(api_key=api_key)

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=8000,
        tools=[
            {
                "type": "brave_search",
                "name": "brave_search"
            }
        ],
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    # Extract final text response
    response_text = ""
    for block in response.content:
        if hasattr(block, 'text'):
            response_text += block.text

    print(f"[OK] Received response from Claude ({len(response_text)} chars)")
    print(f"[INFO] Tool usage: {response.usage}")
    print()

    # Parse response
    df = parse_claude_response(response_text)

    if df.empty:
        print("[ERROR] No stocks parsed from response")
        print("Response text:")
        print(response_text[:1000])
        return

    # Add date column
    df['date'] = next_trading_day

    # Save
    output_dir = project_root / "test_output"
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f"anthropic_trending_{next_trading_day.replace('-', '')}.parquet"
    df.to_parquet(output_file, index=False)

    print(f"[OK] Saved: {output_file}")
    print(f"     Total stocks: {len(df)}")
    print()

    # Preview
    print("Top 5 stocks:")
    for idx, row in df.head(5).iterrows():
        print(f"  {idx+1}. {row['ticker']} - {row['stock_name']}")
        print(f"     Score: {row.get('selection_score', 'N/A')}")
        print(f"     Reason: {row.get('reason', 'N/A')[:100]}...")
        print()

    print("=" * 60)
    print("[OK] Completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
