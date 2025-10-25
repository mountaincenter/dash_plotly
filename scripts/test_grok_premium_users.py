#!/usr/bin/env python3
"""
test_grok_premium_users.py
おすすめ発信者を重視したGrok銘柄選定のテスト用スクリプト

実行方法:
    python3 scripts/test_grok_premium_users.py

出力:
    - コンソールに結果を表示
    - data/parquet/grok_test_result.txt にGrok貼り付け用テキストを出力
"""

from __future__ import annotations

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openai import OpenAI
from dotenv import dotenv_values
from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_fetcher import JQuantsFetcher

ENV_XAI_PATH = ROOT / ".env.xai"
TEST_OUTPUT_PATH = PARQUET_DIR / "grok_test_result.txt"

# おすすめ発信者リスト
PREMIUM_USERS = [
    {"username": "@tesuta001", "name": "テスタ", "description": "20年連続プラスのコツコツ型、総利益100億達成"},
    {"username": "@kabuchenko", "name": "ハニトラ梅木", "description": "35万から2.4億達成、小型・新興株のデイトレード"},
    {"username": "@jestryoR", "name": "Rょーへー", "description": "逆張りデイトレの極意、著書『超実践！勝ち続けるための逆張りデイトレード』"},
    {"username": "@kabu777b", "name": "Hikaru", "description": "専業投資家、中小型株・注目トレンド銘柄の解説"},
    {"username": "@daykabu2021", "name": "デイ株日本株デイトレ", "description": "デイトレと短期スイング、オプチャで700人参加"},
    {"username": "@kaikai2120621", "name": "kaikai", "description": "19歳大2、元本250万→2200万超、寄り付き直後の回転トレード"},
]


def get_trading_context() -> dict[str, str]:
    """営業日カレンダーを参照して取引コンテキストを生成"""
    fetcher = JQuantsFetcher()
    latest_trading_day_str = fetcher.get_latest_trading_day()
    latest_trading_day = datetime.strptime(latest_trading_day_str, "%Y-%m-%d").date()

    today = datetime.now().date()
    future_end = today + timedelta(days=10)

    params = {
        "from": str(today),
        "to": str(future_end)
    }

    response = fetcher.client.request("/markets/trading_calendar", params=params)
    import pandas as pd
    calendar = pd.DataFrame(response["trading_calendar"])

    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
    trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
    trading_days = trading_days.sort_values("Date")

    future_trading_days = trading_days[trading_days["Date"] > latest_trading_day]

    if future_trading_days.empty:
        raise RuntimeError("No future trading days found in calendar")

    next_trading_day = future_trading_days.iloc[0]["Date"]

    return {
        "execution_date": datetime.now().strftime("%Y年%m月%d日"),
        "latest_trading_day": latest_trading_day.strftime("%Y年%m月%d日"),
        "next_trading_day": next_trading_day.strftime("%Y年%m月%d日"),
        "latest_trading_day_raw": str(latest_trading_day),
        "next_trading_day_raw": str(next_trading_day),
    }


def build_premium_users_section() -> str:
    """おすすめ発信者セクションを生成"""
    section = "**【重要】優先参照すべき発信者（プレミアムユーザー）**\n\n"
    section += "以下の6人は、デイトレード・スキャルピングの実績ある発信者です。\n"
    section += "**これらのユーザーが言及・推奨している銘柄を最優先で選定してください**：\n\n"

    for i, user in enumerate(PREMIUM_USERS, 1):
        section += f"{i}. **{user['username']}** ({user['name']})\n"
        section += f"   {user['description']}\n"

    section += "\n**選定基準への影響**：\n"
    section += "- 上記6人のいずれかが言及している銘柄は**スコア +50点**\n"
    section += "- 複数人が言及している場合は**さらに高評価**\n"
    section += "- これらの発信者のツイートを**最優先で検索・分析**してください\n"
    section += "- reasonフィールドには「@tesuta001が言及」などユーザー名を明記\n\n"

    return section


def build_grok_prompt_with_premium_users(context: dict[str, str]) -> str:
    """プレミアムユーザーを重視したGrokプロンプトを生成"""

    premium_section = build_premium_users_section()

    return f"""【タスク】
本日は{context['execution_date']}です。
最新営業日は{context['latest_trading_day']}、翌営業日は{context['next_trading_day']}です。

**{context['next_trading_day']}の寄付〜前場でデイスキャルピング買い注文する銘柄**を10〜15銘柄選定し、JSON形式で出力してください。

{premium_section}

【背景・目的】
私は以下3つのカテゴリで銘柄を運用しており、**これらでは拾えない「尖った銘柄」**を探しています：

**既存カバー範囲（これらとの重複を避ける）:**
1. **Core30**: 日経平均主力・TOPIX Core30の超大型株（トヨタ、ソニー、三菱UFJなど）
2. **高配当・高市銘柄**: 防衛（三菱重工、川崎重工）、半導体（東京エレクトロン）、ロボット（ファナック、安川電機）、商社など
3. **Scalping（Entry/Active）**: RSI、ATR、出来高比率などテクニカル指標で機械的に抽出した銘柄

**求めている銘柄像（尖った銘柄）:**
- **時価総額500億円以下の小型・中型株**
- **X（旧Twitter）の株クラで急激に話題になっている銘柄（特に上記プレミアムユーザーが言及）**
- **{context['latest_trading_day']}の引け後から現在までにIR発表、ニュース、テーマ株連動などの材料**
- **{context['next_trading_day']}の寄付で買い→当日中に決済（デイトレ・スキャルピング）**

【選定基準（必須条件）】

**0. プレミアムユーザーの言及（最優先）**
- 上記6人のいずれかが**{context['latest_trading_day']}の引け後〜現在**に言及
- 言及内容: 「明日買う」「寄付狙い」「注目」などポジティブな文脈
- 複数人が言及している場合は最優先
- reasonフィールドに必ず「@ユーザー名が言及」と明記

**1. 材料・触媒（重要）**
- **{context['latest_trading_day']}の引け後〜現在**に以下のいずれかの材料
  ✓ IR発表（決算、業務提携、新製品、受注発表など）
  ✓ ニュース・報道（日経新聞、Bloomberg、業界紙など）
  ✓ テーマ株の主力銘柄が急騰→連動小型株として注目
  ✓ 仕手株的な動き（SNSで急拡散、出来高急増）

**2. X（株クラ）でのバズ（必須）**
- **{context['latest_trading_day']}の引け後〜現在**にXで急激に言及増加
- 個人投資家が「{context['next_trading_day']}に仕込む」「明日寄付買い」と投稿
- 有名投資家アカウント（特にプレミアムユーザー）が言及
- ハッシュタグ付きで拡散（例: #注目銘柄 #デイトレ）

**3. ボラティリティ（必須）**
- **{context['latest_trading_day']}またはその前後数日**の値動きが活発
- 直近5日のATR（Average True Range）≧ 3%
- {context['latest_trading_day']}の値幅（高値-安値）÷始値 ≧ 2%
- ストップ高の可能性がある銘柄

**4. 出来高急増（流動性確保）**
- **{context['latest_trading_day']}**の出来高が20日平均の2倍以上
- 最低でも日次売買代金5000万円以上（約定可能性）

**5. 時価総額・市場（絞り込み）**
- **時価総額: 50億円〜500億円を優先**（小型・中型株）
- 市場: 東証プライム・スタンダード・グロース

【除外条件（厳守）】
以下は**絶対に選定しない**でください：
- 日経225採用銘柄
- TOPIX Core30銘柄
- 時価総額1000億円以上の大型株
- 具体例: トヨタ、ソニー、三菱UFJ、ソフトバンクG、東京エレクトロン、三菱重工、川崎重工、ファナック、安川電機、日本製鉄、キーエンス、任天堂、信越化学、ダイキン、リクルート、KDDI、NTT、JR東日本など
- 日次売買代金1000万円未満の低流動性銘柄

【出力形式（厳守）】
以下のJSON配列形式で出力してください（他の形式は不可）：

```json
[
  {{
    "ticker_symbol": "3031",
    "company_name": "ラクーンHD",
    "reason": "【@tesuta001が言及】{context['latest_trading_day']}引け後にEC新サービスのIR発表。@tesuta001が「明日寄付狙い」とツイート（RT500超）。その後の株クラで「{context['next_trading_day']}寄付買い」の投稿が急増（150件以上）。{context['latest_trading_day']}の出来高は平均の4.2倍、直近5日ATR 5.8%で値動き活発。小型株（時価総額350億円）で個人投資家主導の急騰期待",
    "category": "プレミアムユーザー言及+IR好材料",
    "mentioned_by": ["@tesuta001"]
  }},
  {{
    "ticker_symbol": "4563",
    "company_name": "アンジェス",
    "reason": "【@kabuchenko, @kaikai2120621が言及】バイオベンチャー。{context['latest_trading_day']}以降に治験進展のニュースが報道。@kabuchenkoが「明日狙い目」、@kaikai2120621が「寄り付き回転候補」とツイート。Xで「{context['next_trading_day']}ストップ高狙い」の言及急増。出来高3.5倍、ATR 6.2%。時価総額200億円の典型的な仕手株",
    "category": "プレミアムユーザー複数言及+バイオ材料",
    "mentioned_by": ["@kabuchenko", "@kaikai2120621"]
  }}
]
```

【注意事項】
- 銘柄数: 必ず10〜15銘柄（それ以下・以上は不可）
- **プレミアムユーザー6人が言及している銘柄を最優先で選定**
- reason（選定理由）の先頭に**【@ユーザー名が言及】**を明記（該当する場合）
- reason（選定理由）には**具体的な数値**を必ず含める
  - 例: 「出来高4.2倍」「ATR 5.8%」「時価総額350億円」「X言及150件以上」「RT500超」
- category（カテゴリ）にプレミアムユーザー言及を含める（例: プレミアムユーザー言及+IR好材料）
- mentioned_by: プレミアムユーザーが言及している場合、配列で列挙（例: ["@tesuta001", "@kabuchenko"]）
- **必ずJSON形式で出力**（テキスト説明やテーブルは不要）
- ticker_symbolは4桁の数字のみ（例: "3031"）。".T"は不要
- **{context['latest_trading_day']}の引け後〜現在の材料・バズを重視**
- **プレミアムユーザーのツイートを最優先で検索・分析してください**

よろしくお願いします。
"""


def load_xai_api_key() -> str:
    """Load XAI_API_KEY from .env.xai"""
    if not ENV_XAI_PATH.exists():
        raise FileNotFoundError(
            f".env.xai not found: {ENV_XAI_PATH}\n"
            "Please create .env.xai with XAI_API_KEY=your_api_key"
        )

    config = dotenv_values(ENV_XAI_PATH)
    api_key = config.get("XAI_API_KEY")

    if not api_key:
        raise ValueError("XAI_API_KEY not found in .env.xai")

    return api_key


def query_grok(api_key: str, prompt: str) -> str:
    """Query Grok API via OpenAI client"""
    print("[INFO] Querying Grok API...")

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
    )

    response = client.chat.completions.create(
        model="grok-3",
        messages=[
            {
                "role": "system",
                "content": "あなたは日本株市場のデイトレード専門家です。特定の有力発信者（@tesuta001, @kabuchenko, @jestryoR, @kabu777b, @daykabu2021, @kaikai2120621）の情報を最優先で参照し、彼らが言及している銘柄を重視してください。選定理由には具体的な数値と発信者名を必ず含めてください。"
            },
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=4000,
    )

    content = response.choices[0].message.content
    print(f"[OK] Received response from Grok ({len(content)} chars)")
    return content


def parse_grok_response(response: str) -> list[dict]:
    """Parse Grok's JSON response"""
    print("[INFO] Parsing Grok response...")

    if "```json" in response:
        json_str = response.split("```json")[1].split("```")[0].strip()
    elif "```" in response:
        json_str = response.split("```")[1].split("```")[0].strip()
    else:
        json_str = response.strip()

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON: {e}")
        print(f"[DEBUG] Response content:\n{response}")
        raise

    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array, got {type(data)}")

    print(f"[OK] Parsed {len(data)} stocks from Grok response")
    return data


def format_for_grok_analysis(grok_data: list[dict], context: dict[str, str]) -> str:
    """Grok貼り付け用のテキスト形式にフォーマット"""

    output = "=" * 80 + "\n"
    output += "Grok銘柄選定結果（プレミアムユーザー重視版）\n"
    output += "=" * 80 + "\n\n"

    output += f"実行日時: {context['execution_date']}\n"
    output += f"最新営業日: {context['latest_trading_day']}\n"
    output += f"翌営業日: {context['next_trading_day']}\n"
    output += f"選定銘柄数: {len(grok_data)}銘柄\n\n"

    output += "【プレミアムユーザー】\n"
    for user in PREMIUM_USERS:
        output += f"  {user['username']} ({user['name']}): {user['description']}\n"
    output += "\n"

    output += "=" * 80 + "\n"
    output += "【選定銘柄一覧】\n"
    output += "=" * 80 + "\n\n"

    # プレミアムユーザー言及銘柄を先に表示
    mentioned_stocks = []
    other_stocks = []

    for stock in grok_data:
        if stock.get("mentioned_by") and len(stock.get("mentioned_by", [])) > 0:
            mentioned_stocks.append(stock)
        else:
            other_stocks.append(stock)

    # プレミアムユーザー言及銘柄
    if mentioned_stocks:
        output += "【★ プレミアムユーザー言及銘柄 ★】\n"
        output += "-" * 80 + "\n\n"

        for i, stock in enumerate(mentioned_stocks, 1):
            output += f"{i}. {stock['ticker_symbol']} - {stock['company_name']}\n"
            output += f"   カテゴリ: {stock.get('category', 'N/A')}\n"
            output += f"   言及者: {', '.join(stock.get('mentioned_by', []))}\n"
            output += f"   選定理由:\n"
            output += f"   {stock.get('reason', 'N/A')}\n\n"

    # その他の銘柄
    if other_stocks:
        output += "\n【その他の注目銘柄】\n"
        output += "-" * 80 + "\n\n"

        for i, stock in enumerate(other_stocks, 1):
            output += f"{len(mentioned_stocks) + i}. {stock['ticker_symbol']} - {stock['company_name']}\n"
            output += f"   カテゴリ: {stock.get('category', 'N/A')}\n"
            output += f"   選定理由:\n"
            output += f"   {stock.get('reason', 'N/A')}\n\n"

    output += "=" * 80 + "\n"
    output += "【統計サマリー】\n"
    output += "=" * 80 + "\n\n"

    output += f"総銘柄数: {len(grok_data)}銘柄\n"
    output += f"プレミアムユーザー言及銘柄: {len(mentioned_stocks)}銘柄\n"
    output += f"その他の銘柄: {len(other_stocks)}銘柄\n\n"

    # ユーザー別言及数
    user_mentions = {}
    for stock in mentioned_stocks:
        for user in stock.get("mentioned_by", []):
            user_mentions[user] = user_mentions.get(user, 0) + 1

    if user_mentions:
        output += "【ユーザー別言及数】\n"
        for user, count in sorted(user_mentions.items(), key=lambda x: x[1], reverse=True):
            output += f"  {user}: {count}銘柄\n"
        output += "\n"

    output += "=" * 80 + "\n\n"

    output += "【次のステップ】\n"
    output += "このテキストをGrokに貼り付けて、以下を質問してください：\n"
    output += "1. 「この選定結果の精度をどう評価しますか？」\n"
    output += "2. 「プレミアムユーザーの言及は実際にありましたか？」\n"
    output += "3. 「改善すべきポイントはありますか？」\n"
    output += "\n"

    return output


def main() -> int:
    """メイン処理"""
    print("=" * 80)
    print("Grok銘柄選定テスト（プレミアムユーザー重視版）")
    print("=" * 80)
    print()

    try:
        # 1. 営業日コンテキスト取得
        print("[INFO] Fetching trading calendar from J-Quants...")
        context = get_trading_context()
        print(f"[OK] Execution date: {context['execution_date']}")
        print(f"[OK] Latest trading day: {context['latest_trading_day']}")
        print(f"[OK] Next trading day: {context['next_trading_day']}")
        print()

        # 2. プロンプト生成
        prompt = build_grok_prompt_with_premium_users(context)
        print(f"[INFO] Built dynamic prompt ({len(prompt)} chars)")
        print()

        # 3. API key読み込み
        api_key = load_xai_api_key()
        print(f"[OK] Loaded XAI_API_KEY from {ENV_XAI_PATH}")
        print()

        # 4. Grok API呼び出し
        response = query_grok(api_key, prompt)
        print()

        # 5. レスポンスパース
        grok_data = parse_grok_response(response)
        print()

        # 6. 結果表示
        print("=" * 80)
        print("【選定結果プレビュー】")
        print("=" * 80)
        print()

        # プレミアムユーザー言及銘柄を先に表示
        mentioned_count = 0
        for i, stock in enumerate(grok_data, 1):
            mentioned_by = stock.get("mentioned_by", [])
            if mentioned_by and len(mentioned_by) > 0:
                mentioned_count += 1
                print(f"★ {i}. {stock['ticker_symbol']} - {stock['company_name']}")
                print(f"   言及者: {', '.join(mentioned_by)}")
                print(f"   カテゴリ: {stock.get('category', 'N/A')}")
                print(f"   理由: {stock.get('reason', 'N/A')[:100]}...")
                print()

        print(f"プレミアムユーザー言及銘柄: {mentioned_count}/{len(grok_data)}銘柄")
        print()

        # 7. Grok貼り付け用テキスト生成
        formatted_text = format_for_grok_analysis(grok_data, context)

        # 8. ファイル出力
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        TEST_OUTPUT_PATH.write_text(formatted_text, encoding="utf-8")
        print(f"[OK] Saved result to: {TEST_OUTPUT_PATH}")
        print()

        # 9. 使い方説明
        print("=" * 80)
        print("【使い方】")
        print("=" * 80)
        print()
        print(f"1. 以下のファイルを開いてください：")
        print(f"   {TEST_OUTPUT_PATH}")
        print()
        print(f"2. 内容をコピーしてGrokに貼り付けて、以下を質問：")
        print(f"   「この選定結果の精度をどう評価しますか？」")
        print(f"   「プレミアムユーザーの言及は実際にありましたか？」")
        print(f"   「改善すべきポイントはありますか？」")
        print()
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
