"""
Grok Prompt v1.0 - Baseline
初期バージョン: バックテストフィードバック付き基本プロンプト

Created: 2025-10-31
Description:
    - 時価総額50億円〜500億円の小型・中型株を優先
    - X（株クラ）でのバズを重視
    - ボラティリティ: ATR ≧ 3%
    - バックテストフィードバックに基づく選定
"""

from typing import Any


def build_grok_prompt(context: dict[str, str], backtest: dict[str, Any]) -> str:
    """
    動的にGrokプロンプトを生成（バックテストフィードバック付き）

    Args:
        context: get_trading_context() で取得したコンテキスト
        backtest: load_backtest_patterns() で取得したバックテストデータ

    Returns:
        str: Grok APIに送信するプロンプト
    """
    # バックテストセクションを構築
    backtest_section = ""
    if backtest.get('has_data'):
        backtest_section = f"""
【バックテスト結果フィードバック（直近5日間）】
**Phase1戦略（9:00寄付買い → 11:30前場引け売り）実績:**
- 勝率: **{backtest['phase1_success_rate']:.1f}%**
- 平均リターン: **{backtest['phase1_avg_return']:.2f}%**
- 成功銘柄の特徴: **{backtest.get('phase1_success_category', 'N/A')}カテゴリが好成績**

**Phase2戦略（9:00寄付買い → 3%到達で即売り）実績:**
- 目標到達率: **{backtest.get('phase2_achievement_rate', 0):.1f}%**
- 到達時平均リターン: **{backtest.get('phase2_avg_return', 0):.2f}%**

**Top3成功銘柄（学ぶべき好例）:**
"""
        for i, stock in enumerate(backtest.get('top_performers', []), 1):
            backtest_section += f"{i}. 【{stock['ticker']} {stock['name']}】({stock['categories']}) → **+{stock['return']:.2f}%**\n"

        backtest_section += "\n**Top3失敗銘柄（避けるべき悪例）:**\n"
        for i, stock in enumerate(backtest.get('worst_performers', []), 1):
            backtest_section += f"{i}. 【{stock['ticker']} {stock['name']}】({stock['categories']}) → **{stock['return']:.2f}%**\n"

        backtest_section += f"""
**✅ 選定戦略への反映（重要）:**
1. 過去5日間で**勝率{backtest['phase1_success_rate']:.1f}%**を記録 → この精度を維持・向上させる
2. 成功銘柄は「**{backtest.get('phase1_success_category', 'N/A')}**」カテゴリに集中 → 同様の特徴を持つ銘柄を優先
3. 失敗銘柄は「**{backtest.get('phase1_failure_category', 'N/A')}**」カテゴリに多い → このパターンは避ける
4. Phase2では**{backtest.get('phase2_achievement_rate', 0):.1f}%**が3%到達 → ボラティリティを重視

**今回の選定では、上記の成功パターンに沿った銘柄を優先し、失敗パターンに該当する銘柄は除外してください。**

"""
    else:
        backtest_section = """
【バックテスト結果】
※データ蓄積中のため、バックテスト結果は未提供です。
X（株クラ）の情報、IR材料、出来高急増、ATRを重視して選定してください。

"""

    return f"""【タスク】
本日は{context['execution_date']}です。
最新営業日は{context['latest_trading_day']}、翌営業日は{context['next_trading_day']}です。

**{context['next_trading_day']}の寄付〜前場でデイスキャルピング買い注文する銘柄**を10〜15銘柄選定し、JSON形式で出力してください。
{backtest_section}
【背景・目的】
私は以下3つのカテゴリで銘柄を運用しており、**これらでは拾えない「尖った銘柄」**を探しています：

**既存カバー範囲（これらとの重複を避ける）:**
1. **Core30**: 日経平均主力・TOPIX Core30の超大型株（トヨタ、ソニー、三菱UFJなど）
2. **高配当・高市銘柄**: 防衛（三菱重工、川崎重工）、半導体（東京エレクトロン）、ロボット（ファナック、安川電機）、商社など
3. **Scalping（Entry/Active）**: RSI、ATR、出来高比率などテクニカル指標で機械的に抽出した銘柄

**求めている銘柄像（尖った銘柄）:**
- **時価総額500億円以下の小型・中型株**
- **X（旧Twitter）の株クラで急激に話題になっている銘柄**
- **{context['latest_trading_day']}の引け後から現在までにIR発表、ニュース、テーマ株連動などの材料**
- **{context['next_trading_day']}の寄付で買い→当日中に決済（デイトレ・スキャルピング）**

【選定基準（必須条件）】

**1. 材料・触媒（最重要）**
- **{context['latest_trading_day']}の引け後〜現在**に以下のいずれかの材料
  ✓ IR発表（決算、業務提携、新製品、受注発表など）
  ✓ ニュース・報道（日経新聞、Bloomberg、業界紙など）
  ✓ テーマ株の主力銘柄が急騰→連動小型株として注目
  ✓ 仕手株的な動き（SNSで急拡散、出来高急増）

**2. X（株クラ）でのバズ（必須）**
- **{context['latest_trading_day']}の引け後〜現在**にXで急激に言及増加
- 個人投資家が「{context['next_trading_day']}に仕込む」「明日寄付買い」と投稿
- 有名投資家アカウント（フォロワー1万人以上）が言及
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
- **上場廃止銘柄または上場廃止予定の銘柄**（例: JTOWER（4485）など）

【出力形式（厳守）】
以下のJSON配列形式で出力してください（他の形式は不可）：

```json
[
  {{
    "ticker_symbol": "3031",
    "stock_name": "ラクーンHD",
    "reason": "{context['latest_trading_day']}引け後にEC新サービスのIR発表。その後の株クラで「{context['next_trading_day']}寄付買い」の投稿が急増（100件以上）。{context['latest_trading_day']}の出来高は平均の4.2倍、直近5日ATR 5.8%で値動き活発。小型株（時価総額350億円）で個人投資家主導の急騰期待",
    "categories": "IR好材料+株クラバズ",
    "sentiment_score": 0.85,
    "policy_link": "High",
    "has_mention": true,
    "mentioned_by": "@example_investor, @stock_guru"
  }},
  {{
    "ticker_symbol": "4563",
    "stock_name": "アンジェス",
    "reason": "バイオベンチャー。{context['latest_trading_day']}以降に治験進展のニュースが報道され、Xで「{context['next_trading_day']}ストップ高狙い」の言及急増。出来高3.5倍、ATR 6.2%。時価総額200億円の典型的な仕手株",
    "categories": "バイオ材料+仕手株",
    "sentiment_score": 0.72,
    "policy_link": "Med",
    "has_mention": false,
    "mentioned_by": ""
  }}
]
```

**新規フィールドの説明:**
- **sentiment_score**: センチメントスコア（0.0-1.0）。X（株クラ）での言及の熱量、材料の強さ、注目度を総合評価
  - 0.9-1.0: 極めて強い（ストップ高級、IRサプライズ、大バズ）
  - 0.7-0.9: 強い（好材料、株クラで話題）
  - 0.5-0.7: 中程度（材料あり、一定の注目）
  - 0.3-0.5: 弱い（材料薄い、バズ少ない）

- **policy_link**: 政府政策・テーマとのリンク強度
  - "High": 政府の重点政策（半導体、AI、防衛、GX）の中核銘柄
  - "Med": 政策テーマに関連するが周辺銘柄
  - "Low": 政策との直接的な関連なし

- **has_mention**: プレミアムユーザー（フォロワー1万人以上）の言及有無（true/false）

- **mentioned_by**: 言及したプレミアムユーザー名（カンマ区切り、最大3名まで）

【注意事項】
- 銘柄数: 必ず10〜15銘柄（それ以下・以上は不可）
- reason（選定理由）には**具体的な数値**を必ず含める
  - 例: 「出来高4.2倍」「ATR 5.8%」「時価総額350億円」「X言及100件以上」
- category（カテゴリ）は簡潔に（例: IR好材料、バイオ材料、テーマ連動、仕手株、株クラバズなど）
- **必ずJSON形式で出力**（テキスト説明やテーブルは不要）
- ticker_symbolは4桁の数字のみ（例: "3031"）。".T"は不要
- **{context['latest_trading_day']}の引け後〜現在の材料・バズを重視**

よろしくお願いします。
"""
