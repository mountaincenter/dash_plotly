"""
Grok Prompt v1.2 - Increased Selection Edition
選定数拡大版: 20〜25銘柄選定 → Pythonフィルタで貸借銘柄に絞り込み

Created: 2025-11-30
Based on: v1_1_web_search.py
Description:
    - 選定数を10-15から20-25に増加（フィルタ後10-15銘柄確保のため）
    - web_search() + x_search() toolを積極活用してリアルタイム情報取得
    - プレミアムユーザー6名の発信を最優先(スコア+50〜+100点)
    - X(株クラ)の一次情報・バズをリアルタイム検証
    - TDnet公式IRを一次ソースとして確認
    - バックテストフィードバック継承(v1_0から)
    - 時価総額50億〜500億円の小型・中型株を優先
    - 検証サマリー必須(プレミアム言及率≥40%)
"""

from typing import Any


def build_grok_prompt(context: dict[str, str], backtest: dict[str, Any]) -> str:
    """
    動的にGrokプロンプトを生成（web_search + x_search 版）

    Args:
        context: get_trading_context() で取得したコンテキスト
        backtest: load_backtest_patterns() で取得したバックテストデータ

    Returns:
        str: Grok APIに送信するプロンプト（web_search tool前提）
    """
    # プレミアムユーザーリスト（実績あるデイトレーダー6名）
    premium_users = [
        {"handle": "tesuta001", "name": "テスタ", "desc": "20年連続プラス、総利益100億達成"},
        {"handle": "kabuchenko", "name": "ハニトラ梅木", "desc": "35万→2.4億、小型株デイトレ"},
        {"handle": "jestryoR", "name": "Rょーへー", "desc": "逆張りデイトレの極意、著書あり"},
        {"handle": "kabu777b", "name": "Hikaru", "desc": "中小型株の急騰察知が得意"},
        {"handle": "daykabu2021", "name": "デイ株日本株デイトレ", "desc": "オプチャ700人参加"},
        {"handle": "kaikai2120621", "name": "kaikai", "desc": "19歳、250万→2200万、寄り付き回転トレード"}
    ]

    premium_section = "【優先参照すべき発信者（プレミアムユーザー）】\n"
    premium_section += "以下の6人は、デイトレード・スキャルピングの実績ある発信者です。\n"
    premium_section += "**これらのツイートを最優先でx_searchツールで検索してください。**\n\n"

    for user in premium_users:
        premium_section += f"- **@{user['handle']}** ({user['name']}) - {user['desc']}\n"

    premium_section += f"""
**検索クエリ例（x_searchツール使用）:**
```
from:@tesuta001 OR from:@kabuchenko OR from:@jestryoR OR from:@kabu777b OR from:@daykabu2021 OR from:@kaikai2120621
since:{context['latest_trading_day_raw']}
(株 OR 銘柄 OR 寄付 OR 狙い OR 注目 OR デイトレ)
```

**選定基準への影響:**
- 上記6人のいずれかが言及している銘柄: **スコア+50点**
- 複数人が言及: **スコア+100点**
- ポジティブ文脈のみ（「明日買う」「寄付狙い」「注目」など）
- reasonフィールドに「@ユーザー名が言及」と明記必須
- **最低40%（8銘柄以上）がプレミアムユーザー言及銘柄である必要あり**
"""

    # バックテストセクション（v1_0から継承）
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
1. 過去5日間で**勝率{backtest['phase1_success_rate']:.1f}%**を記録 → この精度を維持・向上
2. 成功銘柄は「**{backtest.get('phase1_success_category', 'N/A')}**」カテゴリに集中 → 同様の特徴を優先
3. 失敗銘柄は「**{backtest.get('phase1_failure_category', 'N/A')}**」カテゴリに多い → このパターンは避ける
4. Phase2では**{backtest.get('phase2_achievement_rate', 0):.1f}%**が3%到達 → ボラティリティを重視

**今回の選定では、上記の成功パターンに沿った銘柄を優先し、失敗パターンに該当する銘柄は除外してください。**
"""
    else:
        backtest_section = """
【バックテスト結果】
※データ蓄積中のため、バックテスト結果は未提供です。
プレミアムユーザーの発信、IR材料、出来高急増、ATRを重視して選定してください。
"""

    return f"""【タスク】
本日は{context['execution_date']}です。
最新営業日は{context['latest_trading_day']}、翌営業日は{context['next_trading_day']}です。

**あなたは20年デイトレ専業の株アナリストです。**
**{context['next_trading_day']}の寄付〜前場でデイスキャルピング買い注文する銘柄**を20〜25銘柄選定し、JSON形式で出力してください。

**【重要】web_searchツールとx_searchツールを積極的に使用して、一次情報を取得してください。**
- x_search: X(旧Twitter)のリアルタイム投稿・バズ確認
- web_search: TDnet公式IR、ニュース記事、政府公式発表の確認

{premium_section}
{backtest_section}

【背景・目的】
私は以下2つのカテゴリで銘柄を運用しており、**これらでは拾えない「尖った銘柄」**を探しています：

**既存カバー範囲（これらとの重複を避ける）:**
1. **Core30**: 日経平均主力・TOPIX Core30の超大型株（トヨタ、ソニー、三菱UFJなど）
2. **高配当・高市銘柄**: 防衛（三菱重工、川崎重工）、半導体（東京エレクトロン）、ロボット（ファナック、安川電機）、商社など

**求めている銘柄像（尖った銘柄）:**
- **時価総額500億円以下の小型・中型株**
- **X（旧Twitter）の株クラで急激に話題になっている銘柄**（x_searchで確認）
- **{context['latest_trading_day']}の引け後から現在までにIR発表、ニュース、テーマ株連動などの材料**（web_searchで確認）
- **{context['next_trading_day']}の寄付で買い→当日中に決済（デイトレ・スキャルピング）**

【選定基準（必須条件）】

**0. プレミアムユーザーの言及（最優先）**
- **{context['latest_trading_day']}の引け後〜現在**に6人のいずれかがポジティブ言及
- x_searchツールで以下のクエリを実行:
  ```
  from:@tesuta001 OR from:@kabuchenko OR from:@jestryoR OR from:@kabu777b OR from:@daykabu2021 OR from:@kaikai2120621
  since:{context['latest_trading_day_raw']}
  (株 OR 銘柄 OR 寄付 OR 狙い OR 注目)
  ```
- 複数人言及を最優先（スコア+100点）
- **最低8銘柄以上（40%）がプレミアムユーザー言及銘柄である必要あり**

**1. 材料・触媒（最重要）**
- **{context['latest_trading_day']}の引け後〜現在**に以下のいずれかの材料
  ✓ IR発表（決算、業務提携、新製品、受注発表など）
  ✓ ニュース・報道（日経新聞、Bloomberg、業界紙など）
  ✓ テーマ株の主力銘柄が急騰→連動小型株として注目
  ✓ 仕手株的な動き（SNSで急拡散、出来高急増）

- **web_searchツールでIR・ニュースを確認:**
  ```
  # ニュースサイト経由でIR情報取得（優先順）
  site:nikkei.com [銘柄名] (IR OR 決算 OR 適時開示) after:{context['latest_trading_day_raw']}
  site:finance.yahoo.co.jp [銘柄名] 適時開示
  site:kabutan.jp [銘柄名] IR
  site:minkabu.jp [銘柄名] 適時開示

  # 企業公式サイトのIR情報
  [企業名] IR ニュースリリース after:{context['latest_trading_day_raw']}
  ```

- **x_searchツールで企業公式アカウントも確認:**
  ```
  from:[企業公式アカウント] since:{context['latest_trading_day_raw']}
  (IR OR 適時開示 OR プレスリリース OR お知らせ OR 決算)
  ```

注: TDnet公式サイトは検索不可のため、ニュースサイト・Yahoo!ファイナンス・企業公式サイト/X経由でIR情報を確認してください。

**2. X（株クラ）でのバズ（必須）**
- **{context['latest_trading_day']}の引け後〜現在**にXで急激に言及増加
- x_searchツールで以下を確認:
  ```
  [銘柄名 OR ティッカーコード] since:{context['latest_trading_day_raw']}
  (注目 OR 寄付 OR デイトレ OR 狙い OR 買い)
  ```
- 個人投資家が「{context['next_trading_day']}に仕込む」「明日寄付買い」と投稿
- 言及100件以上、RT500以上でボーナス
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
- web_searchで時価総額を確認:
  ```
  site:yahoo.co.jp [銘柄名] 時価総額
  ```

【除外条件（厳守）】
以下は**絶対に選定しない**でください：
- 日経225採用銘柄
- TOPIX Core30銘柄
- 時価総額1000億円以上の大型株
- 具体例: トヨタ(7203)、ソニー(6758)、三菱UFJ(8306)、ソフトバンクG(9984)、東京エレクトロン(8035)、三菱重工(7011)、川崎重工(7012)、ファナック(6954)、安川電機(6506)、日本製鉄(5401)、キーエンス(6861)、任天堂(7974)、信越化学(4063)、ダイキン(6367)、リクルート(6098)、KDDI(9433)、NTT(9432)、JR東日本(9020)など
- 日次売買代金1000万円未満の低流動性銘柄
- **上場廃止銘柄または上場廃止予定の銘柄**
  - web_searchで必ず確認:
    ```
    [銘柄名] [ティッカー] 上場廃止
    site:jpx.co.jp [銘柄名] 上場廃止
    site:yahoo.co.jp [銘柄名] 上場廃止
    ```
  - 上場廃止済み・廃止予定は即除外

【ハルシネーション防止ルール（厳守）】

**絶対禁止事項:**
- ❌ ツール結果にない情報の推定・仮定・想像による生成
- ❌ 架空のURL・数値・銘柄名・IR情報の捏造
- ❌ 「おそらく」「推定」「可能性がある」「〜と思われる」などの曖昧表現
- ❌ ツール未実行での事実主張（必ずweb_search/x_searchで確認）
- ❌ 古い情報の流用（{context['latest_trading_day']}以前の材料は参考程度に）

**必須事項:**
- ✅ 全ての事実（時価総額、出来高、ATR、言及数、IR情報）はweb_search/x_searchの結果から**直接引用のみ**
- ✅ reasonフィールドにツール結果を引用形式で明記:
  - 例: `[web_search: Yahoo!ファイナンス]時価総額350億円`
  - 例: `[x_search: @kabuchenko ツイート]「明日寄付買い」(2025-10-24 16:30投稿)`
  - 例: `[web_search: 日経新聞]EC新サービスIR発表(2025-10-24 15:30配信)`
- ✅ URLは実在するもののみ（ツール結果から取得したURL）
- ✅ 数値は必ずツール結果のまま使用（丸めない、推定しない）
- ✅ 日時情報を正確に記載（ツイート投稿時刻、ニュース配信時刻など）

**出力前の自己検証（内部実行必須）:**
各銘柄について、以下を出力前に確認してください:
1. ❓ この時価総額はweb_searchの結果と一致するか？（Yahoo!等で確認済みか）
2. ❓ このIR情報はweb_searchで実際に見つかったか？（日経・株探等で確認済みか）
3. ❓ このプレミアムユーザーのツイートはx_searchで確認できたか？（実際の投稿が存在するか）
4. ❓ この出来高・ATR数値はツール結果から取得したか？（推定値ではないか）
5. ❓ この除外確認は正しいか？（Core30リストと照合済みか）
6. ❓ 上場廃止確認をweb_searchで実行したか？（jpx.co.jp、Yahoo!等で確認済みか）

→ **いずれか1つでもNoなら、その銘柄は出力しない**

【選定プロセス（5ステップ・Chain of Thought）】
以下の5ステップで選定してください。各ステップでweb_search/x_searchを**必ず実行**し、結果を引用形式でreasonに反映してください。

**ステップ1: プレミアムユーザー発信分析**
- x_searchで6人の{context['latest_trading_day']}引け後〜現在のポジティブ言及を検索
- 候補銘柄をリストアップ（スコア計算: 1人=+50点、複数人=+100点）
- **最低8銘柄確保**

**ステップ2: 除外条件フィルタ**
- 各銘柄を除外条件でフィルタ（Core30非該当、時価総額50〜500億円確認、上場廃止確認）
- web_searchで時価総額・市場を確認
- **web_searchで上場廃止状況を必ず確認**:
  - `[銘柄名] [ティッカー] 上場廃止`
  - `site:jpx.co.jp [銘柄名] 上場廃止`
  - 上場廃止済み・廃止予定は即スキップ
- 違反銘柄は即スキップ

**ステップ3: 材料検証 + Xバズ確認**
- web_searchでIR・ニュース確認（日経、Yahoo!ファイナンス、株探、みんかぶ）
- x_searchで企業公式アカウント確認（IR発表のツイート）
- x_searchでXバズ確認（言及100件以上、RT500以上）
- センチメントスコアを算出（0.0-1.0）

**ステップ4: ボラティリティ・出来高計算**
- ATR≧3%、出来高2倍以上を確認
- 不適合銘柄はスキップ

**ステップ5: 最終検証**
- 重複ゼロ確認
- プレミアム言及率≧40%確認（未達なら再生成）
- 一次情報比率≧50%確認（TDnet/公式X）
- 検証サマリー作成

【出力形式（厳守）】
以下のJSON配列形式で出力してください（他の形式は不可）：

```json
[
  {{
    "ticker_symbol": "3031",
    "stock_name": "ラクーンHD",
    "reason": "【@kabuchenko が言及】[web_search: 日経新聞]{context['latest_trading_day']} 15:30配信、EC新サービスのIR発表。[web_search: Yahoo!ファイナンス]適時開示掲載確認。[x_search: @kabuchenko ツイート]{context['latest_trading_day']} 16:30投稿「{context['next_trading_day']}寄付買い」（RT200超、いいね350）。[x_search: #注目銘柄]X株クラで言及150件以上確認。[web_search: 株探]出来高は20日平均の4.2倍、直近5日ATR 5.8%。[web_search: Yahoo!ファイナンス]時価総額350億円確認。[web_search: jpx.co.jp]上場廃止該当なし。除外確認: Core30リスト照合済み、時価総額500億円未満OK、上場廃止なし",
    "categories": "プレミアム+IR好材料+株クラバズ",
    "sentiment_score": 0.85,
    "policy_link": "High",
    "has_mention": true,
    "mentioned_by": ["@kabuchenko"]
  }},
  {{
    "ticker_symbol": "4563",
    "stock_name": "アンジェス",
    "reason": "【@tesuta001,@kaikai2120621 が複数言及】[web_search: Bloomberg]{context['latest_trading_day']} 17:00配信、治験進展ニュース。[web_search: 株探]IR情報掲載確認。[x_search: @tesuta001]{context['latest_trading_day']} 18:00投稿「明日狙い目」（RT150）。[x_search: @kaikai2120621]{context['latest_trading_day']} 19:30投稿「寄り付き回転」（RT80）。[x_search: ストップ高]X言及200件超確認（RT合計500超）。[web_search: 株探]出来高20日平均の3.5倍、ATR 6.2%。[web_search: Yahoo!]時価総額200億円。[web_search: jpx.co.jp]上場廃止該当なし。除外確認: Core30リスト照合済み、上場廃止なし",
    "categories": "プレミアム複数+バイオ材料+仕手株",
    "sentiment_score": 0.82,
    "policy_link": "Med",
    "has_mention": true,
    "mentioned_by": ["@tesuta001", "@kaikai2120621"]
  }}
]
```

**JSON末尾に検証サマリーオブジェクトを追加:**
```json
{{
  "verification_summary": {{
    "total_stocks": 12,
    "premium_mentioned_stocks": 5,
    "premium_mention_rate": "42%",
    "avg_market_cap": "200億円",
    "duplicates": 0,
    "primary_source_rate": "55%",
    "policy_linked_stocks": 3,
    "avg_sentiment_score": 0.72
  }}
}}
```

**フィールドの説明:**
- **ticker_symbol**: 4桁の数字のみ（例: "3031"）。".T"は不要
- **reason**: 選定理由（必ず具体的な数値とツール引用を含める）
  - プレミアムユーザー言及時は先頭に【@ユーザー名が言及】
  - **ツール引用形式を必ず使用**:
    - `[web_search: ソース名]事実内容`
    - `[x_search: @ユーザー名]ツイート内容(投稿時刻)`
  - 数値例: 「出来高4.2倍」「ATR 5.8%」「時価総額350億円」「X言及150件」「RT200超」
  - 日時情報を正確に記載（ニュース配信時刻、ツイート投稿時刻など）
  - **上場廃止確認を必ず記載**: `[web_search: jpx.co.jp]上場廃止該当なし`
  - 除外確認明記（「除外確認: Core30リスト照合済み、時価総額500億円未満OK、上場廃止なし」など）
  - **推定・想像による情報は一切含めない（ツール結果のみ）**
- **category**: カテゴリ（例: プレミアム+IR好材料、バイオ材料、テーマ連動、仕手株など）
- **sentiment_score**: センチメントスコア（0.0-1.0）
  - 0.9-1.0: 極めて強い（ストップ高級、IRサプライズ、大バズ）
  - 0.7-0.9: 強い（好材料、株クラで話題）
  - 0.5-0.7: 中程度（材料あり、一定の注目）
  - 0.3-0.5: 弱い（材料薄い、バズ少ない）
- **policy_link**: 政府政策・テーマとのリンク強度
  - "High": 政府の重点政策（半導体、AI、防衛、GX）の中核銘柄
  - "Med": 政策テーマに関連するが周辺銘柄
  - "Low": 政策との直接的な関連なし
- **has_mention**: プレミアムユーザー（6人）の言及有無（true/false）
- **mentioned_by**: 言及したプレミアムユーザー名の配列（最大6名）

【検証前チェック（出力前必須）】
生成後、以下をチェックしてください。未達なら再生成:
- ✅ 重複: ticker_symbol重複なし
- ✅ 除外: Core30/高配当大型/時価総額非該当/上場廃止を全確認
- ✅ 上場廃止確認: 全銘柄でweb_search実行済み
- ✅ 銘柄数: 20〜25銘柄
- ✅ プレミアム言及率: ≧40%（8銘柄以上）
- ✅ 一次情報比率: ≧50%（TDnet/公式X確認済み）
- ✅ reason品質: 数値/材料/センチメント/上場廃止確認/除外確認全含む
- ✅ センチメント平均: ≧0.6

【注意事項】
- **web_searchツールとx_searchツールを積極的に使用**してください
- x_search例: `from:@tesuta001 OR from:@kabuchenko since:{context['latest_trading_day_raw']} (株 OR 銘柄)`
- web_search例:
  - `site:nikkei.com [銘柄名] (IR OR 適時開示) after:{context['latest_trading_day_raw']}`
  - `site:finance.yahoo.co.jp [銘柄名] 適時開示`
  - `site:kabutan.jp [銘柄名] IR`
- **{context['latest_trading_day']}の引け後〜現在の材料・バズを重視**
- reasonには必ず一次情報のURL含める（日経、Yahoo!、株探など）
- 除外条件厳守、重複絶対避ける
- **必ずJSON形式で出力**（テキスト説明やテーブルは不要）
- 検証サマリー必須

よろしくお願いします。
"""
