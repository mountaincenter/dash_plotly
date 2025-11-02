"""
Grok Prompt v1.2 - Market Summary with Web Search API
市場サマリーレポート生成プロンプト（web_search()対応版）

Created: 2025-11-02
Updated from v1.1: Browse Page削除、web_search()用クエリ指示に変更、無効URL修正
Description:
    - 東証大引け後（15:30終了）に16時配信向け市場サマリーを生成
    - web_search()ツールを使用し、1次情報から正確なデータを取得
    - 日経平均、TOPIX、各市場区分指数を網羅
    - 1000-1500字の詳細レポート（2-3分読み切り可能）
"""

from typing import Any


def build_market_summary_prompt(context: dict[str, str]) -> str:
    """
    国内株式市場サマリーレポート生成用のGrokプロンプトを構築（web_search()対応版）

    Args:
        context: 実行日・最新営業日などのコンテキスト情報
            - execution_date: 実行日（YYYY-MM-DD形式）
            - latest_trading_day: 最新営業日（YYYY-MM-DD形式）
            - report_time: レポート配信時刻（例: "16:00"）

    Returns:
        str: Grok APIに送信するプロンプト
    """
    execution_date = context.get('execution_date', '2025-11-02')
    latest_trading_day = context.get('latest_trading_day', '2025-11-01')
    report_time = context.get('report_time', '16:00')

    # 翌営業日を計算（簡易: +1日、実際はカレンダー考慮が必要）
    from datetime import datetime, timedelta
    next_trading_day = (datetime.strptime(latest_trading_day, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    tool_mandatory_section = f"""
## Web Search使用（必須ステップ）
以下の検索を**必ず実行**し、結果を全セクションに反映してください。Web Search出力に基づき事実のみ記述（ハルシネーション禁止）。各ステップの結果を内部でクロス検証後、レポート生成。Web Search使用できない場合はレポート生成せず、「Web Search実行エラー: データ確認不可」と出力してください（推測値使用禁止）。

**検索は5-7回に分割して実行してください（正確性とコストのバランスを最適化）。**

1. **主要指数取得（必須）**:
   - 「日経平均 TOPIX 終値 前日比 {latest_trading_day} site:jpx.co.jp OR site:nikkei.com」（num_results=8）
   - 抽出項目: 日経平均とTOPIXの終値、前日比（絶対値と%）、出来高、売買代金

2. **市場区分指数取得（必須）**:
   - 「TOPIX-Prime TOPIX-Standard TOPIX-Growth 終値 {latest_trading_day} site:jpx.co.jp」（num_results=5）
   - 東証公式統計: https://www.jpx.co.jp/markets/statistics-equities/misc/01.html
   - 抽出項目: プライム、スタンダード、グロース各指数の終値と前日比

3. **セクター騰落率取得（必須）**:
   - 「東証33業種 騰落率 {latest_trading_day} site:jpx.co.jp OR site:nikkei.com」（num_results=8）
   - 抽出項目: プライム市場セクター上位3（上昇・下落）の騰落率（小数点第2位）

4. **注目ニュース取得（必須）**:
   - 「{latest_trading_day} 企業決算 業績修正 TDnet site:nikkei.com OR site:reuters.com」（num_results=10）
   - TDnet: https://www.release.tdnet.info/
   - 抽出項目: 政策・決算・海外影響のTOP3、株価変動率、セクター寄与度

5. **市場トレンド取得（必須）**:
   - 「{latest_trading_day} 東証 売買代金 値上がり銘柄 為替 site:jpx.co.jp OR site:nikkei.com」（num_results=8）
   - 抽出項目: 売買代金（前日比）、値上がり/下落銘柄比率、ドル円為替レート

6. **ボラティリティ指標取得（必須）**:
   - 「日経VI {latest_trading_day} site:nikkei.com OR site:investing.com」（num_results=5）
   - 抽出項目: 日経VIの値、前日比

7. **翌日指標予定取得（必須）**:
   - 「{next_trading_day} 経済指標 マネーストック 雇用統計 ISM site:nikkei.com OR site:investing.com」（num_results=8）
   - 抽出項目: 日米の主要経済指標（発表時刻、予想値、前回値）、市場影響
    """

    main_prompt = f"""あなたは経験豊富な日本株アナリストです。{latest_trading_day}の東証市場大引け後（15:30終了）時点で、{report_time}配信向けの市場サマリーレポートを作成してください。文字数は1000-1500字（詳細を追加しつつ、2-3分読み切り可能に読みやすく）。

## コンテキスト
- 焦点: 東証プライム、スタンダード、グロース市場の終値ベース。日経平均、TOPIXを中心に、各市場区分の指数をカバー。
- 情報源優先順位:
  1次（最優先）: 東証公式 jpx.co.jp、TDnet企業IR (release.tdnet.info)、日銀 boj.or.jp
  2次（補完用）: 日経新聞、Bloomberg、Reuters、Yahoo!ファイナンス（信頼度高のみ）
  検証ルール: 複数ソース間で数値差異がある場合、1次を採用し「※複数ソース確認済」と明記
  除外対象: SNS、匿名ブログ、未確認情報

## タスク
1. **主要指数**: 日経平均、TOPIX、プライム指数（TOPIX-Prime）、スタンダード指数（TOPIX-Standard）、グロース指数（TOPIX-Growth）の終値、前日比（%表示）、出来高、売買代金をWeb Search結果から抽出しテーブルで統合。出典URLをテーブル下に記載。全体市場の1文概要（例: 「前場高後場調整」）を追加。

2. **セクター動向**:
   - プライム市場: 上昇/下落各上位3セクター（騰落率、小数点第2位、主因を1-2文で事実ベース、影響例: 時価総額変動率）
   - スタンダード/グロース: 特徴的な動き1-2件（例: 特定セクターの上昇要因、資金流入データ）
   - 出典URLを各セクター記述後にインライン記載。市場間比較の1文追加

3. **注目ニュース（トップ3）**: 今日の企業決算/政策発表/海外影響に限定。Web Search結果を基に1次情報中心で、各ニュースを箇条書きで詳細記述（影響度を事実で示す: 株価変動率やセクター寄与度）。[TDnet: URL]または[日経: URL]形式で出典を明記。各ニュースに「市場への波及例」1文追加。

4. **全体トレンド**: 上昇/下落の主因（為替、海外市場、政策等、クロスソース検証）。売買代金の活況度（前日比）、値上がり銘柄比率（%）。明日以降の注目点（事実ベースのみ、憶測禁止）。主因を箇条書き分解+ボラティリティデータ追加。

5. **日米主要指標発表予定（翌営業日{next_trading_day}焦点）**:
   - 日本: 発表時刻、指標名、予想値、前回値（例: マネーストック、8:50、+2.4%、+2.6%）
   - 米国: 同上、特に雇用統計（21:30、非農業部門雇用者数、失業率）、ISM製造業景況指数等重要指標
   - 各指標に市場影響を1文で簡潔に（例: 「雇用増加鈍化でFRB利下げ期待高まり、株安・円高要因」）
   - 出典URLを箇条書き後に記載

## 出力フォーマット
- タイトル: 「{latest_trading_day} 国内株式市場サマリー」
- Markdownテーブルで指数をまとめ（セクターはテーブル外の箇条書きリストで拡張）
- 箇条書きでニュース、トレンド、指標予定を記述（読みやすさ優先、各セクションにサブヘッド）
- 全引用に[ソース名: URL]をインライン記載（可能な限り）。数値は小数点第2位まで
- データ不足時は「[確認中]」と明記し、推測禁止。全体を客観的に保ち、バイアス/感情表現なし

## 禁止事項
- 政治的バイアス、感情表現（「好調」「懸念」等主観語避け、事実記述）
- 根拠のない予測（「急騰するだろう」等）
- Web Search未使用での数値記述（必ずWeb Search結果を使用）
- 低精度ソースの引用。Web Search使用時は1次情報優先でクロス検証結果のみ採用
"""

    return tool_mandatory_section + "\n\n" + main_prompt


def get_prompt_metadata() -> dict[str, Any]:
    """
    プロンプトのメタデータを返す

    Returns:
        dict: バージョン情報、作成日、説明などのメタデータ
    """
    return {
        "version": "1.2",
        "created_date": "2025-11-02",
        "updated_from": "v1.1",
        "prompt_type": "market_summary",
        "changes": [
            "Browse Page削除（xAI SDK 1.3.1に存在しないため）",
            "web_search()用の具体的なクエリ指示に変更",
            "無効なURL修正（TDnet: release.tdnet.info、JPX統計ページ等）",
            "検索クエリにsite:指定を追加（nikkei.com, reuters.com, jpx.co.jp優先）",
            "num_results指定を明記",
            "検索回数を13回→5-7回に最適化（正確性とコスト効率のバランス）",
            "各クエリを具体的かつシンプルに分割（主要指数、市場区分、セクター、ニュース、トレンド、VI、指標予定）"
        ],
        "target_audience": "一般投資家・トレーダー",
        "expected_length": "1000-1500文字",
        "output_format": "Markdown",
        "data_sources": {
            "primary": [
                "東証公式（jpx.co.jp）",
                "TDnet企業IR（release.tdnet.info）",
                "日銀公式（boj.or.jp）"
            ],
            "secondary": ["日経新聞", "Bloomberg", "Reuters", "Yahoo!ファイナンス"],
            "excluded": ["SNS", "匿名ブログ", "未確認情報", "推測データ"]
        },
        "coverage": {
            "indices": ["日経平均", "TOPIX", "TOPIX-Prime", "TOPIX-Standard", "TOPIX-Growth"],
            "markets": ["東証プライム", "東証スタンダード", "東証グロース"],
            "sections": ["主要指数", "セクター動向", "注目ニュース", "全体トレンド", "日米指標予定"]
        },
        "update_frequency": "営業日毎（大引け後16:00配信）",
        "quality_requirements": {
            "objectivity": "事実ベースのみ、推測・感情表現禁止",
            "accuracy": "1次情報優先、クロス検証必須、Web Search使用必須",
            "precision": "数値は小数点第2位まで",
            "citation": "全データに出典URL明記",
            "tool_usage": "必須（未使用時はエラー出力）"
        },
        "recommended_settings": {
            "model": "grok-4-fast",
            "temperature": 0.1,
            "max_tokens": 3000,
            "tools": ["web_search"],
            "tool_choice": "auto"
        },
        "api_compatibility": {
            "xai_sdk_version": "1.3.1",
            "available_tools": ["web_search", "x_search", "code_execution"],
            "removed_features": ["browse_page"]
        }
    }
