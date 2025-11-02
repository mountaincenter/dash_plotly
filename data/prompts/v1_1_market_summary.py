"""
Grok Prompt v1.1 - Market Summary with Mandatory Tool Usage
市場サマリーレポート生成プロンプト（ツール使用必須版）

Created: 2025-11-01
Updated from v1.0: ツール使用を必須化、ハルシネーション防止を強化
Description:
    - 東証大引け後（15:30終了）に16時配信向け市場サマリーを生成
    - Web Search/Browse Pageツールを必須使用し、1次情報から正確なデータを取得
    - 日経平均、TOPIX、各市場区分指数を網羅
    - 1000-1500字の詳細レポート（2-3分読み切り可能）
"""

from typing import Any


def build_market_summary_prompt(context: dict[str, str]) -> str:
    """
    国内株式市場サマリーレポート生成用のGrokプロンプトを構築（ツール必須版）

    Args:
        context: 実行日・最新営業日などのコンテキスト情報
            - execution_date: 実行日（YYYY-MM-DD形式）
            - latest_trading_day: 最新営業日（YYYY-MM-DD形式）
            - report_time: レポート配信時刻（例: "16:00"）

    Returns:
        str: Grok APIに送信するプロンプト
    """
    execution_date = context.get('execution_date', '2025-11-01')
    latest_trading_day = context.get('latest_trading_day', '2025-10-31')
    report_time = context.get('report_time', '16:00')

    # 翌営業日を計算（簡易: +1日、実際はカレンダー考慮が必要）
    from datetime import datetime, timedelta
    next_trading_day = (datetime.strptime(latest_trading_day, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    tool_mandatory_section = f"""
## ツール使用（必須ステップ）
以下のツールを**必ず使用**し、結果を全セクションに反映してください。ツール出力に基づき事実のみ記述（ハルシネーション禁止）。各ステップの結果を内部でクロス検証後、レポート生成。ツール使用できない場合はレポート生成せず、「ツール実行エラー: データ確認不可」と出力してください（推測値使用禁止）。

1. **主要指数取得（必須）**:
   - Web Searchツールでクエリ「日経平均 TOPIX {latest_trading_day} 終値 site:jpx.co.jp OR site:nikkei.com」を実行（num_results=5）
   - Browse PageツールでURL「https://www.jpx.co.jp/markets/equities/summary/index.html」をinstructions「Extract exact closing prices for Nikkei 225, TOPIX, TOPIX-Prime, TOPIX-Standard, TOPIX-Growth on {latest_trading_day}, including prev day change (absolute and %), volume (billion shares), turnover (trillion yen). Include intraday high/low if available.」で閲覧
   - これらの結果を基にテーブル作成（推測値は使用禁止）

2. **セクター動向取得（必須）**:
   - Web Searchツールでクエリ「東証セクター騰落率 上昇下落上位 {latest_trading_day} site:reuters.com OR site:bloomberg.co.jp」を実行（num_results=10）
   - Browse Pageツールで上位URLをinstructions「List top 3 rising/falling sectors in Prime market with % change (2 decimal), main reasons (1-2 sentences fact-based), and features in Standard/Growth (1-2 items each). Include market comparison.」で閲覧

3. **注目ニュース取得（必須）**:
   - Browse PageツールでURL「https://www.jpx.co.jp/tdnet/」をinstructions「Extract top 3 IR announcements/news on {latest_trading_day}: policy (BOJ), earnings (e.g., major companies), overseas impact (US markets). Include impact facts (stock change %, sector contribution).」で閲覧
   - Web Searchツールでクエリ「日銀政策 企業決算 米国市場 {latest_trading_day} site:nikkei.com OR site:reuters.com」を実行（num_results=5）し補完

4. **全体トレンド取得（必須）**:
   - Web Searchツールでクエリ「東証市場トレンド 為替 売買代金 値上がり率 ボラティリティ {latest_trading_day} site:finance.yahoo.co.jp OR site:nikkei.com」を実行（num_results=5）
   - 結果から主因（箇点分解）、活況度（前日比）、比率、VIXデータ、明日注目点を抽出

5. **日米指標取得（必須）**:
   - Web Searchツールでクエリ「日米経済指標 {next_trading_day} マネーストック 雇用統計 ISM site:investing.com OR site:nikkei.com」を実行（num_results=5）
   - Browse Pageツールで上位URLをinstructions「List Japan/US indicators for {next_trading_day}: time, name, forecast, previous. Add 1-sentence market impact per indicator (e.g., weak employment -> yen strength).」で閲覧
    """

    main_prompt = f"""あなたは経験豊富な日本株アナリストです。{latest_trading_day}の東証市場大引け後（15:30終了）時点で、{report_time}配信向けの市場サマリーレポートを作成してください。文字数は1000-1500字（詳細を追加しつつ、2-3分読み切り可能に読みやすく）。

## コンテキスト
- 焦点: 東証プライム、スタンダード、グロース市場の終値ベース。日経平均、TOPIXを中心に、各市場区分の指数をカバー。
- 情報源優先順位:
  1次（最優先）: 東証公式 jpx.co.jp、TDnet企業IR
  2次（補完用）: 日経新聞、Bloomberg、Reuters、Yahoo!ファイナンス（信頼度高のみ）
  検証ルール: 複数ソース間で数値差異がある場合、1次を採用し「※複数ソース確認済」と明記
  除外対象: SNS、匿名ブログ、未確認情報

## タスク
1. **主要指数**: 日経平均、TOPIX、プライム指数（TOPIX-Prime）、スタンダード指数（TOPIX-Standard）、グロース指数（TOPIX-Growth）の終値、前日比（%表示）、出来高、売買代金をツール結果から抽出しテーブルで統合。出典URLをテーブル下に記載。全体市場の1文概要（例: 「前場高後場調整」）を追加。

2. **セクター動向**:
   - プライム市場: 上昇/下落各上位3セクター（騰落率、小数点第2位、主因を1-2文で事実ベース、影響例: 時価総額変動率）
   - スタンダード/グロース: 特徴的な動き1-2件（例: 特定セクターの上昇要因、資金流入データ）
   - 出典URLを各セクター記述後にインライン記載。市場間比較の1文追加

3. **注目ニュース（トップ3）**: 今日の企業決算/政策発表/海外影響に限定。ツール結果を基に1次情報中心で、各ニュースを箇点で詳細記述（影響度を事実で示す: 株価変動率やセクター寄与度）。[TDnet: URL]または[日経: URL]形式で出典を明記。各ニュースに「市場への波及例」1文追加。

4. **全体トレンド**: 上昇/下落の主因（為替、海外市場、政策等、クロスソース検証）。売買代金の活況度（前日比）、値上がり銘柄比率（%）。明日以降の注目点（事実ベースのみ、憶測禁止）。主因を箇点分解+ボラティリティデータ追加。

5. **日米主要指標発表予定（翌営業日{next_trading_day}焦点）**:
   - 日本: 発表時刻、指標名、予想値、前回値（例: マネーストック、8:50、+2.4%、+2.6%）
   - 米国: 同上、特に雇用統計（21:30、非農業部門雇用者数、失業率）、ISM製造業景況指数等重要指標
   - 各指標に市場影響を1文で簡潔に（例: 「雇用増加鈍化でFRB利下げ期待高まり、株安・円高要因」）
   - 出典URLを箇点後に記載

## 出力フォーマット
- タイトル: 「{latest_trading_day} 国内株式市場サマリー」
- Markdownテーブルで指数をまとめ（セクターはテーブル外の箇点リストで拡張）
- 箇点でニュース、トレンド、指標予定を記述（読みやすさ優先、各セクションにサブヘッド）
- 全引用に[ソース名: URL]をインライン記載（可能な限り）。数値は小数点第2位まで
- データ不足時は「[確認中]」と明記し、推測禁止。全体を客観的に保ち、バイアス/感情表現なし

## 禁止事項
- 政治的バイアス、感情表現（「好調」「懸念」等主観語避け、事実記述）
- 根拠のない予測（「急騰するだろう」等）
- ツール未使用での数値記述（必ずツール結果を使用）
- 低精度ソースの引用。ツール使用時は1次情報優先でクロス検証結果のみ採用
"""

    return tool_mandatory_section + "\n\n" + main_prompt


def get_prompt_metadata() -> dict[str, Any]:
    """
    プロンプトのメタデータを返す

    Returns:
        dict: バージョン情報、作成日、説明などのメタデータ
    """
    return {
        "version": "1.1",
        "created_date": "2025-11-01",
        "updated_from": "v1.0",
        "prompt_type": "market_summary",
        "changes": [
            "ツール使用を必須化（Web Search/Browse Page）",
            "ハルシネーション防止を強化（ツール未使用時はエラー出力）",
            "temperature推奨値を0.1に変更（事実重視）",
            "ツール定義を追加（query_grok関数で使用）"
        ],
        "target_audience": "一般投資家・トレーダー",
        "expected_length": "1000-1500文字",
        "output_format": "Markdown",
        "data_sources": {
            "primary": ["東証公式（jpx.co.jp）", "TDnet企業IR"],
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
            "accuracy": "1次情報優先、クロス検証必須、ツール使用必須",
            "precision": "数値は小数点第2位まで",
            "citation": "全データに出典URL明記",
            "tool_usage": "必須（未使用時はエラー出力）"
        },
        "recommended_settings": {
            "model": "grok-4-fast-reasoning",
            "temperature": 0.1,
            "max_tokens": 3000,
            "tools": ["web_search", "browse_page"],
            "tool_choice": "auto"
        }
    }
