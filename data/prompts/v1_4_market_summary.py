"""
Grok Prompt v1.4 - Market Summary with VI Level Comments
市場サマリーレポート生成プロンプト（日経VI水準コメント対応版）

Created: 2025-12-04
Updated from v1.3: 配信時刻16:45、文字数上限2000、日経VI水準コメント追加
Description:
    - TOPIX/業種別指数データをJ-Quants Standard APIから取得し、プロンプトに埋め込み
    - web_search()はニュース・トレンド取得のみに限定
    - 日経VIの水準コメント（平常/やや警戒/警戒/恐怖）を追加
    - 配信時刻を16:45に変更（J-Quants Standardの更新タイミングに合わせる）
    - 文字数上限を2000字に拡大（読了時間4-5分）
"""

from typing import Any
import pandas as pd


def format_jquants_table(df: pd.DataFrame, columns: list[str]) -> str:
    """
    J-QuantsデータをMarkdownテーブルに変換

    Args:
        df: DataFrame (with change_pct column if available)
        columns: 表示するカラム

    Returns:
        str: Markdownテーブル
    """
    if df.empty:
        return "（データなし）"

    table_lines = ["| " + " | ".join(columns) + " |"]
    table_lines.append("| " + " | ".join(["---"] * len(columns)) + " |")

    for _, row in df.iterrows():
        values = []
        for col in columns:
            if col in row:
                val = row[col]
                if col == 'change_pct' and pd.notna(val):
                    # 前日比は符号付きで表示
                    sign = "+" if val > 0 else ""
                    values.append(f"{sign}{val:.2f}%")
                elif isinstance(val, float):
                    values.append(f"{val:.2f}")
                else:
                    values.append(str(val))
            else:
                values.append("N/A")
        table_lines.append("| " + " | ".join(values) + " |")

    return "\n".join(table_lines)


def build_market_summary_prompt(context: dict[str, Any]) -> str:
    """
    国内株式市場サマリーレポート生成用のGrokプロンプトを構築（日経VI水準コメント対応版）

    Args:
        context: 実行日・最新営業日などのコンテキスト情報
            - execution_date: 実行日（YYYY-MM-DD形式）
            - latest_trading_day: 最新営業日（YYYY-MM-DD形式）
            - report_time: レポート配信時刻（例: "16:45"）
            - jquants_topix: TOPIX系指数DataFrame
            - jquants_sectors: 33業種別指数DataFrame
            - jquants_series: 17業種別指数DataFrame

    Returns:
        str: Grok APIに送信するプロンプト
    """
    execution_date = context.get('execution_date', '2025-12-04')
    latest_trading_day = context.get('latest_trading_day', '2025-12-03')
    report_time = context.get('report_time', '16:45')

    # J-Quantsデータ
    topix_df = context.get('jquants_topix', pd.DataFrame())
    sectors_df = context.get('jquants_sectors', pd.DataFrame())
    series_df = context.get('jquants_series', pd.DataFrame())

    # TOPIX系指数テーブル作成
    topix_table = format_jquants_table(
        topix_df,
        ['name', 'close', 'change_pct']
    )

    # 33業種別指数（騰落率上位5・下位5）
    if not sectors_df.empty and 'change_pct' in sectors_df.columns:
        # 前日比でソート
        sectors_sorted = sectors_df.sort_values('change_pct', ascending=False)
        top5_sectors = format_jquants_table(
            sectors_sorted.head(5),
            ['name', 'close', 'change_pct']
        )
        bottom5_sectors = format_jquants_table(
            sectors_sorted.tail(5),
            ['name', 'close', 'change_pct']
        )
    else:
        top5_sectors = "（データなし）"
        bottom5_sectors = "（データなし）"

    # プロンプト本体
    prompt = f"""あなたは経験豊富な日本株アナリストです。{latest_trading_day}の東証市場大引け後（15:30終了）時点で、{report_time}配信向けの市場サマリーレポートを作成してください。文字数は1000字以上、2000字程度を目安に（最大2000字以内、読了時間4-5分）。

## 提供済みデータ（J-Quants Standard API取得）

### TOPIX系指数（{latest_trading_day}終値）
{topix_table}

### 33業種別指数 上位5業種（{latest_trading_day}終値）
{top5_sectors}

### 33業種別指数 下位5業種（{latest_trading_day}終値）
{bottom5_sectors}

**重要**: 上記の数値データは**確定値**です。推測・補完は不要です。そのまま使用してください。

## Web Search使用（高精度クエリで確実に情報取得）
以下の情報を取得するため、web_search()ツールを使用してください。各クエリは複数回実行し、確実にデータを取得すること：

1. **日経平均株価取得（最優先・必須）**:
   - クエリ1: 「日経平均 {latest_trading_day} 終値 前日比 site:nikkei.com」（num_results=5）
   - クエリ2: 「日経平均株価 {latest_trading_day} 大引け 終値 site:jpx.co.jp」（num_results=3）
   - クエリ3: 「nikkei 225 {latest_trading_day} close site:reuters.com OR site:bloomberg.co.jp」（num_results=3）
   - 抽出項目: 終値、前日比（円）、前日比（%）
   - **必須**: 3つのクエリすべてを実行し、最も信頼できる数値を採用

2. **日経VI取得（必須・水準コメント付き）**:
   - クエリ: 「日経VI {latest_trading_day} 終値 前日比 site:nikkei.com」（num_results=5）
   - 参照URL: https://www.nikkei.com/smartchart/?code=N145%2FO
   - 抽出項目: 日経VI値、前日比（%）
   - **水準判定基準（必ず適用）**:
     * 15-20: 平常
     * 20-25: やや警戒
     * 25超: 警戒
     * 30超: 恐怖
   - **出力形式**: 「日経VI: XX.XX（±X.XX%）- [水準]水準」
   - 例: 「日経VI: 27.70（-3.18%）- 警戒水準」

3. **全体トレンド取得（必須）**:
   - クエリ1: 「東証 {latest_trading_day} 前引け 大引け 売買代金 site:nikkei.com」（num_results=5）
   - クエリ2: 「東証プライム {latest_trading_day} 値上がり 値下がり 銘柄数 site:nikkei.com OR site:reuters.com」（num_results=5）
   - クエリ3: 「東証 {latest_trading_day} 前場 後場 投資家心理 site:nikkei.com OR site:reuters.com」（num_results=5）
   - 抽出項目: 前場後場の動き、売買代金、値上がり/下落銘柄数、投資家心理

4. **為替動向取得（必須）**:
   - クエリ1: 「ドル円 {latest_trading_day} 午前 午後 レート site:nikkei.com」（num_results=5）
   - クエリ2: 「{latest_trading_day} 為替 円安 円高 輸出株 影響 site:reuters.com OR site:bloomberg.co.jp」（num_results=5）
   - 抽出項目: ドル円レート（具体的数値）、円高・円安の動き、輸出セクターへの影響

5. **セクター動向の背景取得（必須）**:
   - クエリ1: 「{latest_trading_day} 東証33業種 上昇 電気ガス 食料品 非鉄金属 site:nikkei.com」（num_results=5）
   - クエリ2: 「{latest_trading_day} 東証33業種 下落 輸送用機器 医薬品 site:nikkei.com OR site:bloomberg.co.jp」（num_results=5）
   - クエリ3: 「{latest_trading_day} セクター別 株価 要因 材料 site:nikkei.com」（num_results=5）
   - 抽出項目: 上位5・下位5業種の騰落要因（材料・ニュース・市場環境）

6. **注目ニュース取得（必須）**:
   - クエリ1: 「{latest_trading_day} 日銀 金融政策決定会合 site:nikkei.com」（num_results=5）
   - クエリ2: 「{latest_trading_day} 企業決算 トヨタ ソニー KDDI site:nikkei.com OR site:reuters.com」（num_results=5）
   - クエリ3: 「{latest_trading_day} 米国市場 ナスダック ダウ 日本株 影響 site:nikkei.com OR site:reuters.com」（num_results=5）
   - 抽出項目: 政策動向（日銀等）、主要企業決算、海外市場の影響

7. **経済指標予定取得（必須・最優先は本日夜の米国指標）**:
   - クエリ1: 「{latest_trading_day} 今夜 米国 経済指標 雇用統計 ISM FOMC site:nikkei.com OR site:investing.com」（num_results=5）
   - クエリ2: 「{latest_trading_day} 米国市場 発表予定 経済指標 site:bloomberg.co.jp OR site:reuters.com」（num_results=5）
   - クエリ3: 「US economic calendar {latest_trading_day} today tonight site:investing.com」（num_results=5）
   - クエリ4: 「{latest_trading_day} 翌営業日 日銀 金融政策決定会合 site:boj.or.jp OR site:nikkei.com」（num_results=5）
   - クエリ5: 「経済指標カレンダー 今週 来週 日米 site:nikkei.com OR site:investing.com」（num_results=5）
   - 抽出項目:
     * 本日夜（米国時間）: 時刻（JST換算）、指標名、予想値、前回値
     * 翌営業日以降: 日付、時刻、指標名
   - **最重要**: 本日夜の米国指標（雇用統計・ISM・FOMC・GDP・CPI等）を最優先で探索
   - **副次的**: 翌営業日以降の日銀会合・主要指標

## タスク

1. **主要指数**:
   - 提供済みのTOPIX系指数テーブルをそのまま使用
   - 日経平均: 上記3つのクエリから取得した終値・前日比を表形式で追加（TOPIX系指数の上に配置）
   - 各指数の特徴を2-3文で説明（例: 「日経平均は輸出関連大型株の影響が強く、TOPIXを上回る上昇率となった。TOPIX-Primeは時価総額上位の大型株で構成され、市場全体の約80%を占める。TOPIX-Standardは中型株中心で本日は小幅上昇にとどまり、TOPIX-Growthは新興成長株を対象とし堅調に推移した。」）

2. **ボラティリティ（日経VI）**:
   - 日経VIの値と前日比を記載
   - **水準判定を必ず付記**:
     * 15-20: 「平常水準」
     * 20-25: 「やや警戒水準」
     * 25超: 「警戒水準」
     * 30超: 「恐怖水準」
   - 形式: 「日経VI: XX.XX（±X.XX%）- [水準]水準」
   - 1文で市場心理への影響を補足（例: 「投資家のリスク回避姿勢がやや強まっている」）

3. **全体トレンド**:
   - 以下の項目を段落形式で記述（箇条書きではなく、流れのある文章として構成）：
     a) 前場後場の動き: 日経平均の前場引け値、後場の推移、終値を時系列で記述
     b) 売買代金: 東証プライムの売買代金（前場・大引け時点）を具体的数値で記述
     c) 値上がり/下落銘柄数: 東証プライムの値上がり・下落・横ばい銘柄数を記述
     d) 投資家心理: 海外勢の動向、機関投資家の売買姿勢、個人投資家の動きを記述
     e) 為替動向: ドル円レートの具体的数値（午前・午後の時刻付き）と輸出株への影響を記述
   - **出典URLは各段落の末尾に括弧書きでまとめて記載**（例: 「〜で引けた。〜が続いた（出典: URL1, URL2）」）
   - 主要な変動要因: 箇条書き3-5項目（各項目に出典URL付き）

4. **セクター動向**:
   - 上位5業種・下位5業種それぞれについて：
     a) 業種名・終値・前日比のテーブルを表示
     b) 各業種の騰落要因を1-2文で記述（材料・ニュース・市場環境を具体的に）
     c) 複数業種に共通する要因がある場合はまとめて記述（例: 「電気ガス・食料品・非鉄金属の3業種は円安進行による輸出メリットが共通要因となった」）
   - プライム/スタンダード/グロース市場の比較: 2-3文で市場間の動きの違いを分析
   - **出典URLは各業種または共通要因の記述末尾に記載**

5. **注目ニュース**:
   - 3-4件のニュースを記載（各ニュースは見出し・内容・市場への影響の3要素を含む）
   - 各ニュースは2-3文で構成（見出しだけでなく、具体的な内容と株価への影響を記述）
   - **出典URLは各ニュースの末尾に括弧書きで記載**

6. **日米経済指標予定**:
   - 上記5つのクエリから取得した情報を統合し、2つのサブセクションに分けて記載

   **6-1. 本日夜（米国市場）**:
   - テーブル構成: 時刻(JST) | 指標名 | 予想値 | 前回値 | 市場への影響 | 出典URL
   - 米雇用統計・ISM・FOMC・GDP・CPI・小売売上高等の重要指標を最優先で記載
   - 日本時間22:00〜翌朝6:00の範囲で発表される指標
   - **最重要**: 本日夜の米国指標は必ず記載。取得できない場合は「本日夜の米国経済指標発表予定は確認できませんでした」と明記

   **6-2. 翌営業日以降**:
   - テーブル構成: 日時 | 指標名 | 予想値 | 前回値 | 市場への影響 | 出典URL
   - 日銀金融政策決定会合・米雇用統計（翌週以降）・日本GDP等を記載
   - **重要**: 本日夜のセクションと合わせて、経済指標予定セクション全体を削除しない

## 出力形式（Markdown）

```markdown
# 国内株式市場サマリー - {latest_trading_day}

## 主要指数

| 指数名 | 終値 | 前日比 | 前日比(%) |
| --- | --- | --- | --- |
| 日経平均 | [終値] | [±XX円] | [±X.XX%] |

| name | close | change_pct |
| --- | --- | --- |
| TOPIX | [数値] | [±X.XX%] |
| TOPIX-Prime | [数値] | [±X.XX%] |
| TOPIX-Standard | [数値] | [±X.XX%] |
| TOPIX-Growth | [数値] | [±X.XX%] |

[各指数の特徴を2-3文で説明。市場間の関係性や本日の特徴的な動きを含む。]

## ボラティリティ

日経VI: [XX.XX]（[±X.XX%]）- [平常/やや警戒/警戒/恐怖]水準

[1文で市場心理への影響を補足]

## 全体トレンド

[前場後場の動き・売買代金の段落] 前場は日経平均が[前場引け値]円で引け、[上昇/下落]基調となった。後場は[推移の説明]し、大引けで[終値]円となった。東証プライムの売買代金は前場時点で概算[XX兆XX億円]、大引け時点で[XX兆XX億円]と[活発/低調]な取引となった[出典1,2]。

[値上がり下落銘柄数・投資家心理の段落] 東証プライムでは値上がり銘柄が[XX]銘柄、下落が[XX]銘柄、横ばいが[XX]銘柄となり、[全体的な傾向]を示した。投資家心理は[海外勢の動向]、[機関投資家の姿勢]、[個人投資家の動き]といった特徴が見られた[出典3,4]。

[為替動向の段落] ドル円レートは午前中（[時刻]頃）[XXX.XX-XX]円、午後（[時刻]頃）[XXX]円[台/半ば]と推移し、[円安/円高]傾向となった。この為替動向により、輸出セクター（自動車・電機等）は[影響の説明]を受けた[出典5,6]。

**主要な変動要因:**
- [要因1の説明][出典7]
- [要因2の説明][出典8]
- [要因3の説明][出典9]
- [要因4の説明][出典10]

## セクター動向

### 上昇上位5業種

| name | close | change_pct |
| --- | --- | --- |
| [業種名1] | [終値] | [+X.XX%] |
| [業種名2] | [終値] | [+X.XX%] |
| [業種名3] | [終値] | [+X.XX%] |
| [業種名4] | [終値] | [+X.XX%] |
| [業種名5] | [終値] | [+X.XX%] |

[各業種の騰落要因を段落形式で記述。共通要因がある場合はまとめて記述。具体的な材料・ニュース・市場環境を含む。][出典11,12]

### 下落下位5業種

| name | close | change_pct |
| --- | --- | --- |
| [業種名1] | [終値] | [-X.XX%] |
| [業種名2] | [終値] | [-X.XX%] |
| [業種名3] | [終値] | [-X.XX%] |
| [業種名4] | [終値] | [-X.XX%] |
| [業種名5] | [終値] | [-X.XX%] |

[各業種の騰落要因を段落形式で記述。共通要因がある場合はまとめて記述。具体的な材料・ニュース・市場環境を含む。][出典13,14]

[市場間比較の段落] プライム市場は[動きの説明]、スタンダード市場は[動きの説明]、グロース市場は[動きの説明]となり、[市場間の関係性や特徴的な動きの分析]。

## 注目ニュース

1. **[ニュース見出し1]**: [具体的な内容を2-3文で記述。数値やキーワードを含む。] この動きにより、[市場への影響を具体的に記述][出典15]

2. **[ニュース見出し2]**: [具体的な内容を2-3文で記述。数値やキーワードを含む。] この動きにより、[市場への影響を具体的に記述][出典16]

3. **[ニュース見出し3]**: [具体的な内容を2-3文で記述。数値やキーワードを含む。] この動きにより、[市場への影響を具体的に記述][出典17]

4. **[ニュース見出し4]**: [具体的な内容を2-3文で記述。数値やキーワードを含む。] この動きにより、[市場への影響を具体的に記述][出典18]

## 日米経済指標予定

### 本日夜（米国市場）

| 時刻(JST) | 指標名 | 予想値 | 前回値 | 市場への影響 | 出典 |
| --- | --- | --- | --- | --- | --- |
| [時刻] | [指標名] | [予想] | [前回] | [影響] | [出典19] |
| [時刻] | [指標名] | [予想] | [前回] | [影響] | [出典20] |

### 翌営業日以降

| 日時 | 指標名 | 予想値 | 前回値 | 市場への影響 | 出典 |
| --- | --- | --- | --- | --- | --- |
| [日時] | [指標名] | [予想] | [前回] | [影響] | [出典21] |
| [日時] | [指標名] | [予想] | [前回] | [影響] | [出典22] |

（総文字数: 約[XXXX]字）

---

## 出典

[出典1,4] [URL]
[出典2] [URL]
[出典3] [URL]
...
```

## 注意事項（厳守）

### データ取得・使用の原則
1. **J-Quantsデータ（TOPIX、業種別指数）**: 提供済みデータをそのまま使用。推測・補完は一切不要。
2. **日経平均**: 3つのクエリすべてを実行し、最も信頼できる数値を採用。取得できない場合は「データ取得失敗」と明記し、セクションを省略しない。
3. **日経VI**: 必ず水準判定（平常/やや警戒/警戒/恐怖）を付記。取得できない場合は「データ取得失敗」と明記。
4. **Web Search実行**: 上記7カテゴリ（日経平均・日経VI・全体トレンド・為替・セクター・ニュース・経済指標）のすべてのクエリを実行すること。一部のクエリのみ実行して終了することは厳禁。

### 出典URLの取り扱い（厳格化・誤認防止）
1. **数値データ（売買代金、銘柄数、為替レート等）**: 出典URL必須。web_search結果から得た数値のみ記載。
2. **分析・解説（セクター要因、市場環境等）**: 以下の2パターンのみ許可：
   - **パターンA（出典付き）**: web_search結果のURL記事に**実際に書かれている内容を直接引用**。記事にない解釈・推測は一切追加しない。
   - **パターンB（出典なし）**: 一般的な市場メカニズムでの説明（例: 「円安は輸出企業の採算を改善させる要因となる」）。出典URLを付けない。
   - **厳禁**: URLの記事に書いていない内容に出典URLを付けること。これは虚偽情報となる。
3. **ニュース**: web_search結果から得たニュースのみ記載。出典URLの記事内容と一致しない見出し・説明は厳禁。
4. **経済指標**: web_search結果から得た指標のみ記載。出典URLの記事に記載がない予想値・前回値は記載しない。

**重要原則**: 出典URLを付ける場合、そのURLの記事に**実際に書かれている内容のみ**を記載すること。記事を読んでいない人が誤認する表現は絶対に使用しないこと。

### 文章構成の原則
1. **段落形式の活用**: 箇条書きを多用せず、流れのある文章として構成すること。
2. **出典URLの配置**: 各段落の末尾にまとめて記載し、本文の読みやすさを優先すること。
3. **情報の統合**: 複数のweb_search結果から得た情報を統合し、MECE（漏れなく重複なく）に整理すること。
4. **共通要因の抽出**: 複数業種・複数銘柄に共通する要因がある場合は、個別記述ではなく共通要因として記述すること。

### 禁止事項
1. **「推定」「推測」「一般知識で補完」「可能性がある」**: これらの表現を使用してはいけない。
2. **[確認中]プレースホルダー**: 使用禁止。
3. **データ取得失敗時の省略**: 日経平均・日経VI・為替動向・経済指標予定（特に本日夜の米国指標）は、データ取得に失敗しても「取得できませんでした」と明記し、セクション自体を削除しないこと。
4. **クエリの部分実行**: 指定されたすべてのweb_searchクエリを実行すること。一部のみ実行して「情報が得られなかった」と結論づけることは厳禁。
5. **経済指標の時間軸ミス**: 本日夜（米国市場）の指標を最優先で記載すること。翌営業日の指標のみを記載して本日夜を省略することは厳禁。
6. **出典URLの悪用（最重要）**: 出典URLの記事に書かれていない内容に、そのURLを出典として付けることは絶対に禁止。これは虚偽情報となり、読者を誤認させる。web_search結果から得られた情報のみを記載すること。
7. **日経VIの水準判定省略**: 日経VIを記載する際は必ず水準判定（平常/やや警戒/警戒/恐怖）を付記すること。数値のみの記載は禁止。

### 品質基準
1. **文字数**: 1000字以上必須、2000字程度を目安に、最大2000字以内厳守。読了時間4-5分。
2. **情報の正確性（最重要）**:
   - 数値データ: web_search結果から得た数値のみ記載、出典URL必須
   - 分析・解説: web_search結果の記事内容 OR 一般的な市場メカニズム（出典なし）のみ
   - **出典URLを付ける場合**: そのURLの記事に実際に書かれている内容のみを記載
   - **出典URLを付けない場合**: 一般的な市場の仕組み・傾向の説明に限定
3. **読みやすさ**: 出典URLばかりが目立つ構成ではなく、文章として読みやすい構成とすること。
4. **網羅性**: 以下のすべてのセクションを含むこと（データ取得失敗時も含む）：
   - 主要指数（日経平均+TOPIX系）
   - **ボラティリティ（日経VI + 水準コメント）**
   - 全体トレンド（前場後場・売買代金・銘柄数・投資家心理・為替）
   - **セクター動向（上昇上位5業種・下落下位5業種の両方とも必須）**: 片方のみの記載は厳禁
   - 注目ニュース（3-4件）
   - 経済指標予定（**本日夜の米国指標**+翌営業日以降）
5. **経済指標の優先順位**: 本日夜（米国市場）の指標 > 翌営業日以降の指標。本日夜の指標がない場合は「確認できませんでした」と明記し、翌営業日のみを記載しない。
6. **誤認防止**: 読者が記事を確認した際に「出典URLの記事にその内容が書かれていない」という事態を絶対に起こさないこと。
"""

    return prompt


def get_prompt_metadata() -> dict[str, Any]:
    """
    プロンプトのメタデータを返す

    Returns:
        dict: バージョン情報、作成日、説明などのメタデータ
    """
    return {
        "version": "1.4",
        "created_date": "2025-12-04",
        "updated_from": "v1.3",
        "prompt_type": "market_summary",
        "changes": [
            "配信時刻を16:00→16:45に変更（J-Quants Standard更新タイミング対応）",
            "文字数上限を1500→2000に拡大（読了時間2-3分→4-5分）",
            "日経VIセクションを独立化、水準コメント（平常/やや警戒/警戒/恐怖）を必須化",
            "日経VI取得用の参照URL明記（https://www.nikkei.com/smartchart/?code=N145%2FO）",
            "J-Quants Light→Standard対応を明記",
            "出典形式を[出典N]に統一、末尾に出典セクション追加",
        ],
        "target_audience": "一般投資家・トレーダー",
        "expected_length": "1000-2000文字（読了時間4-5分）",
        "output_format": "Markdown",
        "data_sources": {
            "jquants": {
                "plan": "Standard",
                "cost": "¥3,300/月",
                "endpoints": ["/v1/indices/topix", "/v1/indices/topix/sectors"],
            },
            "web_search": {
                "primary": [
                    "日経新聞（nikkei.com）",
                    "ロイター（reuters.com）",
                    "JPX公式（jpx.co.jp）",
                ],
                "secondary": ["Bloomberg", "Investing.com"],
                "excluded": ["SNS", "匿名ブログ", "未確認情報", "推測データ"],
            },
        },
        "coverage": {
            "indices": ["日経平均", "TOPIX", "TOPIX-Prime", "TOPIX-Standard", "TOPIX-Growth"],
            "volatility": {
                "indicator": "日経VI",
                "source_url": "https://www.nikkei.com/smartchart/?code=N145%2FO",
                "levels": {
                    "15-20": "平常",
                    "20-25": "やや警戒",
                    "25超": "警戒",
                    "30超": "恐怖",
                },
            },
            "markets": ["東証プライム", "東証スタンダード", "東証グロース"],
            "sections": [
                "主要指数",
                "ボラティリティ（日経VI）",
                "全体トレンド",
                "セクター動向",
                "注目ニュース",
                "日米経済指標予定",
            ],
        },
        "update_frequency": "営業日毎（大引け後16:45配信）",
        "quality_requirements": {
            "objectivity": "事実ベースのみ、推測・感情表現禁止",
            "accuracy": "1次情報優先、クロス検証必須、Web Search使用必須",
            "precision": "数値は小数点第2位まで",
            "citation": "全データに出典URL明記、末尾に出典セクション",
            "vi_level": "日経VI記載時は水準判定必須",
        },
        "recommended_settings": {
            "model": "grok-4-fast",
            "temperature": 0.1,
            "max_tokens": 4000,
            "tools": ["web_search"],
            "tool_choice": "auto",
        },
        "api_compatibility": {
            "xai_sdk_version": "1.3.1+",
            "available_tools": ["web_search", "x_search", "code_execution"],
            "removed_features": ["browse_page"],
        },
        "spec_version": "1.2",
    }
