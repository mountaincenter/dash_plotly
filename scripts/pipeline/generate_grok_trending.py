#!/usr/bin/env python3
"""
generate_grok_trending.py
xAI Grok APIを使って「翌営業日デイスキャルピング買い注文銘柄」を選定

実行方法:
    # パイプライン実行（時刻自動判定）
    python3 scripts/pipeline/generate_grok_trending.py

    # 16時更新（古いデータをクリーンアップ）
    python3 scripts/pipeline/generate_grok_trending.py --time 16:00

    # 26時更新（16時データにマージ）
    python3 scripts/pipeline/generate_grok_trending.py --time 26:00

    # 手動実行（クリーンアップして新規作成）
    python3 scripts/pipeline/generate_grok_trending.py --cleanup

出力:
    data/parquet/grok_trending.parquet

動作仕様:
    - 16時更新: 古いデータを削除 → 10〜15銘柄を新規作成
    - 26時更新: 16時データを保持 → 10〜15銘柄を追加（合計20〜30銘柄）
    - --cleanup: 既存データを削除して新規作成（手動実行用）

備考:
    - .env.xai に XAI_API_KEY が必要
    - all_stocks.parquet と統合できるスキーマ
    - categories: ["GROK"]
    - tags: Grokが返したcategoryをそのまま格納
    - selected_time: "16:00" or "26:00" で更新タイミングを区別
"""

from __future__ import annotations

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from openai import OpenAI
from dotenv import dotenv_values
from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_fetcher import JQuantsFetcher

GROK_TRENDING_PATH = PARQUET_DIR / "grok_trending.parquet"
ENV_XAI_PATH = ROOT / ".env.xai"


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="Generate Grok trending stocks")
    parser.add_argument(
        "--time",
        choices=["16:00", "26:00"],
        default=None,
        help="Update time (16:00 or 26:00). If not specified, defaults to current time-based logic."
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Force cleanup of existing data before generating new stocks (for manual execution)"
    )
    return parser.parse_args()


def get_trading_context() -> dict[str, str]:
    """
    営業日カレンダーを参照して取引コンテキストを生成

    営業日の夜にcron実行される想定：
    - 金曜夜 → 翌営業日は月曜
    - 月曜夜 → 翌営業日は火曜
    - 祝日前は翌営業日まで飛ぶ

    Returns:
        dict: 実行日、最新営業日、翌営業日などの情報
    """
    fetcher = JQuantsFetcher()

    # 最新営業日を取得（J-Quants営業日カレンダーから）
    latest_trading_day_str = fetcher.get_latest_trading_day()
    latest_trading_day = datetime.strptime(latest_trading_day_str, "%Y-%m-%d").date()

    # 営業日カレンダーから今後の営業日を取得
    # 翌営業日を取得するため、今日から+10日の範囲で営業日を取得
    today = datetime.now().date()
    future_end = today + timedelta(days=10)

    params = {
        "from": str(today),
        "to": str(future_end)
    }

    response = fetcher.client.request("/markets/trading_calendar", params=params)
    calendar = pd.DataFrame(response["trading_calendar"])

    # 営業日のみフィルタ（HolidayDivision == "1"）
    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
    trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
    trading_days = trading_days.sort_values("Date")

    # 最新営業日より後の営業日 = 翌営業日
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


def build_grok_prompt(context: dict[str, str]) -> str:
    """
    動的にGrokプロンプトを生成

    Args:
        context: get_trading_context() で取得したコンテキスト

    Returns:
        str: Grok APIに送信するプロンプト
    """
    return f"""【タスク】
本日は{context['execution_date']}です。
最新営業日は{context['latest_trading_day']}、翌営業日は{context['next_trading_day']}です。

**{context['next_trading_day']}の寄付〜前場でデイスキャルピング買い注文する銘柄**を10〜15銘柄選定し、JSON形式で出力してください。

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

【出力形式（厳守）】
以下のJSON配列形式で出力してください（他の形式は不可）：

```json
[
  {{
    "ticker_symbol": "3031",
    "company_name": "ラクーンHD",
    "reason": "{context['latest_trading_day']}引け後にEC新サービスのIR発表。その後の株クラで「{context['next_trading_day']}寄付買い」の投稿が急増（100件以上）。{context['latest_trading_day']}の出来高は平均の4.2倍、直近5日ATR 5.8%で値動き活発。小型株（時価総額350億円）で個人投資家主導の急騰期待",
    "category": "IR好材料+株クラバズ",
    "sentiment_score": 0.85,
    "policy_link": "High",
    "has_mention": true,
    "mentioned_by": "@example_investor, @stock_guru"
  }},
  {{
    "ticker_symbol": "4563",
    "company_name": "アンジェス",
    "reason": "バイオベンチャー。{context['latest_trading_day']}以降に治験進展のニュースが報道され、Xで「{context['next_trading_day']}ストップ高狙い」の言及急増。出来高3.5倍、ATR 6.2%。時価総額200億円の典型的な仕手株",
    "category": "バイオ材料+仕手株",
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
            {"role": "system", "content": "あなたは日本株市場のデイトレード専門家です。銘柄選定の際は具体的な数値と根拠を示してください。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=4000,
    )

    content = response.choices[0].message.content
    print(f"[OK] Received response from Grok ({len(content)} chars)")
    return content


def parse_grok_response(response: str) -> list[dict[str, Any]]:
    """Parse Grok's JSON response"""
    print("[INFO] Parsing Grok response...")

    # JSONブロックを抽出（```json ... ``` や ``` ... ``` で囲まれている場合）
    if "```json" in response:
        json_str = response.split("```json")[1].split("```")[0].strip()
    elif "```" in response:
        json_str = response.split("```")[1].split("```")[0].strip()
    else:
        json_str = response.strip()

    # JSON配列のパース
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


def calculate_selection_score(item: dict[str, Any]) -> float:
    """
    選定時点でのスコアを計算

    Args:
        item: Grok APIレスポンスの1銘柄分のデータ

    Returns:
        float: 選定スコア (0-200点)
    """
    # sentiment_score があれば使用、なければ0.5をデフォルト
    sentiment_score = item.get("sentiment_score", 0.5)
    score = sentiment_score * 100  # ベーススコア (0-100)

    # policy_link があれば加点
    policy_link = item.get("policy_link", "Low")
    policy_bonus = {"High": 30, "Med": 20, "Low": 10}
    score += policy_bonus.get(policy_link, 10)

    # has_mention (プレミアムユーザー言及) があれば加点
    has_mention = item.get("has_mention", False)
    if has_mention:
        score += 50

    return score


def convert_to_all_stocks_schema(grok_data: list[dict], selected_date: str, selected_time: str) -> pd.DataFrame:
    """
    Convert Grok data to all_stocks.parquet compatible schema

    all_stocks.parquet schema:
        ticker, code, stock_name, market, sectors, series, topixnewindexseries,
        categories, tags, date, Close, change_pct, Volume, vol_ratio,
        atr14_pct, rsi14, score, key_signal

    + Grok拡張スキーマ:
        reason, source, selected_time, updated_at, sentiment_score,
        policy_link, has_mention, mentioned_by, selection_score
    """
    print("[INFO] Converting to all_stocks.parquet compatible schema...")

    rows = []
    for idx, item in enumerate(grok_data, 1):
        ticker_symbol = item.get("ticker_symbol", "")
        company_name = item.get("company_name", "")
        reason = item.get("reason", "")
        category = item.get("category", "")
        sentiment_score = item.get("sentiment_score", 0.5)
        policy_link = item.get("policy_link", "Low")
        has_mention = item.get("has_mention", False)
        mentioned_by = item.get("mentioned_by", "")

        # tickerは "1234.T" 形式、codeは "1234" 形式
        ticker = f"{ticker_symbol}.T" if not ticker_symbol.endswith(".T") else ticker_symbol
        code = ticker_symbol.replace(".T", "")

        # 選定スコアを計算
        selection_score = calculate_selection_score(item)

        row = {
            "ticker": ticker,
            "code": code,
            "stock_name": company_name,
            "market": None,  # Grokからは取得できない
            "sectors": None,
            "series": None,
            "topixnewindexseries": None,
            "categories": ["GROK"],  # 固定値（配列形式）
            "tags": category,  # Grokのcategoryをtagsに格納
            "reason": reason,  # 新規カラム: Grokの選定理由
            "date": selected_date,
            "Close": None,
            "change_pct": None,
            "Volume": None,
            "vol_ratio": None,
            "atr14_pct": None,
            "rsi14": None,
            "score": None,
            "key_signal": None,
            "source": "grok",  # 新規カラム: データソース
            "selected_time": selected_time,  # 新規カラム: 選定時刻（16:00 or 26:00）
            "updated_at": datetime.now().isoformat(),  # 新規カラム: 更新日時
            "sentiment_score": sentiment_score,  # センチメントスコア
            "policy_link": policy_link,  # 政策リンク強度
            "has_mention": has_mention,  # プレミアムユーザー言及フラグ
            "mentioned_by": mentioned_by,  # 言及者名
            "selection_score": selection_score,  # 選定時点スコア
            "grok_rank": idx,  # Grokの選定順位
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # selection_scoreで降順ソート（Top5を上位に）
    df = df.sort_values("selection_score", ascending=False).reset_index(drop=True)

    # ランクを再計算（スコア順）
    df["selection_rank"] = range(1, len(df) + 1)

    print(f"[OK] Converted {len(df)} stocks to DataFrame")
    print(f"[INFO] Top 5 by selection_score:")
    for i, row in df.head(5).iterrows():
        print(f"  {row['selection_rank']}. {row['ticker']} - {row['stock_name']} (Score: {row['selection_score']:.1f})")

    return df


def get_jst_date() -> str:
    """
    JST基準の日付を取得（YYYY-MM-DD形式）
    """
    from datetime import timezone, timedelta
    jst = timezone(timedelta(hours=9))
    now_jst = datetime.now(jst)
    return now_jst.strftime("%Y-%m-%d")


def download_manifest_from_s3() -> dict:
    """
    S3から manifest.json をダウンロード

    Returns:
        manifest.json の内容（辞書）
        ダウンロード失敗時は空の辞書
    """
    import os
    import boto3
    import json
    from botocore.exceptions import ClientError

    try:
        bucket = os.getenv('S3_BUCKET', 'stock-api-data')
        key = 'parquet/manifest.json'

        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        manifest_data = response['Body'].read().decode('utf-8')
        manifest = json.loads(manifest_data)

        print(f"[OK] Downloaded manifest.json from S3: s3://{bucket}/{key}")
        return manifest

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"[WARN] manifest.json not found in S3")
        else:
            print(f"[WARN] Failed to download manifest.json from S3: {e}")
        return {}
    except Exception as e:
        print(f"[WARN] Failed to parse manifest.json: {e}")
        return {}


def save_grok_trending(df: pd.DataFrame, selected_time: str, should_merge: bool = False) -> None:
    """
    Save to grok_trending.parquet

    Args:
        df: 新規データ
        selected_time: "16:00" or "26:00"
        should_merge: True なら既存データとマージ（26時更新用）、False なら新規作成
    """
    import os
    import boto3
    from botocore.exceptions import ClientError

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # 26時更新時: S3から既存ファイルをダウンロードしてマージ
    if should_merge:
        # ローカルにファイルがない場合、S3からダウンロード
        if not GROK_TRENDING_PATH.exists():
            print("[INFO] Local file not found, attempting to download from S3...")
            try:
                s3_client = boto3.client('s3')
                bucket = os.getenv('S3_BUCKET', 'stock-api-data')
                key = 'parquet/grok_trending.parquet'

                # S3からダウンロード
                s3_client.download_file(bucket, key, str(GROK_TRENDING_PATH))
                print(f"[OK] Downloaded from S3: s3://{bucket}/{key}")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"[WARN] File not found in S3: s3://{bucket}/{key}")
                    print(f"[WARN] Will create new file instead of merging")
                else:
                    print(f"[WARN] Failed to download from S3: {e}")
                    print(f"[WARN] Will create new file instead of merging")
            except Exception as e:
                print(f"[WARN] Failed to download from S3: {e}")
                print(f"[WARN] Will create new file instead of merging")

    if should_merge and GROK_TRENDING_PATH.exists():
        # 既存データとマージ（26時更新時）
        try:
            existing_df = pd.read_parquet(GROK_TRENDING_PATH)
            print(f"[INFO] Loaded existing data: {len(existing_df)} stocks")

            # 同じselected_timeのデータを削除（上書き）
            if "selected_time" in existing_df.columns:
                existing_df = existing_df[existing_df["selected_time"] != selected_time]
                print(f"[INFO] Removed old {selected_time} data, remaining: {len(existing_df)} stocks")

            # 新データと結合
            df_merged = pd.concat([existing_df, df], ignore_index=True)
            print(f"[INFO] Merged with new data: {len(df_merged)} stocks")

            # 保存
            df_merged.to_parquet(GROK_TRENDING_PATH, index=False)
            print(f"[OK] Saved: {GROK_TRENDING_PATH}")
            print(f"     Total stocks: {len(df_merged)}")
            print(f"     Breakdown by selected_time:")
            for time, count in df_merged["selected_time"].value_counts().items():
                print(f"       {time}: {count} stocks")

        except Exception as e:
            print(f"[WARN] Failed to merge with existing data: {e}, creating new file")
            df.to_parquet(GROK_TRENDING_PATH, index=False)
            print(f"[OK] Saved: {GROK_TRENDING_PATH}")
            print(f"     Total stocks: {len(df)}")
            print(f"     Selected time: {selected_time}")
    else:
        # 新規作成（16時更新時またはクリーンアップ指定時）
        df.to_parquet(GROK_TRENDING_PATH, index=False)
        print(f"[OK] Saved: {GROK_TRENDING_PATH}")
        print(f"     Total stocks: {len(df)}")
        print(f"     Selected time: {selected_time}")


def main() -> int:
    """メイン処理（パイプライン統合版）"""
    print("=" * 60)
    print("Generate Grok Trending Stocks (xAI API)")
    print("=" * 60)

    # コマンドライン引数をパース
    args = parse_args()

    # 手動実行判定（workflow_dispatch）
    import os
    event_name = os.getenv('GITHUB_EVENT_NAME', '')
    is_manual_run = event_name == 'workflow_dispatch'

    # selected_time を決定
    if is_manual_run:
        # 手動実行時は常に16:00扱い
        selected_time = "16:00"
        print("[INFO] Manual execution detected (workflow_dispatch)")
    elif args.time:
        selected_time = args.time
    else:
        # パイプライン実行時は引数なしで実行される想定
        # 現在時刻から推測
        # - 07:00 UTC (JST 16:00) → "16:00"
        # - 17:00 UTC (JST 26:00) → "26:00" (遅延を考慮して16時以降は26:00扱い)
        current_hour = datetime.now().hour
        selected_time = "16:00" if current_hour < 16 else "26:00"

    print(f"[INFO] Update time: {selected_time}")

    # クリーンアップ判定
    should_cleanup = False
    should_merge = False

    if selected_time == "16:00" or is_manual_run:
        # 16時実行 または 手動実行 → 常にクリーンアップ
        should_cleanup = True
        should_merge = False
        print("[INFO] Mode: CLEANUP (16:00 run or manual execution)")
    elif selected_time == "26:00":
        # 26時実行 → manifest.json を確認
        print("[INFO] 26:00 run detected, checking manifest.json...")
        manifest = download_manifest_from_s3()

        # 今日の日付（JST）
        today_jst = get_jst_date()
        print(f"[INFO] Today (JST): {today_jst}")

        # フラグと日付を確認
        grok_flag = manifest.get("grok_update_flag", False)
        grok_date = manifest.get("grok_last_update_date", "")
        grok_time = manifest.get("grok_last_update_time", "")

        print(f"[INFO] Manifest: flag={grok_flag}, date={grok_date}, time={grok_time}")

        if grok_flag and grok_date == today_jst:
            # 16時成功済み → 追記処理
            should_cleanup = False
            should_merge = True
            print("[INFO] Mode: MERGE (16:00 run succeeded today, appending 26:00 data)")
        else:
            # 16時失敗 または 日付が古い → 16時処理をやり直し
            should_cleanup = True
            should_merge = False
            print("[INFO] Mode: CLEANUP (16:00 run failed or outdated, retrying as 16:00)")
    else:
        # 不明な時刻 → デフォルトはクリーンアップ
        should_cleanup = True
        should_merge = False

    # クリーンアップ実行
    if should_cleanup:
        if GROK_TRENDING_PATH.exists():
            GROK_TRENDING_PATH.unlink()
            print("[INFO] Removed old grok_trending.parquet")

    print()

    try:
        # 1. Get trading context (営業日カレンダーから)
        print("[INFO] Fetching trading calendar from J-Quants...")
        context = get_trading_context()
        print(f"[OK] Execution date: {context['execution_date']}")
        print(f"[OK] Latest trading day: {context['latest_trading_day']}")
        print(f"[OK] Next trading day: {context['next_trading_day']}")
        print()

        # 2. Build dynamic prompt
        prompt = build_grok_prompt(context)
        print(f"[INFO] Built dynamic prompt ({len(prompt)} chars)")
        print()

        # 3. Load API key
        api_key = load_xai_api_key()
        print(f"[OK] Loaded XAI_API_KEY from {ENV_XAI_PATH}")
        print()

        # 4. Query Grok
        response = query_grok(api_key, prompt)
        print()

        # 5. Parse response
        grok_data = parse_grok_response(response)
        print()

        # 6. Convert to DataFrame
        selected_date = context['next_trading_day_raw']
        df = convert_to_all_stocks_schema(grok_data, selected_date, selected_time)
        print()

        # 7. Preview
        print("[INFO] Preview (first 5 stocks):")
        print("-" * 80)
        for i, row in df.head(5).iterrows():
            print(f"{i+1}. {row['ticker']} - {row['stock_name']}")
            print(f"   Category: {row['tags']}")
            print(f"   Reason: {row['reason'][:80]}..." if len(row['reason']) > 80 else f"   Reason: {row['reason']}")
            print()

        # 8. Save
        save_grok_trending(df, selected_time, should_merge=should_merge)

        print("\n" + "=" * 60)
        print(f"[OK] Generated {len(df)} Grok trending stocks")
        print(f"[OK] Saved: {GROK_TRENDING_PATH}")
        print("=" * 60)

        return 0

    except FileNotFoundError as e:
        # .env.xai が存在しない場合は空のファイルを作成して終了
        if ".env.xai" in str(e):
            print(f"\n[WARN] {e}")
            print("[WARN] Creating empty grok_trending.parquet (XAI API key not configured)")

            # 空のDataFrameを作成
            empty_df = pd.DataFrame(columns=[
                "ticker", "code", "stock_name", "market", "sectors", "series",
                "topixnewindexseries", "categories", "tags", "reason",
                "date", "Close", "change_pct", "Volume", "vol_ratio",
                "atr14_pct", "rsi14", "score", "key_signal",
                "source", "selected_time", "updated_at"
            ])

            PARQUET_DIR.mkdir(parents=True, exist_ok=True)
            empty_df.to_parquet(GROK_TRENDING_PATH, index=False)
            print(f"[OK] Saved empty: {GROK_TRENDING_PATH}")

            return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

        # エラー時も空のファイルを作成（パイプライン継続のため）
        print("[WARN] Creating empty grok_trending.parquet due to error")
        empty_df = pd.DataFrame(columns=[
            "ticker", "code", "stock_name", "market", "sectors", "series",
            "topixnewindexseries", "categories", "tags", "reason",
            "date", "Close", "change_pct", "Volume", "vol_ratio",
            "atr14_pct", "rsi14", "score", "key_signal",
            "source", "selected_time", "updated_at"
        ])

        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        empty_df.to_parquet(GROK_TRENDING_PATH, index=False)
        print(f"[OK] Saved empty: {GROK_TRENDING_PATH}")

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
