#!/usr/bin/env python3
"""
backtest_grok_prompt.py
Grokプロンプトの自動バックテストスクリプト

機能:
    - 過去N営業日分のプロンプトを自動生成
    - Grok APIに自動投稿
    - 各日の実際の株価で検証
    - 集計レポート出力

使い方:
    # 過去10営業日でバックテスト
    python3 scripts/backtest_grok_prompt.py --days 10

    # プロンプトファイルを指定
    python3 scripts/backtest_grok_prompt.py --days 10 --prompt data/parquet/grok_prompt_final.txt

出力:
    - data/parquet/backtest_results/YYYYMMDD_HHMMSS/
      - prompt_YYYYMMDD.txt (各日のプロンプト)
      - result_YYYYMMDD.json (Grokの選定結果)
      - validation_YYYYMMDD.csv (検証結果)
      - summary.csv (全期間の集計)
      - report.txt (レポート)
"""

from __future__ import annotations

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from openai import OpenAI
from dotenv import dotenv_values
from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_fetcher import JQuantsFetcher

# 検証スクリプトの関数をインポート
from scripts.validate_grok_result import (
    validate_grok_stocks,
    EXCLUDED_CODES
)

ENV_XAI_PATH = ROOT / ".env.xai"


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="Grok prompt backtest")
    parser.add_argument(
        "--days",
        type=int,
        default=10,
        help="Number of trading days to backtest (default: 10)"
    )
    parser.add_argument(
        "--prompt",
        type=str,
        default=str(PARQUET_DIR / "grok_prompt_final.txt"),
        help="Path to prompt template file"
    )
    return parser.parse_args()


def get_past_trading_days(n_days: int) -> list[dict[str, str]]:
    """
    過去N営業日の日付リストを取得

    Returns:
        [
            {
                "base_date": "2025-10-22",      # 基準日
                "latest_trading_day": "2025-10-21",  # 最新営業日
                "next_trading_day": "2025-10-23"     # 翌営業日
            },
            ...
        ]
    """
    fetcher = JQuantsFetcher()

    # 営業日カレンダーを取得（過去60日分）
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)

    params = {
        "from": str(start_date),
        "to": str(end_date)
    }

    response = fetcher.client.request("/markets/trading_calendar", params=params)
    calendar = pd.DataFrame(response["trading_calendar"])

    # 営業日のみフィルタ
    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
    trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
    trading_days = trading_days.sort_values("Date", ascending=False)

    # 過去N営業日を取得（最新は除く）
    trading_days_list = trading_days["Date"].tolist()

    if len(trading_days_list) < n_days + 2:
        raise ValueError(f"Not enough trading days. Found {len(trading_days_list)}, need {n_days + 2}")

    result = []

    # 最新営業日を除いて、過去N営業日分を取得
    # 例: 今日が10/25(金)の場合
    # - 10/24(木) を基準日として、10/25(金)向けの銘柄を選定
    # - 10/23(水) を基準日として、10/24(木)向けの銘柄を選定
    for i in range(1, n_days + 1):
        if i >= len(trading_days_list) - 1:
            break

        next_trading_day = trading_days_list[i - 1]  # 翌営業日（予測対象日）
        base_date = trading_days_list[i]              # 基準日（プロンプト実行日想定）
        latest_trading_day = trading_days_list[i + 1] # 最新営業日（基準日の前営業日）

        result.append({
            "base_date": str(base_date),
            "latest_trading_day": str(latest_trading_day),
            "next_trading_day": str(next_trading_day)
        })

    print(f"[OK] Found {len(result)} trading days for backtest")
    return result


def build_prompt_for_date(template_path: Path, date_info: dict[str, str]) -> str:
    """
    日付情報を元にプロンプトを生成

    Args:
        template_path: プロンプトテンプレートファイルのパス
        date_info: 日付情報（base_date, latest_trading_day, next_trading_day）

    Returns:
        日付が埋め込まれたプロンプト
    """
    with open(template_path, 'r', encoding='utf-8') as f:
        template = f.read()

    # 日付を日本語形式に変換
    base_date_dt = datetime.strptime(date_info["base_date"], "%Y-%m-%d")
    latest_dt = datetime.strptime(date_info["latest_trading_day"], "%Y-%m-%d")
    next_dt = datetime.strptime(date_info["next_trading_day"], "%Y-%m-%d")

    base_date_ja = base_date_dt.strftime("%Y年%m月%d日")
    latest_ja = latest_dt.strftime("%Y年%m月%d日")
    next_ja = next_dt.strftime("%Y年%m月%d日")

    # テンプレート内の日付を置換
    # 既存のプロンプトは "2025年10月25日" のような形式を想定
    prompt = template

    # 簡易的な置換（より高度な場合はテンプレートエンジン使用）
    # 固定の日付文字列を検索して置換
    prompt = prompt.replace("本日は2025年10月25日", f"本日は{base_date_ja}")
    prompt = prompt.replace("最新営業日は2025年10月24日", f"最新営業日は{latest_ja}")
    prompt = prompt.replace("翌営業日は2025年10月27日", f"翌営業日は{next_ja}")

    # ISO形式の日付も置換（プロンプトに含まれている場合）
    prompt = prompt.replace("2025-10-24", date_info["latest_trading_day"])
    prompt = prompt.replace("2025-10-27", date_info["next_trading_day"])

    return prompt


def query_grok(api_key: str, prompt: str) -> str:
    """Grok APIを呼び出し"""
    print("  [INFO] Querying Grok API...")

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
    )

    response = client.chat.completions.create(
        model="grok-3",
        messages=[
            {
                "role": "system",
                "content": "あなたは日本株市場のデイトレード専門家です。銘柄選定の際は具体的な数値と根拠を示してください。"
            },
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=4000,
    )

    content = response.choices[0].message.content
    print(f"  [OK] Received response from Grok ({len(content)} chars)")
    return content


def parse_grok_response(response: str) -> list[dict]:
    """Grokレスポンスをパース"""
    # JSONブロックを抽出
    if "```json" in response:
        json_str = response.split("```json")[1].split("```")[0].strip()
    elif "```" in response:
        json_str = response.split("```")[1].split("```")[0].strip()
    else:
        json_str = response.strip()

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"  [ERROR] Failed to parse JSON: {e}")
        print(f"  [DEBUG] Response:\n{response[:500]}...")
        raise

    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array, got {type(data)}")

    return data


def run_backtest(n_days: int, prompt_template_path: Path, output_dir: Path, api_key: str):
    """
    バックテストを実行

    Args:
        n_days: バックテスト期間（営業日数）
        prompt_template_path: プロンプトテンプレートファイル
        output_dir: 出力ディレクトリ
        api_key: XAI API Key
    """
    # 1. 過去N営業日の日付リストを取得
    print("\n" + "=" * 80)
    print("過去の営業日を取得中...")
    print("=" * 80)
    trading_days = get_past_trading_days(n_days)

    # 2. 各日付でバックテスト
    results = []

    for i, date_info in enumerate(trading_days, 1):
        print("\n" + "=" * 80)
        print(f"[{i}/{len(trading_days)}] {date_info['base_date']} → {date_info['next_trading_day']}")
        print("=" * 80)

        # 2-1. プロンプト生成
        print("  [INFO] Generating prompt...")
        prompt = build_prompt_for_date(prompt_template_path, date_info)

        prompt_file = output_dir / f"prompt_{date_info['base_date']}.txt"
        prompt_file.write_text(prompt, encoding="utf-8")
        print(f"  [OK] Saved prompt: {prompt_file}")

        # 2-2. Grok API呼び出し
        try:
            response = query_grok(api_key, prompt)
            grok_data = parse_grok_response(response)

            # 結果を保存
            result_file = output_dir / f"result_{date_info['base_date']}.json"
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(grok_data, f, ensure_ascii=False, indent=2)
            print(f"  [OK] Saved result: {result_file}")

        except Exception as e:
            print(f"  [ERROR] Grok API call failed: {e}")
            continue

        # 2-3. 検証
        print("  [INFO] Validating results...")
        try:
            df = validate_grok_stocks(grok_data, date_info["next_trading_day"])

            # 検証結果を保存
            validation_file = output_dir / f"validation_{date_info['base_date']}.csv"
            df.to_csv(validation_file, index=False, encoding="utf-8-sig")
            print(f"  [OK] Saved validation: {validation_file}")

            # サマリーを集計
            summary = {
                "base_date": date_info["base_date"],
                "next_trading_day": date_info["next_trading_day"],
                "total_stocks": len(df),
                "market_cap_ok_count": df["market_cap_in_range"].sum(),
                "market_cap_ok_rate": df["market_cap_in_range"].mean() * 100,
                "excluded_count": df["is_excluded"].sum(),
                "mentioned_count": (df["mentioned_by"] != "").sum(),
                "mentioned_rate": (df["mentioned_by"] != "").mean() * 100,
                "avg_change_pct": df["next_day_change_pct"].mean() if "next_day_change_pct" in df.columns else None,
                "win_rate": ((df["next_day_change_pct"] > 0).sum() / df["next_day_change_pct"].notna().sum() * 100) if "next_day_change_pct" in df.columns else None,
                "volatile_count": (df["next_day_range_pct"] >= 2.0).sum() if "next_day_range_pct" in df.columns else None,
                "volatile_rate": ((df["next_day_range_pct"] >= 2.0).sum() / df["next_day_range_pct"].notna().sum() * 100) if "next_day_range_pct" in df.columns else None,
            }

            results.append(summary)

        except Exception as e:
            print(f"  [ERROR] Validation failed: {e}")
            import traceback
            traceback.print_exc()
            continue

    # 3. 全期間の集計レポート
    print("\n" + "=" * 80)
    print("集計レポート生成中...")
    print("=" * 80)

    if not results:
        print("[ERROR] No valid results to summarize")
        return

    summary_df = pd.DataFrame(results)

    # CSVで保存
    summary_file = output_dir / "summary.csv"
    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved summary: {summary_file}")

    # テキストレポート生成
    report = generate_report(summary_df)
    report_file = output_dir / "report.txt"
    report_file.write_text(report, encoding="utf-8")
    print(f"[OK] Saved report: {report_file}")

    # コンソールに表示
    print("\n" + report)


def generate_report(summary_df: pd.DataFrame) -> str:
    """集計レポートを生成"""
    report = "=" * 80 + "\n"
    report += "Grokプロンプト バックテスト結果\n"
    report += "=" * 80 + "\n\n"

    report += f"対象期間: {summary_df['base_date'].min()} 〜 {summary_df['base_date'].max()}\n"
    report += f"バックテスト日数: {len(summary_df)}営業日\n"
    report += f"総選定銘柄数: {summary_df['total_stocks'].sum()}銘柄\n\n"

    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += "【時価総額適合率】\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"平均適合率: {summary_df['market_cap_ok_rate'].mean():.1f}%\n"
    report += f"最高: {summary_df['market_cap_ok_rate'].max():.1f}% ({summary_df.loc[summary_df['market_cap_ok_rate'].idxmax(), 'base_date']})\n"
    report += f"最低: {summary_df['market_cap_ok_rate'].min():.1f}% ({summary_df.loc[summary_df['market_cap_ok_rate'].idxmin(), 'base_date']})\n\n"

    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += "【除外銘柄混入】\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"総除外銘柄数: {summary_df['excluded_count'].sum()}銘柄\n"
    report += f"除外銘柄が含まれた日数: {(summary_df['excluded_count'] > 0).sum()}日 / {len(summary_df)}日\n\n"

    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += "【プレミアムユーザー言及率】\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"平均言及率: {summary_df['mentioned_rate'].mean():.1f}%\n"
    report += f"総言及銘柄数: {summary_df['mentioned_count'].sum()}銘柄 / {summary_df['total_stocks'].sum()}銘柄\n\n"

    if summary_df["avg_change_pct"].notna().any():
        report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        report += "【翌日パフォーマンス】\n"
        report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        report += f"平均変化率: {summary_df['avg_change_pct'].mean():.2f}%\n"
        report += f"平均勝率: {summary_df['win_rate'].mean():.1f}%\n"
        report += f"値幅2%以上の平均的中率: {summary_df['volatile_rate'].mean():.1f}%\n\n"

        report += "【日別パフォーマンス】\n"
        for _, row in summary_df.iterrows():
            report += f"{row['base_date']} → {row['next_trading_day']}: "
            report += f"変化率{row['avg_change_pct']:+.2f}%, 勝率{row['win_rate']:.1f}%, "
            report += f"ボラ的中率{row['volatile_rate']:.1f}%\n"

    report += "\n" + "=" * 80 + "\n"

    return report


def main():
    """メイン処理"""
    args = parse_args()

    print("=" * 80)
    print("Grokプロンプト 自動バックテスト")
    print("=" * 80)
    print(f"バックテスト期間: 過去{args.days}営業日")
    print(f"プロンプトテンプレート: {args.prompt}")
    print()

    # 出力ディレクトリ作成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = PARQUET_DIR / "backtest_results" / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"出力ディレクトリ: {output_dir}")
    print()

    # プロンプトテンプレート確認
    prompt_template_path = Path(args.prompt)
    if not prompt_template_path.exists():
        print(f"[ERROR] Prompt template not found: {prompt_template_path}")
        return 1

    # API Key読み込み
    if not ENV_XAI_PATH.exists():
        print(f"[ERROR] .env.xai not found: {ENV_XAI_PATH}")
        print("Please create .env.xai with XAI_API_KEY=your_api_key")
        return 1

    config = dotenv_values(ENV_XAI_PATH)
    api_key = config.get("XAI_API_KEY")

    if not api_key:
        print("[ERROR] XAI_API_KEY not found in .env.xai")
        return 1

    print("[OK] XAI_API_KEY loaded")
    print()

    try:
        # バックテスト実行
        run_backtest(args.days, prompt_template_path, output_dir, api_key)

        print("\n" + "=" * 80)
        print("バックテスト完了")
        print("=" * 80)
        print(f"結果: {output_dir}")

        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
