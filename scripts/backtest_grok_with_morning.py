#!/usr/bin/env python3
"""
backtest_grok_with_morning.py
前場パフォーマンス分析付きGrokバックテスト

機能:
    - 過去N営業日分のプロンプトを自動生成
    - Grok APIに自動投稿
    - デイリーパフォーマンス（始値→終値）
    - 前場パフォーマンス（9:00→11:30）を両方計算
    - プレミアムユーザー効果の分析
    - 政策連動度別の分析
    - HTMLレポート生成

使い方:
    python3 scripts/backtest_grok_with_morning.py --days 5
"""

from __future__ import annotations

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta, time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yfinance as yf
from openai import OpenAI
from dotenv import dotenv_values
from common_cfg.paths import PARQUET_DIR
from scripts.lib.jquants_fetcher import JQuantsFetcher

ENV_XAI_PATH = ROOT / ".env.xai"


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="Grok prompt backtest with morning session")
    parser.add_argument(
        "--days",
        type=int,
        default=5,
        help="Number of trading days to backtest (default: 5)"
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
                "base_date": "2025-10-23",           # プロンプト実行日（大引け後）
                "latest_trading_day": "2025-10-22",  # その時点での最新営業日
                "next_trading_day": "2025-10-24"     # 予想対象日
            },
            ...
        ]
    """
    fetcher = JQuantsFetcher()

    # 営業日カレンダーを取得
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)

    params = {
        "from": str(start_date),
        "to": str(end_date)
    }

    response = fetcher.client.request("/markets/trading_calendar", params=params)
    calendar = pd.DataFrame(response["trading_calendar"])

    # 営業日のみフィルタ（HolidayDivision == "1" が営業日）
    trading_days = calendar[calendar["HolidayDivision"] == "1"].copy()
    trading_days["Date"] = pd.to_datetime(trading_days["Date"]).dt.date
    trading_dates = sorted(trading_days["Date"].tolist(), reverse=True)

    result = []
    for i in range(n_days):
        if i + 1 >= len(trading_dates):
            break

        next_trading_day = trading_dates[i]
        latest_trading_day = trading_dates[i + 1]
        base_date = next_trading_day  # プロンプトは前日大引け後を想定

        result.append({
            "base_date": str(base_date),
            "latest_trading_day": str(latest_trading_day),
            "next_trading_day": str(next_trading_day)
        })

    return result[::-1]  # 古い順に並べ替え


def build_prompt_for_date(template_path: Path, date_info: dict[str, str]) -> str:
    """
    日付を埋め込んだプロンプトを生成
    """
    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()

    # テンプレート内の日付プレースホルダーを置換
    # 例: {{BASE_DATE}} → 2025-10-23
    prompt = template.replace("{{BASE_DATE}}", date_info["base_date"])
    prompt = prompt.replace("{{LATEST_TRADING_DAY}}", date_info["latest_trading_day"])
    prompt = prompt.replace("{{NEXT_TRADING_DAY}}", date_info["next_trading_day"])

    # または、シンプルに固定文字列を置換
    # "本日は2025年10月25日" → "本日は2025年10月23日"
    base_dt = datetime.strptime(date_info["base_date"], "%Y-%m-%d")

    prompt = prompt.replace("本日は2025年10月25日", f"本日は{base_dt.year}年{base_dt.month}月{base_dt.day}日")
    prompt = prompt.replace("2025年10月24日", date_info["latest_trading_day"].replace("-", "年", 1).replace("-", "月") + "日")
    prompt = prompt.replace("2025年10月27日", date_info["next_trading_day"].replace("-", "年", 1).replace("-", "月") + "日")

    return prompt


def call_grok_api(prompt: str, api_key: str) -> dict[str, Any]:
    """
    Grok APIを呼び出して銘柄選定結果を取得
    """
    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1"
    )

    try:
        completion = client.chat.completions.create(
            model="grok-4-fast-reasoning",
            messages=[
                {"role": "system", "content": "あなたは優秀な株式アナリストです。指示に従ってJSON形式で銘柄を選定してください。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=4000
        )

        response_text = completion.choices[0].message.content

        # JSONを抽出（```json ... ``` で囲まれている場合）
        if "```json" in response_text:
            json_start = response_text.find("```json") + 7
            json_end = response_text.find("```", json_start)
            response_text = response_text[json_start:json_end].strip()

        # JSON配列をパース
        result = json.loads(response_text)

        return {"success": True, "data": result}

    except json.JSONDecodeError as e:
        return {"success": False, "error": f"JSON parse error: {e}", "raw": response_text}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_stock_performance(ticker: str, target_date: str) -> dict[str, Any]:
    """
    指定日の株価パフォーマンスを取得（デイリー + 前場）

    Returns:
        {
            'daily_open': float,
            'daily_close': float,
            'daily_change_pct': float,
            'morning_open': float,  # 9:00始値
            'morning_close': float,  # 11:30終値
            'morning_change_pct': float,
            'should_take_profit': bool  # 前場で利確すべきだったか
        }
    """
    ticker_symbol = f"{ticker}.T"

    try:
        # 日次データ取得
        stock = yf.Ticker(ticker_symbol)
        next_date = (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        hist = stock.history(start=target_date, end=next_date)

        if hist.empty:
            return None

        daily_data = hist.iloc[0]
        daily_open = daily_data['Open']
        daily_close = daily_data['Close']
        daily_change_pct = ((daily_close - daily_open) / daily_open) * 100

        # 5分足データ取得（前場）
        hist_5m = stock.history(period="5d", interval="5m")

        if not hist_5m.empty:
            hist_5m.index = hist_5m.index.tz_localize(None)
            target_day_data = hist_5m[hist_5m.index.date == pd.Timestamp(target_date).date()]

            morning_session = target_day_data[
                (target_day_data.index.time >= time(9, 0)) &
                (target_day_data.index.time <= time(11, 30))
            ]

            if not morning_session.empty:
                morning_open = morning_session.iloc[0]['Open']
                morning_close = morning_session.iloc[-1]['Close']
                morning_change_pct = ((morning_close - morning_open) / morning_open) * 100

                # 前場で利確すべきだったか判定（前場+2%以上 & デイリー < 前場-1%）
                should_take_profit = (morning_change_pct > 2.0) and (daily_change_pct < morning_change_pct - 1.0)

                return {
                    'daily_open': daily_open,
                    'daily_close': daily_close,
                    'daily_change_pct': daily_change_pct,
                    'morning_open': morning_open,
                    'morning_close': morning_close,
                    'morning_change_pct': morning_change_pct,
                    'should_take_profit': should_take_profit
                }

        # 5分足データがない場合はデイリーのみ
        return {
            'daily_open': daily_open,
            'daily_close': daily_close,
            'daily_change_pct': daily_change_pct,
            'morning_open': None,
            'morning_close': None,
            'morning_change_pct': None,
            'should_take_profit': False
        }

    except Exception as e:
        print(f"  [ERROR] Failed to get performance for {ticker}: {e}")
        return None


def validate_and_analyze(grok_result: list[dict], target_date: str) -> pd.DataFrame:
    """
    Grok結果を検証し、実際のパフォーマンスと比較
    """
    results = []

    for stock in grok_result:
        ticker = stock.get("ticker_symbol", "")

        if not ticker:
            continue

        print(f"  Analyzing {ticker}...")

        perf = get_stock_performance(ticker, target_date)

        if perf is None:
            continue

        results.append({
            'ticker': ticker,
            'company_name': stock.get('company_name', ''),
            'category': stock.get('category', ''),
            'mentioned_by': ','.join(stock.get('mentioned_by', [])),
            'has_mention': len(stock.get('mentioned_by', [])) > 0,
            'sentiment_score': stock.get('sentiment_score', 0),
            'policy_link': stock.get('policy_link', ''),
            'daily_open': perf['daily_open'],
            'daily_close': perf['daily_close'],
            'daily_change_pct': perf['daily_change_pct'],
            'morning_open': perf['morning_open'],
            'morning_close': perf['morning_close'],
            'morning_change_pct': perf['morning_change_pct'],
            'should_take_profit': perf['should_take_profit']
        })

    return pd.DataFrame(results)


def generate_summary_report(all_results: pd.DataFrame, output_dir: Path):
    """
    集計レポートを生成
    """
    # デイリーパフォーマンス
    df_daily = all_results[all_results['daily_change_pct'].notna()].copy()
    daily_win_rate = (df_daily['daily_change_pct'] > 0).sum() / len(df_daily) * 100 if len(df_daily) > 0 else 0
    daily_avg_change = df_daily['daily_change_pct'].mean() if len(df_daily) > 0 else 0

    # 前場パフォーマンス
    df_morning = all_results[all_results['morning_change_pct'].notna()].copy()
    morning_win_rate = (df_morning['morning_change_pct'] > 0).sum() / len(df_morning) * 100 if len(df_morning) > 0 else 0
    morning_avg_change = df_morning['morning_change_pct'].mean() if len(df_morning) > 0 else 0

    # 前場利確すべきだった銘柄
    should_take_profit_count = df_morning['should_take_profit'].sum()

    # プレミアムユーザー言及効果
    mentioned = df_daily[df_daily['has_mention'] == True]
    not_mentioned = df_daily[df_daily['has_mention'] == False]

    mentioned_win_rate = (mentioned['daily_change_pct'] > 0).sum() / len(mentioned) * 100 if len(mentioned) > 0 else 0
    not_mentioned_win_rate = (not_mentioned['daily_change_pct'] > 0).sum() / len(not_mentioned) * 100 if len(not_mentioned) > 0 else 0

    # 政策連動度別
    policy_stats = {}
    for policy in ['High', 'Med', 'Low']:
        subset = df_daily[df_daily['policy_link'] == policy]
        if len(subset) > 0:
            policy_stats[policy] = {
                'count': len(subset),
                'win_rate': (subset['daily_change_pct'] > 0).sum() / len(subset) * 100,
                'avg_change': subset['daily_change_pct'].mean()
            }

    # レポート作成
    report_lines = [
        "=" * 80,
        "Grok前場スキャルピング バックテスト結果",
        "=" * 80,
        "",
        f"総選定銘柄数: {len(df_daily)}銘柄",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "【デイリーパフォーマンス（9:00寄付 → 15:30大引け）】",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"勝率: {daily_win_rate:.1f}%",
        f"平均変化率: {daily_avg_change:+.2f}%",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "【前場パフォーマンス（9:00寄付 → 11:30前引け）】",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"勝率: {morning_win_rate:.1f}%",
        f"平均変化率: {morning_avg_change:+.2f}%",
        f"前場で利確すべきだった銘柄: {should_take_profit_count}銘柄",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "【プレミアムユーザー言及効果】",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"言及あり勝率: {mentioned_win_rate:.1f}% ({len(mentioned)}銘柄)",
        f"言及なし勝率: {not_mentioned_win_rate:.1f}% ({len(not_mentioned)}銘柄)",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "【政策連動度別パフォーマンス】",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    for policy, stats in policy_stats.items():
        report_lines.append(f"{policy}: 勝率{stats['win_rate']:.1f}%, 平均{stats['avg_change']:+.2f}% ({stats['count']}銘柄)")

    report_lines.append("")
    report_lines.append("=" * 80)

    # ファイル保存
    report_path = output_dir / "report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    print("\n" + "\n".join(report_lines))
    print(f"\n[INFO] Report saved: {report_path}")


def run_backtest(n_days: int, prompt_template_path: Path, api_key: str):
    """
    バックテスト実行
    """
    print(f"\n[INFO] Starting backtest for past {n_days} trading days...")

    # 出力ディレクトリ作成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = PARQUET_DIR / "backtest_results" / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Output directory: {output_dir}")

    # 過去の営業日を取得
    trading_days = get_past_trading_days(n_days)
    print(f"[INFO] Retrieved {len(trading_days)} trading days")

    all_results = []

    for i, date_info in enumerate(trading_days, 1):
        base_date = date_info["base_date"]
        next_date = date_info["next_trading_day"]

        print(f"\n[{i}/{len(trading_days)}] Processing {base_date} → {next_date}")

        # プロンプト生成
        prompt = build_prompt_for_date(prompt_template_path, date_info)
        prompt_file = output_dir / f"prompt_{base_date}.txt"
        with open(prompt_file, "w", encoding="utf-8") as f:
            f.write(prompt)

        # Grok API呼び出し
        print("  Calling Grok API...")
        grok_response = call_grok_api(prompt, api_key)

        if not grok_response["success"]:
            print(f"  [ERROR] Grok API failed: {grok_response.get('error')}")
            continue

        grok_result = grok_response["data"]

        # 結果保存
        result_file = output_dir / f"result_{base_date}.json"
        with open(result_file, "w", encoding="utf-8") as f:
            json.dump(grok_result, f, ensure_ascii=False, indent=2)

        print(f"  Grok selected {len(grok_result)} stocks")

        # パフォーマンス検証
        print(f"  Validating performance on {next_date}...")
        df_result = validate_and_analyze(grok_result, next_date)

        # 日付情報を追加
        df_result['base_date'] = base_date
        df_result['target_date'] = next_date

        # CSV保存
        csv_file = output_dir / f"validation_{base_date}.csv"
        df_result.to_csv(csv_file, index=False, encoding="utf-8-sig")

        all_results.append(df_result)

    # 全期間の結果を統合
    if all_results:
        df_all = pd.concat(all_results, ignore_index=True)

        # サマリーCSV保存
        summary_file = output_dir / "summary.csv"
        df_all.to_csv(summary_file, index=False, encoding="utf-8-sig")
        print(f"\n[INFO] Summary saved: {summary_file}")

        # レポート生成
        generate_summary_report(df_all, output_dir)

    print(f"\n✅ Backtest completed! Results saved in: {output_dir}")


def main():
    args = parse_args()

    # API key読み込み
    env = dotenv_values(ENV_XAI_PATH)
    api_key = env.get("XAI_API_KEY")

    if not api_key:
        print(f"[ERROR] XAI_API_KEY not found in {ENV_XAI_PATH}")
        return 1

    prompt_path = Path(args.prompt)
    if not prompt_path.exists():
        print(f"[ERROR] Prompt template not found: {prompt_path}")
        return 1

    run_backtest(args.days, prompt_path, api_key)

    return 0


if __name__ == "__main__":
    sys.exit(main())
