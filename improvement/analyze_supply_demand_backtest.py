#!/usr/bin/env python3
"""
需給データ × grok_trendingパフォーマンス の相関分析

1. archive/backtest の全ファイルを結合
2. J-Quantsから過去の需給データ（backtest_date - 1d）を取得
3. 需給指標とパフォーマンスの相関を分析
4. 結果をHTMLで出力
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import time

project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from scripts.lib.jquants_client import JQuantsClient


def load_backtest_archive() -> pd.DataFrame:
    """archive/backtestの全ファイルを結合"""
    archive_dir = project_root / "data/parquet/archive/backtest"
    files = sorted(archive_dir.glob("grok_trending_*.parquet"))

    print(f"[1/4] バックテストファイル読み込み: {len(files)}ファイル")

    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    print(f"  総行数: {len(combined)}")
    print(f"  日付範囲: {combined['backtest_date'].min()} ~ {combined['backtest_date'].max()}")

    return combined


def ticker_to_code(ticker: str) -> str:
    """ticker (7280.T) → J-Quantsコード (72800)"""
    return ticker.replace('.T', '0')


def fetch_margin_data_batch(client: JQuantsClient, tickers: list, dates: list) -> pd.DataFrame:
    """
    複数銘柄・複数日付の需給データを一括取得

    J-Quantsは日付範囲指定可能なので、効率的に取得
    """
    print(f"[2/4] J-Quantsから需給データ取得中...")

    # ユニークなticker/dateの組み合わせを作成
    unique_tickers = list(set(tickers))
    min_date = (min(dates) - timedelta(days=7)).date()  # date型に変換
    max_date = max(dates).date()

    print(f"  銘柄数: {len(unique_tickers)}")
    print(f"  日付範囲: {min_date} ~ {max_date}")

    all_margin_data = []

    for i, ticker in enumerate(unique_tickers, 1):
        code = ticker_to_code(ticker)

        if i % 50 == 0 or i == len(unique_tickers):
            print(f"  [{i}/{len(unique_tickers)}] 処理中...")

        try:
            data = client.request(
                "/markets/margin-interest",
                params={
                    "code": code,
                    "from": str(min_date),
                    "to": str(max_date)
                }
            )
            records = data.get("data", [])

            for r in records:
                r["ticker"] = ticker

            all_margin_data.extend(records)

        except Exception as e:
            # 429の場合は待機してリトライ
            if "429" in str(e):
                print(f"  レート制限、30秒待機...")
                time.sleep(30)
                try:
                    data = client.request(
                        "/markets/margin-interest",
                        params={
                            "code": code,
                            "from": str(min_date),
                            "to": str(max_date)
                        }
                    )
                    records = data.get("data", [])
                    for r in records:
                        r["ticker"] = ticker
                    all_margin_data.extend(records)
                except:
                    pass

        # レート制限対策（間隔を長めに）
        time.sleep(1.0)

    if not all_margin_data:
        print("  警告: 需給データが取得できませんでした")
        return pd.DataFrame()

    df = pd.DataFrame(all_margin_data)
    print(f"  取得件数: {len(df)}")

    return df


def merge_and_analyze(backtest_df: pd.DataFrame, margin_df: pd.DataFrame) -> pd.DataFrame:
    """需給データをマージして分析"""
    print(f"[3/4] データマージ・分析中...")

    if margin_df.empty:
        print("  需給データなし、スキップ")
        return backtest_df

    # margin_dfの日付をdatetime化
    margin_df["Date"] = pd.to_datetime(margin_df["Date"])

    # backtest_dfの日付をdatetime化
    backtest_df["backtest_date"] = pd.to_datetime(backtest_df["backtest_date"])

    # backtest_date - 1d を計算（前日の需給を参照）
    backtest_df["margin_lookup_date"] = backtest_df["backtest_date"] - timedelta(days=1)

    # 週末を考慮（金曜のbacktestなら木曜の需給、月曜なら金曜）
    # 簡易的に、最も近い過去の需給データを使う

    merged_records = []

    for _, row in backtest_df.iterrows():
        ticker = row["ticker"]
        lookup_date = row["margin_lookup_date"]

        # この銘柄の需給データを取得
        ticker_margin = margin_df[margin_df["ticker"] == ticker].copy()

        if ticker_margin.empty:
            merged_records.append({
                **row.to_dict(),
                "margin_date": None,
                "margin_long_vol": None,
                "margin_short_vol": None,
                "margin_sl_ratio": None,
            })
            continue

        # lookup_date以前で最も近い日付のデータを取得
        past_data = ticker_margin[ticker_margin["Date"] <= lookup_date]

        if past_data.empty:
            merged_records.append({
                **row.to_dict(),
                "margin_date": None,
                "margin_long_vol": None,
                "margin_short_vol": None,
                "margin_sl_ratio": None,
            })
            continue

        latest = past_data.sort_values("Date", ascending=False).iloc[0]

        long_vol = latest.get("LongVol", 0) or 0
        short_vol = latest.get("ShrtVol", 0) or 0
        sl_ratio = long_vol / short_vol if short_vol > 0 else None

        merged_records.append({
            **row.to_dict(),
            "margin_date": latest["Date"],
            "margin_long_vol": long_vol,
            "margin_short_vol": short_vol,
            "margin_sl_ratio": sl_ratio,
        })

    result = pd.DataFrame(merged_records)

    # 需給データがある行数
    has_margin = result["margin_sl_ratio"].notna().sum()
    print(f"  需給データあり: {has_margin}/{len(result)} ({has_margin/len(result)*100:.1f}%)")

    return result


def calculate_correlations(df: pd.DataFrame) -> dict:
    """需給指標とパフォーマンスの相関を計算"""

    # 需給データがある行のみ
    df_with_margin = df[df["margin_sl_ratio"].notna()].copy()

    if len(df_with_margin) < 10:
        return {"error": "需給データが少なすぎます"}

    results = {}

    # 貸借倍率のビン分け
    df_with_margin["sl_ratio_bin"] = pd.cut(
        df_with_margin["margin_sl_ratio"],
        bins=[0, 1, 3, 10, float("inf")],
        labels=["<1 (売り優勢)", "1-3", "3-10", ">10 (買い優勢)"]
    )

    # 各ビンごとのパフォーマンス
    performance_cols = ["phase1_return", "phase2_return", "phase3_1pct_return"]

    for col in performance_cols:
        if col in df_with_margin.columns:
            grouped = df_with_margin.groupby("sl_ratio_bin")[col].agg(["mean", "count", "std"])
            results[col] = grouped.to_dict()

    # 勝率
    win_cols = ["phase1_win", "phase2_win", "phase3_1pct_win"]
    for col in win_cols:
        if col in df_with_margin.columns:
            grouped = df_with_margin.groupby("sl_ratio_bin")[col].mean()
            results[f"{col}_rate"] = grouped.to_dict()

    # 相関係数
    for col in performance_cols:
        if col in df_with_margin.columns:
            corr = df_with_margin["margin_sl_ratio"].corr(df_with_margin[col])
            results[f"corr_sl_ratio_{col}"] = corr

    return results


def generate_html(df: pd.DataFrame, correlations: dict) -> str:
    """分析結果のHTMLを生成"""

    df_with_margin = df[df["margin_sl_ratio"].notna()].copy()

    html = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>需給 × パフォーマンス 相関分析</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #0d1117;
            color: #c9d1d9;
            padding: 20px;
            max-width: 1400px;
            margin: 0 auto;
        }
        h1 { color: #58a6ff; }
        h2 { color: #8b949e; margin-top: 30px; }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }
        th, td {
            border: 1px solid #30363d;
            padding: 10px;
            text-align: right;
        }
        th { background: #21262d; color: #58a6ff; }
        tr:nth-child(even) { background: #161b22; }
        .positive { color: #3fb950; }
        .negative { color: #f85149; }
        .highlight { background: #1f3d1f; }
        .summary-box {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
        }
        .metric {
            display: inline-block;
            margin: 10px 20px;
            text-align: center;
        }
        .metric-value {
            font-size: 24px;
            font-weight: bold;
        }
        .metric-label {
            font-size: 12px;
            color: #8b949e;
        }
    </style>
</head>
<body>
    <h1>需給 × パフォーマンス 相関分析</h1>
"""

    # サマリー
    html += f"""
    <div class="summary-box">
        <div class="metric">
            <div class="metric-value">{len(df)}</div>
            <div class="metric-label">総バックテスト数</div>
        </div>
        <div class="metric">
            <div class="metric-value">{len(df_with_margin)}</div>
            <div class="metric-label">需給データあり</div>
        </div>
        <div class="metric">
            <div class="metric-value">{df['backtest_date'].nunique()}</div>
            <div class="metric-label">日数</div>
        </div>
        <div class="metric">
            <div class="metric-value">{df['ticker'].nunique()}</div>
            <div class="metric-label">銘柄数</div>
        </div>
    </div>
"""

    # 貸借倍率別パフォーマンス
    html += """
    <h2>貸借倍率別パフォーマンス</h2>
    <p>貸借倍率 = 買残 ÷ 売残（低いほど売り優勢 → 買い戻し需要）</p>
    <table>
        <tr>
            <th>貸借倍率</th>
            <th>件数</th>
            <th>Phase1 平均リターン</th>
            <th>Phase1 勝率</th>
            <th>Phase2 平均リターン</th>
            <th>Phase2 勝率</th>
        </tr>
"""

    if "phase1_return" in correlations:
        phase1_data = correlations["phase1_return"]
        phase1_win = correlations.get("phase1_win_rate", {})
        phase2_data = correlations.get("phase2_return", {})
        phase2_win = correlations.get("phase2_win_rate", {})

        for bin_name in ["<1 (売り優勢)", "1-3", "3-10", ">10 (買い優勢)"]:
            count = phase1_data.get("count", {}).get(bin_name, 0)
            p1_mean = phase1_data.get("mean", {}).get(bin_name, None)
            p1_win = phase1_win.get(bin_name, None)
            p2_mean = phase2_data.get("mean", {}).get(bin_name, None) if phase2_data else None
            p2_win = phase2_win.get(bin_name, None) if phase2_win else None

            p1_class = "positive" if p1_mean and p1_mean > 0 else "negative"
            p2_class = "positive" if p2_mean and p2_mean > 0 else "negative"
            highlight = "highlight" if bin_name == "<1 (売り優勢)" else ""

            p1_mean_str = f"{p1_mean:.2f}%" if p1_mean is not None else "-"
            p1_win_str = f"{p1_win*100:.1f}%" if p1_win is not None else "-"
            p2_mean_str = f"{p2_mean:.2f}%" if p2_mean is not None else "-"
            p2_win_str = f"{p2_win*100:.1f}%" if p2_win is not None else "-"
            count_str = str(int(count)) if count else "-"

            html += f"""
        <tr class="{highlight}">
            <td style="text-align:left">{bin_name}</td>
            <td>{count_str}</td>
            <td class="{p1_class}">{p1_mean_str}</td>
            <td>{p1_win_str}</td>
            <td class="{p2_class}">{p2_mean_str}</td>
            <td>{p2_win_str}</td>
        </tr>
"""

    html += """
    </table>
"""

    # 相関係数
    html += """
    <h2>相関係数（貸借倍率 vs パフォーマンス）</h2>
    <table>
        <tr>
            <th>指標</th>
            <th>相関係数</th>
            <th>解釈</th>
        </tr>
"""

    for key, value in correlations.items():
        if key.startswith("corr_"):
            name = key.replace("corr_sl_ratio_", "")
            if isinstance(value, (int, float)) and not np.isnan(value):
                interpretation = ""
                if value < -0.1:
                    interpretation = "貸借倍率低い → リターン高い傾向"
                elif value > 0.1:
                    interpretation = "貸借倍率高い → リターン高い傾向"
                else:
                    interpretation = "相関なし"

                html += f"""
        <tr>
            <td>{name}</td>
            <td>{value:.3f}</td>
            <td>{interpretation}</td>
        </tr>
"""

    html += """
    </table>

    <h2>分析の解釈</h2>
    <div class="summary-box">
        <p><strong>期待される結果:</strong></p>
        <ul>
            <li>貸借倍率 < 1（売り優勢）の銘柄は、買い戻し需要で上昇しやすい</li>
            <li>相関係数が負であれば、需給分析が有効</li>
        </ul>
        <p><strong>注意点:</strong></p>
        <ul>
            <li>J-Quantsの需給データは週次更新のため、タイムラグあり</li>
            <li>サンプル数が少ない場合は統計的有意性に注意</li>
        </ul>
    </div>
</body>
</html>
"""

    return html


def main():
    print("=" * 60)
    print("需給 × パフォーマンス 相関分析")
    print("=" * 60)

    # 1. バックテストデータ読み込み
    backtest_df = load_backtest_archive()

    # 2. J-Quantsから需給データ取得
    client = JQuantsClient()

    tickers = backtest_df["ticker"].tolist()
    dates = pd.to_datetime(backtest_df["backtest_date"]).tolist()

    margin_df = fetch_margin_data_batch(client, tickers, dates)

    # 3. マージ・分析
    merged_df = merge_and_analyze(backtest_df, margin_df)

    # 4. 相関計算
    print("[4/4] 相関分析・HTML出力...")
    correlations = calculate_correlations(merged_df)

    # 5. 出力
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)

    # HTML
    html = generate_html(merged_df, correlations)
    html_path = output_dir / "supply_demand_backtest_analysis.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"  HTML: {html_path}")

    # parquet
    parquet_path = output_dir / "backtest_with_supply_demand.parquet"
    merged_df.to_parquet(parquet_path, index=False)
    print(f"  Parquet: {parquet_path}")

    print()
    print("=" * 60)
    print("完了!")
    print(f"ブラウザで開く: file://{html_path.resolve()}")


if __name__ == "__main__":
    main()
