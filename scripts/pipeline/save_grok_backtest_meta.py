#!/usr/bin/env python3
"""
save_grok_backtest_meta.py
Grok選定のバックテストメタ情報を保存

バックテスト結果から以下の情報を抽出してparquetに保存:
- 5日間の勝率・平均リターン
- Top5戦略 vs Top10戦略の比較
- 前場戦略 vs デイリー戦略の比較
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import download_file
from common_cfg.s3cfg import load_s3_config
import boto3

def calculate_selection_score(row):
    """選定時点でのスコアを計算"""
    score = row.get('sentiment_score', 0.5) * 100
    policy_bonus = {'High': 30, 'Med': 20, 'Low': 10}
    score += policy_bonus.get(row.get('policy_link', 'Low'), 10)
    if row.get('has_mention', False):
        score += 50
    return score


def generate_grok_backtest_meta() -> pd.DataFrame:
    """
    バックテスト結果からメタ情報を生成

    優先順位:
    1. S3のbacktest/ディレクトリのアーカイブファイル（16:00 JST実行で生成）
    2. backtest_results/ディレクトリのCSVファイル（旧形式・互換性用）

    Returns:
        pd.DataFrame: バックテストメタ情報
    """
    # 1. S3からbacktest/ディレクトリのアーカイブファイルを取得
    cfg = load_s3_config()
    if cfg:
        try:
            s3_client = boto3.client('s3')

            # S3のbacktest/ディレクトリをリスト
            prefix = f"{cfg.prefix}backtest/grok_trending_"
            response = s3_client.list_objects_v2(
                Bucket=cfg.bucket,
                Prefix=prefix
            )

            if 'Contents' in response:
                # grok_trending_YYYYMMDD.parquet ファイルのみ抽出
                archive_files = [
                    obj['Key'] for obj in response['Contents']
                    if obj['Key'].endswith('.parquet') and 'grok_trending_2' in obj['Key']
                ]
                archive_files = sorted(archive_files)

                if len(archive_files) >= 5:
                    print(f"[INFO] Loading backtest data from S3: {len(archive_files)} files")

                    # 最新5日分のファイルをダウンロード
                    recent_files = archive_files[-5:]
                    dfs = []

                    # 一時ディレクトリ作成
                    temp_dir = PARQUET_DIR / "temp" / "backtest"
                    temp_dir.mkdir(parents=True, exist_ok=True)

                    for s3_key in recent_files:
                        try:
                            # ファイル名から日付を抽出
                            filename = s3_key.split('/')[-1]
                            date_str = filename.replace('grok_trending_', '').replace('.parquet', '')

                            # S3からダウンロード（prefixを除去してから渡す）
                            temp_file = temp_dir / filename
                            relative_key = s3_key.replace(cfg.prefix, "", 1)
                            download_file(cfg, relative_key, temp_file)

                            # 読み込み
                            df_day = pd.read_parquet(temp_file)
                            df_day['archive_date'] = date_str
                            dfs.append(df_day)

                            # クリーンアップ
                            temp_file.unlink(missing_ok=True)

                            print(f"[OK] Loaded {s3_key}: {len(df_day)} records")
                        except Exception as e:
                            print(f"[WARN] Failed to read {s3_key}: {e}")

                    if dfs:
                        df = pd.concat(dfs, ignore_index=True)
                        print(f"[OK] Loaded {len(df)} records from {len(dfs)} S3 archive files")
                        return _calculate_backtest_stats(df, source="archive")
                else:
                    print(f"[INFO] Not enough archive files in S3 ({len(archive_files)}/5)")
        except Exception as e:
            print(f"[WARN] Failed to load from S3: {e}")

    # 2. 旧形式のbacktest_results/ディレクトリを確認
    backtest_dir = ROOT / "data/parquet/backtest_results"

    if not backtest_dir.exists():
        print("[WARN] No backtest results or archives found")
        return pd.DataFrame()

    latest_result = sorted(backtest_dir.glob("*/summary.csv"))
    if not latest_result:
        print("[WARN] No summary.csv found in backtest results")
        return pd.DataFrame()

    latest_result_dir = latest_result[-1].parent

    print(f"[INFO] Loading backtest results from: {latest_result_dir}")

    # summary.csvを読み込み
    df = pd.read_csv(latest_result_dir / "summary.csv")
    return _calculate_backtest_stats(df, source="csv", result_dir=latest_result_dir)


def _calculate_backtest_stats(df: pd.DataFrame, source: str, result_dir=None) -> pd.DataFrame:
    """
    バックテストデータから統計を計算

    Args:
        df: バックテストデータ
        source: データソース（"archive" or "csv"）
        result_dir: CSV形式の場合の結果ディレクトリ

    Returns:
        pd.DataFrame: バックテストメタ情報
    """
    from datetime import datetime

    # 基本統計
    total_stocks = len(df)
    unique_stocks = df['ticker'].nunique()

    # 日付範囲の取得（ソースによって列名が異なる可能性）
    if 'target_date' in df.columns:
        date_range = f"{df['target_date'].min()} to {df['target_date'].max()}"
    elif 'archive_date' in df.columns:
        # YYYYMMDD形式をYYYY-MM-DDに変換
        dates = df['archive_date'].unique()
        dates_formatted = [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in sorted(dates)]
        date_range = f"{dates_formatted[0]} to {dates_formatted[-1]}"
    else:
        date_range = "N/A"

    # デイリー戦略の統計
    if 'daily_change_pct' in df.columns:
        daily_win_rate = (df['daily_change_pct'] > 0).sum() / len(df) * 100
        daily_avg_return = df['daily_change_pct'].mean()
    else:
        daily_win_rate = 0
        daily_avg_return = 0

    # 前場戦略の統計
    if 'morning_change_pct' in df.columns:
        morning_win_rate = (df['morning_change_pct'] > 0).sum() / len(df) * 100
        morning_avg_return = df['morning_change_pct'].mean()
    else:
        morning_win_rate = 0
        morning_avg_return = 0

    # プレミアム言及効果
    mention_win_rate = 0
    no_mention_win_rate = 0

    if 'has_mention' in df.columns and 'morning_change_pct' in df.columns:
        has_mention_df = df[df['has_mention'] == True]
        no_mention_df = df[df['has_mention'] == False]

        if len(has_mention_df) > 0:
            mention_win_rate = (has_mention_df['morning_change_pct'] > 0).sum() / len(has_mention_df) * 100
        if len(no_mention_df) > 0:
            no_mention_win_rate = (no_mention_df['morning_change_pct'] > 0).sum() / len(no_mention_df) * 100

    # Top5戦略の計算
    top5_morning_win_rate = 0
    top5_morning_avg_return = 0

    if source == "csv" and result_dir:
        # CSV形式の場合は別ファイルから読み込み
        top5_csv = result_dir / "top5_selection_details.csv"
        if top5_csv.exists():
            df_top5 = pd.read_csv(top5_csv)
            if 'morning_change_pct' in df_top5.columns:
                top5_morning_win_rate = (df_top5['morning_change_pct'] > 0).sum() / len(df_top5) * 100
                top5_morning_avg_return = df_top5['morning_change_pct'].mean()
    elif source == "archive":
        # アーカイブ形式の場合は選定スコアで上位5件を抽出
        if 'selection_score' in df.columns and 'morning_change_pct' in df.columns:
            # 各日付ごとにTop5を抽出
            df_with_date = df.copy()
            if 'archive_date' in df_with_date.columns:
                top5_list = []
                for date in df_with_date['archive_date'].unique():
                    df_date = df_with_date[df_with_date['archive_date'] == date]
                    top5_date = df_date.nlargest(5, 'selection_score')
                    top5_list.append(top5_date)

                if top5_list:
                    df_top5 = pd.concat(top5_list, ignore_index=True)
                    top5_morning_win_rate = (df_top5['morning_change_pct'] > 0).sum() / len(df_top5) * 100
                    top5_morning_avg_return = df_top5['morning_change_pct'].mean()

    # バックテスト日時
    if source == "csv" and result_dir:
        backtest_date = result_dir.name
    else:
        backtest_date = datetime.now().strftime("%Y%m%d_%H%M%S")

    # メタ情報DataFrame
    meta_data = {
        "metric": [
            "total_stocks",
            "unique_stocks",
            "date_range",
            "daily_win_rate",
            "daily_avg_return",
            "morning_win_rate",
            "morning_avg_return",
            "top5_morning_win_rate",
            "top5_morning_avg_return",
            "mention_win_rate",
            "no_mention_win_rate",
            "backtest_date",
        ],
        "value": [
            str(total_stocks),
            str(unique_stocks),
            date_range,
            f"{daily_win_rate:.1f}%",
            f"{daily_avg_return:.2f}%",
            f"{morning_win_rate:.1f}%",
            f"{morning_avg_return:.2f}%",
            f"{top5_morning_win_rate:.1f}%",
            f"{top5_morning_avg_return:.2f}%",
            f"{mention_win_rate:.1f}%",
            f"{no_mention_win_rate:.1f}%",
            backtest_date,
        ]
    }

    df_meta = pd.DataFrame(meta_data)

    print(f"[OK] Generated backtest meta: {len(df_meta)} metrics")
    return df_meta


def generate_top_stocks() -> pd.DataFrame:
    """
    最新のバックテスト結果からTop5/Top10銘柄リストを生成

    優先順位:
    1. S3のbacktest/ディレクトリのアーカイブファイル
    2. backtest_results/ディレクトリのCSVファイル

    Returns:
        pd.DataFrame: Top5/Top10銘柄リスト
    """
    # 1. S3からアーカイブファイルを取得
    cfg = load_s3_config()
    if cfg:
        try:
            s3_client = boto3.client('s3')

            # S3のbacktest/ディレクトリをリスト
            prefix = f"{cfg.prefix}backtest/grok_trending_"
            response = s3_client.list_objects_v2(
                Bucket=cfg.bucket,
                Prefix=prefix
            )

            if 'Contents' in response:
                # grok_trending_YYYYMMDD.parquet ファイルのみ抽出
                archive_files = [
                    obj['Key'] for obj in response['Contents']
                    if obj['Key'].endswith('.parquet') and 'grok_trending_2' in obj['Key']
                ]
                archive_files = sorted(archive_files)

                if len(archive_files) >= 5:
                    print(f"[INFO] Loading top stocks from S3: {len(archive_files)} files")

                    # 最新5日分のファイルをダウンロード
                    recent_files = archive_files[-5:]
                    dfs = []

                    # 一時ディレクトリ作成
                    temp_dir = PARQUET_DIR / "temp" / "backtest"
                    temp_dir.mkdir(parents=True, exist_ok=True)

                    for s3_key in recent_files:
                        try:
                            # ファイル名から日付を抽出
                            filename = s3_key.split('/')[-1]
                            date_str = filename.replace('grok_trending_', '').replace('.parquet', '')
                            # YYYY-MM-DD形式に変換
                            target_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

                            # S3からダウンロード（prefixを除去してから渡す）
                            temp_file = temp_dir / filename
                            relative_key = s3_key.replace(cfg.prefix, "", 1)
                            download_file(cfg, relative_key, temp_file)

                            # 読み込み
                            df_day = pd.read_parquet(temp_file)
                            df_day['target_date'] = target_date
                            dfs.append(df_day)

                            # クリーンアップ
                            temp_file.unlink(missing_ok=True)

                            print(f"[OK] Loaded {s3_key}: {len(df_day)} records")
                        except Exception as e:
                            print(f"[WARN] Failed to read {s3_key}: {e}")

                    if dfs:
                        df = pd.concat(dfs, ignore_index=True)
                        print(f"[OK] Loaded {len(df)} records from {len(dfs)} S3 archive files")
                        return _extract_top_stocks_from_df(df)
                else:
                    print(f"[INFO] Not enough archive files in S3 ({len(archive_files)}/5)")
        except Exception as e:
            print(f"[WARN] Failed to load from S3: {e}")

    # 2. 旧形式のbacktest_results/ディレクトリを確認
    backtest_dir = ROOT / "data/parquet/backtest_results"

    if not backtest_dir.exists():
        print("[WARN] No backtest results or archives found")
        return pd.DataFrame()

    latest_result = sorted(backtest_dir.glob("*/summary.csv"))
    if not latest_result:
        print("[WARN] No summary.csv found in backtest results")
        return pd.DataFrame()

    latest_result_dir = latest_result[-1].parent

    # summary.csvを読み込み
    df = pd.read_csv(latest_result_dir / "summary.csv")
    return _extract_top_stocks_from_df(df)


def _extract_top_stocks_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    データフレームからTop5/Top10銘柄を抽出

    Args:
        df: バックテストデータ

    Returns:
        pd.DataFrame: Top5/Top10銘柄リスト
    """
    # 各日付ごとにスコア計算してTop5/Top10を抽出
    all_top_stocks = []

    for date in df['target_date'].unique():
        df_date = df[df['target_date'] == date].copy()

        # selection_scoreが既に存在しない場合は計算
        if 'selection_score' not in df_date.columns:
            df_date['selection_score'] = df_date.apply(calculate_selection_score, axis=1)

        # スコアでソート
        df_date = df_date.sort_values('selection_score', ascending=False)

        # Top5
        top5 = df_date.head(5).copy()
        top5['rank'] = range(1, len(top5) + 1)
        top5['category'] = 'top5'

        # Top10
        top10 = df_date.head(10).copy()
        top10['rank'] = range(1, len(top10) + 1)
        top10['category'] = 'top10'

        all_top_stocks.extend(top5.to_dict('records'))
        all_top_stocks.extend(top10.to_dict('records'))

    if not all_top_stocks:
        return pd.DataFrame()

    df_top_stocks = pd.DataFrame(all_top_stocks)

    # 必要なカラムのみ抽出
    columns_to_keep = [
        'target_date', 'ticker', 'company_name', 'selection_score',
        'rank', 'category', 'sentiment_score', 'policy_link', 'has_mention',
        'morning_change_pct', 'daily_change_pct'
    ]

    # 存在するカラムのみフィルター
    columns_to_keep = [col for col in columns_to_keep if col in df_top_stocks.columns]
    df_top_stocks = df_top_stocks[columns_to_keep]

    print(f"[OK] Generated top stocks: {len(df_top_stocks)} entries")
    return df_top_stocks


def main():
    """メイン処理"""
    print("=" * 60)
    print("Save Grok Backtest Meta")
    print("=" * 60)

    df_meta = generate_grok_backtest_meta()

    if df_meta.empty:
        print("[WARN] No backtest meta generated")
        return 0

    # 保存
    output_file = PARQUET_DIR / "grok_backtest_meta.parquet"
    df_meta.to_parquet(output_file, index=False)

    print(f"\n[OK] Saved: {output_file}")
    print(f"\nBacktest Summary:")
    print("=" * 60)
    for _, row in df_meta.iterrows():
        print(f"{row['metric']:30} : {row['value']}")
    print("=" * 60)

    # Top5/Top10銘柄リストを生成・保存
    print("\n" + "=" * 60)
    print("Generating Top Stocks List")
    print("=" * 60)

    df_top_stocks = generate_top_stocks()

    if not df_top_stocks.empty:
        top_stocks_file = PARQUET_DIR / "grok_top_stocks.parquet"
        df_top_stocks.to_parquet(top_stocks_file, index=False)
        print(f"\n[OK] Saved: {top_stocks_file}")
        print(f"Total entries: {len(df_top_stocks)}")
        print(f"Date range: {df_top_stocks['target_date'].min()} to {df_top_stocks['target_date'].max()}")
    else:
        print("[WARN] No top stocks generated")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
