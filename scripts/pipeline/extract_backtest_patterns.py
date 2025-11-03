#!/usr/bin/env python3
"""
extract_backtest_patterns.py
バックテストデータから成功・失敗パターンを自動抽出

Phase1とPhase2の両方のバックテスト結果を分析し、
Grokプロンプトに組み込むための統計とパターンを抽出します。

出力ファイル:
- grok_backtest_patterns.parquet: パターン分析結果
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import boto3
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import download_file
from common_cfg.s3cfg import load_s3_config


def load_backtest_archives(days: int = 5) -> pd.DataFrame:
    """
    バックテストアーカイブから最近N日分のデータを読み込み（S3から取得）

    Args:
        days: 読み込む日数（デフォルト5日）

    Returns:
        pd.DataFrame: 統合されたバックテストデータ
    """
    # S3設定を取得
    cfg = load_s3_config()
    if not cfg:
        print("[WARN] S3 not configured - cannot load backtest archives")
        return pd.DataFrame()

    try:
        s3_client = boto3.client(
            's3',
            region_name=cfg.region,
            endpoint_url=cfg.endpoint_url
        )

        # S3上のbacktest/grok_trending_*.parquetファイルをリスト
        bucket = cfg.bucket or "stock-api-data"
        prefix = (cfg.prefix or "parquet/").rstrip("/") + "/backtest/grok_trending_"

        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' not in response:
            print(f"[WARN] No backtest files found in S3: s3://{bucket}/{prefix}*")
            return pd.DataFrame()

        # grok_trending_YYYYMMDD.parquetパターンのファイルのみ抽出
        archive_files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith('.parquet') and 'grok_trending_2' in obj['Key']
        ]
        archive_files = sorted(archive_files)

        if len(archive_files) < days:
            print(f"[WARN] Only {len(archive_files)} archive files found (requested {days} days)")
            if len(archive_files) == 0:
                return pd.DataFrame()

        # 最新N日分を読み込み
        recent_files = archive_files[-days:]
        dfs = []

        # 一時ディレクトリを作成
        temp_dir = PARQUET_DIR / "temp" / "backtest"
        temp_dir.mkdir(parents=True, exist_ok=True)

        for s3_key in recent_files:
            try:
                # S3からダウンロード
                filename = s3_key.split('/')[-1]
                temp_file = temp_dir / filename

                if not download_file(cfg, s3_key, temp_file):
                    print(f"[WARN] Failed to download from S3: {s3_key}")
                    continue

                # Parquetファイルを読み込み
                df_day = pd.read_parquet(temp_file)

                # ファイル名から日付を抽出（grok_trending_YYYYMMDD.parquet）
                date_str = filename.split('_')[-1].replace('.parquet', '')  # YYYYMMDD
                target_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                df_day['target_date'] = target_date

                dfs.append(df_day)
                print(f"[OK] Loaded from S3: {filename} ({len(df_day)} records)")

                # 一時ファイルを削除
                temp_file.unlink(missing_ok=True)

            except Exception as e:
                print(f"[WARN] Failed to process {s3_key}: {e}")

        if not dfs:
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)
        print(f"\n[OK] Total loaded: {len(df)} records from {len(dfs)} days")
        return df

    except Exception as e:
        print(f"[ERROR] Failed to load backtest archives from S3: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def calculate_phase2_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase2メトリクスを計算（5分足データが必要な場合はスキップ）

    Args:
        df: バックテストデータ

    Returns:
        pd.DataFrame: Phase2メトリクスを追加したデータ
    """
    # Phase2カラムが既に存在する場合はそのまま返す
    if 'target_3pct_achieved' in df.columns:
        print("[INFO] Phase2 metrics already calculated")
        return df

    # Phase2メトリクスがない場合はPhase1のみで進める
    print("[INFO] Phase2 metrics not available, using Phase1 only")
    return df


def extract_success_patterns(df: pd.DataFrame) -> dict:
    """
    成功パターンを抽出

    成功の定義:
    - Phase1: morning_change_pct > 0
    - Phase2: target_3pct_achieved == True

    Returns:
        dict: 成功パターンの統計情報
    """
    patterns = {
        'phase1': {},
        'phase2': {},
        'common': {}
    }

    # Phase1成功パターン（前場でプラスリターン）
    if 'morning_change_pct' in df.columns:
        phase1_success = df[df['morning_change_pct'] > 0].copy()

        patterns['phase1'] = {
            'total_count': len(phase1_success),
            'win_rate': len(phase1_success) / len(df) * 100 if len(df) > 0 else 0,
            'avg_return': phase1_success['morning_change_pct'].mean() if len(phase1_success) > 0 else 0,
            'median_return': phase1_success['morning_change_pct'].median() if len(phase1_success) > 0 else 0,
            'top_categories': phase1_success['category'].value_counts().head(5).to_dict() if 'category' in phase1_success.columns else {},
            'avg_sentiment_score': phase1_success['sentiment_score'].mean() if 'sentiment_score' in phase1_success.columns else 0,
            'mention_rate': (phase1_success['has_mention'].sum() / len(phase1_success) * 100) if 'has_mention' in phase1_success.columns and len(phase1_success) > 0 else 0,
            'policy_link_dist': phase1_success['policy_link'].value_counts().to_dict() if 'policy_link' in phase1_success.columns else {},
        }

        print(f"\n[Phase1 Success] {len(phase1_success)}銘柄 (勝率: {patterns['phase1']['win_rate']:.1f}%)")
        print(f"  平均リターン: {patterns['phase1']['avg_return']:.2f}%")
        print(f"  上位カテゴリ: {list(patterns['phase1']['top_categories'].keys())[:3]}")

    # Phase2成功パターン（3%目標到達）
    if 'target_3pct_achieved' in df.columns:
        phase2_success = df[df['target_3pct_achieved'] == True].copy()

        patterns['phase2'] = {
            'total_count': len(phase2_success),
            'achievement_rate': len(phase2_success) / len(df) * 100 if len(df) > 0 else 0,
            'avg_return': phase2_success['phase2_return_3pct'].mean() if 'phase2_return_3pct' in phase2_success.columns and len(phase2_success) > 0 else 0,
            'top_categories': phase2_success['category'].value_counts().head(5).to_dict() if 'category' in phase2_success.columns else {},
            'avg_sentiment_score': phase2_success['sentiment_score'].mean() if 'sentiment_score' in phase2_success.columns else 0,
            'mention_rate': (phase2_success['has_mention'].sum() / len(phase2_success) * 100) if 'has_mention' in phase2_success.columns and len(phase2_success) > 0 else 0,
            'policy_link_dist': phase2_success['policy_link'].value_counts().to_dict() if 'policy_link' in phase2_success.columns else {},
        }

        print(f"\n[Phase2 Success] {len(phase2_success)}銘柄 (到達率: {patterns['phase2']['achievement_rate']:.1f}%)")
        print(f"  平均リターン: {patterns['phase2']['avg_return']:.2f}%")
        print(f"  上位カテゴリ: {list(patterns['phase2']['top_categories'].keys())[:3]}")

    return patterns


def extract_failure_patterns(df: pd.DataFrame) -> dict:
    """
    失敗パターンを抽出

    失敗の定義:
    - Phase1: morning_change_pct < -2%（大きな下落）
    - Phase2: target_3pct_achieved == False かつ morning_change_pct < 0

    Returns:
        dict: 失敗パターンの統計情報
    """
    patterns = {
        'phase1': {},
        'phase2': {},
        'common': {}
    }

    # Phase1失敗パターン（前場で-2%以上の下落）
    if 'morning_change_pct' in df.columns:
        phase1_failure = df[df['morning_change_pct'] < -2.0].copy()

        patterns['phase1'] = {
            'total_count': len(phase1_failure),
            'failure_rate': len(phase1_failure) / len(df) * 100 if len(df) > 0 else 0,
            'avg_loss': phase1_failure['morning_change_pct'].mean() if len(phase1_failure) > 0 else 0,
            'worst_loss': phase1_failure['morning_change_pct'].min() if len(phase1_failure) > 0 else 0,
            'top_categories': phase1_failure['category'].value_counts().head(5).to_dict() if 'category' in phase1_failure.columns else {},
            'avg_sentiment_score': phase1_failure['sentiment_score'].mean() if 'sentiment_score' in phase1_failure.columns else 0,
            'mention_rate': (phase1_failure['has_mention'].sum() / len(phase1_failure) * 100) if 'has_mention' in phase1_failure.columns and len(phase1_failure) > 0 else 0,
            'policy_link_dist': phase1_failure['policy_link'].value_counts().to_dict() if 'policy_link' in phase1_failure.columns else {},
        }

        print(f"\n[Phase1 Failure] {len(phase1_failure)}銘柄 (失敗率: {patterns['phase1']['failure_rate']:.1f}%)")
        print(f"  平均損失: {patterns['phase1']['avg_loss']:.2f}%")
        print(f"  上位カテゴリ: {list(patterns['phase1']['top_categories'].keys())[:3]}")

    # Phase2失敗パターン（目標未到達 & マイナスリターン）
    if 'target_3pct_achieved' in df.columns and 'morning_change_pct' in df.columns:
        phase2_failure = df[
            (df['target_3pct_achieved'] == False) &
            (df['morning_change_pct'] < 0)
        ].copy()

        patterns['phase2'] = {
            'total_count': len(phase2_failure),
            'failure_rate': len(phase2_failure) / len(df) * 100 if len(df) > 0 else 0,
            'avg_loss': phase2_failure['morning_change_pct'].mean() if len(phase2_failure) > 0 else 0,
            'top_categories': phase2_failure['category'].value_counts().head(5).to_dict() if 'category' in phase2_failure.columns else {},
            'avg_sentiment_score': phase2_failure['sentiment_score'].mean() if 'sentiment_score' in phase2_failure.columns else 0,
            'mention_rate': (phase2_failure['has_mention'].sum() / len(phase2_failure) * 100) if 'has_mention' in phase2_failure.columns and len(phase2_failure) > 0 else 0,
            'policy_link_dist': phase2_failure['policy_link'].value_counts().to_dict() if 'policy_link' in phase2_failure.columns else {},
        }

        print(f"\n[Phase2 Failure] {len(phase2_failure)}銘柄 (失敗率: {patterns['phase2']['failure_rate']:.1f}%)")
        print(f"  平均損失: {patterns['phase2']['avg_loss']:.2f}%")
        print(f"  上位カテゴリ: {list(patterns['phase2']['top_categories'].keys())[:3]}")

    return patterns


def generate_top_performers(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """
    Top N成功銘柄を抽出

    Args:
        df: バックテストデータ
        n: 抽出する銘柄数

    Returns:
        pd.DataFrame: Top N成功銘柄
    """
    if 'morning_change_pct' not in df.columns:
        return pd.DataFrame()

    # Phase1リターンでソート
    df_sorted = df.sort_values('morning_change_pct', ascending=False)
    top_performers = df_sorted.head(n).copy()

    # 必要なカラムのみ抽出
    columns_to_keep = [
        'target_date', 'ticker', 'company_name', 'category',
        'sentiment_score', 'policy_link', 'has_mention',
        'morning_change_pct', 'daily_change_pct'
    ]

    # Phase2カラムがあれば追加
    if 'target_3pct_achieved' in top_performers.columns:
        columns_to_keep.extend(['target_3pct_achieved', 'phase2_return_3pct'])

    # 存在するカラムのみフィルター
    columns_to_keep = [col for col in columns_to_keep if col in top_performers.columns]
    top_performers = top_performers[columns_to_keep]

    print(f"\n[Top {n} Performers]")
    for idx, row in top_performers.iterrows():
        ticker = row['ticker']
        name = row.get('company_name', 'N/A')
        return_pct = row['morning_change_pct']
        category = row.get('category', 'N/A')
        print(f"  {ticker} {name[:15]:<15} | {return_pct:>6.2f}% | {category}")

    return top_performers


def generate_worst_performers(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """
    Worst N失敗銘柄を抽出

    Args:
        df: バックテストデータ
        n: 抽出する銘柄数

    Returns:
        pd.DataFrame: Worst N失敗銘柄
    """
    if 'morning_change_pct' not in df.columns:
        return pd.DataFrame()

    # Phase1リターンでソート（昇順）
    df_sorted = df.sort_values('morning_change_pct', ascending=True)
    worst_performers = df_sorted.head(n).copy()

    # 必要なカラムのみ抽出
    columns_to_keep = [
        'target_date', 'ticker', 'company_name', 'category',
        'sentiment_score', 'policy_link', 'has_mention',
        'morning_change_pct', 'daily_change_pct'
    ]

    # 存在するカラムのみフィルター
    columns_to_keep = [col for col in columns_to_keep if col in worst_performers.columns]
    worst_performers = worst_performers[columns_to_keep]

    print(f"\n[Worst {n} Performers]")
    for idx, row in worst_performers.iterrows():
        ticker = row['ticker']
        name = row.get('company_name', 'N/A')
        return_pct = row['morning_change_pct']
        category = row.get('category', 'N/A')
        print(f"  {ticker} {name[:15]:<15} | {return_pct:>6.2f}% | {category}")

    return worst_performers


def create_pattern_summary(success_patterns: dict, failure_patterns: dict) -> pd.DataFrame:
    """
    パターン分析結果をDataFrameに変換

    Args:
        success_patterns: 成功パターン
        failure_patterns: 失敗パターン

    Returns:
        pd.DataFrame: パターンサマリー
    """
    summary_data = []

    # Phase1成功パターン
    if success_patterns.get('phase1'):
        p1_success = success_patterns['phase1']
        summary_data.append({
            'pattern_type': 'phase1_success',
            'count': p1_success.get('total_count', 0),
            'rate': p1_success.get('win_rate', 0),
            'avg_return': p1_success.get('avg_return', 0),
            'avg_sentiment': p1_success.get('avg_sentiment_score', 0),
            'mention_rate': p1_success.get('mention_rate', 0),
            'top_category': list(p1_success.get('top_categories', {}).keys())[0] if p1_success.get('top_categories') else 'N/A',
        })

    # Phase1失敗パターン
    if failure_patterns.get('phase1'):
        p1_failure = failure_patterns['phase1']
        summary_data.append({
            'pattern_type': 'phase1_failure',
            'count': p1_failure.get('total_count', 0),
            'rate': p1_failure.get('failure_rate', 0),
            'avg_return': p1_failure.get('avg_loss', 0),
            'avg_sentiment': p1_failure.get('avg_sentiment_score', 0),
            'mention_rate': p1_failure.get('mention_rate', 0),
            'top_category': list(p1_failure.get('top_categories', {}).keys())[0] if p1_failure.get('top_categories') else 'N/A',
        })

    # Phase2成功パターン
    if success_patterns.get('phase2') and success_patterns['phase2']:
        p2_success = success_patterns['phase2']
        summary_data.append({
            'pattern_type': 'phase2_success',
            'count': p2_success.get('total_count', 0),
            'rate': p2_success.get('achievement_rate', 0),
            'avg_return': p2_success.get('avg_return', 0),
            'avg_sentiment': p2_success.get('avg_sentiment_score', 0),
            'mention_rate': p2_success.get('mention_rate', 0),
            'top_category': list(p2_success.get('top_categories', {}).keys())[0] if p2_success.get('top_categories') else 'N/A',
        })

    # Phase2失敗パターン
    if failure_patterns.get('phase2') and failure_patterns['phase2']:
        p2_failure = failure_patterns['phase2']
        summary_data.append({
            'pattern_type': 'phase2_failure',
            'count': p2_failure.get('total_count', 0),
            'rate': p2_failure.get('failure_rate', 0),
            'avg_return': p2_failure.get('avg_loss', 0),
            'avg_sentiment': p2_failure.get('avg_sentiment_score', 0),
            'mention_rate': p2_failure.get('mention_rate', 0),
            'top_category': list(p2_failure.get('top_categories', {}).keys())[0] if p2_failure.get('top_categories') else 'N/A',
        })

    if not summary_data:
        return pd.DataFrame()

    return pd.DataFrame(summary_data)


def main():
    """メイン処理"""
    print("=" * 80)
    print("Extract Backtest Patterns for Grok Prompt Enhancement")
    print("=" * 80)

    # バックテストデータ読み込み（最近5日分）
    df = load_backtest_archives(days=5)

    if df.empty:
        print("\n[WARN] No backtest data found - skipping pattern extraction")
        print("This is expected on first run or when backtest directory is empty")
        return 0

    # Phase2メトリクス計算（可能な場合）
    df = calculate_phase2_metrics(df)

    # 成功パターン抽出
    print("\n" + "=" * 80)
    print("Extracting Success Patterns")
    print("=" * 80)
    success_patterns = extract_success_patterns(df)

    # 失敗パターン抽出
    print("\n" + "=" * 80)
    print("Extracting Failure Patterns")
    print("=" * 80)
    failure_patterns = extract_failure_patterns(df)

    # Top/Worst銘柄抽出
    print("\n" + "=" * 80)
    print("Extracting Top/Worst Performers")
    print("=" * 80)
    df_top = generate_top_performers(df, n=10)
    df_worst = generate_worst_performers(df, n=10)

    # パターンサマリー作成
    df_summary = create_pattern_summary(success_patterns, failure_patterns)

    # 保存
    output_file = PARQUET_DIR / "grok_backtest_patterns.parquet"
    df_summary.to_parquet(output_file, index=False)
    print(f"\n[OK] Saved pattern summary: {output_file}")

    # Top/Worst銘柄も保存
    if not df_top.empty:
        top_file = PARQUET_DIR / "grok_top_performers.parquet"
        df_top.to_parquet(top_file, index=False)
        print(f"[OK] Saved top performers: {top_file}")

    if not df_worst.empty:
        worst_file = PARQUET_DIR / "grok_worst_performers.parquet"
        df_worst.to_parquet(worst_file, index=False)
        print(f"[OK] Saved worst performers: {worst_file}")

    # サマリー表示
    print("\n" + "=" * 80)
    print("Pattern Summary")
    print("=" * 80)
    if not df_summary.empty:
        print(df_summary.to_string(index=False))
    print("=" * 80)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
