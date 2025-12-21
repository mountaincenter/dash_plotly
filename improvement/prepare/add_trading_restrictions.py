#!/usr/bin/env python3
"""
add_trading_restrictions.py
grok_trending.parquet に取引制限カラムを追加するスクリプト

追加カラム:
- margin_code: '1'=信用(空売り不可), '2'=貸借(全取引可), '3'=その他(信用取引不可)
- margin_code_name: マージンコードの名称
- jsf_restricted: 日証金申込停止銘柄フラグ
- is_shortable: 空売り可能フラグ (margin_code='2' かつ jsf_restricted=False)

使い方:
    python prepare/add_trading_restrictions.py
    python prepare/add_trading_restrictions.py --input data/grok_trending.parquet --output data/grok_trending.parquet
"""

import argparse
import pandas as pd
from pathlib import Path


def load_margin_code_master(data_dir: Path) -> dict:
    """MarginCodeマスターを読み込み、ticker→margin_codeのマップを返す"""
    margin_path = data_dir / "margin_code_master.parquet"
    if not margin_path.exists():
        print(f"[WARN] MarginCodeマスターなし: {margin_path}")
        return {}, {}

    margin_df = pd.read_parquet(margin_path)
    margin_code_map = dict(zip(margin_df['ticker'], margin_df['margin_code']))
    margin_name_map = dict(zip(margin_df['ticker'], margin_df['margin_code_name']))

    print(f"[INFO] MarginCode読み込み: {len(margin_code_map)}銘柄")
    return margin_code_map, margin_name_map


def load_jsf_restrictions(base_dir: Path) -> set:
    """日証金制限データを読み込み、申込停止銘柄コードのセットを返す"""
    jsf_path = base_dir / "data" / "parquet" / "jsf_seigenichiran.csv"

    if not jsf_path.exists():
        print(f"[WARN] 日証金CSVなし: {jsf_path}")
        return set()

    try:
        jsf = pd.read_csv(jsf_path, skiprows=4)
        stop_codes = set(jsf[jsf['実施措置'] == '申込停止']['銘柄コード'].astype(str))
        print(f"[INFO] 日証金申込停止: {len(stop_codes)}銘柄")
        return stop_codes
    except Exception as e:
        print(f"[ERROR] 日証金CSV読み込みエラー: {e}")
        return set()


def add_trading_restrictions(
    grok_df: pd.DataFrame,
    margin_code_map: dict,
    margin_name_map: dict,
    jsf_stop_codes: set
) -> pd.DataFrame:
    """grok_trendingにトレーディング制限カラムを追加"""
    df = grok_df.copy()

    # ticker から .T を除去したコードを作成
    df['_code'] = df['ticker'].str.replace('.T', '', regex=False)

    # margin_code追加 (デフォルトは'2'=貸借)
    df['margin_code'] = df['ticker'].map(margin_code_map).fillna('2')
    df['margin_code_name'] = df['ticker'].map(margin_name_map).fillna('貸借')

    # 日証金制限フラグ
    df['jsf_restricted'] = df['_code'].isin(jsf_stop_codes)

    # 空売り可能判定
    # margin_code='2'（貸借）かつ 日証金申込停止でない
    df['is_shortable'] = (df['margin_code'] == '2') & (~df['jsf_restricted'])

    # 一時カラム削除
    df = df.drop(columns=['_code'])

    return df


def print_summary(df: pd.DataFrame):
    """サマリーを表示"""
    print("\n=== 取引制限サマリー ===")
    print(f"総銘柄数: {len(df)}")

    # margin_code分布
    print("\nMarginCode分布:")
    for code in ['1', '2', '3']:
        count = len(df[df['margin_code'] == code])
        name = df[df['margin_code'] == code]['margin_code_name'].iloc[0] if count > 0 else ''
        print(f"  {code} ({name}): {count}件")

    # 日証金制限
    jsf_count = df['jsf_restricted'].sum()
    print(f"\n日証金申込停止: {jsf_count}件")

    # 空売り可能
    shortable_count = df['is_shortable'].sum()
    print(f"空売り可能: {shortable_count}件")

    # 取引可能性
    print("\n取引可能性:")
    print(f"  買いシグナル対象（信用可）: {len(df[df['margin_code'].isin(['1', '2'])])}件")
    print(f"  売りシグナル対象（空売り可）: {shortable_count}件")


def main():
    parser = argparse.ArgumentParser(description='grok_trending.parquetに取引制限カラムを追加')
    parser.add_argument('--input', '-i', type=str, default='data/grok_trending.parquet',
                        help='入力parquetファイルパス')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='出力parquetファイルパス（省略時は入力と同じ）')
    parser.add_argument('--dry-run', action='store_true',
                        help='実際に保存せずにサマリーのみ表示')
    args = parser.parse_args()

    # パス解決
    base_dir = Path(__file__).resolve().parent.parent.parent  # dash_plotly/
    improvement_dir = base_dir / "improvement"
    data_dir = improvement_dir / "data"

    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = improvement_dir / args.input

    output_path = Path(args.output) if args.output else input_path
    if not output_path.is_absolute():
        output_path = improvement_dir / args.output if args.output else input_path

    print(f"[INFO] Base dir: {base_dir}")
    print(f"[INFO] Input: {input_path}")
    print(f"[INFO] Output: {output_path}")

    # 入力ファイル読み込み
    if not input_path.exists():
        print(f"[ERROR] 入力ファイルなし: {input_path}")
        return 1

    grok_df = pd.read_parquet(input_path)
    print(f"[INFO] grok_trending読み込み: {len(grok_df)}件")
    print(f"[INFO] 既存カラム: {list(grok_df.columns)}")

    # マスターデータ読み込み
    margin_code_map, margin_name_map = load_margin_code_master(data_dir)
    jsf_stop_codes = load_jsf_restrictions(base_dir)

    # カラム追加
    result_df = add_trading_restrictions(grok_df, margin_code_map, margin_name_map, jsf_stop_codes)

    # サマリー表示
    print_summary(result_df)

    # 保存
    if not args.dry_run:
        result_df.to_parquet(output_path, index=False)
        print(f"\n[OK] 保存完了: {output_path}")
    else:
        print("\n[DRY-RUN] 保存スキップ")

    return 0


if __name__ == "__main__":
    exit(main())
