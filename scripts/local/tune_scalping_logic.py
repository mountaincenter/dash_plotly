#!/usr/bin/env python3
"""
ローカル実行: スキャルピングロジック調整用スクリプト
銘柄選定条件を段階的に分析・調整する
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from scripts.lib.jquants_client import JQuantsClient
from scripts.lib.jquants_fetcher import JQuantsFetcher
from scripts.lib.screener import ScalpingScreener
from common_cfg.paths import PARQUET_DIR


def analyze_conditions_step_by_step(df_latest: pd.DataFrame):
    """条件を段階的に適用して通過率を分析"""
    print("\n" + "=" * 80)
    print("段階的条件分析（overall_rating除く）")
    print("=" * 80)

    total = len(df_latest)
    print(f"Total stocks: {total}")
    print()

    # Entry条件を段階的に適用
    print("--- Entry条件 (初心者向け) ---")

    df = df_latest.copy()

    # 価格帯
    df = df[(df['Close'] >= 100) & (df['Close'] <= 1500)]
    print(f"1. Price (100-1500円): {len(df):4d}/{total} ({len(df)/total*100:5.1f}%)")

    # 流動性
    df = df[df['Volume'] * df['Close'] >= 100_000_000]
    print(f"2. Liquidity >= 100M: {len(df):4d}/{total} ({len(df)/total*100:5.1f}%)")

    # ATR範囲
    df = df[(df['atr14_pct'] >= 1.0) & (df['atr14_pct'] <= 3.5)]
    print(f"3. ATR (1.0-3.5%):    {len(df):4d}/{total} ({len(df)/total*100:5.1f}%)")

    # 変動率
    df = df[(df['change_pct'] >= -3.0) & (df['change_pct'] <= 3.0)]
    print(f"4. Change (-3~+3%):   {len(df):4d}/{total} ({len(df)/total*100:5.1f}%)")

    entry_before_rating = len(df)
    print(f"\n→ Entry候補（overall_rating適用前）: {entry_before_rating}件")

    print()

    # Active条件を段階的に適用
    print("--- Active条件 (上級者向け) ---")

    df = df_latest.copy()

    # 価格帯
    df = df[(df['Close'] >= 100) & (df['Close'] <= 3000)]
    print(f"1. Price (100-3000円): {len(df):4d}/{total} ({len(df)/total*100:5.1f}%)")

    # 流動性（OR条件）
    df = df[(df['Volume'] * df['Close'] >= 50_000_000) | (df['vol_ratio'] >= 150)]
    print(f"2. Liquidity (50M or vol>=150%): {len(df):4d}/{total} ({len(df)/total*100:5.1f}%)")

    # ATR
    df = df[df['atr14_pct'] >= 2.5]
    print(f"3. ATR >= 2.5%:       {len(df):4d}/{total} ({len(df)/total*100:5.1f}%)")

    # 変動率（絶対値）
    df = df[df['change_pct'].abs() >= 2.0]
    print(f"4. |Change| >= 2.0%:  {len(df):4d}/{total} ({len(df)/total*100:5.1f}%)")

    active_before_rating = len(df)
    print(f"\n→ Active候補（overall_rating適用前）: {active_before_rating}件")

    return entry_before_rating, active_before_rating


def quick_filter_and_evaluate(df_latest: pd.DataFrame, meta_df: pd.DataFrame, client: JQuantsClient):
    """高速フィルタリング → テクニカル評価 → 最終選定"""
    print("\n" + "=" * 80)
    print("高速フィルタリング戦略")
    print("=" * 80)

    total = len(df_latest)

    # STEP 1: 単純条件で高速フィルタリング（Entry/Active共通の緩い条件）
    print(f"\n[STEP 1] 単純条件フィルタリング（テクニカル評価前）")
    print(f"  元データ: {total}件")

    df_filtered = df_latest[
        (df_latest['Close'] >= 100) &
        (df_latest['Close'] <= 3000) &  # Active上限
        (df_latest['Volume'] * df_latest['Close'] >= 50_000_000)  # Active下限
    ].copy()

    filtered_count = len(df_filtered)
    print(f"  絞り込み後: {filtered_count}件 ({filtered_count/total*100:.1f}%)")
    print(f"  削減率: {(total-filtered_count)/total*100:.1f}%")

    # STEP 2: 絞られた銘柄のみテクニカル評価
    print(f"\n[STEP 2] テクニカル評価実行中（{filtered_count}件）...")
    print("  ※ 時間がかかります。お待ちください...")

    screener = ScalpingScreener()
    df_evaluated = screener.evaluate_technical_ratings(df_filtered)

    # overall_rating分布を表示
    print(f"\n  overall_rating分布:")
    rating_counts = df_evaluated['overall_rating'].value_counts()
    for rating, count in rating_counts.items():
        print(f"    {rating}: {count}件 ({count/filtered_count*100:.1f}%)")

    # STEP 3: Entry/Active選定
    print(f"\n[STEP 3] Entry/Active選定")

    # Entry選定
    entry_tickers = set()
    df_entry = screener.generate_entry_list(df_evaluated, meta_df, top_n=100)
    if not df_entry.empty:
        entry_tickers = set(df_entry['ticker'])

    print(f"  Entry候補: {len(df_entry)}件")
    if len(df_entry) > 0:
        print(f"    Top 5:")
        for i, row in df_entry.head(5).iterrows():
            print(f"      {row['ticker']}: {row['stock_name']} - {row['key_signal']}")

    # Active選定
    df_active = screener.generate_active_list(df_evaluated, meta_df, entry_tickers, top_n=100)

    print(f"  Active候補: {len(df_active)}件")
    if len(df_active) > 0:
        print(f"    Top 5:")
        for i, row in df_active.head(5).iterrows():
            print(f"      {row['ticker']}: {row['stock_name']} - {row['key_signal']}")

    return df_entry, df_active


def suggest_parameter_adjustments(entry_count: int, active_count: int):
    """パラメータ調整案を提示"""
    print("\n" + "=" * 80)
    print("パラメータ調整案")
    print("=" * 80)

    target = 15

    print(f"\n目標: Entry {target}件前後、Active {target}件前後")
    print(f"現状: Entry {entry_count}件、Active {active_count}件")
    print()

    # Entry調整案
    if entry_count < target * 0.7:  # 10件未満
        print("【Entry】候補数が少なすぎます。条件を緩和してください:")
        print("  案1: 価格帯を拡大 (100-1500円 → 100-2000円)")
        print("  案2: ATR範囲を拡大 (1.0-3.5% → 0.8-4.0%)")
        print("  案3: 変動率を拡大 (-3~+3% → -5~+5%)")
        print("  案4: 流動性を緩和 (100M → 80M)")
    elif entry_count > target * 1.5:  # 23件以上
        print("【Entry】候補数が多すぎます。条件を厳しくしてください:")
        print("  案1: 価格帯を縮小 (100-1500円 → 200-1200円)")
        print("  案2: ATR範囲を縮小 (1.0-3.5% → 1.2-3.0%)")
        print("  案3: 変動率を縮小 (-3~+3% → -2~+2%)")
        print("  案4: top_nを調整 (現在100 → 15)")
    else:
        print("【Entry】候補数は適正範囲です！")

    print()

    # Active調整案
    if active_count < target * 0.7:
        print("【Active】候補数が少なすぎます。条件を緩和してください:")
        print("  案1: ATR下限を緩和 (2.5% → 2.0%)")
        print("  案2: 変動率を緩和 (|change| >= 2.0% → >= 1.5%)")
        print("  案3: 流動性を緩和 (50M or vol>=150% → 40M or vol>=130%)")
    elif active_count > target * 1.5:
        print("【Active】候補数が多すぎます。条件を厳しくしてください:")
        print("  案1: ATR下限を厳しく (2.5% → 3.0%)")
        print("  案2: 変動率を厳しく (|change| >= 2.0% → >= 2.5%)")
        print("  案3: top_nを調整 (現在100 → 15)")
    else:
        print("【Active】候補数は適正範囲です！")


def main() -> int:
    print("=" * 80)
    print("スキャルピングロジック調整ツール（ローカル実行）")
    print("=" * 80)

    # データ読み込み
    print("\n[準備] データ読み込み中...")

    # J-Quants接続確認
    try:
        client = JQuantsClient()
        print(f"  ✓ J-Quants接続: {client.plan}")
    except Exception as e:
        print(f"  ✗ J-Quants接続失敗: {e}")
        return 1

    # meta_jquants.parquet を使用
    print("  meta_jquants.parquet からデータ取得します...")

    meta_jquants_path = PARQUET_DIR / "meta_jquants.parquet"

    if not meta_jquants_path.exists():
        print(f"  ✗ meta_jquants.parquet が見つかりません: {meta_jquants_path}")
        print("  ヒント: scripts/pipeline/create_meta_jquants.py を先に実行してください")
        return 1

    try:
        # meta_jquants読み込み
        meta_jq = pd.read_parquet(meta_jquants_path)
        print(f"  ✓ meta_jquants読み込み: {len(meta_jq)}件")

        # ティッカーリスト取得
        tickers = meta_jq['ticker'].unique().tolist()
        codes = [t.replace(".T", "") for t in tickers]

        # 株価データ取得（過去60日分）
        print(f"  株価データ取得中（{len(codes)}銘柄、過去60日分）...")
        print("  ※ J-Quants APIを使用するため時間がかかります（1-2分）")

        fetcher = JQuantsFetcher(client)

        from datetime import datetime, timedelta
        to_date = datetime.now().date()
        from_date = to_date - timedelta(days=60)

        df_prices = fetcher.get_prices_daily_batch(codes, from_date=from_date, to_date=to_date, batch_delay=0.5)

        if df_prices.empty:
            print(f"  ✗ 株価データ取得失敗")
            return 1

        # yfinance形式に変換
        df_prices = fetcher.convert_to_yfinance_format(df_prices)

        print(f"  ✓ 株価データ取得: {len(df_prices)}件")

    except Exception as e:
        print(f"  ✗ データ取得失敗: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # メタデータ読み込み
    meta_path = PARQUET_DIR / "meta.parquet"
    if meta_path.exists():
        meta_df = pd.read_parquet(meta_path)
        print(f"  ✓ メタデータ読み込み: {len(meta_df)}件")
    else:
        meta_df = pd.DataFrame()
        print(f"  ⚠ メタデータなし")

    # テクニカル指標計算
    print("\n  テクニカル指標計算中...")
    screener = ScalpingScreener()
    df_latest = screener.calculate_technical_indicators(df_prices)

    # 最新日のみ抽出
    latest_date = df_latest['date'].max()
    df_latest = df_latest[df_latest['date'] == latest_date].copy()
    print(f"  ✓ 最新日データ: {latest_date.strftime('%Y-%m-%d')} ({len(df_latest)}件)")

    # 段階的条件分析（高速）
    entry_before, active_before = analyze_conditions_step_by_step(df_latest)

    # 自動的にテクニカル評価を実行（ローカル実行時）
    import os
    auto_run = os.getenv("AUTO_RUN", "1")  # デフォルトで自動実行

    print("\n" + "=" * 80)
    if auto_run == "1":
        print("テクニカル評価を自動実行します...")
        print("=" * 80)
        choice = "1"
    else:
        print("次のステップを選択してください:")
        print("=" * 80)
        print(f"1. テクニカル評価を実行（{entry_before + active_before}件程度が対象、5-10分）")
        print("2. パラメータ調整案のみ表示（高速）")
        print("3. 終了")
        choice = input("\n選択 (1/2/3): ").strip()

    if choice == "1":
        # テクニカル評価実行
        df_entry, df_active = quick_filter_and_evaluate(df_latest, meta_df, client)

        # 調整案提示
        suggest_parameter_adjustments(len(df_entry), len(df_active))

    elif choice == "2":
        # 暫定的な候補数で調整案提示（overall_rating考慮せず）
        print("\n※ overall_rating適用前の候補数で暫定的に提案します")
        suggest_parameter_adjustments(entry_before, active_before)
    else:
        print("\n終了します。")
        return 0

    print("\n" + "=" * 80)
    print("✅ 完了")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
