#!/usr/bin/env python3
"""
generate_mock_screened_scored.py
テクニカル指標付きデータにスコアリングを適用（全件）

入力:
- data/parquet/test/mock_screened_raw_20251020.parquet

出力:
- data/parquet/test/mock_screened_scored_20251020.parquet

処理内容:
1. 生データ読み込み
2. Entry条件判定 + スコアリング（該当全件）
3. Active条件判定 + スコアリング（該当全件）
4. 両スコアを1ファイルに統合して保存

出力カラム:
- 基本情報: ticker, stock_name, market, sectors, date
- 価格・テクニカル: Close, change_pct, Volume, vol_ratio, atr14_pct, rsi14, overall_rating
- Entry評価: entry_filter_passed, entry_score, entry_rank
- Active評価: active_filter_passed, active_score, active_rank
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
from common_cfg.paths import PARQUET_DIR

TEST_DIR = PARQUET_DIR / "test"
INPUT_PATH = TEST_DIR / "mock_screened_500stocks_raw.parquet"
OUTPUT_PATH = TEST_DIR / "mock_screened_500stocks_scored.parquet"


def apply_entry_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """
    Entry条件のフィルタ判定 + スコアリング

    フィルタ条件:
    - 価格帯: 100-1500円
    - 流動性: Volume × Close >= 100,000,000円
    - ボラティリティ: 1.0% <= ATR <= 3.5%
    - 変動率: -3.0% <= change_pct <= +3.0%
    - テクニカル評価: 売り系以外（中立 + 買い + 強い買い）

    スコアリング（100点満点）:
    1. overall_rating: 40点
    2. change_pct: 20点
    3. vol_ratio: 20点
    4. atr14_pct: 10点
    5. rsi14: 10点
    """
    df = df.copy()

    # フィルタ条件判定
    filter_conditions = (
        (df['Close'] >= 100) &
        (df['Close'] <= 1500) &
        (df['Volume'] * df['Close'] >= 100_000_000) &
        (df['atr14_pct'] >= 1.0) &
        (df['atr14_pct'] <= 3.5) &
        (df['change_pct'] >= -3.0) &
        (df['change_pct'] <= 3.0) &
        (~df['overall_rating'].isin(['売り', '強い売り']))
    )

    df['entry_filter_passed'] = filter_conditions

    # スコアリング（条件通過銘柄のみ）- データ相関分析V2（より aggressive）
    df['entry_score'] = 0.0

    # 1. RSI（最重要指標）: 50点
    # 相関分析: RSI高い→損失（-0.1003 = 最強の負相関）
    # → RSI低い（30-45）を最も強く評価する
    def score_rsi(rsi):
        if pd.isna(rsi):
            return 20
        # RSI 30-45: 売られ過ぎから回復期（最高評価）
        if 30 <= rsi <= 40:
            return 50  # スイートスポット
        elif 40 < rsi <= 45:
            return 45  # 良好
        elif 25 <= rsi < 30:
            return 40  # 底打ち期
        elif 45 < rsi <= 50:
            return 30  # 中立やや良
        elif 50 < rsi <= 55:
            return 20  # 中立
        elif 55 < rsi <= 60:
            return 10  # やや買われ過ぎ
        else:
            return 0   # 極端な値（0, 100含む）は完全減点

    df.loc[filter_conditions, 'entry_score'] += df.loc[filter_conditions, 'rsi14'].apply(score_rsi)

    # 2. 変動率（重要指標）: 30点
    # 相関分析: change_pct高い→損失（-0.0730）
    # → 小さい変動を強く評価
    def score_change(change_pct):
        abs_change = abs(change_pct)
        if 0.0 <= abs_change <= 0.3:
            return 30  # ほぼ動いていない
        elif 0.3 < abs_change <= 0.6:
            return 25  # 小さい変動
        elif 0.6 < abs_change <= 1.0:
            return 18  # やや小さい
        elif 1.0 < abs_change <= 1.5:
            return 10  # 中程度
        elif 1.5 < abs_change <= 2.0:
            return 3   # 大きい
        else:
            return 0   # 極端な変動は完全減点

    df.loc[filter_conditions, 'entry_score'] += df.loc[filter_conditions, 'change_pct'].apply(score_change)

    # 3. ATR（補助指標）: 10点
    # 相関分析: atr14_pct弱い負相関（-0.0336）→ 重みを減らす
    def score_atr(atr_pct):
        if pd.isna(atr_pct):
            return 5
        if 1.5 <= atr_pct <= 2.5:
            return 10
        elif 1.0 <= atr_pct < 1.5 or 2.5 < atr_pct <= 3.0:
            return 7
        else:
            return 3

    df.loc[filter_conditions, 'entry_score'] += df.loc[filter_conditions, 'atr14_pct'].apply(score_atr)

    # 4. 出来高比率（補助指標）: 10点
    # 相関分析: vol_ratio効果なし（-0.0021）→ 重みを大幅に減らす
    def score_vol_ratio(vol_ratio):
        if pd.isna(vol_ratio):
            return 5
        if 100 <= vol_ratio <= 130:
            return 10
        elif 80 <= vol_ratio < 100:
            return 7
        elif 130 < vol_ratio <= 150:
            return 5
        else:
            return 2

    df.loc[filter_conditions, 'entry_score'] += df.loc[filter_conditions, 'vol_ratio'].apply(score_vol_ratio)

    # スコアでランク付け（日付ごとに条件通過銘柄のみ）
    df['entry_rank'] = np.nan

    if 'date' in df.columns:
        # 日付ごとにランク付け
        for date in df['date'].unique():
            date_mask = df['date'] == date
            date_passed = date_mask & filter_conditions

            if date_passed.sum() > 0:
                date_df = df.loc[date_passed].copy()
                date_df = date_df.sort_values('entry_score', ascending=False).reset_index(drop=True)
                date_df['entry_rank'] = range(1, len(date_df) + 1)
                df.loc[date_passed, 'entry_rank'] = date_df['entry_rank'].values
    else:
        # 日付カラムがない場合は全体でランク付け
        passed = df[filter_conditions].copy()
        if len(passed) > 0:
            passed = passed.sort_values('entry_score', ascending=False).reset_index(drop=True)
            passed['entry_rank'] = range(1, len(passed) + 1)
            df.loc[filter_conditions, 'entry_rank'] = passed['entry_rank'].values

    return df


def apply_active_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """
    Active条件のフィルタ判定 + スコアリング

    フィルタ条件:
    - 価格帯: 100-3000円
    - 流動性: Volume × Close >= 50,000,000円 OR vol_ratio >= 150%
    - ボラティリティ: ATR >= 2.5%
    - 変動率: |change_pct| >= 2.0%
    - テクニカル評価: 条件なし

    スコアリング（100点満点）:
    1. ATR: 40点
    2. change_pct: 30点
    3. vol_ratio: 20点
    4. rsi14: 10点
    """
    df = df.copy()

    # フィルタ条件判定
    filter_conditions = (
        (df['Close'] >= 100) &
        (df['Close'] <= 3000) &
        ((df['Volume'] * df['Close'] >= 50_000_000) | (df['vol_ratio'] >= 150)) &
        (df['atr14_pct'] >= 2.5) &
        (df['change_pct'].abs() >= 2.0)
    )

    df['active_filter_passed'] = filter_conditions

    # スコアリング（条件通過銘柄のみ）
    df['active_score'] = 0.0

    # 1. ATR（高ボラティリティ重視）: 40点
    def score_atr_active(atr_pct):
        if pd.isna(atr_pct):
            return 5
        if atr_pct >= 5.0:
            return 40
        elif 4.0 <= atr_pct < 5.0:
            return 35
        elif 3.0 <= atr_pct < 4.0:
            return 25
        elif 2.5 <= atr_pct < 3.0:
            return 15
        else:
            return 5

    df.loc[filter_conditions, 'active_score'] += df.loc[filter_conditions, 'atr14_pct'].apply(score_atr_active)

    # 2. 変動率（大きな動き）: 30点
    def score_change_active(change_pct):
        abs_change = abs(change_pct)
        if abs_change >= 5.0:
            return 30
        elif 4.0 <= abs_change < 5.0:
            return 25
        elif 3.0 <= abs_change < 4.0:
            return 20
        elif 2.0 <= abs_change < 3.0:
            return 15
        else:
            return 5

    df.loc[filter_conditions, 'active_score'] += df.loc[filter_conditions, 'change_pct'].apply(score_change_active)

    # 3. 出来高比率（急騰・急落の兆候）: 20点
    def score_vol_ratio_active(vol_ratio):
        if pd.isna(vol_ratio):
            return 5
        if vol_ratio >= 200:
            return 20
        elif 150 <= vol_ratio < 200:
            return 15
        elif 100 <= vol_ratio < 150:
            return 10
        else:
            return 5

    df.loc[filter_conditions, 'active_score'] += df.loc[filter_conditions, 'vol_ratio'].apply(score_vol_ratio_active)

    # 4. RSI（極端な状態を許容）: 10点
    def score_rsi_active(rsi):
        if pd.isna(rsi):
            return 4
        # 反転期待（買われすぎ・売られすぎ）
        if (20 <= rsi <= 30) or (70 <= rsi <= 80):
            return 10
        elif (30 < rsi <= 40) or (60 <= rsi < 70):
            return 8
        elif 40 < rsi < 60:  # 中立は低評価
            return 6
        else:
            return 4

    df.loc[filter_conditions, 'active_score'] += df.loc[filter_conditions, 'rsi14'].apply(score_rsi_active)

    # スコアでランク付け（日付ごとに条件通過銘柄のみ）
    df['active_rank'] = np.nan

    if 'date' in df.columns:
        # 日付ごとにランク付け
        for date in df['date'].unique():
            date_mask = df['date'] == date
            date_passed = date_mask & filter_conditions

            if date_passed.sum() > 0:
                date_df = df.loc[date_passed].copy()
                date_df = date_df.sort_values('active_score', ascending=False).reset_index(drop=True)
                date_df['active_rank'] = range(1, len(date_df) + 1)
                df.loc[date_passed, 'active_rank'] = date_df['active_rank'].values
    else:
        # 日付カラムがない場合は全体でランク付け
        passed = df[filter_conditions].copy()
        if len(passed) > 0:
            passed = passed.sort_values('active_score', ascending=False).reset_index(drop=True)
            passed['active_rank'] = range(1, len(passed) + 1)
            df.loc[filter_conditions, 'active_rank'] = passed['active_rank'].values

    return df


def main() -> int:
    """スコアリング適用"""
    print("=" * 60)
    print("Generate Mock Screened Scored Data")
    print("=" * 60)

    # [STEP 1] 生データ読み込み
    print("\n[STEP 1] Loading raw screened data...")
    try:
        if not INPUT_PATH.exists():
            print(f"  ✗ File not found: {INPUT_PATH}")
            print(f"  → Please run generate_mock_screened_raw.py first")
            return 1

        df = pd.read_parquet(INPUT_PATH)
        print(f"  ✓ Loaded {len(df):,} rows")
        if 'date' in df.columns:
            print(f"  ✓ Date range: {df['date'].min()} to {df['date'].max()}")
            print(f"  ✓ Number of dates: {df['date'].nunique()}")
            print(f"  ✓ Stocks: {df['ticker'].nunique()}")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 2] Entry スコアリング
    print("\n[STEP 2] Applying Entry scoring...")
    try:
        df = apply_entry_scoring(df)
        entry_passed = df['entry_filter_passed'].sum()
        total_rows = len(df)
        print(f"  ✓ Entry filter passed: {entry_passed:,} rows ({entry_passed/total_rows*100:.1f}%)")

        if entry_passed > 0:
            entry_scored = df[df['entry_filter_passed']]
            print(f"  ✓ Entry score range: {entry_scored['entry_score'].min():.1f} - {entry_scored['entry_score'].max():.1f}")

            if 'date' in df.columns:
                # 日付ごとの統計
                dates_with_entry = entry_scored.groupby('date').size()
                print(f"  ✓ Dates with Entry candidates: {len(dates_with_entry)}")
                print(f"  ✓ Avg candidates per day: {dates_with_entry.mean():.1f}")
            else:
                # Top 15閾値
                if len(entry_scored) >= 15:
                    print(f"  ✓ Top 15 threshold: {entry_scored.nsmallest(15, 'entry_rank')['entry_score'].min():.1f}")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 3] Active スコアリング
    print("\n[STEP 3] Applying Active scoring...")
    try:
        df = apply_active_scoring(df)
        active_passed = df['active_filter_passed'].sum()
        total_rows = len(df)
        print(f"  ✓ Active filter passed: {active_passed:,} rows ({active_passed/total_rows*100:.1f}%)")

        if active_passed > 0:
            active_scored = df[df['active_filter_passed']]
            print(f"  ✓ Active score range: {active_scored['active_score'].min():.1f} - {active_scored['active_score'].max():.1f}")

            if 'date' in df.columns:
                # 日付ごとの統計
                dates_with_active = active_scored.groupby('date').size()
                print(f"  ✓ Dates with Active candidates: {len(dates_with_active)}")
                print(f"  ✓ Avg candidates per day: {dates_with_active.mean():.1f}")
            else:
                # Top 15閾値
                if len(active_scored) >= 15:
                    print(f"  ✓ Top 15 threshold: {active_scored.nsmallest(15, 'active_rank')['active_score'].min():.1f}")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # [STEP 4] 保存
    print("\n[STEP 4] Saving scored data...")
    try:
        TEST_DIR.mkdir(parents=True, exist_ok=True)

        df.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)
        print(f"  ✓ Saved: {OUTPUT_PATH}")

        # データサマリー
        print("\n--- Data Summary ---")
        print(f"Total rows: {len(df):,}")
        if 'date' in df.columns:
            print(f"Unique stocks: {df['ticker'].nunique():,}")
            print(f"Date range: {df['date'].min()} to {df['date'].max()}")
            print(f"Number of dates: {df['date'].nunique()}")
        print(f"Entry candidates: {entry_passed:,}")
        print(f"Active candidates: {active_passed:,}")
        print(f"Both passed: {(df['entry_filter_passed'] & df['active_filter_passed']).sum():,}")
        print(f"File size: {OUTPUT_PATH.stat().st_size / 1024 / 1024:.2f} MB")

    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 60)
    print("✅ Scored data generated successfully!")
    print("=" * 60)
    print(f"\nOutput: {OUTPUT_PATH}")
    print("\nNext steps:")
    print("1. Analyze score distributions")
    print("2. Check top 50-100 candidates")
    print("3. Identify why same stocks are always selected")
    print("4. Tune conditions to increase variety")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
