#!/usr/bin/env python3
"""
compare_v2_0_3_vs_v2_1_0.py

v2.0.3 と v2.1.0 のスコアリングロジックを比較し、判定差分を分析

入力: improvement/data/grok_analysis_merged_20251121_with_indicators.parquet
出力: improvement/data/v2_1_0_comparison_results.parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

# パス設定
IMPROVEMENT_DATA_DIR = ROOT / "improvement" / "data"
INPUT_FILE = IMPROVEMENT_DATA_DIR / "grok_analysis_merged_20251121_with_indicators.parquet"
OUTPUT_FILE = IMPROVEMENT_DATA_DIR / "v2_1_0_comparison_results.parquet"


def calculate_v2_0_3_score_and_action(row: pd.Series) -> tuple[int, str, list[str]]:
    """
    v2.0.3 のスコアリングロジックを適用

    v2.0.3 ルール:
    - 5,000-10,000円: 強制「買い」
    - 10,000円以上: 強制「売り」
    - それ以外: スコアベース判定

    Returns:
        (score, action, reasons)
    """
    score = 0
    reasons = []
    prev_close = row.get('prev_day_close')

    # 価格帯ロジック（v2.0.3）
    if pd.notna(prev_close):
        if 5000 <= prev_close < 10000:
            return (100, '買い', ['5,000-10,000円（強制買い）'])
        elif prev_close >= 10000:
            return (-100, '売り', ['10,000円以上（強制売り）'])

    # スコアベース判定
    # Grokランクスコア
    grok_rank = row.get('grok_rank', 999)
    total_stocks = row.get('total_stocks', 1)

    relative_position = (grok_rank - 1) / max(total_stocks - 1, 1)

    if relative_position <= 0.25:
        score += 40
        reasons.append(f'Grokランク上位25%')
    elif relative_position <= 0.50:
        score += 20
        reasons.append(f'Grokランク上位50%')
    elif relative_position <= 0.75:
        pass
    else:
        score -= 10
        reasons.append(f'Grokランク下位25%')

    # その他のファクター（簡略版）
    if pd.notna(row.get('prev_day_change_pct')):
        change_pct = row['prev_day_change_pct']
        if change_pct < -5:
            score += 15
            reasons.append(f'前日-5%以上下落')
        elif change_pct > 10:
            score -= 10
            reasons.append(f'前日+10%以上急騰')

    # アクション判定
    if score >= 30:
        action = '買い'
    elif score <= -20:
        action = '売り'
    else:
        action = '静観'

    return (score, action, reasons)


def calculate_v2_1_0_score_and_action(row: pd.Series) -> tuple[int, str, list[str]]:
    """
    v2.1.0 のスコアリングロジックを適用

    v2.1.0 改善:
    - Grokランク勝率を最重視（配点変更）
    - RSI < 30: +20、RSI > 70: -10
    - volume_change_20d > 2.0: +15
    - -2.0 < price_vs_sma5_pct < 0: +15
    - 価格帯ロジックは維持（5,000-10,000円: 買い、10,000円以上: 売り）

    Returns:
        (score, action, reasons)
    """
    score = 0
    reasons = []
    prev_close = row.get('prev_day_close')

    # 価格帯ロジック（v2.0.3と同じ）
    if pd.notna(prev_close):
        if 5000 <= prev_close < 10000:
            return (100, '買い', ['5,000-10,000円（強制買い）'])
        elif prev_close >= 10000:
            return (-100, '売り', ['10,000円以上（強制売り）'])

    # スコアベース判定（v2.1.0: 配点変更）
    # 1. Grokランク勝率を最重視
    grok_rank = row.get('grok_rank', 999)
    total_stocks = row.get('total_stocks', 1)

    # バックテスト勝率（仮に勝率情報がない場合はランクのみで判定）
    relative_position = (grok_rank - 1) / max(total_stocks - 1, 1)

    if relative_position <= 0.25:
        score += 50  # v2.0.3: 40 → v2.1.0: 50
        reasons.append(f'Grokランク上位25%（重視）')
    elif relative_position <= 0.50:
        score += 30  # v2.0.3: 20 → v2.1.0: 30
        reasons.append(f'Grokランク上位50%')
    elif relative_position <= 0.75:
        pass
    else:
        score -= 30  # v2.0.3: -10 → v2.1.0: -30
        reasons.append(f'Grokランク下位25%（減点強化）')

    # 2. RSI追加（新規）
    rsi_14d = row.get('rsi_14d')
    if pd.notna(rsi_14d):
        if rsi_14d < 30:
            score += 20
            reasons.append(f'RSI {rsi_14d:.1f}（売られすぎ）')
        elif rsi_14d > 70:
            score -= 10
            reasons.append(f'RSI {rsi_14d:.1f}（買われすぎ）')

    # 3. 出来高急増（新規）
    volume_change_20d = row.get('volume_change_20d')
    if pd.notna(volume_change_20d) and volume_change_20d > 2.0:
        score += 15
        reasons.append(f'出来高{volume_change_20d:.1f}倍（注目急増）')

    # 4. 5日線押し目（新規）
    price_vs_sma5_pct = row.get('price_vs_sma5_pct')
    if pd.notna(price_vs_sma5_pct) and -2.0 < price_vs_sma5_pct < 0:
        score += 15
        reasons.append(f'5日線押し目{price_vs_sma5_pct:.1f}%（反発期待）')

    # その他のファクター（v2.0.3と同じ）
    if pd.notna(row.get('prev_day_change_pct')):
        change_pct = row['prev_day_change_pct']
        if change_pct < -5:
            score += 15
            reasons.append(f'前日-5%以上下落')
        elif change_pct > 10:
            score -= 10
            reasons.append(f'前日+10%以上急騰')

    # アクション判定
    if score >= 30:
        action = '買い'
    elif score <= -20:
        action = '売り'
    else:
        action = '静観'

    return (score, action, reasons)


def main() -> int:
    print("=" * 60)
    print("v2.0.3 vs v2.1.0 Comparison")
    print("=" * 60)

    # [STEP 1] データ読み込み
    print("\n[STEP 1] Loading data...")

    if not INPUT_FILE.exists():
        print(f"  ✗ File not found: {INPUT_FILE}")
        return 1

    df = pd.read_parquet(INPUT_FILE)
    print(f"  ✓ Loaded: {len(df)} records, {len(df.columns)} columns")

    # [STEP 2] v2.0.3 判定（元データの v2_action を使用）
    print("\n[STEP 2] Using original v2_action (v2.0.3)...")

    # 元データに v2_action, v2_score, v2_reasons_json が含まれているのでそれを使う
    if 'v2_action' not in df.columns:
        print("  ✗ Error: v2_action column not found in input data")
        return 1

    df['v2_0_3_action'] = df['v2_action']
    df['v2_0_3_score'] = df.get('v2_score', 0)  # スコアがあれば使う
    df['v2_0_3_reasons'] = df.get('v2_reasons_json', '[]')  # 理由があれば使う

    print(f"  ✓ v2.0.3 actions (original): {df['v2_0_3_action'].value_counts().to_dict()}")

    # [STEP 3] v2.1.0スコア計算
    print("\n[STEP 3] Calculating v2.1.0 scores...")

    v2_1_0_scores = []
    v2_1_0_actions = []
    v2_1_0_reasons = []

    for _, row in df.iterrows():
        score, action, reasons = calculate_v2_1_0_score_and_action(row)
        v2_1_0_scores.append(score)
        v2_1_0_actions.append(action)
        v2_1_0_reasons.append(reasons)

    df['v2_1_0_score'] = v2_1_0_scores
    df['v2_1_0_action'] = v2_1_0_actions
    df['v2_1_0_reasons'] = v2_1_0_reasons

    print(f"  ✓ v2.1.0 actions: {df['v2_1_0_action'].value_counts().to_dict()}")

    # [STEP 4] 差分分析
    print("\n[STEP 4] Analyzing differences...")

    df['action_changed'] = df['v2_0_3_action'] != df['v2_1_0_action']
    df['score_diff'] = df['v2_1_0_score'] - df['v2_0_3_score']

    changed_count = df['action_changed'].sum()
    print(f"  ✓ Action changed: {changed_count}/{len(df)} records ({changed_count/len(df)*100:.1f}%)")

    if changed_count > 0:
        print(f"\n  Changes breakdown:")
        for _, row in df[df['action_changed']].iterrows():
            print(f"    {row['ticker']} ({row['backtest_date']}): {row['v2_0_3_action']} → {row['v2_1_0_action']} (score: {row['v2_0_3_score']} → {row['v2_1_0_score']})")

    # [STEP 5] 保存
    print("\n[STEP 5] Saving results...")
    df.to_parquet(OUTPUT_FILE, engine="pyarrow", index=False)
    print(f"  ✓ Saved: {OUTPUT_FILE}")
    print(f"  Columns: {len(df.columns)}")

    print("\n✅ Comparison completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
