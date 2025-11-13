#!/usr/bin/env python3
"""
analyze_morning_afternoon.py
前場・後場の動きを分析（塊として）

実行方法:
    python3 scripts/analyze_morning_afternoon.py

分析内容:
    1. 11:30前場引け時点の状況（利益/損失の分布）
    2. TOPIX下落時の「前場損失→12:30撤退」シミュレーション
    3. TOPIX下落時の「前場利益→そのまま保有」の効果
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
from common_cfg.paths import PARQUET_DIR

# パス定義
ARCHIVE_WITH_MARKET_PATH = PARQUET_DIR / "backtest" / "grok_trending_archive_with_market.parquet"


def main():
    """前場・後場分析のメイン処理"""

    if not ARCHIVE_WITH_MARKET_PATH.exists():
        print(f"[ERROR] マーケットデータ付きアーカイブが見つかりません: {ARCHIVE_WITH_MARKET_PATH}")
        return

    # データ読み込み
    df = pd.read_parquet(ARCHIVE_WITH_MARKET_PATH)

    if df.empty:
        print("[WARN] バックテストデータが空です")
        return

    # 期間
    start_date = df['backtest_date'].min()
    end_date = df['backtest_date'].max()
    total_stocks = len(df)

    print("=" * 80)
    print(f"前場・後場パフォーマンス分析（塊として）")
    print(f"期間: {start_date} 〜 {end_date}")
    print(f"総取引数: {total_stocks}銘柄")
    print("=" * 80)
    print()

    # 前場引け時点の利益を計算（Phase1 = 9:00→11:30）
    df['morning_profit'] = df['profit_per_100_shares_phase1']

    # 後場の利益を計算（Phase2 - Phase1 = 後場のみの利益）
    df['afternoon_profit'] = df['profit_per_100_shares_phase2'] - df['profit_per_100_shares_phase1']

    # TOPIX下落フラグ
    df_with_topix = df[df['daily_topix_return'].notna()].copy()
    df_with_topix['topix_down'] = df_with_topix['daily_topix_return'] < 0

    print("=" * 80)
    print("【1. 前場引け（11:30）時点の状況】")
    print("=" * 80)
    print()

    print(f"前場利益プラス: {(df['morning_profit'] > 0).sum()}銘柄 ({(df['morning_profit'] > 0).sum() / len(df) * 100:.1f}%)")
    print(f"前場利益マイナス: {(df['morning_profit'] < 0).sum()}銘柄 ({(df['morning_profit'] < 0).sum() / len(df) * 100:.1f}%)")
    print()
    print(f"前場平均利益: ¥{df['morning_profit'].mean():.1f}")
    print(f"前場中央値: ¥{df['morning_profit'].median():.1f}")
    print()

    # 前場の利益分布
    print("前場利益の分布:")
    print(f"  +3%以上: {(df['morning_profit'] >= df['buy_price'] * 3).sum()}銘柄")
    print(f"  +1〜3%: {((df['morning_profit'] >= df['buy_price']) & (df['morning_profit'] < df['buy_price'] * 3)).sum()}銘柄")
    print(f"  0〜+1%: {((df['morning_profit'] >= 0) & (df['morning_profit'] < df['buy_price'])).sum()}銘柄")
    print(f"  0〜-1%: {((df['morning_profit'] < 0) & (df['morning_profit'] >= -df['buy_price'])).sum()}銘柄")
    print(f"  -1〜-3%: {((df['morning_profit'] < -df['buy_price']) & (df['morning_profit'] >= -df['buy_price'] * 3)).sum()}銘柄")
    print(f"  -3%以下: {(df['morning_profit'] < -df['buy_price'] * 3).sum()}銘柄")
    print()

    print("=" * 80)
    print("【2. TOPIX下落時の戦略比較（塊として）】")
    print("=" * 80)
    print()

    # TOPIX下落時のデータ
    df_topix_down = df_with_topix[df_with_topix['topix_down']].copy()
    print(f"TOPIX下落日: {len(df_topix_down)}銘柄")
    print()

    # パターンA: 前場損失 → 12:30即撤退（前場損失で確定）
    morning_loss = df_topix_down[df_topix_down['morning_profit'] < 0].copy()
    pattern_a_loss = morning_loss['morning_profit'].sum()
    pattern_a_count = len(morning_loss)

    # パターンB: 前場損失 → 大引けまで保有（Phase2）
    pattern_b_profit = morning_loss['profit_per_100_shares_phase2'].sum()

    print("📉 前場損失 × TOPIX下落")
    print(f"   対象銘柄: {pattern_a_count}銘柄")
    print()
    print(f"   パターンA（12:30即撤退）: ¥{pattern_a_loss:,.1f}")
    print(f"   パターンB（大引けまで保有）: ¥{pattern_b_profit:,.1f}")
    print(f"   差額: ¥{pattern_b_profit - pattern_a_loss:,.1f}")
    if pattern_a_count > 0:
        print(f"   1銘柄あたり: ¥{(pattern_b_profit - pattern_a_loss) / pattern_a_count:,.1f}")
    print()

    if pattern_b_profit > pattern_a_loss:
        print(f"   ✅ 結論: 大引けまで保有の方が ¥{pattern_b_profit - pattern_a_loss:,.1f} 有利")
        print(f"           （後場で平均 ¥{morning_loss['afternoon_profit'].mean():,.1f} 回復）")
    else:
        print(f"   ✅ 結論: 12:30即撤退の方が ¥{pattern_a_loss - pattern_b_profit:,.1f} 有利")
        print(f"           （後場でさらに ¥{morning_loss['afternoon_profit'].mean():,.1f} 下落）")
    print()

    # パターンC: 前場利益 → そのまま保有
    morning_profit = df_topix_down[df_topix_down['morning_profit'] > 0].copy()
    pattern_c_morning = morning_profit['morning_profit'].sum()
    pattern_c_phase2 = morning_profit['profit_per_100_shares_phase2'].sum()
    pattern_c_count = len(morning_profit)

    print("📈 前場利益 × TOPIX下落")
    print(f"   対象銘柄: {pattern_c_count}銘柄")
    print()
    print(f"   パターンC（11:30即利確）: ¥{pattern_c_morning:,.1f}")
    print(f"   パターンD（大引けまで保有）: ¥{pattern_c_phase2:,.1f}")
    print(f"   差額: ¥{pattern_c_phase2 - pattern_c_morning:,.1f}")
    if pattern_c_count > 0:
        print(f"   1銘柄あたり: ¥{(pattern_c_phase2 - pattern_c_morning) / pattern_c_count:,.1f}")
    print()

    if pattern_c_phase2 > pattern_c_morning:
        print(f"   ✅ 結論: 大引けまで保有の方が ¥{pattern_c_phase2 - pattern_c_morning:,.1f} 有利")
        print(f"           （後場でさらに ¥{morning_profit['afternoon_profit'].mean():,.1f} 上昇）")
    else:
        print(f"   ✅ 結論: 11:30即利確の方が ¥{pattern_c_morning - pattern_c_phase2:,.1f} 有利")
        print(f"           （後場で ¥{morning_profit['afternoon_profit'].mean():,.1f} 吐き出す）")
    print()

    print("=" * 80)
    print("【3. TOPIX上昇時の参考データ】")
    print("=" * 80)
    print()

    df_topix_up = df_with_topix[~df_with_topix['topix_down']].copy()
    print(f"TOPIX上昇日: {len(df_topix_up)}銘柄")
    print()
    print(f"前場平均利益: ¥{df_topix_up['morning_profit'].mean():.1f}")
    print(f"後場平均利益: ¥{df_topix_up['afternoon_profit'].mean():.1f}")
    print(f"大引け平均利益（Phase2）: ¥{df_topix_up['profit_per_100_shares_phase2'].mean():.1f}")
    print()

    print("=" * 80)
    print("【4. 推奨戦略（塊として）】")
    print("=" * 80)
    print()

    # TOPIX下落時のベスト戦略
    topix_down_best_loss = max(pattern_a_loss, pattern_b_profit)
    topix_down_best_profit = max(pattern_c_morning, pattern_c_phase2)
    topix_down_total = topix_down_best_loss + topix_down_best_profit

    print("TOPIX下落時:")
    if pattern_b_profit > pattern_a_loss:
        print("  前場損失 → ❌ 12:30撤退せず、大引けまで保有")
    else:
        print("  前場損失 → ✅ 12:30で即撤退（損失確定）")

    if pattern_c_phase2 > pattern_c_morning:
        print("  前場利益 → ❌ 11:30利確せず、大引けまで保有")
    else:
        print("  前場利益 → ✅ 11:30で即利確")

    print(f"  → TOPIX下落時の最適利益: ¥{topix_down_total:,.1f}")
    print()

    # TOPIX上昇時
    topix_up_phase2 = df_topix_up['profit_per_100_shares_phase2'].sum()
    print("TOPIX上昇時:")
    print("  → 大引けまで保有（Phase2）")
    print(f"  → TOPIX上昇時の利益: ¥{topix_up_phase2:,.1f}")
    print()

    print("=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
