#!/usr/bin/env python3
"""
売買タイミング分析: 規模・状況別の傾向調査

分析項目:
1. ボラティリティ別（高ボラ/中ボラ/低ボラ）
2. 株価水準別（高価格帯/中価格帯/低価格帯）
3. 売買推奨別（買い/売り/静観）
4. スコア別（高スコア/低スコア）
5. 出来高別（高出来高/低出来高）
"""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / 'test_output'

# 入力ファイル
TIMING_ANALYSIS = OUTPUT_DIR / 'timing_analysis_results.parquet'
GROK_ANALYSIS = ROOT / 'data' / 'parquet' / 'backtest' / 'grok_analysis_merged.parquet'

# 出力ファイル
FACTOR_ANALYSIS_OUTPUT = OUTPUT_DIR / 'timing_factor_analysis.txt'


def analyze_by_factor(df, factor_name, factor_column):
    """特定の要因別に分析"""
    results = []
    results.append(f"\n{'='*60}")
    results.append(f"{factor_name}別の分析")
    results.append(f"{'='*60}\n")

    if factor_column not in df.columns:
        results.append(f"⚠️  {factor_column} カラムが存在しません\n")
        return results

    # 要因でグループ化
    for factor_value in sorted(df[factor_column].dropna().unique()):
        subset = df[df[factor_column] == factor_value]

        if len(subset) == 0:
            continue

        # 前場 vs 大引の比較
        morning_better = (subset['better_profit_timing'] == 'morning_close').sum()
        day_better = (subset['better_profit_timing'] == 'day_close').sum()
        total = len(subset)

        morning_pct = (morning_better / total * 100) if total > 0 else 0
        day_pct = (day_better / total * 100) if total > 0 else 0

        # 平均利益率
        avg_morning = subset['profit_morning_pct'].mean()
        avg_day = subset['profit_day_close_pct'].mean()

        # 勝率
        win_rate_morning = (subset['is_win_morning'].sum() / total * 100) if total > 0 else 0
        win_rate_day = (subset['is_win_day_close'].sum() / total * 100) if total > 0 else 0

        results.append(f"【{factor_value}】（{total}件）")
        results.append(f"  有利なタイミング:")
        results.append(f"    前場不成: {morning_better}件 ({morning_pct:.1f}%)")
        results.append(f"    大引不成: {day_better}件 ({day_pct:.1f}%)")
        results.append(f"  平均利益率:")
        results.append(f"    前場不成: {avg_morning:+.2f}%")
        results.append(f"    大引不成: {avg_day:+.2f}%")
        results.append(f"  勝率:")
        results.append(f"    前場不成: {win_rate_morning:.1f}%")
        results.append(f"    大引不成: {win_rate_day:.1f}%")

        # 推奨
        if morning_pct > day_pct:
            better = "前場不成が有利"
        elif day_pct > morning_pct:
            better = "大引不成が有利"
        else:
            better = "同等"
        results.append(f"  → {better}")
        results.append("")

    return results


def main():
    print("="*60)
    print("売買タイミング分析: 規模・状況別の傾向調査")
    print("="*60)

    # データ読み込み
    df = pd.read_parquet(TIMING_ANALYSIS)
    df['backtest_date'] = pd.to_datetime(df['backtest_date'])

    # grok_analysis_merged.parquetから追加情報を取得
    grok_df = pd.read_parquet(GROK_ANALYSIS)
    grok_df['backtest_date'] = pd.to_datetime(grok_df['backtest_date'])

    # 2025-11-14のデータのみ
    df = df[df['backtest_date'] == '2025-11-14'].copy()
    grok_df = grok_df[grok_df['backtest_date'] == '2025-11-14'].copy()

    # マージ（technical_dataからボラティリティと出来高を取得）
    merge_cols = ['ticker', 'backtest_date', 'daily_volatility', 'morning_volatility',
                  'prev_day_volume', 'market_cap']
    available_cols = ['ticker', 'backtest_date']
    for col in merge_cols[2:]:
        if col in grok_df.columns:
            available_cols.append(col)

    df = df.merge(grok_df[available_cols], on=['ticker', 'backtest_date'], how='left')

    # ボラティリティレベルを分類（daily_volatilityを3段階に）
    if 'daily_volatility' in df.columns:
        df['volatility_level'] = pd.qcut(df['daily_volatility'], q=3,
                                          labels=['低ボラ', '中ボラ', '高ボラ'],
                                          duplicates='drop')

    # 時価総額レベルを分類
    if 'market_cap' in df.columns:
        df['market_cap_level'] = pd.qcut(df['market_cap'], q=3,
                                          labels=['小型株', '中型株', '大型株'],
                                          duplicates='drop')

    print(f"\n分析対象: {len(df)}件（2025-11-14）\n")

    all_results = []
    all_results.append("="*60)
    all_results.append("売買タイミング最適化分析 - 規模・状況別レポート")
    all_results.append("="*60)
    all_results.append(f"分析日: 2025-11-14")
    all_results.append(f"分析件数: {len(df)}件")
    all_results.append("")

    # 1. ボラティリティ別
    if 'volatility_level' in df.columns:
        results = analyze_by_factor(df, "ボラティリティ", "volatility_level")
        all_results.extend(results)

    # 2. 時価総額別
    if 'market_cap_level' in df.columns:
        results = analyze_by_factor(df, "時価総額", "market_cap_level")
        all_results.extend(results)

    # 3. 株価水準別（3分割）
    df['price_level'] = pd.qcut(df['buy_price'], q=3, labels=['低価格帯', '中価格帯', '高価格帯'], duplicates='drop')
    results = analyze_by_factor(df, "株価水準", "price_level")
    all_results.extend(results)

    # 4. 売買推奨別
    if 'recommendation_action' in df.columns:
        results = analyze_by_factor(df, "売買推奨", "recommendation_action")
        all_results.extend(results)

    # 5. スコア別（高/中/低）
    if 'recommendation_score' in df.columns:
        df_with_score = df[df['recommendation_score'].notna()].copy()
        if len(df_with_score) > 0:
            # スコアを3段階に分類
            score_bins = [-np.inf, 20, 50, np.inf]
            score_labels = ['低スコア(-∞〜20)', '中スコア(20〜50)', '高スコア(50〜)']
            df_with_score['score_level'] = pd.cut(df_with_score['recommendation_score'],
                                                   bins=score_bins, labels=score_labels)
            results = analyze_by_factor(df_with_score, "スコア", "score_level")
            all_results.extend(results)

    # 6. 出来高別
    if 'prev_day_volume' in df.columns:
        df['volume_level'] = pd.qcut(df['prev_day_volume'], q=3, labels=['低出来高', '中出来高', '高出来高'], duplicates='drop')
        results = analyze_by_factor(df, "出来高", "volume_level")
        all_results.extend(results)

    # 結果を出力
    output_text = "\n".join(all_results)
    print(output_text)

    # ファイルに保存
    with open(FACTOR_ANALYSIS_OUTPUT, 'w', encoding='utf-8') as f:
        f.write(output_text)

    print(f"\n{'='*60}")
    print(f"分析結果を保存しました: {FACTOR_ANALYSIS_OUTPUT}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
