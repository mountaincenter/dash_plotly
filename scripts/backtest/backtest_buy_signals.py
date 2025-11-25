#!/usr/bin/env python3
"""
買いシグナルのバックテスト
v2判断で「買い」と判定された銘柄について、寄り付きで買って11:30/15:30で売った場合の勝率を検証
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, time, timedelta
import warnings
warnings.filterwarnings('ignore')

# v2判断結果を読み込み
judgments_df = pd.read_parquet('/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/data/parquet/backtest/grok_analysis_v2_judgments.parquet')

# 買いシグナルのみフィルタ
buy_signals = judgments_df[judgments_df['v2_action'] == '買い'].copy()

print(f"買いシグナル総数: {len(buy_signals)}件")
print(f"期間: {buy_signals['selection_date'].min()} ~ {buy_signals['selection_date'].max()}")
print()

backtest_results = []

for idx, row in buy_signals.iterrows():
    ticker = row['ticker']
    selection_date_str = row['selection_date']
    v2_score = row['v2_score']

    # 日付をdatetimeに変換
    target_date = pd.to_datetime(selection_date_str)

    print(f"[{idx+1}/{len(buy_signals)}] {ticker} ({row['company_name']}) - {selection_date_str}")

    try:
        yf_ticker = yf.Ticker(ticker)

        # 5分足データ取得（Phase1用）
        hist_5m = yf_ticker.history(period='60d', interval='5m')
        if hist_5m.empty:
            print(f"  ⚠ 5分足データなし")
            continue

        # 対象日のデータをフィルタ
        day_5m = hist_5m[hist_5m.index.date == target_date.date()]
        if day_5m.empty:
            print(f"  ⚠ {selection_date_str}の5分足データなし")
            continue

        # 寄り付き価格（9:00）を取得 - 買いエントリー
        morning_start = day_5m[(day_5m.index.time >= time(9, 0)) & (day_5m.index.time <= time(9, 10))]
        if morning_start.empty:
            print(f"  ⚠ 9:00データなし")
            continue
        open_price = morning_start.iloc[0]['Open']

        # Phase1: 11:30の終値（5分足）- 売りエグジット候補
        phase1_data = day_5m[(day_5m.index.time >= time(11, 25)) & (day_5m.index.time <= time(11, 30))]
        phase1_close = None
        phase1_return_pct = None
        phase1_win = None

        if not phase1_data.empty:
            phase1_close = phase1_data.iloc[-1]['Close']
            # 買いロング: (終値 - 始値) / 始値
            phase1_return_pct = ((phase1_close - open_price) / open_price) * 100
            phase1_win = phase1_return_pct > 0

        # Phase2: 15:30の終値（日足）- 売りエグジット確定
        hist_1d = yf_ticker.history(period='60d', interval='1d')

        # タイムゾーンを合わせて検索
        hist_1d_dates = [d.date() for d in hist_1d.index]
        if target_date.date() not in hist_1d_dates:
            print(f"  ⚠ {selection_date_str}の日足データなし（市場休業日の可能性）")
            continue

        # 該当日のデータを取得
        matching_idx = [i for i, d in enumerate(hist_1d.index) if d.date() == target_date.date()]
        if not matching_idx:
            print(f"  ⚠ {selection_date_str}の日足データなし")
            continue

        phase2_close = hist_1d.iloc[matching_idx[0]]['Close']
        phase2_return_pct = ((phase2_close - open_price) / open_price) * 100
        phase2_win = phase2_return_pct > 0

        print(f"  Open: {open_price:.1f}円")
        if phase1_close:
            print(f"  Phase1(11:30): {phase1_close:.1f}円 → {phase1_return_pct:+.2f}% {'✅' if phase1_win else '❌'}")
        print(f"  Phase2(15:30): {phase2_close:.1f}円 → {phase2_return_pct:+.2f}% {'✅' if phase2_win else '❌'}")

        backtest_results.append({
            'selection_date': selection_date_str,
            'ticker': ticker,
            'company_name': row['company_name'],
            'v2_score': v2_score,
            'open_price': open_price,
            'phase1_close': phase1_close,
            'phase1_return_pct': phase1_return_pct,
            'phase1_win': phase1_win,
            'phase2_close': phase2_close,
            'phase2_return_pct': phase2_return_pct,
            'phase2_win': phase2_win
        })

    except Exception as e:
        print(f"  ❌ エラー: {e}")
        continue

print("\n" + "="*60)

# 結果集計
results_df = pd.DataFrame(backtest_results)

if len(results_df) > 0:
    # Phase1集計
    phase1_valid = results_df[results_df['phase1_win'].notna()]
    phase1_wins = phase1_valid['phase1_win'].sum()
    phase1_total = len(phase1_valid)
    phase1_win_rate = (phase1_wins / phase1_total * 100) if phase1_total > 0 else 0
    phase1_avg_return = phase1_valid['phase1_return_pct'].mean()

    # Phase2集計
    phase2_valid = results_df[results_df['phase2_win'].notna()]
    phase2_wins = phase2_valid['phase2_win'].sum()
    phase2_total = len(phase2_valid)
    phase2_win_rate = (phase2_wins / phase2_total * 100) if phase2_total > 0 else 0
    phase2_avg_return = phase2_valid['phase2_return_pct'].mean()

    print("=== 買いシグナル（ロング）の勝率 ===")
    print(f"Phase1勝率: {phase1_wins}/{phase1_total} ({phase1_win_rate:.1f}%)")
    print(f"Phase1平均リターン: {phase1_avg_return:+.2f}%")
    print()
    print(f"Phase2勝率: {phase2_wins}/{phase2_total} ({phase2_win_rate:.1f}%)")
    print(f"Phase2平均リターン: {phase2_avg_return:+.2f}%")

    # 結果を保存
    output_path = '/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/data/parquet/backtest/buy_signals_backtest.parquet'
    results_df.to_parquet(output_path, index=False)
    print(f"\n結果を保存: {output_path}")

    # 詳細表示
    print("\n=== Phase2結果の内訳 ===")
    results_sorted = results_df.sort_values('phase2_return_pct', ascending=False)
    for _, r in results_sorted.iterrows():
        status = "✅" if r['phase2_win'] else "❌"
        print(f"{status} {r['selection_date']} {r['ticker']:8s} {r['company_name']:20s} スコア:{r['v2_score']:3d} → {r['phase2_return_pct']:+6.2f}%")

else:
    print("⚠ バックテスト結果がありません")
