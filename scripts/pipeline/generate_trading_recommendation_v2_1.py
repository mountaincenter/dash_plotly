#!/usr/bin/env python3
"""
generate_trading_recommendation_v2_1.py

Grok推奨銘柄に対する売買判断生成（v2.1版 - 2階建てアーキテクチャ）

処理フロー:
1. improvement/data/grok_trending.parquet を読み込み
2. v2.0.3 スコアリング・判定を適用
3. テクニカル指標を計算（prices_max_1d.parquetから）
4. v2.1 判断を適用（2階建てアーキテクチャ）
5. improvement/data/trading_recommendation.json を出力

2階建てアーキテクチャ:
- 1階（スコアリング層）: 全銘柄に統一したスコアリング（データ蓄積）
- 2階（定性判定層）: 価格帯強制判定、2段階変化阻止、v2.0.3売り保持
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime
import json

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

# 既存のv2_0_3スクリプトから関数をインポート
sys.path.append(str(ROOT / "improvement" / "scripts"))
from generate_trading_recommendation_v2_0_3 import (
    calculate_rank_score_improved,
    fetch_jquants_fundamentals,
    fetch_prices_from_parquet,
    load_backtest_stats,
    get_jquants_client
)

# パス設定（環境変数でオーバーライド可能）
import os

IMPROVEMENT_DIR = ROOT / "improvement"
# INPUT_FILE: 環境変数 INPUT_GROK_TRENDING が設定されていればそれを使用
INPUT_FILE = Path(os.getenv("INPUT_GROK_TRENDING", IMPROVEMENT_DIR / "data" / "grok_trending.parquet"))
PRICES_FILE = ROOT / "data" / "parquet" / "prices_max_1d.parquet"
# OUTPUT_JSON: 環境変数 OUTPUT_RECOMMENDATION_JSON が設定されていればそれを使用
OUTPUT_JSON = Path(os.getenv("OUTPUT_RECOMMENDATION_JSON", IMPROVEMENT_DIR / "data" / "trading_recommendation.json"))


def calculate_technical_indicators(ticker: str, prices_df: pd.DataFrame) -> dict:
    """
    prices_max_1d.parquetからテクニカル指標を計算

    Args:
        ticker: ティッカーコード
        prices_df: 全銘柄の株価データ

    Returns:
        テクニカル指標の辞書
    """
    try:
        # ティッカーでフィルタ
        ticker_df = prices_df[prices_df['ticker'] == ticker].copy()

        if ticker_df.empty:
            return {}

        # 日付でソート
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])
        ticker_df = ticker_df.sort_values('date', ascending=False)

        if len(ticker_df) < 2:
            return {}

        # 最新30日分
        recent_df = ticker_df.head(30)

        # RSI計算（14日）
        rsi_14d = None
        if len(recent_df) >= 15:
            closes = recent_df['Close'].values[:15][::-1]  # 古い順に並べ替え
            deltas = np.diff(closes)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)

            avg_gain = np.mean(gains)
            avg_loss = np.mean(losses)

            if avg_loss != 0:
                rs = avg_gain / avg_loss
                rsi_14d = 100 - (100 / (1 + rs))

        # 出来高変化（20日平均比）
        volume_change_20d = None
        if len(recent_df) >= 21:
            latest_volume = recent_df.iloc[0]['Volume']
            avg_volume_20d = recent_df.iloc[1:21]['Volume'].mean()
            if avg_volume_20d > 0:
                volume_change_20d = latest_volume / avg_volume_20d

        # 5日線乖離率
        price_vs_sma5_pct = None
        if len(recent_df) >= 5:
            latest_close = recent_df.iloc[0]['Close']
            sma5 = recent_df.iloc[0:5]['Close'].mean()
            if sma5 > 0:
                price_vs_sma5_pct = ((latest_close - sma5) / sma5) * 100

        return {
            'rsi_14d': rsi_14d,
            'volume_change_20d': volume_change_20d,
            'price_vs_sma5_pct': price_vs_sma5_pct
        }

    except Exception as e:
        print(f"Warning: {ticker} のテクニカル指標計算失敗: {e}")
        return {}


def apply_v3_strategy(v2_1_action: str, prev_close: float) -> tuple[str, int, str, str]:
    """
    v3.0 戦略: シグナル + 価格帯 → アクション + 保有期間

    Args:
        v2_1_action: v2.1のアクション（'買い', '売り', '静観'）
        prev_close: 前日終値

    Returns:
        tuple: (v3_action, v3_holding_days, v3_label, v3_reason)
        - v3_action: '買い', '売り', '静観'
        - v3_holding_days: 0 (当日), 5 (5日保有)
        - v3_label: 表示用ラベル
        - v3_reason: 理由
    """
    if pd.isna(prev_close) or prev_close <= 0:
        return v2_1_action, 0, v2_1_action, '当日決済'

    # 買いシグナル
    if v2_1_action == '買い':
        if 7500 <= prev_close < 10000:
            return '買い', 5, '買い5日', '5日スイング（7,500-10,000円帯）'
        elif 5000 <= prev_close < 7500:
            return '買い', 0, '買い', '当日決済'
        else:
            return '買い', 0, '買い', '当日決済'

    # 静観シグナル
    elif v2_1_action == '静観':
        if 1500 <= prev_close < 3000:
            return '買い', 5, '買い5日', '5日転換（1,500-3,000円帯）'
        else:
            return '静観', 0, '静観', ''

    # 売りシグナル
    elif v2_1_action == '売り':
        if 2000 <= prev_close < 10000:
            return '売り', 5, '売り5日', '5日スイング（2,000-10,000円帯）'
        else:
            return '売り', 0, '売り', '当日決済'

    return v2_1_action, 0, v2_1_action, '当日決済'


def calculate_v2_0_3_score_and_action(row: pd.Series, backtest_stats: dict,
                                      fundamentals: dict, price_data: dict,
                                      total_stocks: int) -> tuple[int, str, list[str]]:
    """
    v2.0.3のスコアリング・判定ロジック

    Returns:
        (score, action, reasons)
    """
    ticker = row['ticker']
    grok_rank = row['grok_rank']

    score = 0
    reasons = []

    # バックテスト勝率
    backtest_win_rate = backtest_stats['rank_win_rates'].get(grok_rank)
    backtest_win_rate_decimal = backtest_win_rate / 100 if backtest_win_rate else None

    # 改良版ランクスコア
    rank_score = calculate_rank_score_improved(grok_rank, total_stocks, backtest_win_rate_decimal)
    score = rank_score

    reason_text = f'Grokランク{grok_rank}/{total_stocks}'
    if backtest_win_rate:
        reason_text += f'（過去勝率{backtest_win_rate:.1f}%）'
    reasons.append(reason_text)

    # 財務情報による判断
    if fundamentals:
        if fundamentals.get('roe') and fundamentals['roe'] > 15:
            score += 20
            reasons.append(f"ROE {fundamentals['roe']:.1f}%（優良）")
        elif fundamentals.get('roe') and fundamentals['roe'] < 0:
            score -= 15
            reasons.append(f"ROE {fundamentals['roe']:.1f}%（赤字）")

        if fundamentals.get('operatingProfitGrowth'):
            growth = fundamentals['operatingProfitGrowth']
            if growth > 50:
                score += 25
                reasons.append(f"営業利益成長+{growth:.1f}%")
            elif growth < -30:
                score -= 20
                reasons.append(f"営業利益成長{growth:.1f}%（減益）")

    # 株価情報による判断
    if price_data:
        daily_change = price_data.get('dailyChangePct', 0)
        if daily_change < -3:
            score += 15
            reasons.append(f"前日{daily_change:.1f}%下落（リバウンド期待）")
        elif daily_change > 10:
            score -= 10
            reasons.append(f"前日+{daily_change:.1f}%急騰（過熱感）")

        atr_pct = price_data.get('atrPct')
        if atr_pct:
            if atr_pct < 3.0:
                score += 10
                reasons.append("低ボラ（安定）")
            elif atr_pct > 8.0:
                score -= 15
                reasons.append("高ボラ（リスク大）")

        current_price = price_data.get('currentPrice')
        ma25 = price_data.get('ma25')
        if current_price and ma25:
            if current_price > ma25 * 1.05:
                score -= 10
                reasons.append("25日線から+5%以上乖離")
            elif current_price < ma25 * 0.95:
                score += 10
                reasons.append("25日線から-5%以上乖離（割安）")

    # 価格帯による補正（v2.0.1）
    current_price = price_data.get('current_price', 0) if price_data else 0
    if 5000 <= current_price <= 8000:
        score += 25
        reasons.append(f"価格帯5000-8000円（適正範囲）")

    # 行動決定（スコアベース）
    if score >= 40:
        action = '買い'
    elif score >= 20:
        action = '買い'
    elif score <= -30:
        action = '売り'
    elif score <= -15:
        action = '売り'
    else:
        action = '静観'

    # v2.0.3: 価格帯による強制判定
    if 5000 <= current_price < 10000:
        action = '買い'
        reasons.append(f"5,000-10,000円範囲（{current_price:,.0f}円）→ロング戦略")
    elif current_price >= 10000:
        action = '売り'
        reasons.append(f"10,000円超え（{current_price:,.0f}円）→ショート戦略")

    return (score, action, reasons)


def calculate_v2_1_score_and_action(v2_0_3_action: str, v2_0_3_score: int,
                                    prev_close: float, technical: dict,
                                    grok_rank: int, total_stocks: int) -> tuple[int, str, list[str]]:
    """
    v2.1のスコアリング・判定ロジック（2階建てアーキテクチャ）

    1階（スコアリング層）: 全銘柄に統一したスコアリング
    2階（定性判定層）: 価格帯強制、2段階変化阻止、v2.0.3売り保持

    Returns:
        (score, action, reasons)
    """
    score = 0
    reasons = []

    # === 1階: スコアリング層（全銘柄で計算してデータ蓄積） ===

    # 1. Grokランク配点強化
    relative_position = (grok_rank - 1) / max(total_stocks - 1, 1)

    if relative_position <= 0.25:
        score += 50  # v2.0.3: 40 → v2.1: 50
        reasons.append(f'Grokランク上位25%（強化）')
    elif relative_position <= 0.50:
        score += 30  # v2.0.3: 20 → v2.1: 30
        reasons.append(f'Grokランク上位50%')
    elif relative_position <= 0.75:
        pass
    else:
        score -= 30  # v2.0.3: -10 → v2.1: -30
        reasons.append(f'Grokランク下位25%（減点強化）')

    # 2. RSI
    rsi_14d = technical.get('rsi_14d')
    if pd.notna(rsi_14d):
        if rsi_14d < 30:
            score += 20
            reasons.append(f'RSI {rsi_14d:.1f}（売られすぎ）')
        elif rsi_14d > 70:
            score -= 10
            reasons.append(f'RSI {rsi_14d:.1f}（買われすぎ）')

    # 3. 出来高急増
    volume_change_20d = technical.get('volume_change_20d')
    if pd.notna(volume_change_20d) and volume_change_20d > 2.0:
        score += 15
        reasons.append(f'出来高{volume_change_20d:.1f}倍（注目急増）')

    # 4. 5日線押し目
    price_vs_sma5_pct = technical.get('price_vs_sma5_pct')
    if pd.notna(price_vs_sma5_pct) and -2.0 < price_vs_sma5_pct < 0:
        score += 15
        reasons.append(f'5日線押し目{price_vs_sma5_pct:.1f}%')

    # === 2階: 定性判定層（強制判定） ===

    # 優先度1: 価格帯強制判定（最優先、v2.0.3/v2.1共通）
    if pd.notna(prev_close):
        if prev_close >= 10000:
            reasons.append('【価格帯10,000円以上→売り強制】')
            return (score, '売り', reasons)
        elif 5000 <= prev_close < 10000:
            reasons.append('【価格帯5,000-10,000円→買い強制】')
            return (score, '買い', reasons)

    # スコアベース仮判定（1階の結果）
    if score >= 30:
        action = '買い'
    elif score <= -20:
        action = '売り'
    else:
        action = '静観'

    # 優先度2: 2段階変化阻止（買い↔売り）
    if v2_0_3_action == '買い' and action == '売り':
        action = '買い'
        reasons.append('【2段階変化阻止: 買い→売り→買い】')

    # 優先度3: v2.0.3売り判定の保持
    elif v2_0_3_action == '売り':
        action = '売り'
        reasons.append('【v2.0.3売り判定を保持】')

    return (score, action, reasons)


def main():
    """メイン処理"""
    print("=== Trading Recommendation V2.1 生成 ===\n")

    # 1. grok_trending.parquet 読み込み
    print("1. grok_trending.parquet 読み込み中...")
    if not INPUT_FILE.exists():
        print(f"エラー: {INPUT_FILE} が見つかりません")
        return 1

    grok_df = pd.read_parquet(INPUT_FILE)
    print(f"   読み込み完了: {len(grok_df)} 銘柄")

    # 必須カラムの存在確認
    required_columns = ['ticker', 'grok_rank', 'stock_name', 'date']
    missing_columns = [col for col in required_columns if col not in grok_df.columns]
    if missing_columns:
        print(f"エラー: 必須カラムが欠損しています: {missing_columns}")
        return 1

    # technicalDataDate を grok_trending.parquet の date から取得
    grok_df['date'] = pd.to_datetime(grok_df['date'])
    technical_data_date = grok_df['date'].max().strftime('%Y-%m-%d')
    print(f"   Technical data date: {technical_data_date}")

    # 2. prices_max_1d.parquet 読み込み
    print("\n2. prices_max_1d.parquet 読み込み中...")
    if not PRICES_FILE.exists():
        print(f"エラー: {PRICES_FILE} が見つかりません")
        return 1

    prices_df = pd.read_parquet(PRICES_FILE)
    print(f"   読み込み完了: {len(prices_df)} レコード")

    # 3. バックテスト統計読み込み
    print("\n3. バックテスト統計読み込み中...")
    backtest_stats = load_backtest_stats()

    # 4. 各銘柄の判断を生成
    print("\n4. 各銘柄の判断を生成中...")
    recommendations = []
    total_stocks = len(grok_df)

    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        print(f"   処理中: {ticker} ({idx+1}/{total_stocks})")

        # 財務情報取得
        fundamentals = fetch_jquants_fundamentals(ticker)

        # 株価情報取得
        price_data = fetch_prices_from_parquet(ticker, lookback_days=30)

        if price_data is None:
            print(f"   警告: {ticker} の株価データが取得できません。スキップします。")
            continue

        # v2.0.3判断を生成
        v2_0_3_score, v2_0_3_action, v2_0_3_reasons = calculate_v2_0_3_score_and_action(
            row, backtest_stats, fundamentals, price_data, total_stocks
        )

        # テクニカル指標を計算
        technical = calculate_technical_indicators(ticker, prices_df)

        # v2.1判断を生成
        prev_close = price_data.get('prevClose', 0)
        v2_1_score, v2_1_action, v2_1_reasons = calculate_v2_1_score_and_action(
            v2_0_3_action, v2_0_3_score, prev_close, technical,
            row['grok_rank'], total_stocks
        )

        # 損切り水準（価格帯別）
        if v2_1_action == '売り':
            # 売りは一律5%
            stop_loss_pct = 5.0
        elif v2_1_action == '買い':
            # 買いは価格帯別
            if prev_close >= 10000:
                stop_loss_pct = 2.5
            elif prev_close >= 5000:
                stop_loss_pct = 3.0  # 5,000-10,000円: 3%
            elif prev_close >= 3000:
                stop_loss_pct = 3.0
            elif prev_close >= 1000:
                stop_loss_pct = 5.0
            else:
                # 1000円以下は損切りなし
                stop_loss_pct = 0.0
        else:
            # 静観はデフォルト
            stop_loss_pct = 3.0

        # 取引制限情報（grok_trending.parquetから取得）
        margin_code = row.get('margin_code', '2')
        margin_code_name = row.get('margin_code_name', '貸借')
        jsf_restricted = row.get('jsf_restricted', False)
        is_shortable = row.get('is_shortable', True)

        # 取引制限判定
        # 売りシグナルで空売り不可 → 取引制限
        # 信用取引不可（margin_code='3'）→ 取引制限
        is_restricted = False
        restriction_reason = None
        if str(margin_code) == '3':
            is_restricted = True
            restriction_reason = '信用取引不可（その他）'
        elif v2_1_action == '売り' and not is_shortable:
            is_restricted = True
            if jsf_restricted:
                restriction_reason = '日証金申込停止（空売り不可）'
            elif str(margin_code) == '1':
                restriction_reason = '信用銘柄（空売り不可）'
            else:
                restriction_reason = '空売り制限'

        # v3戦略を適用
        v3_action, v3_holding_days, v3_label, v3_reason = apply_v3_strategy(v2_1_action, prev_close)

        recommendations.append({
            'ticker': ticker,
            'stock_name': row.get('stock_name', ''),
            'grok_rank': int(row.get('grok_rank', 0)),
            'prev_day_close': prev_close,
            'prev_day_change_pct': price_data.get('dailyChangePct', 0),
            'atr_pct': price_data.get('atrPct', 0),
            'v2_0_3_action': v2_0_3_action,
            'v2_0_3_score': v2_0_3_score,
            'v2_0_3_reasons': ' / '.join(v2_0_3_reasons),
            'v2_1_action': v2_1_action,
            'v2_1_score': v2_1_score,
            'v2_1_reasons': v2_1_reasons,
            'rsi_14d': technical.get('rsi_14d'),
            'volume_change_20d': technical.get('volume_change_20d'),
            'price_vs_sma5_pct': technical.get('price_vs_sma5_pct'),
            'stop_loss_pct': round(stop_loss_pct, 1),
            'settlement_timing': '大引け',
            # 取引制限情報
            'margin_code': str(margin_code),
            'margin_code_name': margin_code_name,
            'jsf_restricted': bool(jsf_restricted),
            'is_shortable': bool(is_shortable),
            'is_restricted': is_restricted,
            'restriction_reason': restriction_reason,
            # v3戦略
            'v3_action': v3_action,
            'v3_holding_days': v3_holding_days,
            'v3_reason': v3_reason,
            'v3_label': v3_label
        })

    # 5. v2_1スコア順にソート
    recommendations.sort(key=lambda x: x['v2_1_score'], reverse=True)

    # エラー: 推奨銘柄が0件の場合
    if len(recommendations) == 0:
        print("\n❌ ERROR: No recommendations generated")
        print("   All stocks were skipped due to missing price data")
        print("   This indicates a critical data issue")
        return 1

    # 6. JSON出力
    print(f"\n5. JSON出力中: {OUTPUT_JSON}")

    # カウント集計（制限銘柄は別カテゴリ）
    buy_count = sum(1 for r in recommendations if r['v2_1_action'] == '買い' and not r['is_restricted'])
    sell_count = sum(1 for r in recommendations if r['v2_1_action'] == '売り' and not r['is_restricted'])
    hold_count = sum(1 for r in recommendations if r['v2_1_action'] == '静観' and not r['is_restricted'])
    restricted_count = sum(1 for r in recommendations if r['is_restricted'])

    output_data = {
        'generated_at': datetime.now().isoformat(),
        'strategy_version': 'v2.1',
        'dataSource': {
            'backtestCount': len(backtest_stats['rank_win_rates']) if backtest_stats else 0,
            'technicalDataDate': technical_data_date
        },
        'total_stocks': len(recommendations),
        'buy_count': buy_count,
        'hold_count': hold_count,
        'sell_count': sell_count,
        'restricted_count': restricted_count,
        'stocks': recommendations
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"   完了: {len(recommendations)} 銘柄")
    print(f"   買い: {buy_count}, 静観: {hold_count}, 売り: {sell_count}, 取引制限: {restricted_count}")

    print("\n=== 完了 ===")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
