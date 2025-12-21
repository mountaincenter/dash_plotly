#!/usr/bin/env python3
"""
pipeline/generate_trading_recommendation_v2.py

Grok推奨銘柄に対する売買判断レポート生成（v2.0.3 価格帯ロジック）
23:00パイプライン用モジュール

v2.0.3 ルール:
- 5,000-10,000円: 強制「買い」
- 10,000円以上: 強制「売り」
- それ以外: スコアベース判定

出力:
- data/parquet/backtest/trading_recommendation.json
"""

from __future__ import annotations

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
from common_cfg.paths import PARQUET_DIR

# パス設定
GROK_TRENDING_PATH = PARQUET_DIR / "grok_trending.parquet"
BACKTEST_DIR = PARQUET_DIR / "backtest"
BACKTEST_DATA_PATH = BACKTEST_DIR / "grok_analysis_merged.parquet"
PRICES_DIR = PARQUET_DIR
OUTPUT_JSON_PATH = BACKTEST_DIR / "trading_recommendation.json"


def calculate_rank_score(grok_rank: int, total_stocks: int, backtest_win_rate: float | None = None) -> int:
    """
    ランクスコア計算

    Args:
        grok_rank: Grokランク (1-based)
        total_stocks: 総銘柄数
        backtest_win_rate: バックテスト勝率 (0.0-1.0)

    Returns:
        スコア
    """
    # 相対位置（0.0=トップ, 1.0=最下位）
    relative_position = (grok_rank - 1) / max(total_stocks - 1, 1)

    # 基本スコア
    if relative_position <= 0.25:  # 上位25%
        base_score = 40
    elif relative_position <= 0.50:  # 上位50%
        base_score = 20
    elif relative_position <= 0.75:  # 上位75%
        base_score = 0
    else:  # 下位25%
        base_score = -10

    # バックテスト勝率による調整
    if backtest_win_rate is not None:
        if backtest_win_rate >= 0.70:
            base_score += 30
        elif backtest_win_rate >= 0.60:
            base_score += 20
        elif backtest_win_rate >= 0.50:
            base_score += 10
        elif backtest_win_rate <= 0.30:
            base_score -= 20

    return base_score


def fetch_prices_from_parquet(ticker: str, lookback_days: int = 30) -> dict[str, Any] | None:
    """prices parquetから株価情報を取得"""
    try:
        prices_file = PRICES_DIR / 'prices_max_1d.parquet'

        if not prices_file.exists():
            return None

        df = pd.read_parquet(prices_file)
        ticker_df = df[df['ticker'] == ticker].copy()

        if ticker_df.empty:
            return None

        ticker_df['date'] = pd.to_datetime(ticker_df['date'])
        ticker_df = ticker_df.sort_values('date', ascending=False)
        ticker_df = ticker_df.dropna(subset=['Close'])

        recent_df = ticker_df.head(lookback_days)

        if len(recent_df) < 2:
            return None

        latest = recent_df.iloc[0]
        prev = recent_df.iloc[1]

        price_data = {
            'current_price': float(latest['Close']),
            'prev_close': float(latest['Close']),
            'daily_change_pct': float((latest['Close'] - prev['Close']) / prev['Close'] * 100),
            'volume': int(latest['Volume']),
            'high_52w': float(recent_df['High'].max()),
            'low_52w': float(recent_df['Low'].min()),
        }

        # ATR計算（14日）
        if len(recent_df) >= 14:
            recent_14 = recent_df.head(14)
            atr = (recent_14['High'] - recent_14['Low']).mean()
            price_data['atr'] = float(atr)
            price_data['atr_pct'] = float(atr / latest['Close'] * 100)
        else:
            price_data['atr'] = None
            price_data['atr_pct'] = None

        # 移動平均
        if len(recent_df) >= 5:
            price_data['ma5'] = float(recent_df['Close'].head(5).mean())
        if len(recent_df) >= 25:
            price_data['ma25'] = float(recent_df['Close'].head(25).mean())

        return price_data

    except Exception as e:
        print(f"  Warning: {ticker} 株価取得失敗: {e}")
        return None


def load_backtest_stats() -> dict[str, Any]:
    """バックテストデータから統計情報を読み込み"""
    try:
        df = pd.read_parquet(BACKTEST_DATA_PATH)

        # phase2_winがない場合はphase1_winを使用
        if 'phase2_win' not in df.columns:
            if 'phase1_win' in df.columns:
                df['phase2_win'] = df['phase1_win']
                df['phase2_return'] = df['phase1_return']
            else:
                # バックテストデータがない場合
                return {'rank_win_rates': {}, 'rank_avg_returns': {}}

        # ランク別統計
        rank_stats = df.groupby('grok_rank').agg({
            'phase2_win': ['sum', 'count', 'mean'],
            'phase2_return': 'mean'
        }).round(3)

        rank_win_rates = {}
        rank_avg_returns = {}

        for rank in rank_stats.index:
            win_rate = rank_stats.loc[rank, ('phase2_win', 'mean')]
            avg_return = rank_stats.loc[rank, ('phase2_return', 'mean')]

            rank_win_rates[int(rank)] = win_rate * 100
            rank_avg_returns[int(rank)] = avg_return * 100

        return {
            'rank_win_rates': rank_win_rates,
            'rank_avg_returns': rank_avg_returns,
        }
    except Exception as e:
        print(f"  Warning: バックテスト統計取得失敗: {e}")
        return {'rank_win_rates': {}, 'rank_avg_returns': {}}


def determine_action_v2_0_3(
    ticker: str,
    stock_name: str,
    grok_rank: int,
    total_stocks: int,
    backtest_stats: dict[str, Any],
    price_data: dict[str, Any] | None
) -> dict[str, Any]:
    """
    v2.0.3 売買判断ロジック

    Returns:
        dict: {
            'action': 'buy' | 'sell' | 'hold',
            'score': int,
            'confidence': 'high' | 'medium' | 'low',
            'stop_loss': float,
            'reasons': list[dict]
        }
    """
    # バックテスト勝率
    backtest_win_rate = backtest_stats['rank_win_rates'].get(grok_rank)
    backtest_win_rate_decimal = backtest_win_rate / 100 if backtest_win_rate else None

    # ランクスコア
    rank_score = calculate_rank_score(grok_rank, total_stocks, backtest_win_rate_decimal)

    score = rank_score
    reasons = []
    confidence = 'medium'

    # === ルール1: Grokランク ===
    reason_text = f'Grokランク{grok_rank}/{total_stocks}'
    if backtest_win_rate:
        reason_text += f'（過去勝率{backtest_win_rate:.1f}%）'
    reasons.append({
        'type': 'grok_rank',
        'description': reason_text,
        'impact': rank_score
    })

    # === ルール2: 株価情報 ===
    if price_data:
        # 前日変化率
        daily_change = price_data.get('daily_change_pct', 0)
        if daily_change < -3:
            score += 15
            reasons.append({
                'type': 'price_change',
                'description': f"前日{daily_change:.1f}%下落（リバウンド期待）",
                'impact': 15
            })
        elif daily_change > 10:
            score -= 10
            reasons.append({
                'type': 'price_change',
                'description': f"前日+{daily_change:.1f}%急騰（過熱感）",
                'impact': -10
            })

        # ATRボラティリティ
        atr_pct = price_data.get('atr_pct')
        if atr_pct:
            if atr_pct < 3.0:
                score += 10
                reasons.append({
                    'type': 'volatility',
                    'description': "低ボラ（安定）",
                    'impact': 10
                })
            elif atr_pct > 8.0:
                score -= 15
                reasons.append({
                    'type': 'volatility',
                    'description': "高ボラ（リスク大）",
                    'impact': -15
                })

    # === 初期判定（スコアベース） ===
    if score >= 40:
        action = 'buy'
        confidence = 'high'
    elif score >= 20:
        action = 'buy'
        confidence = 'medium'
    elif score <= -30:
        action = 'sell'
        confidence = 'high'
    elif score <= -15:
        action = 'sell'
        confidence = 'medium'
    else:
        action = 'hold'
        confidence = 'low'

    # === v2.0.3: 価格帯による強制判定 ===
    current_price = price_data.get('current_price', 0) if price_data else 0

    if 5000 <= current_price < 10000:
        # 強制ロング
        action = 'buy'
        confidence = 'high'
        reasons.append({
            'type': 'price_forced_buy',
            'description': f"5,000-10,000円範囲（{current_price:,.0f}円）→強制買い",
            'impact': 0  # スコアに影響しないが判定を上書き
        })

    elif current_price >= 10000:
        # 強制ショート
        action = 'sell'
        confidence = 'high'
        reasons.append({
            'type': 'price_forced_sell',
            'description': f"10,000円以上（{current_price:,.0f}円）→強制売り",
            'impact': 0  # スコアに影響しないが判定を上書き
        })

    # 損切りライン計算
    atr_pct = price_data.get('atr_pct') if price_data else None
    if action == 'sell':
        stop_loss = max(5.0, min(atr_pct * 1.2, 10.0)) if atr_pct else 7.0
    else:
        stop_loss = max(2.0, min(atr_pct * 0.8, 5.0)) if atr_pct else 3.0

    return {
        'action': action,
        'score': score,
        'confidence': confidence,
        'stop_loss': stop_loss,
        'reasons': reasons,
        'price_data': price_data,
    }


def main() -> int:
    """メイン処理"""
    print("\n" + "=" * 60)
    print("Trading Recommendation Generation (v2.0.3)")
    print("=" * 60)

    # 1. Grok推奨銘柄読み込み
    if not GROK_TRENDING_PATH.exists():
        print(f"❌ Error: {GROK_TRENDING_PATH} が見つかりません")
        return 1

    print(f"\n[1/4] Loading Grok trending stocks...")
    df = pd.read_parquet(GROK_TRENDING_PATH)
    total_stocks = len(df)
    print(f"  ✅ {total_stocks} stocks loaded")

    # 2. バックテスト統計読み込み
    print(f"\n[2/4] Loading backtest statistics...")
    backtest_stats = load_backtest_stats()
    backtest_count = len(backtest_stats['rank_win_rates'])
    print(f"  ✅ Backtest stats: {backtest_count} ranks")

    # 3. 各銘柄の分析
    print(f"\n[3/4] Analyzing stocks...")
    json_stocks = []

    for idx, row in df.iterrows():
        ticker = row['ticker']
        stock_name = row['stock_name']
        grok_rank = int(row['grok_rank'])

        print(f"  [{idx+1}/{total_stocks}] {ticker} {stock_name} (ランク{grok_rank})")

        # 株価情報取得
        price_data = fetch_prices_from_parquet(ticker)

        # 売買判断
        result = determine_action_v2_0_3(
            ticker, stock_name, grok_rank, total_stocks, backtest_stats, price_data
        )

        # JSON用データ構築
        technical_data = {}
        if price_data:
            atr_pct = price_data.get('atr_pct')
            if atr_pct is not None:
                if atr_pct < 3.0:
                    atr_level = 'low'
                    volatility_level = '低ボラ'
                elif atr_pct < 5.0:
                    atr_level = 'medium'
                    volatility_level = '中ボラ'
                else:
                    atr_level = 'high'
                    volatility_level = '高ボラ'
            else:
                atr_level = 'medium'
                volatility_level = '中ボラ'

            technical_data = {
                'prevClose': price_data.get('prev_close'),
                'prevDayChangePct': price_data.get('daily_change_pct'),
                'atr': {
                    'value': price_data.get('atr_pct'),
                    'level': atr_level
                },
                'volume': price_data.get('volume'),
                'volatilityLevel': volatility_level
            }

        json_stock = {
            'ticker': ticker,
            'stockName': stock_name,
            'grokRank': grok_rank,
            'technicalData': technical_data,
            'recommendation': {
                'action': result['action'],
                'score': result['score'],
                'confidence': result['confidence'],
                'stopLoss': {
                    'percent': round(result['stop_loss'], 1),
                    'calculation': f"ATR {price_data.get('atr_pct'):.1f}% ベース" if price_data and price_data.get('atr_pct') else "デフォルト"
                },
                'reasons': result['reasons']
            },
            'categories': []
        }

        json_stocks.append(json_stock)
        print(f"    → {result['action']} (score: {result['score']:+d}, confidence: {result['confidence']})")

    # 4. JSON保存
    print(f"\n[4/4] Saving JSON...")

    buy_count = sum(1 for s in json_stocks if s['recommendation']['action'] == 'buy')
    sell_count = sum(1 for s in json_stocks if s['recommendation']['action'] == 'sell')
    hold_count = sum(1 for s in json_stocks if s['recommendation']['action'] == 'hold')

    json_data = {
        'version': '2.0.3',
        'generatedAt': datetime.now().isoformat(),
        'dataSource': {
            'backtestCount': backtest_count,
            'backtestPeriod': {
                'start': '2025-11-04',
                'end': datetime.now().strftime('%Y-%m-%d')
            },
            'technicalDataDate': datetime.now().strftime('%Y-%m-%d')
        },
        'summary': {
            'total': total_stocks,
            'buy': buy_count,
            'sell': sell_count,
            'hold': hold_count
        },
        'stocks': json_stocks,
    }

    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    print(f"  ✅ Saved: {OUTPUT_JSON_PATH}")
    print(f"\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  買い: {buy_count} stocks")
    print(f"  売り: {sell_count} stocks")
    print(f"  静観: {hold_count} stocks")
    print("=" * 60 + "\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
