"""
Grok推奨銘柄に対する売買判断レポート生成（V2 - Deep Search版）

改良点：
1. ランクスコアの見直し（下位ランクが不当にマイナスにならないように）
2. J-Quants APIからの財務情報取得（全銘柄）
3. prices parquetファイルからの株価情報取得
4. 総合的な判断ロジック
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf
import json
import sys

# J-Quants クライアント
sys.path.append(str(Path(__file__).parent.parent))
from scripts.lib.jquants_client import JQuantsClient

# パス設定
BASE_DIR = Path(__file__).parent.parent
LATEST_GROK_PATH = BASE_DIR / 'data' / 'parquet' / 'grok_trending.parquet'
BACKTEST_DATA_PATH = BASE_DIR / 'test_output' / 'grok_analysis_base_latest.parquet'
PRICES_DIR = BASE_DIR / 'data' / 'parquet'
OUTPUT_HTML_PATH = BASE_DIR / 'test_output' / 'trading_recommendation_v2.html'
OUTPUT_JSON_PATH = BASE_DIR / 'data' / 'parquet' / 'backtest' / 'trading_recommendation.json'

# 注: JSONはarchive_trading_recommendation.pyがgrok_analysis_merged.parquetにマージします

# J-Quantsクライアント
jquants_client = None


def get_jquants_client():
    """J-Quantsクライアントを取得（シングルトン）"""
    global jquants_client
    if jquants_client is None:
        jquants_client = JQuantsClient()
    return jquants_client


def calculate_rank_score_improved(grok_rank, total_stocks, backtest_win_rate=None):
    """
    改良版ランクスコア計算

    問題点：
    - 旧版では下位ランク（例: 12位/12）が不当にマイナスになる
    - 銘柄数が多い日（13銘柄）では特に顕著

    改良点：
    - ランクの相対位置を考慮（上位X%）
    - 下位ランクでもバックテスト勝率が高ければプラススコア
    - 銘柄数に応じて調整
    """
    # 相対位置（0.0=トップ, 1.0=最下位）
    relative_position = (grok_rank - 1) / max(total_stocks - 1, 1)

    # 基本スコア（相対位置ベース）
    if relative_position <= 0.25:  # 上位25%
        base_score = 40
    elif relative_position <= 0.50:  # 上位50%
        base_score = 20
    elif relative_position <= 0.75:  # 上位75%
        base_score = 0
    else:  # 下位25%
        base_score = -10  # 旧版の-50から大幅に改善

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


def fetch_jquants_fundamentals(ticker):
    """J-Quants APIから財務情報を取得"""
    try:
        client = get_jquants_client()
        code = ticker.replace('.T', '').ljust(5, '0')

        # 財務情報取得
        statements_response = client.request('/fins/statements', params={'code': code})

        if 'statements' not in statements_response or not statements_response['statements']:
            return None

        # 最新の決算データ
        statements = sorted(
            statements_response['statements'],
            key=lambda x: x.get('DisclosedDate', ''),
            reverse=True
        )

        latest = statements[0]

        # フィールドを文字列から数値に変換
        def parse_number(value):
            if not value or value == '':
                return 0
            try:
                return float(value)
            except:
                return 0

        fundamentals = {
            'eps': parse_number(latest.get('EarningsPerShare')),
            'operatingProfit': parse_number(latest.get('OperatingProfit')),
            'ordinaryProfit': parse_number(latest.get('OrdinaryProfit')),
            'netIncome': parse_number(latest.get('Profit')),
            'totalAssets': parse_number(latest.get('TotalAssets')),
            'equity': parse_number(latest.get('Equity')),
            'disclosedDate': latest.get('DisclosedDate', ''),
            'fiscalYear': latest.get('CurrentFiscalYearStartDate', ''),
        }

        # ROE計算
        if fundamentals['equity'] > 0 and fundamentals['netIncome'] != 0:
            fundamentals['roe'] = fundamentals['netIncome'] / fundamentals['equity'] * 100
        else:
            fundamentals['roe'] = 0

        # 前年比較（YoY成長率）
        if len(statements) >= 2:
            prev = statements[1]
            prev_operating = parse_number(prev.get('OperatingProfit'))
            if prev_operating != 0:
                fundamentals['operatingProfitGrowth'] = (
                    (fundamentals['operatingProfit'] - prev_operating) / abs(prev_operating) * 100
                )
            else:
                fundamentals['operatingProfitGrowth'] = None
        else:
            fundamentals['operatingProfitGrowth'] = None

        return fundamentals

    except Exception as e:
        print(f"Warning: {ticker} のJ-Quants財務情報取得失敗: {e}")
        return None


def fetch_prices_from_parquet(ticker, lookback_days=30):
    """prices parquetファイルから株価情報を取得"""
    try:
        # prices_max_1d.parquet を使用
        prices_file = PRICES_DIR / 'prices_max_1d.parquet'

        if not prices_file.exists():
            print(f"Warning: {prices_file} が見つかりません")
            return None

        df = pd.read_parquet(prices_file)

        # ティッカーでフィルタ
        ticker_df = df[df['ticker'] == ticker].copy()

        if ticker_df.empty:
            return None

        # 日付でソート
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])
        ticker_df = ticker_df.sort_values('date', ascending=False)

        # NaNを除外
        ticker_df = ticker_df.dropna(subset=['Close'])

        # 最新N日分
        recent_df = ticker_df.head(lookback_days)

        if len(recent_df) < 2:
            return None

        # 各種指標計算（カラム名は大文字）
        latest = recent_df.iloc[0]
        prev = recent_df.iloc[1]

        price_data = {
            'currentPrice': float(latest['Close']),
            'prevClose': float(prev['Close']),
            'dailyChangePct': float((latest['Close'] - prev['Close']) / prev['Close'] * 100),
            'volume': int(latest['Volume']),
            'high52w': float(recent_df['High'].max()),
            'low52w': float(recent_df['Low'].min()),
        }

        # ATR計算（14日）
        if len(recent_df) >= 14:
            recent_14 = recent_df.head(14)
            atr = (recent_14['High'] - recent_14['Low']).mean()
            price_data['atr'] = float(atr)
            price_data['atrPct'] = float(atr / latest['Close'] * 100)
        else:
            price_data['atr'] = None
            price_data['atrPct'] = None

        # 移動平均
        if len(recent_df) >= 5:
            price_data['ma5'] = float(recent_df['Close'].head(5).mean())
        if len(recent_df) >= 25:
            price_data['ma25'] = float(recent_df['Close'].head(25).mean())

        return price_data

    except Exception as e:
        print(f"Warning: {ticker} の株価parquet取得失敗: {e}")
        return None


def load_backtest_stats():
    """バックテストデータから統計情報を読み込み"""
    try:
        df = pd.read_parquet(BACKTEST_DATA_PATH)

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

            rank_win_rates[rank] = win_rate * 100
            rank_avg_returns[rank] = avg_return * 100

        return {
            'rank_win_rates': rank_win_rates,
            'rank_avg_returns': rank_avg_returns,
        }
    except Exception as e:
        print(f"Warning: バックテストデータ読み込み失敗: {e}")
        return {
            'rank_win_rates': {},
            'rank_avg_returns': {},
        }


def determine_action_comprehensive_v2(row, backtest_stats, fundamentals, price_data, total_stocks):
    """総合的な売買判断（V2 - Deep Search版）"""

    ticker = row['ticker']
    stock_name = row['stock_name']
    grok_rank = row['grok_rank']

    # バックテスト勝率
    backtest_win_rate = backtest_stats['rank_win_rates'].get(grok_rank)
    backtest_win_rate_decimal = backtest_win_rate / 100 if backtest_win_rate else None

    # 改良版ランクスコア
    rank_score = calculate_rank_score_improved(grok_rank, total_stocks, backtest_win_rate_decimal)

    score = rank_score
    reasons = []
    reasons_structured = []
    confidence = '中'

    # === ルール1: Grokランクスコア ===
    reason_text = f'Grokランク{grok_rank}/{total_stocks}'
    if backtest_win_rate:
        reason_text += f'（過去勝率{backtest_win_rate:.1f}%）'
    reasons.append(reason_text)
    reasons_structured.append({
        'type': 'grok_rank',
        'description': reason_text,
        'impact': rank_score
    })

    # === ルール2: 財務情報による判断 ===
    if fundamentals:
        # ROEによる判断
        if fundamentals.get('roe') and fundamentals['roe'] > 15:
            score += 20
            reason_text = f"ROE {fundamentals['roe']:.1f}%（優良）"
            reasons.append(reason_text)
            reasons_structured.append({
                'type': 'fundamentals_roe',
                'description': reason_text,
                'impact': 20
            })
        elif fundamentals.get('roe') and fundamentals['roe'] < 0:
            score -= 15
            reason_text = f"ROE {fundamentals['roe']:.1f}%（赤字）"
            reasons.append(reason_text)
            reasons_structured.append({
                'type': 'fundamentals_roe',
                'description': reason_text,
                'impact': -15
            })

        # 営業利益成長率
        if fundamentals.get('operatingProfitGrowth'):
            growth = fundamentals['operatingProfitGrowth']
            if growth > 50:
                score += 25
                reason_text = f"営業利益成長+{growth:.1f}%"
                reasons.append(reason_text)
                reasons_structured.append({
                    'type': 'fundamentals_growth',
                    'description': reason_text,
                    'impact': 25
                })
            elif growth < -30:
                score -= 20
                reason_text = f"営業利益成長{growth:.1f}%（減益）"
                reasons.append(reason_text)
                reasons_structured.append({
                    'type': 'fundamentals_growth',
                    'description': reason_text,
                    'impact': -20
                })

    # === ルール3: 株価情報による判断 ===
    if price_data:
        # 前日変化率
        daily_change = price_data.get('dailyChangePct', 0)
        if daily_change < -3:
            score += 15
            reason_text = f"前日{daily_change:.1f}%下落（リバウンド期待）"
            reasons.append(reason_text)
            reasons_structured.append({
                'type': 'price_change',
                'description': reason_text,
                'impact': 15
            })
        elif daily_change > 10:
            score -= 10
            reason_text = f"前日+{daily_change:.1f}%急騰（過熱感）"
            reasons.append(reason_text)
            reasons_structured.append({
                'type': 'price_change',
                'description': reason_text,
                'impact': -10
            })

        # ATRボラティリティ
        atr_pct = price_data.get('atrPct')
        if atr_pct:
            if atr_pct < 3.0:
                score += 10
                reason_text = "低ボラ（安定）"
                reasons.append(reason_text)
                reasons_structured.append({
                    'type': 'volatility',
                    'description': reason_text,
                    'impact': 10
                })
            elif atr_pct > 8.0:
                score -= 15
                reason_text = "高ボラ（リスク大）"
                reasons.append(reason_text)
                reasons_structured.append({
                    'type': 'volatility',
                    'description': reason_text,
                    'impact': -15
                })

        # 移動平均との位置関係
        current_price = price_data.get('currentPrice')
        ma25 = price_data.get('ma25')
        if current_price and ma25:
            if current_price > ma25 * 1.05:
                score -= 10
                reason_text = "25日線から+5%以上乖離"
                reasons.append(reason_text)
                reasons_structured.append({
                    'type': 'moving_average',
                    'description': reason_text,
                    'impact': -10
                })
            elif current_price < ma25 * 0.95:
                score += 10
                reason_text = "25日線から-5%以上乖離（割安）"
                reasons.append(reason_text)
                reasons_structured.append({
                    'type': 'moving_average',
                    'description': reason_text,
                    'impact': 10
                })

    # === 行動決定 ===
    if score >= 40:
        action = '買い'
        confidence = '高'
    elif score >= 20:
        action = '買い'
        confidence = '中'
    elif score <= -30:
        action = '売り'
        confidence = '高'
    elif score <= -15:
        action = '売り'
        confidence = '中'
    else:
        action = '静観'

    # 損切りライン計算
    atr_pct = price_data.get('atrPct') if price_data else None
    if action == '売り':
        if atr_pct:
            stop_loss = max(5.0, min(atr_pct * 1.2, 10.0))
        else:
            stop_loss = 7.0
    else:
        if atr_pct:
            stop_loss = max(2.0, min(atr_pct * 0.8, 5.0))
        else:
            stop_loss = 3.0

    return {
        'action': action,
        'reasons_text': ' / '.join(reasons),
        'reasons_structured': reasons_structured,
        'confidence': confidence,
        'score': score,
        'stop_loss': stop_loss,
        'fundamentals': fundamentals,
        'price_data': price_data,
    }


def generate_recommendation_report_v2():
    """売買判断レポート生成（V2）"""

    print("=== Grok推奨銘柄 売買判断レポート生成 V2 ===\n")

    # バックテスト統計
    print("バックテストデータ読み込み中...")
    backtest_stats = load_backtest_stats()

    # 最新Grokデータ
    print("最新Grok推奨銘柄読み込み中...")
    df = pd.read_parquet(LATEST_GROK_PATH)
    total_stocks = len(df)
    print(f"対象銘柄数: {total_stocks}銘柄\n")

    # 各銘柄の分析
    results = []
    json_stocks = []

    for idx, row in df.iterrows():
        ticker = row['ticker']
        stock_name = row['stock_name']
        grok_rank = row['grok_rank']

        print(f"[{idx+1}/{total_stocks}] {ticker} {stock_name} (ランク{grok_rank}) 分析中...")

        # 1. J-Quants財務情報
        print(f"  - J-Quants財務情報取得中...")
        fundamentals = fetch_jquants_fundamentals(ticker)

        # 2. 株価parquet情報
        print(f"  - 株価情報取得中...")
        price_data = fetch_prices_from_parquet(ticker)

        # 3. 総合判断
        result = determine_action_comprehensive_v2(
            row, backtest_stats, fundamentals, price_data, total_stocks
        )

        # HTML用データ
        results.append({
            'ticker': ticker,
            'stock_name': stock_name,
            'grok_rank': grok_rank,
            'action': result['action'],
            'confidence': result['confidence'],
            'score': result['score'],
            'stop_loss': f"{result['stop_loss']:.1f}%",
            'reason': result['reasons_text'],
            'roe': f"{fundamentals.get('roe'):.1f}%" if fundamentals and fundamentals.get('roe') else 'N/A',
            'growth': f"{fundamentals.get('operatingProfitGrowth'):+.1f}%" if fundamentals and fundamentals.get('operatingProfitGrowth') else 'N/A',
        })

        # JSON用データ
        action_map = {'買い': 'buy', '売り': 'sell', '静観': 'hold'}
        confidence_map = {'高': 'high', '中': 'medium', '低': 'low'}

        # technicalData構造に変換
        technical_data = {}
        atr_pct = None
        if price_data:
            # ATR level判定
            atr_pct = price_data.get('atrPct')
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
                'prevClose': price_data.get('prevClose'),
                'prevDayChangePct': price_data.get('dailyChangePct'),
                'atr': {
                    'value': price_data.get('atr'),
                    'level': atr_level
                },
                'volume': price_data.get('volume'),
                'volatilityLevel': volatility_level
            }

        json_stock = {
            'ticker': ticker,
            'stockName': stock_name,
            'grokRank': int(grok_rank),
            'technicalData': technical_data,
            'recommendation': {
                'action': action_map[result['action']],
                'score': int(result['score']),
                'confidence': confidence_map[result['confidence']],
                'stopLoss': {
                    'percent': round(result['stop_loss'], 1),
                    'calculation': f"ATR {atr_pct:.1f}% ベース" if atr_pct is not None else "デフォルト"
                },
                'reasons': result['reasons_structured']
            },
            'categories': []
        }

        json_stocks.append(json_stock)
        print(f"  → 判定: {result['action']} (スコア: {result['score']:+d}, 信頼度: {result['confidence']})\n")

    result_df = pd.DataFrame(results)

    # 分類
    buy_stocks = result_df[result_df['action'] == '買い'].sort_values('score', ascending=False)
    sell_stocks = result_df[result_df['action'] == '売り'].sort_values('score')
    hold_stocks = result_df[result_df['action'] == '静観']

    # JSON保存
    json_data = {
        'version': '2.0',
        'generatedAt': datetime.now().isoformat(),
        'dataSource': {
            'backtestCount': len(backtest_stats['rank_win_rates']) if backtest_stats else 0,
            'backtestPeriod': {
                'start': '2025-11-04',
                'end': '2025-11-13'
            },
            'technicalDataDate': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        },
        'summary': {
            'total': total_stocks,
            'buy': len(buy_stocks),
            'sell': len(sell_stocks),
            'hold': len(hold_stocks)
        },
        'stocks': json_stocks,
    }

    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    print(f"\n=== 完了 ===")
    print(f"JSON保存: {OUTPUT_JSON_PATH}")
    print(f"\n【サマリー】")
    print(f"  買い: {len(buy_stocks)}銘柄")
    print(f"  売り: {len(sell_stocks)}銘柄")
    print(f"  静観: {len(hold_stocks)}銘柄")

    if len(buy_stocks) > 0:
        print(f"\n【買い候補】")
        for _, row in buy_stocks.iterrows():
            print(f"  - {row['ticker']} {row['stock_name']} (スコア: +{row['score']}, ROE: {row['roe']}, 成長: {row['growth']})")

    if len(sell_stocks) > 0:
        print(f"\n【売り候補】")
        for _, row in sell_stocks.iterrows():
            print(f"  - {row['ticker']} {row['stock_name']} (スコア: {row['score']}, ROE: {row['roe']}, 成長: {row['growth']})")


if __name__ == '__main__':
    generate_recommendation_report_v2()
