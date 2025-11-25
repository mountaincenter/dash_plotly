#!/usr/bin/env python3
"""
Generate deep_analysis_YYYY-MM-DD_base.json with automated data collection

自動収集データ:
- companyInfo: J-Quants /listed/info
- fundamentals: J-Quants /fins/statements + yfinance
- priceAnalysis: parquet + yfinance (60日分の価格データから分析)
- stockPrice: parquet prices_max_1d.parquet
- earnings: yfinance quarterly earnings

手動入力データ（空プレースホルダー）:
- analyst: Claude Code WebSearch結果を手動入力
- latestNews: Claude Code WebSearch結果を手動入力
- webMaterials: Claude Code WebSearch結果を手動入力

Usage:
  python3 scripts/deep_search/generate_deep_analysis_base.py

Output:
  data/parquet/backtest/analysis/deep_analysis_YYYY-MM-DD_base.json
"""
import sys
import json
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import os
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# J-Quants API設定
# Read from .env.jquants if available
ENV_JQUANTS_FILE = ROOT / '.env.jquants'
JQUANTS_MAIL_ADDRESS = ''
JQUANTS_PASSWORD = ''

if ENV_JQUANTS_FILE.exists():
    with open(ENV_JQUANTS_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('JQUANTS_MAIL_ADDRESS='):
                JQUANTS_MAIL_ADDRESS = line.split('=', 1)[1]
            elif line.startswith('JQUANTS_PASSWORD='):
                JQUANTS_PASSWORD = line.split('=', 1)[1]

# Get refresh token from mail/password with retry
JQUANTS_REFRESH_TOKEN = ''
if JQUANTS_MAIL_ADDRESS and JQUANTS_PASSWORD:
    import time
    auth_url = "https://api.jquants.com/v1/token/auth_user"
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            print(f"🔐 Authenticating J-Quants (attempt {attempt + 1}/{max_retries})...")
            # Use json= instead of data= as per official documentation
            auth_response = requests.post(
                auth_url,
                json={
                    "mailaddress": JQUANTS_MAIL_ADDRESS,
                    "password": JQUANTS_PASSWORD
                },
                timeout=10
            )
            auth_response.raise_for_status()
            JQUANTS_REFRESH_TOKEN = auth_response.json()['refreshToken']
            print(f"✅ J-Quants authentication successful")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"❌ J-Quants authentication failed after {max_retries} attempts: {e}")
                print("Cannot proceed without J-Quants data. Please check:")
                print("  1. JQUANTS_MAIL_ADDRESS and JQUANTS_PASSWORD in .env.jquants")
                print("  2. J-Quants API status (https://jpx-jquants.com/)")
                sys.exit(1)

# パス設定
TRADING_RECOMMENDATION_JSON = ROOT / 'data' / 'parquet' / 'backtest' / 'trading_recommendation.json'
OUTPUT_DIR = ROOT / 'data' / 'parquet' / 'backtest' / 'analysis'
PRICES_1D_PARQUET = ROOT / 'data' / 'parquet' / 'prices_max_1d.parquet'
PRICES_60D_PARQUET = ROOT / 'data' / 'parquet' / 'prices_60d_5m.parquet'
META_JQUANTS_PARQUET = ROOT / 'data' / 'parquet' / 'meta_jquants.parquet'


class JQuantsClient:
    """J-Quants API Client"""

    def __init__(self, refresh_token: str):
        self.refresh_token = refresh_token
        self.id_token = None
        self._authenticate()

    def _authenticate(self):
        """認証してIDトークン取得"""
        if not self.refresh_token:
            raise ValueError("JQUANTS_REFRESH_TOKEN not set")

        url = f"https://api.jquants.com/v1/token/auth_refresh?refreshtoken={self.refresh_token}"

        try:
            response = requests.post(url, timeout=10)
            response.raise_for_status()
            self.id_token = response.json()['idToken']
            print("✅ J-Quants ID token acquired")
        except Exception as e:
            raise Exception(f"J-Quants ID token acquisition failed: {e}")

    def get_listed_info(self, ticker: str) -> Optional[Dict]:
        """上場情報取得"""
        if not self.id_token:
            return None

        # Convert ticker format: 1234.T -> 12340
        code = ticker.replace('.T', '0')

        url = "https://api.jquants.com/v1/listed/info"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {"code": code}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('info'):
                return data['info'][0]
            return None
        except Exception as e:
            print(f"  ⚠️  J-Quants listed/info failed for {ticker}: {e}")
            return None

    def get_financial_statements(self, ticker: str) -> Optional[Dict]:
        """財務諸表取得"""
        if not self.id_token:
            return None

        code = ticker.replace('.T', '0')

        url = "https://api.jquants.com/v1/fins/statements"
        headers = {"Authorization": f"Bearer {self.id_token}"}
        params = {"code": code}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('statements'):
                # 最新のデータを取得
                statements = sorted(data['statements'],
                                   key=lambda x: x.get('DisclosedDate', ''),
                                   reverse=True)
                return statements[0] if statements else None
            return None
        except Exception as e:
            print(f"  ⚠️  J-Quants fins/statements failed for {ticker}: {e}")
            return None


def load_trading_recommendation() -> Dict:
    """trading_recommendation.json読み込み"""
    with open(TRADING_RECOMMENDATION_JSON, 'r', encoding='utf-8') as f:
        return json.load(f)


def detect_target_date(recommendation_data: Dict) -> str:
    """対象日付を検出（technicalDataDate + 1日）"""
    technical_date = recommendation_data['dataSource']['technicalDataDate']
    date_obj = datetime.strptime(technical_date, '%Y-%m-%d')
    target_date = date_obj + timedelta(days=1)
    return target_date.strftime('%Y-%m-%d')


def load_parquet_data() -> Dict[str, pd.DataFrame]:
    """Parquetファイル読み込み"""
    data = {}

    if PRICES_1D_PARQUET.exists():
        data['prices_1d'] = pd.read_parquet(PRICES_1D_PARQUET)
        print(f"  ✅ Loaded prices_max_1d.parquet: {len(data['prices_1d'])} records")

    if PRICES_60D_PARQUET.exists():
        data['prices_60d'] = pd.read_parquet(PRICES_60D_PARQUET)
        print(f"  ✅ Loaded prices_60d_5m.parquet: {len(data['prices_60d'])} records")

    if META_JQUANTS_PARQUET.exists():
        data['meta_jquants'] = pd.read_parquet(META_JQUANTS_PARQUET)
        print(f"  ✅ Loaded meta_jquants.parquet: {len(data['meta_jquants'])} records")

    return data


def fetch_company_info(ticker: str, jquants: JQuantsClient, parquet_data: Dict) -> Dict:
    """companyInfo取得（7 keys）"""
    info = {
        'companyName': '',
        'companyNameEnglish': '',
        'sector17': '',
        'sector33': '',
        'marketCode': '',
        'marketName': '',
        'scaleCategory': ''
    }

    # J-Quants /listed/info
    listed_info = jquants.get_listed_info(ticker)
    if listed_info:
        info['companyName'] = listed_info.get('CompanyName', '')
        info['companyNameEnglish'] = listed_info.get('CompanyNameEnglish', '')
        info['sector17'] = listed_info.get('Sector17CodeName', '')
        info['sector33'] = listed_info.get('Sector33CodeName', '')
        info['marketCode'] = listed_info.get('MarketCode', '')
        info['marketName'] = listed_info.get('MarketCodeName', '')
        info['scaleCategory'] = listed_info.get('ScaleCategory', '')

    # Fallback: meta_jquants.parquet
    if not info['companyName'] and 'meta_jquants' in parquet_data:
        meta_df = parquet_data['meta_jquants']
        meta_row = meta_df[meta_df['ticker'] == ticker]
        if not meta_row.empty:
            info['companyName'] = meta_row.iloc[0].get('name', '')
            info['sector33'] = meta_row.iloc[0].get('sector_33', '')

    return info


def fetch_fundamentals(ticker: str, jquants: JQuantsClient) -> Dict:
    """fundamentals取得（15 keys）"""
    fundamentals = {
        'disclosedDate': '',
        'fiscalYear': '',
        'fiscalPeriod': '',
        'eps': None,
        'bps': None,
        'operatingProfit': None,
        'ordinaryProfit': None,
        'netIncome': None,
        'revenue': None,
        'totalAssets': None,
        'equity': None,
        'roe': None,
        'roa': None,
        'revenueGrowthYoY': None,
        'profitGrowthYoY': None
    }

    # J-Quants /fins/statements (per DATA_SOURCE_MAPPING.md)
    statements = jquants.get_financial_statements(ticker)
    if statements:
        fundamentals['disclosedDate'] = statements.get('DisclosedDate', '')
        fundamentals['fiscalYear'] = statements.get('CurrentFiscalYearStartDate', '')  # per docs
        fundamentals['fiscalPeriod'] = statements.get('TypeOfCurrentPeriod', '')
        fundamentals['eps'] = statements.get('EarningsPerShare')  # 実績値
        fundamentals['bps'] = statements.get('BookValuePerShare')
        fundamentals['operatingProfit'] = statements.get('OperatingProfit')  # 実績値（per docs）
        fundamentals['ordinaryProfit'] = 0.0  # J-Quants APIにデータなし（per docs）
        fundamentals['netIncome'] = statements.get('Profit')  # 実績値（per docs）
        fundamentals['revenue'] = statements.get('NetSales')  # 実績値（per docs）
        fundamentals['totalAssets'] = statements.get('TotalAssets')
        fundamentals['equity'] = statements.get('Equity')
        fundamentals['roe'] = statements.get('ResultROE')
        fundamentals['roa'] = statements.get('ResultROA')

    # Fallback: yfinance
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        if not fundamentals['eps']:
            fundamentals['eps'] = info.get('trailingEps')
        if not fundamentals['bps']:
            fundamentals['bps'] = info.get('bookValue')
        if not fundamentals['revenue']:
            fundamentals['revenue'] = info.get('totalRevenue')
        if not fundamentals['roe']:
            fundamentals['roe'] = info.get('returnOnEquity')
        if not fundamentals['roa']:
            fundamentals['roa'] = info.get('returnOnAssets')

        # Growth rates from financials
        financials = stock.financials
        if not financials.empty and 'Total Revenue' in financials.index:
            revenue_series = financials.loc['Total Revenue'].sort_index()
            if len(revenue_series) >= 2:
                recent = revenue_series.iloc[-1]
                previous = revenue_series.iloc[-2]
                if previous != 0:
                    fundamentals['revenueGrowthYoY'] = ((recent - previous) / abs(previous)) * 100

        if not financials.empty and 'Operating Income' in financials.index:
            profit_series = financials.loc['Operating Income'].sort_index()
            if len(profit_series) >= 2:
                recent = profit_series.iloc[-1]
                previous = profit_series.iloc[-2]
                if previous != 0:
                    fundamentals['profitGrowthYoY'] = ((recent - previous) / abs(previous)) * 100

    except Exception as e:
        print(f"  ⚠️  yfinance fundamentals failed for {ticker}: {e}")

    return fundamentals


def analyze_price_data(ticker: str, target_date: str, parquet_data: Dict) -> Dict:
    """priceAnalysis取得（5 keys）- 60日分の価格データから分析"""
    analysis = {
        'trend': '',
        'priceMovement': '',
        'volumeAnalysis': '',
        'technicalLevels': '',
        'patternAnalysis': ''
    }

    # prices_60d_5m.parquet から60日分のデータ取得
    if 'prices_60d' not in parquet_data:
        return analysis

    df_60d = parquet_data['prices_60d']
    ticker_data = df_60d[df_60d['ticker'] == ticker].copy()

    if ticker_data.empty:
        return analysis

    # date列をdatetimeに変換
    ticker_data['date'] = pd.to_datetime(ticker_data['date'])
    ticker_data = ticker_data.sort_values('date')

    # 最新60日分
    recent_60d = ticker_data.tail(60)

    if len(recent_60d) < 10:
        return analysis

    # Trend analysis
    closes = recent_60d['Close'].values
    ma_20 = recent_60d['Close'].rolling(20).mean().iloc[-1] if len(recent_60d) >= 20 else closes[-1]
    ma_60 = recent_60d['Close'].rolling(60).mean().iloc[-1] if len(recent_60d) >= 60 else closes[-1]
    current_price = closes[-1]

    if current_price > ma_20 and ma_20 > ma_60:
        analysis['trend'] = '上昇トレンド（価格 > MA20 > MA60）'
    elif current_price < ma_20 and ma_20 < ma_60:
        analysis['trend'] = '下降トレンド（価格 < MA20 < MA60）'
    else:
        analysis['trend'] = 'レンジ相場'

    # Price movement
    price_change_10d = ((closes[-1] - closes[-10]) / closes[-10] * 100) if len(closes) >= 10 else 0
    price_change_30d = ((closes[-1] - closes[-30]) / closes[-30] * 100) if len(closes) >= 30 else 0

    analysis['priceMovement'] = f'10日変化: {price_change_10d:+.2f}%, 30日変化: {price_change_30d:+.2f}%'

    # Volume analysis
    volumes = recent_60d['Volume'].values
    avg_volume = volumes.mean()
    recent_volume = volumes[-1]
    volume_ratio = (recent_volume / avg_volume) if avg_volume > 0 else 1

    if volume_ratio > 1.5:
        analysis['volumeAnalysis'] = f'出来高急増（平均比 {volume_ratio:.2f}倍）'
    elif volume_ratio < 0.5:
        analysis['volumeAnalysis'] = f'出来高減少（平均比 {volume_ratio:.2f}倍）'
    else:
        analysis['volumeAnalysis'] = f'出来高平常（平均比 {volume_ratio:.2f}倍）'

    # Technical levels
    high_60d = closes.max()
    low_60d = closes.min()
    current_position = (current_price - low_60d) / (high_60d - low_60d) * 100 if high_60d != low_60d else 50

    analysis['technicalLevels'] = f'60日レンジ: ¥{low_60d:.0f}-¥{high_60d:.0f}（現在位置: {current_position:.1f}%）'

    # Pattern analysis (simple)
    last_5_closes = closes[-5:]
    if all(last_5_closes[i] < last_5_closes[i+1] for i in range(len(last_5_closes)-1)):
        analysis['patternAnalysis'] = '5日連続陽線'
    elif all(last_5_closes[i] > last_5_closes[i+1] for i in range(len(last_5_closes)-1)):
        analysis['patternAnalysis'] = '5日連続陰線'
    else:
        analysis['patternAnalysis'] = '明確なパターンなし'

    return analysis


def fetch_stock_price(ticker: str, target_date: str, parquet_data: Dict) -> Dict:
    """stockPrice取得（4 keys）"""
    price_info = {
        'current': None,
        f'change_{target_date}': None,
        f'volumeChange_{target_date}': None,
        'materialExhaustion': ''
    }

    # prices_max_1d.parquet
    if 'prices_1d' in parquet_data:
        df_1d = parquet_data['prices_1d']

        # 対象日付のデータ
        target_data = df_1d[
            (df_1d['ticker'] == ticker) &
            (df_1d['date'] == target_date)
        ]

        if not target_data.empty:
            row = target_data.iloc[0]
            price_info['current'] = row.get('Close')

            # 前日データ
            prev_date = (pd.to_datetime(target_date) - timedelta(days=1)).strftime('%Y-%m-%d')
            prev_data = df_1d[
                (df_1d['ticker'] == ticker) &
                (df_1d['date'] == prev_date)
            ]

            if not prev_data.empty:
                prev_close = prev_data.iloc[0].get('Close')
                if prev_close and prev_close != 0:
                    price_info[f'change_{target_date}'] = ((row.get('Close') - prev_close) / prev_close * 100)

                prev_volume = prev_data.iloc[0].get('Volume')
                if prev_volume and prev_volume != 0:
                    price_info[f'volumeChange_{target_date}'] = ((row.get('Volume') - prev_volume) / prev_volume * 100)

    # Material exhaustion判定（簡易版）
    if price_info['current'] and price_info[f'change_{target_date}']:
        if abs(price_info[f'change_{target_date}']) > 5 and price_info[f'volumeChange_{target_date}'] and price_info[f'volumeChange_{target_date}'] > 100:
            price_info['materialExhaustion'] = '材料出尽くしの可能性'
        else:
            price_info['materialExhaustion'] = '通常'

    return price_info


def fetch_earnings(ticker: str, jquants: JQuantsClient) -> Dict:
    """earnings取得（7 keys）- J-Quants /fins/statements から取得"""
    earnings_info = {
        'date': '',
        'quarter': '',
        'revenue': None,
        'revenueGrowth': None,
        'operatingProfit': None,
        'operatingProfitGrowth': None,
        'evaluation': ''
    }

    try:
        # J-Quants /fins/statements から取得
        statements = jquants.get_financial_statements(ticker)
        if statements:
            earnings_info['date'] = statements.get('DisclosedDate', '')
            earnings_info['quarter'] = statements.get('TypeOfCurrentPeriod', '')

            # revenue（億円表示）
            revenue = statements.get('NetSales')
            if revenue:
                try:
                    revenue_val = float(revenue) if isinstance(revenue, str) else revenue
                    earnings_info['revenue'] = f"{revenue_val / 100000000:.3f}億円"
                except (ValueError, TypeError):
                    earnings_info['revenue'] = None

            # operatingProfit（億円表示）
            op_profit = statements.get('OperatingProfit')
            if op_profit:
                try:
                    op_profit_val = float(op_profit) if isinstance(op_profit, str) else op_profit
                    earnings_info['operatingProfit'] = f"{op_profit_val / 100000000:.3f}億円"
                except (ValueError, TypeError):
                    earnings_info['operatingProfit'] = None

            # Growth rates - 前年同期比
            revenue_growth = statements.get('ChangeInRevenueYoY')
            if revenue_growth:
                earnings_info['revenueGrowth'] = revenue_growth

            op_profit_growth = statements.get('ChangeInOperatingIncomeYoY')
            if op_profit_growth:
                earnings_info['operatingProfitGrowth'] = op_profit_growth

            # Evaluation
            if op_profit_growth and op_profit_growth > 50:
                earnings_info['evaluation'] = '大幅増益'
            elif op_profit_growth and op_profit_growth > 20:
                earnings_info['evaluation'] = '増益'
            elif op_profit_growth and op_profit_growth < -20:
                earnings_info['evaluation'] = '減益'
            else:
                earnings_info['evaluation'] = '横ばい'

    except Exception as e:
        print(f"  ⚠️  J-Quants earnings failed for {ticker}: {e}")

    return earnings_info


def create_empty_placeholders(stock_data: Dict) -> Dict:
    """手動入力用の空プレースホルダー作成"""
    return {
        'analyst': {
            'hasCoverage': False,
            'targetPrice': None,
            'upside': None,
            'rating': 'カバレッジなし'
        },
        'latestNews': [],
        'webMaterials': {},
        'adjustmentReasons': [],
        'risks': [],
        'opportunities': [],
        'sectorTrend': '',
        'marketSentiment': '',
        'newsHeadline': ''
    }


def generate_deep_analysis(stock: Dict, target_date: str, technical_data_date: str, jquants: JQuantsClient, parquet_data: Dict) -> Dict:
    """1銘柄のdeep_analysis生成

    Args:
        target_date: ファイル名用の日付（翌日の取引日、例: 2025-11-19）
        technical_data_date: データ参照用の日付（前日終値、例: 2025-11-18）
    """
    ticker = stock['ticker']
    print(f"\n  [{ticker}] {stock['stockName']}")

    # 自動収集データ（technical_data_dateを使用）
    company_info = fetch_company_info(ticker, jquants, parquet_data)
    fundamentals = fetch_fundamentals(ticker, jquants)
    price_analysis = analyze_price_data(ticker, technical_data_date, parquet_data)
    stock_price = fetch_stock_price(ticker, technical_data_date, parquet_data)
    earnings = fetch_earnings(ticker, jquants)

    # 手動入力用プレースホルダー
    manual_placeholders = create_empty_placeholders(stock)

    # 統合
    analysis = {
        'ticker': ticker,
        'stockName': stock['stockName'],
        'grokRank': stock['grokRank'],
        'v2Score': stock['recommendation']['score'],
        'finalScore': stock['recommendation']['score'],  # 初期値はv2Scoreと同じ
        'scoreAdjustment': 0,
        'recommendation': stock['recommendation']['action'],
        'confidence': stock['recommendation']['confidence'],
        'companyInfo': company_info,
        'fundamentals': fundamentals,
        'priceAnalysis': price_analysis,
        'stockPrice': stock_price,
        'earnings': earnings,
        **manual_placeholders,
        'dayTradeScore': 0,
        'dayTradeRecommendation': 'hold',
        'dayTradeReasons': [],
        'verdict': '',
        'technicalDataActual': stock.get('technicalData', {}),
        'originalTechnicalData': stock.get('technicalData', {}),
        'originalRecommendation': stock.get('recommendation', {})
    }

    return analysis


def main():
    print("=" * 80)
    print("Generate deep_analysis_YYYY-MM-DD_base.json")
    print("=" * 80)

    # Step 1: Load trading_recommendation.json
    print("\n[Step 1] Loading trading_recommendation.json...")
    if not TRADING_RECOMMENDATION_JSON.exists():
        print(f"❌ File not found: {TRADING_RECOMMENDATION_JSON}")
        sys.exit(1)

    recommendation_data = load_trading_recommendation()
    stocks = recommendation_data['stocks']
    print(f"✅ Loaded {len(stocks)} stocks")

    # Step 2: Detect target date
    print("\n[Step 2] Detecting target date...")
    target_date = detect_target_date(recommendation_data)  # ファイル名用（翌日の取引日）
    technical_data_date = recommendation_data['dataSource']['technicalDataDate']  # データ参照用（前日終値）
    print(f"✅ Target date: {target_date} (data reference: {technical_data_date})")

    # Step 3: Initialize J-Quants client
    print("\n[Step 3] Initializing J-Quants client...")
    jquants = JQuantsClient(JQUANTS_REFRESH_TOKEN)

    # Step 4: Load parquet data
    print("\n[Step 4] Loading parquet files...")
    parquet_data = load_parquet_data()

    # Step 5: Generate deep analysis for each stock
    print("\n[Step 5] Generating deep analysis...")
    stock_analyses = []

    for stock in stocks:
        try:
            analysis = generate_deep_analysis(stock, target_date, technical_data_date, jquants, parquet_data)
            stock_analyses.append(analysis)
            print(f"    ✅ Completed")
        except Exception as e:
            print(f"    ❌ Failed: {e}")
            continue

    # Step 6: Save output
    print("\n[Step 6] Saving output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_DIR / f'deep_analysis_{target_date}.json'
    output_data = {'stockAnalyses': stock_analyses}

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved: {output_file}")

    # Summary
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Target date: {target_date}")
    print(f"Total stocks: {len(stock_analyses)}")
    print(f"Output: {output_file}")
    print()
    print("⚠️  Next step: Manual WebSearch required")
    print()
    print("For each stock, run Claude Code WebSearch:")
    print('  Query: "{ticker} {stockName} アナリスト 目標株価 コンセンサス 2025"')
    print()
    print("Then manually edit the JSON file to fill:")
    print("  - analyst.hasCoverage")
    print("  - analyst.targetPrice")
    print("  - analyst.upside")
    print("  - analyst.rating")
    print("  - latestNews")
    print("  - webMaterials")
    print()
    print(f"After editing, rename to: deep_analysis_{target_date}.json")
    print(f"Then run: python3 scripts/deep_search/validate_deep_analysis.py {target_date}")


if __name__ == '__main__':
    main()
