# Deep Analysis Data Source Mapping

このドキュメントは `deep_analysis_YYYY-MM-DD.json` の各カラムがどのデータソースから取得されるかを定義します。

## データソース一覧

1. **J-Quants API** - 日本の金融データAPI（要認証）
2. **yfinance** - Yahoo Finance Python ライブラリ
3. **WebSearch** - Claude Code の WebSearch 機能（手動実行）
4. **計算** - 他のデータから計算

---

## カラム定義

### 基本情報

| カラム名 | データソース | 取得方法 | 備考 |
|---------|------------|---------|------|
| `ticker` | - | 入力データ | 例: "6269.T" |
| `stockName` | J-Quants | `/listed/info` API | CompanyName |
| `grokRank` | - | 入力データ | Grokランキング |

### 企業情報 (companyInfo)

| カラム名 | データソース | 取得方法 | 備考 |
|---------|------------|---------|------|
| `companyName` | J-Quants | `/listed/info` → CompanyName | 日本語企業名 |
| `companyNameEnglish` | J-Quants | `/listed/info` → CompanyNameEnglish | 英語企業名 |
| `sector17` | J-Quants | `/listed/info` → Sector17CodeName | 17業種分類 |
| `sector33` | J-Quants | `/listed/info` → Sector33CodeName | 33業種分類 |
| `marketCode` | J-Quants | `/listed/info` → MarketCode | 市場コード |
| `marketName` | J-Quants | `/listed/info` → MarketCodeName | 市場名（プライム等）|
| `scaleCategory` | J-Quants | `/listed/info` → ScaleCategory | 規模区分 |

**実装例:**
```python
from scripts.lib.jquants_client import JQuantsClient

client = JQuantsClient()
code = ticker.replace('.T', '').ljust(5, '0')
response = client.request('/listed/info', params={'code': code})
info = response['info'][0]

companyInfo = {
    'companyName': info.get('CompanyName'),
    'companyNameEnglish': info.get('CompanyNameEnglish'),
    'sector17': info.get('Sector17CodeName'),
    'sector33': info.get('Sector33CodeName'),
    'marketCode': info.get('MarketCode'),
    'marketName': info.get('MarketCodeName'),
    'scaleCategory': info.get('ScaleCategory'),
}
```

---

### 財務データ (fundamentals)

| カラム名 | データソース | 取得方法 | 備考 |
|---------|------------|---------|------|
| `disclosedDate` | J-Quants | `/fins/statements` → DisclosedDate | 開示日 |
| `fiscalYear` | J-Quants | `/fins/statements` → CurrentFiscalYearStartDate | 会計年度 |
| `fiscalPeriod` | J-Quants | `/fins/statements` → TypeOfCurrentPeriod | 期間（1Q, 2Q等）|
| `eps` | J-Quants | `/fins/statements` → EarningsPerShare | 1株あたり利益 |
| `bps` | yfinance | 計算: Equity / SharesOutstanding | 1株あたり純資産 |
| `operatingProfit` | J-Quants | `/fins/statements` → OperatingProfit | 営業利益 |
| `ordinaryProfit` | - | - | J-Quants APIにデータなし（0.0固定）|
| `netIncome` | J-Quants | `/fins/statements` → Profit | 純利益 |
| `revenue` | J-Quants | `/fins/statements` → NetSales | 売上高 |
| `totalAssets` | J-Quants | `/fins/statements` → TotalAssets | 総資産 |
| `equity` | J-Quants | `/fins/statements` → Equity | 純資産 |
| `roe` | 計算 | NetIncome / Equity * 100 | 自己資本利益率 |
| `roa` | 計算 | NetIncome / TotalAssets * 100 | 総資産利益率 |
| `revenueGrowthYoY` | 計算 | (今期 - 前期) / 前期 * 100 | 売上成長率 |
| `profitGrowthYoY` | 計算 | (今期 - 前期) / 前期 * 100 | 利益成長率 |

**実装例:**
```python
# BPS計算（yfinance）
import yfinance as yf

yf_ticker = yf.Ticker(ticker)
balance_sheet = yf_ticker.balance_sheet
info = yf_ticker.info

shares = info.get('sharesOutstanding')
equity = balance_sheet.loc['Total Equity Gross Minority Interest'].iloc[0]
bps = equity / shares if shares and equity else 0.0
```

---

### 価格分析 (priceAnalysis)

| カラム名 | データソース | 取得方法 | 備考 |
|---------|------------|---------|------|
| `trend` | 計算 | MA比較で判定 | "強い上昇トレンド"等 |
| `priceMovement.5day` | yfinance | 過去5日間の価格データから計算 | changePct, high, low等 |
| `priceMovement.10day` | yfinance | 過去10日間の価格データから計算 | 同上 |
| `priceMovement.30day` | yfinance | 過去30日間の価格データから計算 | 同上 |
| `volumeAnalysis.latest` | yfinance | 最新の出来高 | - |
| `volumeAnalysis.avg30day` | yfinance | 30日平均出来高 | - |
| `volumeAnalysis.ratio` | 計算 | latest / avg30day | 出来高比率 |
| `volumeAnalysis.level` | 計算 | ratio > 1.5 → "高" | 出来高レベル |
| `technicalLevels.ma5` | yfinance | 5日移動平均 | - |
| `technicalLevels.ma10` | yfinance | 10日移動平均 | - |
| `technicalLevels.ma25` | yfinance | 25日移動平均 | - |
| `technicalLevels.supportLevels` | 計算 | [ma25, ma10, ma5] | サポートライン |
| `technicalLevels.resistanceLevels` | 計算 | [current*1.02, *1.05, *1.08] | レジスタンスライン |
| `patternAnalysis` | 計算 | 価格パターン検出 | "5日連続上昇"等 |

**実装例:**
```python
import yfinance as yf
from datetime import datetime, timedelta

yf_ticker = yf.Ticker(ticker)
end_date = datetime.strptime(target_date, '%Y-%m-%d')
start_date = end_date - timedelta(days=90)

hist = yf_ticker.history(start=start_date, end=end_date + timedelta(days=1))

# トレンド判定
ma5 = hist['Close'].iloc[-5:].mean()
ma10 = hist['Close'].iloc[-10:].mean()
ma25 = hist['Close'].iloc[-25:].mean()
current_price = hist['Close'].iloc[-1]

if current_price > ma5 > ma10 > ma25:
    trend = '強い上昇トレンド'
elif current_price > ma10:
    trend = '上昇トレンド'
# ... 他の条件
```

---

### 株価情報 (stockPrice)

| カラム名 | データソース | 取得方法 | 備考 |
|---------|------------|---------|------|
| `current` | yfinance | 対象日の終値 | - |
| `change_YYYY-MM-DD` | yfinance | (当日 - 前日) / 前日 * 100 | 前日比％ |
| `volumeChange_YYYY-MM-DD` | yfinance | (当日 - 前日) / 前日 * 100 | 出来高変化率％ |
| `materialExhaustion` | - | false固定 | 材料出尽くし判定 |

---

### アナリスト予想 (analyst)

| カラム名 | データソース | 取得方法 | 備考 |
|---------|------------|---------|------|
| `targetPrice` | WebSearch | 手動検索 | みんかぶ、株予報Pro等 |
| `upside` | 計算 | (target - current) / current * 100 | 目標株価までの上昇率％ |
| `rating` | WebSearch | 手動検索 | "買い"、"中立"、"カバレッジなし"等 |

**WebSearch手順:**
1. 各銘柄について以下のクエリで検索:
   ```
   {証券コード} {銘柄名} アナリスト 目標株価 コンセンサス 2025
   ```
2. 検索結果から以下を抽出:
   - **みんかぶ**: アナリストコンセンサス目標株価
   - **株予報Pro**: 目標株価コンセンサス
   - **IFIS株予報**: レーティングとコンセンサス
3. データがない場合:
   - `targetPrice`: 0.0
   - `upside`: "0.0%"
   - `rating`: "カバレッジなし"

**例（ニデック 6594の場合）:**
```
WebSearch: "6594 ニデック アナリスト 目標株価 コンセンサス 2025"

結果:
- 目標株価: 3,336円
- upside: 48.53%
- rating: 買い
```

---

### 決算情報 (earnings)

| カラム名 | データソース | 取得方法 | 備考 |
|---------|------------|---------|------|
| `revenueGrowth` | yfinance | financials から計算 | "XX.XX%" 形式 |
| `operatingProfitGrowth` | yfinance | financials から計算 | "XX.XX%" 形式 |

**実装例:**
```python
import yfinance as yf

yf_ticker = yf.Ticker(ticker)
financials = yf_ticker.financials

if 'Total Revenue' in financials.index:
    revenues = financials.loc['Total Revenue']
    if len(revenues) >= 2:
        latest = revenues.iloc[0]
        previous = revenues.iloc[1]
        growth = ((latest - previous) / previous * 100)
        revenueGrowth = f"{growth:.2f}%"
```

---

### Deep Analysis固有フィールド

以下のフィールドは別途Deep Analysis生成プロセスで作成されます:

| カラム名 | データソース | 備考 |
|---------|------------|------|
| `v2Score` | AI生成 | v2スコア |
| `finalScore` | AI生成 | 最終スコア |
| `scoreAdjustment` | AI生成 | スコア調整値 |
| `recommendation` | AI生成 | 推奨アクション |
| `confidence` | AI生成 | 信頼度 |
| `verdict` | AI生成 | 総評 |
| `dayTradeScore` | AI生成 | デイトレスコア |
| `dayTradeRecommendation` | AI生成 | デイトレ推奨 |
| `dayTradeReasons` | AI生成 | デイトレ理由 |
| `sectorTrend` | AI生成 | セクタートレンド |
| `marketSentiment` | AI生成 | 市場センチメント |
| `newsHeadline` | AI生成 | ニュースヘッドライン |
| `latestNews` | AI生成 | 最新ニュース配列 |
| `risks` | AI生成 | リスク配列 |
| `opportunities` | AI生成 | 機会配列 |
| `adjustmentReasons` | AI生成 | 調整理由配列 |
| `webMaterials` | WebSearch | Web検索結果 |
| `originalRecommendation` | AI生成 | 元の推奨 |
| `originalTechnicalData` | - | 元のテクニカルデータ |

---

## データ取得の完全な流れ

### 1. 基本データ収集
```bash
# J-Quants認証情報確認
cat /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/.env.jquants

# スクリプト実行
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly
python3 scripts/fetch_deep_analysis_data.py
```

### 2. アナリストデータ収集（手動）
各銘柄について以下を実行:

```python
# Claude Code WebSearch
WebSearch: "{code} {name} アナリスト 目標株価 コンセンサス 2025"

# 例:
# 6269 三井海洋開発 → targetPrice: 9600, rating: 買い
# 7018 内海造船 → targetPrice: 0, rating: カバレッジなし
# ...
```

### 3. データ統合
```bash
# 全データを統合してJSONファイル作成
# deep_analysis_YYYY-MM-DD.json を生成
```

### 4. Parquetマージ
```bash
# grok_analysis_merged.parquet にマージ
python3 scripts/pipeline/enrich_grok_analysis_with_deep_analysis.py
```

---

## 品質チェックリスト

データ生成時に以下を確認:

- [ ] null値が存在しない
- [ ] 空文字列が存在しない
- [ ] デフォルト値（"該当なし"、"不明"等）が存在しない
- [ ] アナリストカバレッジなしの銘柄は明示的に "カバレッジなし"
- [ ] 数値フィールドは0.0以上の実数値
- [ ] パーセント表記は "XX.XX%" 形式
- [ ] 日付フィールドは "YYYY-MM-DD" 形式

---

## エラーハンドリング

データ取得失敗時の対応:

| データソース | エラー時の対応 |
|------------|--------------|
| J-Quants API | APIにデータがない場合は0.0または空配列 |
| yfinance | データがない場合は0.0または計算不可 |
| WebSearch | カバレッジなしの場合は "カバレッジなし" |

**重要**: null、空文字列、"該当なし"等のデフォルト値は絶対に使用しない。

---

## データ取得優先順位（API呼び出し最小化）

既存のparquetファイルを最大限活用してAPI呼び出しを削減します。

### 優先順位ルール

1. **既存parquetファイル** （最優先）
2. **J-Quants API** （parquetにない場合）
3. **yfinance** （J-Quantsにない場合）
4. **WebSearch** （手動、アナリストデータのみ）

---

## 利用可能なParquetファイル

### 価格データ

| ファイル | 内容 | 利用可能なカラム |
|---------|------|-----------------|
| `prices_max_1d.parquet` | 日次OHLCV | date, Open, High, Low, Close, Volume, ticker |
| `prices_60d_5m.parquet` | 5分足（60日分）| date, Open, High, Low, Close, Volume, ticker |
| `prices_730d_1h.parquet` | 1時間足（730日分）| date, Open, High, Low, Close, Volume, ticker |

**活用方法:**
```python
import pandas as pd
from datetime import datetime

# 対象日の価格データ取得
prices_df = pd.read_parquet('data/parquet/prices_max_1d.parquet')
prices_df['date'] = pd.to_datetime(prices_df['date'])

target_date = datetime.strptime('2025-11-18', '%Y-%m-%d')
day_prices = prices_df[
    (prices_df['date'].dt.date == target_date.date()) &
    (prices_df['ticker'] == ticker)
]

if not day_prices.empty:
    # parquetから取得
    stockPrice = {
        'current': day_prices['Close'].iloc[0],
        'high': day_prices['High'].iloc[0],
        'low': day_prices['Low'].iloc[0],
        # ...
    }
else:
    # yfinanceから取得（フォールバック）
    yf_ticker = yf.Ticker(ticker)
    # ...
```

### メタデータ

| ファイル | 内容 | 利用可能なカラム |
|---------|------|-----------------|
| `meta_jquants.parquet` | 株式基本情報 | ticker, code, stock_name, market, sectors, series, topixnewindexseries |

**活用方法:**
```python
# 銘柄名・セクター情報取得
meta_df = pd.read_parquet('data/parquet/meta_jquants.parquet')
stock_meta = meta_df[meta_df['ticker'] == ticker]

if not stock_meta.empty:
    companyInfo = {
        'companyName': stock_meta['stock_name'].iloc[0],
        'sector33': stock_meta['sectors'].iloc[0] if 'sectors' in stock_meta else None,
        # ...
    }
else:
    # J-Quants APIから取得（フォールバック）
    # ...
```

---

## データ取得フローチャート

### 価格データ (stockPrice, priceAnalysis)

```
1. prices_max_1d.parquet を確認
   ├─ データあり → parquetから取得 ✅
   └─ データなし → yfinance API 呼び出し ⚠️

2. priceAnalysis計算用の履歴データ
   ├─ prices_730d_1h.parquet で30日分確認
   ├─ データあり → parquetから計算 ✅
   └─ データなし → yfinance.history() 呼び出し ⚠️
```

### 企業情報 (companyInfo)

```
1. meta_jquants.parquet を確認
   ├─ stock_name あり → parquetから取得 ✅
   └─ なし → 次へ

2. J-Quants /listed/info API
   ├─ CompanyNameEnglish, Sector等 → APIから取得 ⚠️
   └─ エラー → デフォルト値（避ける）
```

### 財務データ (fundamentals)

```
J-Quants /fins/statements API
├─ 直接APIから取得 ⚠️
│  （parquetに財務データなし）
└─ 計算可能項目（ROE, ROA）は取得データから計算
```

### アナリストデータ (analyst)

```
WebSearch（手動） - 常に実施
├─ "{code} {name} アナリスト 目標株価 コンセンサス 2025"
└─ 検索結果から手動抽出
```

---

## 実装例: データ取得の最適化

```python
import pandas as pd
from pathlib import Path
from datetime import datetime

# Parquetファイルパス
PRICES_1D = Path('data/parquet/prices_max_1d.parquet')
META_JQUANTS = Path('data/parquet/meta_jquants.parquet')

def get_stock_price_optimized(ticker, target_date):
    """
    価格データ取得（parquet優先）
    """
    # 1. parquetから取得を試みる
    if PRICES_1D.exists():
        prices_df = pd.read_parquet(PRICES_1D)
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        
        target_dt = pd.to_datetime(target_date)
        day_data = prices_df[
            (prices_df['date'].dt.date == target_dt.date()) &
            (prices_df['ticker'] == ticker)
        ]
        
        if not day_data.empty:
            print(f"  ✅ {ticker}: 価格データをparquetから取得")
            return {
                'current': float(day_data['Close'].iloc[0]),
                'high': float(day_data['High'].iloc[0]),
                'low': float(day_data['Low'].iloc[0]),
                # ...
            }
    
    # 2. parquetにない場合のみyfinance
    print(f"  ⚠️  {ticker}: yfinance APIから取得")
    import yfinance as yf
    yf_ticker = yf.Ticker(ticker)
    # ... yfinance処理
    
def get_company_info_optimized(ticker):
    """
    企業情報取得（parquet → J-Quants API）
    """
    # 1. parquetから基本情報
    if META_JQUANTS.exists():
        meta_df = pd.read_parquet(META_JQUANTS)
        stock_meta = meta_df[meta_df['ticker'] == ticker]
        
        if not stock_meta.empty:
            print(f"  ✅ {ticker}: メタデータをparquetから取得")
            company_info = {
                'companyName': stock_meta['stock_name'].iloc[0],
                'sector33': stock_meta.get('sectors', [''])[0] if 'sectors' in stock_meta.columns else '',
            }
        else:
            company_info = {}
    else:
        company_info = {}
    
    # 2. 詳細情報はJ-Quants APIから
    print(f"  ⚠️  {ticker}: 詳細情報をJ-Quants APIから取得")
    from scripts.lib.jquants_client import JQuantsClient
    
    client = JQuantsClient()
    code = ticker.replace('.T', '').ljust(5, '0')
    response = client.request('/listed/info', params={'code': code})
    
    if response and 'info' in response:
        info = response['info'][0]
        company_info.update({
            'companyNameEnglish': info.get('CompanyNameEnglish', ''),
            'sector17': info.get('Sector17CodeName', ''),
            'marketCode': info.get('MarketCode', ''),
            # ...
        })
    
    return company_info
```

---

## API呼び出し削減効果

### Before（全てAPI呼び出し）
- 12銘柄 × 価格データ = 12 yfinance calls
- 12銘柄 × 企業情報 = 12 J-Quants calls
- 12銘柄 × 財務データ = 12 J-Quants calls
- **合計: 36 API calls**

### After（parquet優先）
- 価格データ: parquetから取得 = 0 API calls ✅
- 企業情報: parquetから一部 + J-Quants詳細 = 12 J-Quants calls
- 財務データ: J-Quants必須 = 12 J-Quants calls
- **合計: 24 API calls（33%削減）**

---

## チェックリスト: データ取得前の確認

データ取得スクリプトを書く前に確認:

- [ ] 該当データがparquetファイルに存在するか？
- [ ] parquetのデータ更新日時は最新か？
- [ ] parquetにticker・日付でフィルタ可能か？
- [ ] parquetにないデータのみAPIから取得するか？
- [ ] API呼び出し回数を最小化しているか？

