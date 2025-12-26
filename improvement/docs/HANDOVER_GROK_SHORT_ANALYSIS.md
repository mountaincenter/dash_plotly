# Grok空売り日別分析 - Next.js実装 引き継ぎ資料

## 概要

Python (generate_day_trade_daily_html.py) で実装した空売り分析HTMLをNext.jsフロントエンドに移植するための資料。

## 元データ

### データソース
```
S3: s3://python-stock-yfinance/parquet/backtest/grok_trending_archive.parquet
ローカル: /data/parquet/backtest/grok_trending_archive.parquet
```

### 使用カラム
| カラム名 | 型 | 説明 |
|---------|-----|------|
| selection_date | datetime | 銘柄選定日（23時選定） |
| ticker | string | 銘柄コード (例: 6629.T) |
| stock_name | string | 銘柄名 |
| buy_price | float | 買値（寄付き価格） |
| shortable | bool | 制度信用可否 |
| day_trade | bool | いちにち信用可否 |
| day_trade_available_shares | float | いちにち信用の貸株数（0=取引不可、NaN=データなし） |
| profit_per_100_shares_phase1 | float | 前場引け損益（100株あたり、ロング基準） |
| profit_per_100_shares_phase2 | float | 大引け損益（100株あたり、ロング基準） |
| phase1_win | bool | 前場勝ち（ロング基準） |
| phase2_win | bool | 大引け勝ち（ロング基準） |

### データフィルタリング
```python
# バックテストデータのみ
df = df[df['buy_price'].notna()]

# 2025-11-04以降（それ以前はデータ品質問題あり）
df = df[df['selection_date'] >= '2025-11-04']

# 制度信用 or いちにち信用のみ
df = df[(df['shortable'] == True) | ((df['day_trade'] == True) & (df['shortable'] == False))]
```

## 信用区分の分類（3種類）

```typescript
// 重要: 表示用とロジック用で2種類の区分を管理

// 表示用区分（3種類）
type MarginTypeDisplay = '制度信用' | 'いちにち信用' | 'いちにち信用(0株)';

function getMarginTypeDisplay(row: Row): MarginTypeDisplay {
  if (row.shortable) return '制度信用';
  if (row.day_trade_available_shares === 0) return 'いちにち信用(0株)';
  return 'いちにち信用';
}

// ロジック用区分（2種類）- 集計に使用
type MarginTypeBase = '制度信用' | 'いちにち信用';

function getMarginTypeBase(row: Row): MarginTypeBase {
  return row.shortable ? '制度信用' : 'いちにち信用';
}
```

## 空売り損益の計算（符号反転）

```typescript
// 重要: バックテストデータはロング基準なので符号反転
const shortP1 = -row.profit_per_100_shares_phase1;  // 前場引け
const shortP2 = -row.profit_per_100_shares_phase2;  // 大引け
const shortWin1 = !row.phase1_win;  // 勝ち判定も反転
const shortWin2 = !row.phase2_win;
```

## 除0株ロジック

```typescript
// 除0株 = いちにち信用(0株)を除外
// NaN（2025-12-19以前のデータ）は含める

function isEx0(row: Row): boolean {
  // 制度信用は常に含める
  if (row.shortable) return true;

  // いちにち信用: 0株のみ除外、NaNは含める
  const shares = row.day_trade_available_shares;
  return shares === null || shares === undefined || isNaN(shares) || shares > 0;
}
```

## 期間フィルタの定義

### トップカード用
```typescript
const maxDate = getMaxDate(data);

// 日別 = 直近1日
const daily = data.filter(r => r.selection_date === maxDate);

// 週別 = 直近5営業日
const uniqueDates = [...new Set(data.map(r => r.selection_date))].sort().reverse();
const recent5Days = uniqueDates.slice(0, 5);
const weekly = data.filter(r => recent5Days.includes(r.selection_date));

// 月別 = 最新日が所属する月
const maxMonth = maxDate.slice(0, 7); // "YYYY-MM"
const monthly = data.filter(r => r.selection_date.startsWith(maxMonth));

// 全期間 = フィルタなし
const all = data;
```

### 詳細セクション用（グルーピング）
```typescript
// 日別: YYYY-MM-DD でグループ
const dailyKey = (date: Date) => formatDate(date, 'yyyy-MM-dd');

// 週別: YYYY/W## でグループ（ISO週番号）
const weeklyKey = (date: Date) => `${getYear(date)}/W${getISOWeek(date).toString().padStart(2, '0')}`;

// 月別: YYYY/MM でグループ
const monthlyKey = (date: Date) => formatDate(date, 'yyyy/MM');
```

## 比較色ロジック

```typescript
// 前場と大引けで良い方に色をつける
function getCompareClasses(p1: number, p2: number): [string, string] {
  if (p1 >= 0 && p2 >= 0) {
    // 両方プラス: 大きい方に緑
    if (p1 > p2) return ['text-green-500', ''];
    if (p2 > p1) return ['', 'text-green-500'];
    return ['', ''];
  } else if (p1 < 0 && p2 < 0) {
    // 両方マイナス: 絶対値大きい方（悪い方）に赤
    if (Math.abs(p1) > Math.abs(p2)) return ['text-red-500', ''];
    if (Math.abs(p2) > Math.abs(p1)) return ['', 'text-red-500'];
    return ['', ''];
  } else {
    // 符号が異なる: プラスに緑、マイナスに赤
    return [
      p1 > 0 ? 'text-green-500' : 'text-red-500',
      p2 > 0 ? 'text-green-500' : 'text-red-500'
    ];
  }
}

// 勝率色
function getWinrateClass(rate: number): string {
  if (rate > 50) return 'text-green-500';
  if (rate < 50) return 'text-red-500';
  return '';
}
```

## UI構成

### 1. トップカード（3枚）
- 総件数: `{count}件` / `制度{n} / いちにち{n}`
- 前場引け: `{profit}円` / `勝率 {rate}%`
- 大引け: `{profit}円` / `勝率 {rate}%`

各カードに2つのタブ:
- **期間タブ**: 日別 / 週別 / 月別 / 全期間
- **全数/除0株タブ**: 全数 / 除0株

### 2. 曜日別カード（月〜金）
各曜日に2枚のカード:
- **制度信用カード**: 価格帯別テーブル
- **いちにち信用カード**: 価格帯別テーブル + 全数/除0株タブ

価格帯:
```typescript
const PRICE_RANGES = [
  { label: '~1,000円', min: 0, max: 1000 },
  { label: '1,000~3,000円', min: 1000, max: 3000 },
  { label: '3,000~5,000円', min: 3000, max: 5000 },
  { label: '5,000~10,000円', min: 5000, max: 10000 },
  { label: '10,000円~', min: 10000, max: Infinity },
];
```

### 3. 詳細セクション（折りたたみ）
タブ切り替え: 日別 / 週別 / 月別

各行のサマリー表示:
```
{期間} {件数}件  前場 +X,XXX(+X,XXX) 大引 +X,XXX(+X,XXX)
                      ↑全数  ↑除0株      ↑全数  ↑除0株
```

展開時のテーブル:
- 日別: 銘柄 / 区分 / 買値 / 株数 / 前場損益 / 大引損益
- 週別・月別: 銘柄 / 日付 / 区分 / 買値 / 株数 / 前場損益 / 大引損益

## 実装上の注意点

### 1. 符号反転を忘れない
バックテストデータはロング基準。空売り表示では必ず符号反転。

### 2. NaN の扱い
`day_trade_available_shares` の NaN は「データなし」であり「取引可能」として扱う。0のみが「取引不可」。

### 3. 除0株の計算順序
集計は `margin_type_base`（2種類）で行い、表示は `margin_type`（3種類）を使う。

### 4. 週番号
ISO週番号（%V）を使用。日本のカレンダー週ではない。

### 5. データ品質
2025-11-04より前のデータは品質問題があるためフィルタで除外している。

## 参照ファイル

| ファイル | 説明 |
|---------|------|
| `improvement/scripts/generate_day_trade_daily_html.py` | Python実装（約900行） |
| `improvement/output/grok_day_trade_daily.html` | 生成されたHTML（参考用） |
| `stock-frontend/app/dev/stock-results/page.tsx` | 期間切り替えUIの参考実装 |

## API設計案

```typescript
// GET /api/grok-short-analysis
interface GrokShortAnalysisResponse {
  // 期間別統計（トップカード用）
  periodStats: {
    daily: PeriodStats;
    weekly: PeriodStats;
    monthly: PeriodStats;
    all: PeriodStats;
  };

  // 曜日別データ
  weekdayData: WeekdayData[];

  // 詳細データ
  details: {
    daily: DailyDetail[];
    weekly: WeeklyDetail[];
    monthly: MonthlyDetail[];
  };
}

interface PeriodStats {
  all: Stats;  // 全数
  ex0: Stats;  // 除0株
}

interface Stats {
  count: number;
  seidoCount: number;
  ichinichiCount: number;
  p1: number;
  p2: number;
  win1: number;  // 勝率%
  win2: number;
}
```

## テスト観点

1. **符号**: 空売り損益が正しく反転されているか
2. **除0株**: NaNが含まれ、0のみ除外されているか
3. **期間**: 日別が1日、週別が5営業日、月別が該当月になっているか
4. **比較色**: 前場/大引けの色ロジックが正しいか
5. **グルーピング**: 週番号・月のグループが正しいか
