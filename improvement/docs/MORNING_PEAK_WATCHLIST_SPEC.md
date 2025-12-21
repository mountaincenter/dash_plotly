# 朝高パターン常習犯リスト 要件定義

## 1. 概要

朝の寄り付き直後に高値を付け、その後大幅に下落するパターンを繰り返す銘柄（常習犯）を検出し、空売り候補としてリスト化する。

## 2. パターン定義

### 2.1 朝高パターンの条件
```
前場高値（9:00-11:30の最高値）= 日中高値（9:00-15:00の最高値）
AND
終値 ≦ 日中高値 × 0.95（高値から-5%以上下落）
```

### 2.2 常習犯の定義
- **3回以上**: 常習犯リスト（`morning_peak_watchlist.parquet`）
- **1-2回**: 予備リスト（`morning_peak_1_2_times.parquet`）

## 3. データソース

| ファイル | 説明 |
|---------|------|
| `data/surge_candidates_5m.parquet` | 5分足データ（約3ヶ月分） |
| `data/meta_jquants.parquet` | 銘柄メタ情報（銘柄名、市場区分） |

### 3.1 5分足データのカラム
- `ticker`: 銘柄コード（例: 4425.T）
- `Datetime`: タイムスタンプ
- `Open`, `High`, `Low`, `Close`: OHLC
- `Volume`: 出来高

## 4. 出力ファイル

### 4.1 Parquetファイル

#### `data/morning_peak_watchlist.parquet`（常習犯: 3回以上）
| カラム | 型 | 説明 |
|--------|-----|------|
| ticker | str | 銘柄コード |
| morning_peak_count | int | パターン発生回数 |
| avg_drop | float | 平均崩れ幅（%） |
| stock_name | str | 銘柄名 |
| market | str | 市場区分 |
| latest_close | float | 最新株価 |

#### `data/morning_peak_1_2_times.parquet`（1-2回）
| カラム | 型 | 説明 |
|--------|-----|------|
| ticker | str | 銘柄コード |
| pattern_count | int | パターン発生回数 |
| avg_crash | float | 平均崩れ幅（%） |
| stock_name | str | 銘柄名 |
| market | str | 市場区分 |
| price | float | 最新株価 |

### 4.2 HTMLファイル

#### `output/morning_peak_watchlist.html`
- 常習犯リストのインタラクティブ表示
- チェックボックス機能:
  - 制度信用可
  - いちにち信用売可
  - 不可
- 進捗表示バー
- CSVエクスポート機能
- フィルター（銘柄名検索、市場区分）

### 4.3 CSVファイル（チェックボックス状態保存）

#### `output/morning_peak_shortable.csv`
| カラム | 型 | 説明 |
|--------|-----|------|
| ticker | str | 銘柄コード |
| stock_name | str | 銘柄名 |
| shortable | bool | 制度信用可 |
| day_trade | bool | いちにち信用売可 |
| ng | bool | 不可 |

## 5. 実行スクリプト

### 5.1 `scripts/generate_morning_peak_watchlist.py`
Parquetファイル生成

```
入力:
  - data/surge_candidates_5m.parquet
  - data/meta_jquants.parquet

出力:
  - data/morning_peak_watchlist.parquet
  - data/morning_peak_1_2_times.parquet
```

### 5.2 HTML生成（未実装 → 要実装）
現状: アドホックなPythonスクリプトで手動生成
課題: HTML再生成時にチェックボックス状態が消失

## 6. トレーサビリティ要件（TODO）

### 6.1 現状の問題
- パターン検出の再現性が担保されていない
- 5分足データが更新されると過去の結果と整合しない
- 「なぜこの銘柄がリストに入った/消えた」を追跡できない

### 6.2 必要な改善

#### A. 生成メタデータの記録
```json
{
  "generated_at": "2024-12-08T03:00:00+09:00",
  "source_file": "surge_candidates_5m.parquet",
  "source_date_range": ["2024-09-08", "2024-12-05"],
  "source_row_count": 1234567,
  "source_ticker_count": 3000,
  "result_count": 462,
  "script_version": "1.0.0"
}
```

#### B. 銘柄別詳細ログ
各銘柄がパターン該当した日付と崩れ幅を記録:
```
ticker,date,high_to_close
4425.T,2024-10-15,-8.5
4425.T,2024-11-02,-6.2
4425.T,2024-11-20,-7.8
```

#### C. 差分レポート
前回生成時との差分を記録:
- 新規追加銘柄
- 削除された銘柄
- カウント変化した銘柄

## 7. HTML仕様詳細

### 7.1 テーブルカラム
| 順位 | コード | 銘柄名 | 市場 | 朝高回数 | 平均崩れ | 株価 | 制度信用可 | いちにち信用売可 | 不可 |

### 7.2 数値フォーマット
- 株価: 右寄せ、カンマ区切り、「円」付き（例: 7,220円）
- 回数: 右寄せ
- 崩れ: 右寄せ、%付き（例: -8.8%）

### 7.3 色分け
- 朝高回数・平均崩れに応じた色分け:
  - high（赤系）: 危険度高
  - mid（黄系）: 中程度
  - low（緑系）: 低め

### 7.4 チェックボックス保存
HTMLのJavaScriptでCSVエクスポート → 手動保存
再生成時はCSVから状態を復元

## 8. 運用フロー

```
1. 5分足データ更新（surge_candidates_5m.parquet）
2. generate_morning_peak_watchlist.py 実行
3. HTML生成（チェックボックス状態をCSVから復元）
4. ユーザーがチェックボックスを更新
5. CSVエクスポートで状態保存
```

## 9. 更新履歴

| 日付 | 変更内容 |
|------|---------|
| 2024-12-08 | 初版作成 |
