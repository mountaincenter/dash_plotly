# data-pipeline.yml 実行フロー

## 1. Cron設定（起動トリガー）

| 時刻（JST） | Cron（UTC） | 説明 |
|-----------|-----------|------|
| **16:00** | `0 7 * * *` | 株価データ更新のみ（大引け後） |
| **23:00** | `0 14 * * *` | Grok選定 + 株価データ更新 |

毎日土日祝含めて起動されます。

---

## 2. 実行モード判定（Determine execution mode）

```bash
CURRENT_HOUR=$(TZ=Asia/Tokyo date +%H)
```

| 条件 | モード | skip_trading_check | skip_grok | 説明 |
|-----|-------|-------------------|-----------|------|
| `force_grok = true` | `grok_forced` | ✅ true | ❌ false | 手動実行：強制Grok選定 |
| `CURRENT_HOUR = 23` | `grok_daily` | ✅ true | ❌ false | 23時：Grok選定実行 |
| その他（16時） | `stock_data` | ❌ false | ✅ true | 16時：株価データのみ |

---

## 3. 営業日チェック（Check trading day window）

**実行条件:**
```yaml
if: skip_trading_day_check != 'true' AND skip_trading_check != 'true'
```

| モード | 営業日チェック実行？ | チェック内容 |
|-------|---------------|------------|
| `stock_data`（16時） | ✅ **実行** | `scripts/check_trading_day.py` で当日が営業日かチェック |
| `grok_daily`（23時） | ❌ **スキップ** | チェックせずに必ず実行 |
| `grok_forced`（手動） | ❌ **スキップ** | チェックせずに必ず実行 |

**check_trading_day.py の役割:**
- 16:00-02:00 JST の実行ウィンドウ内かチェック
- 当日が営業日かチェック（J-Quants calendar）
- 営業日でなければ `exit 1` でパイプライン停止

---

## 4. パイプライン実行（Run Data Pipeline）

**環境変数:**
```bash
SKIP_GROK_GENERATION=${{ steps.exec_mode.outputs.skip_grok }}
```

| モード | SKIP_GROK_GENERATION | 実行内容 |
|-------|---------------------|---------|
| `stock_data`（16時） | ✅ **true** | 株価データ更新のみ |
| `grok_daily`（23時） | ❌ **false** | 株価データ更新 + **Grok選定** |
| `grok_forced`（手動） | ❌ **false** | 株価データ更新 + **Grok選定** |

実行スクリプト: `python scripts/run_pipeline_scalping_skip_add_grok.py`

---

## 5. 実際の動作フロー

### 16時（JST）の実行

```
16:00 cron起動
  ↓
[実行モード判定] → stock_data モード
  ↓
[営業日チェック] ← check_trading_day.py 実行
  ↓
  営業日？
  ├─ YES → パイプライン実行（SKIP_GROK=true）
  │          1. 金曜日（休業日前営業日）のみ
  │             - meta_jquants.parquet 更新
  │             - meta.parquet 更新
  │             - all_stocks.parquet 更新
  │
  │          2. all_stocks.parquetに基づき株価データを取得
  │             - prices_{period}_{interval}.parquet 生成
  │             - tech_snapshot_1d.parquet 生成
  │
  │          3. 前営業日で選定された銘柄のバックアップ
  │             - backtest/grok_trending_YYYYMMDD.parquet 保存
  │               （YYYYMMDDはcron実行日）
  │
  │          4. バックテスト実施
  │             - backtest/grok_trending_archive.parquet に本日分を追加
  │               ⚠️ 上書き厳禁：必ず追加のみ、既存データには絶対触らない
  │
  │          5. 市場サマリーの生成
  │             - data/parquet/market_summary/structured/YYYY-MM-DD.json
  │
  └─ NO  → 終了（パイプライン実行しない）
```

### 23時（JST）の実行（現状）

```
23:00 cron起動
  ↓
[実行モード判定] → grok_daily モード
  ↓
[営業日チェック] ← ⚠️ スキップされる（skip_trading_check=true）
  ↓
パイプライン実行（SKIP_GROK=false）← 毎日必ず実行される（問題）
  - 株価データ更新
  - Grok選定実行 ← generate_grok_trending.py
  - バックテスト + アーカイブ
```

---

## 6. 問題点

### 現在の23時実行

| 日時 | 実行される？ | 問題 |
|-----|-----------|------|
| 木曜23時 | ✅ 実行される | OK（翌日金曜は営業日） |
| 金曜23時 | ✅ **実行される** | ❌ 問題（翌日土曜は休業日） |
| 土曜23時 | ✅ **実行される** | ❌ 問題（翌日日曜は休業日） |
| 日曜23時（祝前） | ✅ **実行される** | ❌ 問題（翌日祝日は休業日） |
| 日曜23時（通常） | ✅ 実行される | OK（翌日月曜は営業日） |

### 期待する動作

| 日時 | 実行されるべき？ | 理由 |
|-----|--------------|------|
| 木曜23時 | ✅ 実行 | 翌日金曜は営業日 |
| 金曜23時 | ❌ **スキップ** | 翌日土曜は休業日 |
| 土曜23時 | ❌ **スキップ** | 翌日日曜は休業日 |
| 日曜23時（祝前） | ❌ **スキップ** | 翌日祝日は休業日 |
| 日曜23時（通常） | ✅ 実行 | 翌日月曜は営業日 |
| 祝日23時 | ✅ 実行 | 翌日が営業日の場合 |

---

## 7. 必要な修正

23時実行時に**「翌日が営業日かどうか」のチェック**が必要です：

### 修正案

1. **新しいチェックスクリプト作成**: `scripts/check_next_day_trading.py`
   - J-Quantsカレンダーから明日（翌営業日ではなく暦日の明日）が営業日かチェック
   - 営業日なら `exit 0`（実行すべき）
   - 休業日なら `exit 1`（スキップすべき）

2. **ワークフローに翌日営業日チェックを追加**
   ```yaml
   - name: Check next day is trading day (for 23:00 run)
     id: check_next_trading
     if: steps.exec_mode.outputs.mode == 'grok_daily'
     run: |
       python scripts/check_next_day_trading.py
       EXIT_CODE=$?

       if [ $EXIT_CODE -eq 0 ]; then
         echo "✅ Next day is trading day - will execute Grok selection"
       else
         echo "❌ Next day is NOT trading day - skipping Grok selection"
       fi

       exit $EXIT_CODE
   ```

3. **パイプライン実行の条件を修正**
   ```yaml
   - name: Run Data Pipeline
     if: |
       (steps.exec_mode.outputs.mode != 'grok_daily') ||
       (steps.check_next_trading.conclusion == 'success')
     run: |
       ...
   ```

---

## 8. 実装後の23時実行フロー

```
23:00 cron起動
  ↓
[実行モード判定] → grok_daily モード
  ↓
[翌日営業日チェック] ← check_next_day_trading.py 実行 (NEW)
  ↓
  翌日が営業日？
  ├─ YES → パイプライン実行（SKIP_GROK=false）
  │
  │          1. grok_trending.parquet の date カラムに対応する
  │             backtest/grok_trending_YYYYMMDD.parquet が存在することを確認
  │             （バックテストデータ消失防止）
  │
  │          2. grok_trending.parquet の date カラムに対応する
  │             backtest/grok_trending_archive.parquet に該当日データが
  │             存在することを確認
  │             （バックテストデータ消失防止）
  │
  │          3. grok_trending.parquet をカラムだけ残しクリーンにする
  │             （銘柄情報のみ削除）
  │
  │          4. Grok銘柄選定実行
  │             - generate_grok_trending.py 実行
  │
  │          5. all_stocks.parquet の更新
  │             - Grok選定銘柄を追加
  │
  │          6. 更新されたall_stocks.parquetに基づき株価データ生成
  │             - prices_{period}_{interval}.parquet 生成
  │             - tech_snapshot_1d.parquet 生成
  │
  └─ NO  → 終了（パイプライン実行しない）
```

---

## 9. スクリプト仕様

### scripts/check_next_day_trading.py

```python
"""
翌日（暦日）が営業日かどうかをチェック

Exit codes:
  0: 翌日は営業日 → パイプライン実行すべき
  1: 翌日は休業日 → パイプラインスキップすべき
"""
```

**チェックロジック:**
1. J-Quants `/markets/trading_calendar` APIから明日の日付を取得
2. 明日の日付のHolidayDivisionを確認
   - "1" (営業日) → `exit 0`（実行）
   - "0" (休業日：土日祝) → `exit 1`（スキップ）
   - "2" (特別休業日：年末年始12月30日など) → `exit 1`（スキップ）

**J-Quants HolidayDivision 仕様:**
| 値 | 意味 | 動作 |
|---|------|------|
| "1" | 営業日 | ✅ Grok選定実行 |
| "0" | 休業日（土日祝） | ❌ スキップ |
| "2" | 特別休業日（年末年始） | ❌ スキップ |

**判定例:**
- 金曜23時実行 → 土曜日をチェック → HolidayDivision="0" → `exit 1`（スキップ）
- 木曜23時実行 → 金曜日をチェック → HolidayDivision="1" → `exit 0`（実行）
- 日曜23時実行 → 月曜日をチェック → HolidayDivision="1" or "0" → 結果に応じて判定
- 12月29日23時実行 → 12月30日をチェック → HolidayDivision="2" → `exit 1`（スキップ）
