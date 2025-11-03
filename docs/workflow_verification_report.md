# ワークフロー検証レポート

## 実行日時
2025-11-03

## 検証目的
既存の `.github/workflows/data-pipeline.yml` と作成したドキュメント `workflow_execution_flow.md` の間に齟齬がないかを確認し、データ消失リスクを最小化する。

---

## 1. 実行モード判定（Determine execution mode）

### ワークフロー実装（Line 77-100）

| 時刻 | モード | skip_trading_check | skip_grok |
|-----|-------|--------------------|-----------|
| 23時 | grok_daily | ✅ true | ❌ false |
| 16時 | stock_data | ❌ false | ✅ true |

### ドキュメント記載
✅ **一致しています**

---

## 2. 営業日チェック（Check trading day window）

### ワークフロー実装（Line 102-118）
- 条件: `skip_trading_day_check != 'true' AND skip_trading_check != 'true'`
- **16時のみ実行**（当日が営業日かチェック）
- **23時はスキップ**（毎日実行される）

### ドキュメント記載
✅ **一致しています**

### ⚠️ 問題点
**23時には翌日営業日チェックが実装されていない**
- 金曜23時、土曜23時、日曜23時すべて実行される
- 翌日が休業日でもGrok選定が実行される
- **データとコストの無駄**

---

## 3. パイプライン実行（Run Data Pipeline）

### ワークフロー実装（Line 127-168）
```bash
SKIP_GROK_GENERATION=${{ steps.exec_mode.outputs.skip_grok }}
python scripts/run_pipeline_scalping_skip_add_grok.py
```

### パイプライン内部処理（run_pipeline_scalping_skip_add_grok.py）

#### 23時実行（SKIP_GROK=false）のステップ:
1. `pipeline.update_meta_jquants`（週次更新）
2. **`pipeline.generate_grok_trending`**（Grok選定）← 重要
3. `pipeline.create_all_stocks`（銘柄統合）
4. `pipeline.fetch_prices`（価格データ取得）
5. `pipeline.fetch_index_prices`
6. `pipeline.fetch_currency_prices`
7. `pipeline.update_topix_prices`
8. `pipeline.update_sectors_prices`
9. `pipeline.update_series_prices`
10. `pipeline.save_grok_backtest_meta`（バックテストメタ生成）
11. `pipeline.extract_backtest_patterns`
12. `pipeline.update_manifest`

#### 16時実行（SKIP_GROK=true）のステップ:
1. `pipeline.update_meta_jquants`（週次更新：金曜のみ）
2. （Grok選定スキップ）
3. `pipeline.create_all_stocks`（銘柄統合）
4. `pipeline.fetch_prices`（価格データ取得）
5. `pipeline.fetch_index_prices`
6. `pipeline.fetch_currency_prices`
7. `pipeline.update_topix_prices`
8. `pipeline.update_sectors_prices`
9. `pipeline.update_series_prices`
10. **`pipeline.save_backtest_to_archive`**（バックテストアーカイブ保存）← 重要
11. `pipeline.save_grok_backtest_meta`（バックテストメタ生成）
12. `pipeline.extract_backtest_patterns`
13. **`pipeline.generate_market_summary`**（市場サマリー生成）← 16時のみ
14. `pipeline.update_manifest`

### ドキュメント記載
⚠️ **部分的に不一致**

**ドキュメントでの記載:**
- 16時: 金曜のみmeta更新、株価データ取得、バックアップ、バックテスト、市場サマリー
- 23時: バックアップ確認、クリーンアップ、Grok選定、all_stocks更新、株価データ生成

**実際の実装:**
- meta更新は週次（金曜）だが、パイプライン内で自動判定される
- バックテストアーカイブ保存は**パイプライン内**で実行される（16時のみ）
- 23時のバックアップ確認・クリーンアップは**実装されていない**

---

## 4. バックテストとアーカイブ（重要な齟齬）

### ❌ 重大な齟齬発見

#### ワークフローの「Run backtest and archive」ステップ（Line 170-207）

```yaml
- name: Run backtest and archive
  if: success()  # ← 16時と23時の両方で実行される
  run: |
    # S3から既存アーカイブをダウンロード
    # バックテストを実行
    python scripts/pipeline/save_backtest_to_archive.py
```

**問題点:**
1. このステップは `if: success()` のみで条件分岐していない
2. **16時と23時の両方で実行される**
3. しかし、パイプライン内でも16時に `pipeline.save_backtest_to_archive` が実行される
4. **16時に2回バックテストが実行される可能性（重複）**
5. **23時にもバックテストが実行される（意味なし、まだ取引されていない）**

#### ワークフローの「Archive GROK trending for backtest」ステップ（Line 328-387）

```yaml
- name: Archive GROK trending for backtest
  if: success() && steps.exec_mode.outputs.skip_grok == 'true'  # ← 16時のみ
  run: |
    DATE=$(TZ=Asia/Tokyo date +%Y%m%d)
    cp grok_trending.parquet backtest/grok_trending_${DATE}.parquet
    aws s3 cp ... # S3にアップロード
```

**機能:**
- grok_trending.parquetを backtest/grok_trending_YYYYMMDD.parquet にコピー
- S3にアップロード
- 7日以前のファイルを削除

**このステップは正しい:**
- 16時のみ実行
- 前営業日23時に選定された銘柄をバックアップ

---

## 5. 市場サマリー生成

### ワークフロー実装（Line 389-433）
```yaml
- name: Generate market summary
  if: success() && steps.exec_mode.outputs.skip_grok == 'true'  # ← 16時のみ
  run: |
    if [ "$RUN_TIME" = "16:00" ]; then
      python3 scripts/pipeline/generate_market_summary.py
    fi
```

### ドキュメント記載
✅ **一致しています**

---

## 6. 整理：実際の実行フロー

### 16時の実行（現状）

```
16:00 cron起動
  ↓
[実行モード判定] → stock_data モード
  ↓
[営業日チェック] ← check_trading_day.py 実行
  ↓
  営業日？
  ├─ YES → 【パイプライン実行】（SKIP_GROK=true）
  │          1. meta_jquants更新（金曜のみ）
  │          2. all_stocks.parquet統合
  │          3. 株価データ取得
  │          4. pipeline.save_backtest_to_archive 実行 ← バックテスト
  │          5. バックテストメタ生成
  │          6. 市場サマリー生成
  │          7. Manifest生成・S3アップロード
  │
  │       → 【ワークフロー: Run backtest and archive】
  │          ❌ 問題: パイプライン内で既に実行済み（重複）
  │
  │       → 【ワークフロー: Archive GROK trending for backtest】
  │          ✅ grok_trending.parquet → backtest/grok_trending_YYYYMMDD.parquet
  │          ✅ S3にアップロード
  │
  │       → 【ワークフロー: Generate market summary】
  │          ✅ 16時のみ実行
  │
  └─ NO  → 終了
```

### 23時の実行（現状）

```
23:00 cron起動
  ↓
[実行モード判定] → grok_daily モード
  ↓
[営業日チェック] ← ⚠️ スキップされる
  ↓
【パイプライン実行】（SKIP_GROK=false）← 毎日実行される
          1. meta_jquants更新（金曜のみ）
          2. generate_grok_trending.py 実行 ← Grok選定
          3. all_stocks.parquet統合
          4. 株価データ取得
          5. バックテストメタ生成（空）
          6. Manifest生成・S3アップロード
  ↓
【ワークフロー: Run backtest and archive】
  ❌ 問題: 23時に実行される意味がない（まだ取引されていない）
```

---

## 7. ドキュメントとの齟齬まとめ

| 項目 | ドキュメント記載 | 実際の実装 | 齟齬 |
|------|---------------|-----------|------|
| **16時: 営業日チェック** | ✅ 実行 | ✅ 実行 | ✅ 一致 |
| **16時: meta更新** | 金曜のみ | 金曜のみ（自動判定） | ✅ 一致 |
| **16時: バックテスト** | 1回実行 | **2回実行（重複）** | ❌ 不一致 |
| **16時: バックアップ** | ✅ 実行 | ✅ 実行 | ✅ 一致 |
| **16時: 市場サマリー** | ✅ 実行 | ✅ 実行 | ✅ 一致 |
| **23時: 翌日営業日チェック** | ✅ 必要 | ❌ **未実装** | ❌ 不一致 |
| **23時: バックアップ確認** | ✅ 必要 | ❌ **未実装** | ❌ 不一致 |
| **23時: クリーンアップ** | ✅ 必要 | ❌ **未実装** | ❌ 不一致 |
| **23時: Grok選定** | ✅ 実行 | ✅ 実行 | ✅ 一致 |
| **23時: バックテスト** | ❌ 実行しない | ❌ **実行される** | ❌ 不一致 |

---

## 8. 必要な修正

### 修正1: 翌日営業日チェックの追加（23時）
```yaml
- name: Check next day is trading day
  id: check_next_trading
  if: steps.exec_mode.outputs.mode == 'grok_daily'
  run: |
    python scripts/check_next_day_trading.py
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
      echo "✅ Next day is trading day"
    else
      echo "❌ Next day is NOT trading day - will skip"
    fi
    exit $EXIT_CODE
```

### 修正2: パイプライン実行条件の修正
```yaml
- name: Run Data Pipeline
  if: |
    (steps.exec_mode.outputs.mode == 'stock_data') ||
    (steps.exec_mode.outputs.mode == 'grok_daily' && steps.check_next_trading.conclusion == 'success')
  run: |
    ...
```

### 修正3: Run backtest and archiveステップの条件修正
```yaml
- name: Run backtest and archive
  if: success() && steps.exec_mode.outputs.skip_grok == 'true'  # ← 16時のみに変更
  run: |
    ...
```

**理由:** パイプライン内で既にバックテスト実行されるため、ワークフローで再実行は不要

### 修正4: 23時のバックアップ確認・クリーンアップ（新規追加）
```yaml
- name: Verify and cleanup before Grok selection
  if: success() && steps.exec_mode.outputs.mode == 'grok_daily'
  run: |
    echo "1. Verifying backup files exist..."
    python scripts/verify_grok_backup.py

    echo "2. Cleaning grok_trending.parquet..."
    python scripts/cleanup_grok_trending.py
```

---

## 9. データ消失リスク評価

### 現状のリスク

| リスク項目 | 深刻度 | 説明 |
|-----------|-------|------|
| 23時に翌日休業日でも実行 | 🟡 中 | データは無駄だがコスト増のみ |
| 23時にバックアップ確認なし | 🔴 **高** | 上書き前の確認がない |
| 23時にクリーンアップなし | 🟡 中 | 古いデータが残る可能性 |
| 16時にバックテスト重複実行 | 🟢 低 | 無駄だが結果は同じ |

### 修正後のリスク軽減

| 修正 | 効果 |
|------|------|
| 翌日営業日チェック追加 | コスト削減、無駄な実行回避 |
| バックアップ確認追加 | **データ消失防止**（最重要） |
| クリーンアップ追加 | データ整合性向上 |
| バックテスト条件修正 | パフォーマンス向上 |

---

## 10. 結論

### 重大な齟齬（修正必須）
1. ✅ **23時の翌日営業日チェックが未実装**
2. ✅ **23時のバックアップ確認が未実装**（データ消失リスク）
3. ✅ **23時のクリーンアップが未実装**
4. ⚠️ **16時のバックテスト重複実行**
5. ⚠️ **23時のバックテスト不要実行**

### 推奨修正順序
1. **最優先: バックアップ確認スクリプト作成**（データ消失防止）
2. **高優先: 翌日営業日チェック実装**（コスト削減）
3. **中優先: クリーンアップスクリプト作成**（データ整合性）
4. **低優先: バックテスト条件修正**（パフォーマンス）
