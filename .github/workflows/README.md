# GitHub Actions Workflows

このディレクトリには3つのGitHub Actions workflowファイルが含まれています。

## 📋 Workflow一覧

### 1. data-pipeline.yml ⭐ メインパイプライン

**目的**: J-Quants + yfinanceデータパイプライン（02-06）の自動実行

**トリガー**:
- `schedule: '0 7 * * *'` - 毎日 16:00 JST（市場終了後）
- `schedule: '0 17 * * *'` - 毎日 02:00 JST（深夜フォールバック）
- `workflow_dispatch` - 手動実行

**処理内容**:
1. **02**: meta_jquants.parquet作成（J-Quants全銘柄、週次金曜日更新、7日間鮮度管理）
2. **03**: scalping_{entry,active}.parquet生成（J-Quants価格データから選定）
3. **04**: all_stocks.parquet作成（meta.parquet + scalping統合）
4. **05**: prices_*.parquet取得（yfinance、複数期間・インターバル）
5. **06**: manifest.json生成 + S3一括アップロード + 古ファイル削除

**障害時動作**:
- **J-Quants障害**: 空のmeta_jquants.parquet, scalping_*.parquetを作成 → 静的銘柄（meta.parquet）のみでyfinance更新継続
- **yfinance個別銘柄失敗**: 失敗銘柄をスキップ、他の銘柄で処理継続
- **yfinance完全失敗**: パイプライン停止

**重複実行防止**:
- 02:00 JST実行時: manifest.jsonのupdate_flagをチェック
- 既に当日更新済みならスキップ

**通知**:
- Slack通知（成功/失敗/スキップ）
- 統計情報表示（銘柄数、最新データ日付など）

**手動実行オプション**:
- `force_meta_jquants`: trueにすると7日間鮮度に関わらずmeta_jquants.parquetを強制再作成

---

### 2. s3-sync.yml

**目的**: S3のクリーンアップ（manifest.jsonベースの調整）

**トリガー**:
- `workflow_dispatch` - 手動実行のみ

**処理内容**:
1. S3からmanifest.jsonをダウンロード
2. manifest.jsonに記載されたファイルがS3に存在するか確認
3. manifest.jsonに記載されていないファイルをS3から削除
4. 削除後のS3状態を表示

**dry-runモード**:
- デフォルトで有効
- dry_run=trueで実行すると削除せずに確認のみ

**使用場面**:
- S3に不要なファイルが蓄積した場合
- manifest.jsonと実際のS3状態を同期したい場合

---

### 3. build-push-ecr.yml

**目的**: Dockerイメージのビルド・ECRプッシュ

**トリガー**:
- `push: branches: [main]` - mainブランチへのpush
- `workflow_dispatch` - 手動実行

**処理内容**:
1. ECRリポジトリの存在確認・作成
2. Dockerイメージのビルド
3. ECRへのプッシュ（latest + SHA tagの2つ）
4. Image digestの出力

**使用場面**:
- アプリケーションコードの変更をデプロイする場合
- データパイプラインとは独立して実行

---

## 🗂️ アーカイブファイル

`archive/` ディレクトリには旧workflowファイルが保存されています：
- `jquants-scalping.yml` - 旧J-Quantsワークフロー
- `yfinance-data-update.yml` - 旧yfinanceワークフロー

これらは統合されて `data-pipeline.yml` に置き換えられました。

---

## 📊 実行フロー図

```
┌─────────────────────────────────────────────────────────────┐
│                     data-pipeline.yml                        │
│                                                              │
│  Trigger: 16:00 JST (daily) / 02:00 JST (fallback)        │
└─────────────────────────────────────────────────────────────┘
                           ↓
        ┌──────────────────────────────────────┐
        │  02: meta_jquants.parquet (J-Quants) │
        │  - 金曜日: 強制更新                    │
        │  - 平日: S3から取得 → 7日間鮮度チェック │
        │  - 障害時: 空ファイル作成              │
        └──────────────────────────────────────┘
                           ↓
        ┌──────────────────────────────────────┐
        │  03: scalping_*.parquet              │
        │  - J-Quants価格データから選定         │
        │  - 障害時: 空ファイル作成              │
        └──────────────────────────────────────┘
                           ↓
        ┌──────────────────────────────────────┐
        │  04: all_stocks.parquet              │
        │  - meta.parquet + scalping統合       │
        │  - J-Quants障害時は静的銘柄のみ       │
        └──────────────────────────────────────┘
                           ↓
        ┌──────────────────────────────────────┐
        │  05: prices_*.parquet (yfinance)     │
        │  - 60d_15m, 60d_5m, 730d_1h, max_1d │
        │  - 個別銘柄失敗: スキップして継続      │
        └──────────────────────────────────────┘
                           ↓
        ┌──────────────────────────────────────┐
        │  06: manifest.json + S3アップロード   │
        │  - manifest.json生成                 │
        │  - 全parquetファイルをS3に一括upload  │
        │  - S3の古いファイルを削除             │
        └──────────────────────────────────────┘
                           ↓
                  ✅ 完了 (Slack通知)
```

---

## 🔧 必要なシークレット・変数

### Secrets (Repository secrets)
- `AWS_ROLE_ARN`: AWS IAM RoleのARN（OIDC認証用）
- `JQUANTS_REFRESH_TOKEN`: J-Quants API リフレッシュトークン
- `SLACK_INCOMING_WEBHOOK_URL`: Slack通知用Webhook URL

### Variables (Repository variables)
- `AWS_REGION`: AWSリージョン（例: `ap-northeast-1`）
- `DATA_BUCKET` または `S3_BUCKET`: S3バケット名
- `PARQUET_PREFIX`: S3プレフィックス（例: `parquet/`）
- `ECR_REPOSITORY`: ECRリポジトリ名（build-push-ecr.ymlで使用）

### Environment
- `AWS_OIDC`: GitHub Actions環境名（OIDC認証用）

---

## 🚀 ローカル開発との連携

### S3から最新データをダウンロード
```bash
python scripts/sync/download_from_s3.py
```

### 緊急時にmeta_jquants.parquetを手動更新
```bash
python scripts/manual/update_meta_jquants.py
```

### パイプライン全体をローカルで実行
```bash
python scripts/run_pipeline.py
```

---

## 📝 ログの確認

GitHub Actionsの実行ログ:
1. GitHubリポジトリ → "Actions"タブ
2. 該当のワークフロー実行を選択
3. 各ステップのログを確認

Slack通知:
- 成功/失敗/スキップ時に自動通知
- 統計情報（銘柄数、最新データ日付など）を含む
