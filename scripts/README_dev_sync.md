# 開発環境データ同期

## 概要

`dev_sync.sh` は、本番環境（S3）の最新データを開発環境にダウンロードするスクリプトです。
manifest.jsonのタイムスタンプを比較して、S3の方が新しい場合のみ同期します。

## 使い方

### 基本的な使い方

```bash
# S3の方が新しければ同期、ローカルの方が新しければスキップ
./scripts/dev_sync.sh
```

### 強制同期

```bash
# ローカルの変更を破棄してS3から強制的に同期
./scripts/dev_sync.sh --force
```

## 動作パターン

### パターン1: 初回実行（ローカルにmanifest.jsonがない）
```
📥 Checking for updates from S3...
ℹ️ Local manifest.json not found. Syncing all data...
✅ Development environment initialized with production data
```
→ 全データをダウンロード

### パターン2: S3の方が新しい
```
📥 Checking for updates from S3...
Local timestamp: 2025-10-16T12:00:00+00:00
S3 timestamp:    2025-10-16T15:00:00+00:00

✅ S3 data is newer. Syncing...
✅ Development environment updated with production data
```
→ 同期実行

### パターン3: ローカルと同じ
```
📥 Checking for updates from S3...
Local timestamp: 2025-10-16T15:00:00+00:00
S3 timestamp:    2025-10-16T15:00:00+00:00

ℹ️ Local data is up to date. No sync needed.
```
→ スキップ

### パターン4: ローカルの方が新しい（開発中の変更あり）
```
📥 Checking for updates from S3...
Local timestamp: 2025-10-16T18:00:00+00:00
S3 timestamp:    2025-10-16T15:00:00+00:00

⚠️ Local data is NEWER than S3 (development changes detected)
   Keeping local data. Run with --force to overwrite.
```
→ ローカルの変更を保護

## ワークフロー

### 日常的な開発

```bash
# 1. 朝一番でS3から最新データを取得
./scripts/dev_sync.sh

# 2. 開発作業
# - Dockerコンテナが data/parquet をマウント
# - 最新データでテスト

# 3. 必要に応じてローカルで実験
python jquants/generate_scalping_final.py  # ローカルで実行

# 4. 実験結果はS3にアップロードしない（開発専用）
```

### GitHub Actionsの自動更新後

```bash
# GitHub Actionsが16:00に実行された後
./scripts/dev_sync.sh
# → 自動的に最新データに更新される
```

## 技術的な詳細

### タイムスタンプの比較

manifest.jsonの `generated_at` フィールドを比較:

```json
{
  "generated_at": "2025-10-16T12:27:41.793891+00:00",
  "items": [...]
}
```

ISO 8601形式なので、文字列の辞書順比較で新旧判定が可能。

### 競合の防止

- **GitHub Actions → S3**: 16:00, 02:00に自動更新
- **開発環境 → ローカル**: 読み取り専用（S3にアップロードしない）
- **競合なし**: ローカルの変更はS3に影響しない

### データの分離

```
本番データ (S3):
  - GitHub Actionsが自動更新
  - 開発環境はここから読み取り

開発データ (ローカル):
  - 実験用にローカルで生成
  - S3にアップロードしない（.gitignoreで除外）
```

## トラブルシューティング

### AWS認証エラー

```bash
# AWS CLIの設定を確認
aws configure list

# .env.s3の設定を確認
cat .env.s3
```

### manifest.jsonが壊れている

```bash
# 強制的にS3から再取得
./scripts/dev_sync.sh --force
```

### S3バケットが見つからない

```bash
# バケットの確認
aws s3 ls s3://dash-plotly/parquet/
```
