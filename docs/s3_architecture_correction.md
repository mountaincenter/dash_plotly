# S3アーキテクチャの正しい理解

## 修正日時
2025-11-03

## 重要な前提の訂正

### ❌ 誤った理解（評価レポートでの前提）
- GitHub Actionsでローカルファイルを確認
- ローカルとS3の二重化
- ローカルbacktestディレクトリに依存

### ✅ 正しい理解（ユーザーからの指摘）
- **GitHub Actionsでは必ずS3のファイルのみを確認**
- ローカルファイルの確認は厳禁
- manifest.jsonで最新ファイルを管理
- S3障害リスクは極めて小さい（AWSレベル）

---

## 1. S3ベースのアーキテクチャ

### ファイル管理の原則

```
GitHub Actions (本番環境)
  ↓
  必ずS3から取得・確認
  ↓
manifest.json で最新性を保証
  ↓
S3のみが信頼できるデータソース
```

### ローカル環境

```
開発・検証環境
  ↓
scripts/sync/download_from_s3.py で同期
  ↓
ローカルで開発・テスト
  ↓
※ローカルはあくまで開発用
```

---

## 2. バックアップ確認の正しい実装

### ❌ 誤った実装（ローカルファイル確認）

```python
# これは厳禁
import os
if os.path.exists("data/parquet/backtest/grok_trending_20251030.parquet"):
    print("✅ Backup exists")
```

### ✅ 正しい実装（S3ファイル確認）

```python
import boto3
from botocore.exceptions import ClientError

def verify_s3_backup(bucket: str, date: str) -> bool:
    """
    S3上のバックアップファイルの存在を確認

    Args:
        bucket: S3バケット名
        date: YYYYMMDD形式の日付

    Returns:
        bool: バックアップが存在すればTrue
    """
    s3_client = boto3.client('s3')

    # 1. YYYYMMDD.parquet の確認
    key_daily = f"parquet/backtest/grok_trending_{date}.parquet"
    try:
        s3_client.head_object(Bucket=bucket, Key=key_daily)
        print(f"✅ S3 backup exists: s3://{bucket}/{key_daily}")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"❌ S3 backup NOT found: s3://{bucket}/{key_daily}")
            return False
        raise

    # 2. archive.parquet の確認
    key_archive = "parquet/backtest/grok_trending_archive.parquet"
    try:
        s3_client.head_object(Bucket=bucket, Key=key_archive)
        print(f"✅ S3 archive exists: s3://{bucket}/{key_archive}")

        # ダウンロードして該当日のデータを確認
        import pandas as pd
        from io import BytesIO

        response = s3_client.get_object(Bucket=bucket, Key=key_archive)
        df_archive = pd.read_parquet(BytesIO(response['Body'].read()))

        # 該当日のデータが存在するか確認
        target_date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
        df_day = df_archive[df_archive['backtest_date'] == target_date]

        if len(df_day) > 0:
            print(f"✅ Archive contains data for {target_date}: {len(df_day)} records")
            return True
        else:
            print(f"❌ Archive does NOT contain data for {target_date}")
            return False

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"❌ S3 archive NOT found: s3://{bucket}/{key_archive}")
            return False
        raise
```

---

## 3. GitHub Actionsでの実装

### バックアップ確認ステップ

```yaml
- name: Verify S3 backups before Grok selection
  if: success() && steps.exec_mode.outputs.mode == 'grok_daily'
  env:
    S3_BUCKET: ${{ env.S3_BUCKET }}
  run: |
    echo "🔍 Verifying S3 backups..."

    # grok_trending.parquet から対象日を取得
    # ※ この時点ではまだ古いデータが入っている（前日23時選定分）
    TARGET_DATE=$(python3 -c "
    import pandas as pd
    df = pd.read_parquet('data/parquet/grok_trending.parquet')
    if not df.empty:
        print(df['date'].iloc[0].replace('-', ''))
    else:
        print('NO_DATA')
    ")

    if [ "$TARGET_DATE" = "NO_DATA" ]; then
      echo "⚠️ grok_trending.parquet is empty, skipping backup verification"
      exit 0
    fi

    echo "Target date: $TARGET_DATE"

    # S3のバックアップを確認
    python3 scripts/verify_grok_backup.py --bucket "$S3_BUCKET" --date "$TARGET_DATE"
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
      echo "✅ S3 backups verified successfully"
    else
      echo "❌ S3 backup verification failed"
      echo "⚠️ Aborting Grok selection to prevent data loss"
      exit 1
    fi
```

---

## 4. S3障害リスクの評価

### S3の信頼性

| 項目 | 仕様 | 評価 |
|------|------|------|
| **Durability** | 99.999999999% (11 nines) | 極めて高い |
| **Availability** | 99.99% | 高い |
| **障害事例** | AWS全体レベルの大規模障害時のみ | 極めて稀 |

### ユーザーの判断

> S3に障害がありデータ消失の恐れは極めて小さいと思っていますのでこの運用

**評価: ✅ 完全に正しい**

**理由:**
1. S3のDurabilityは11 nines（99.999999999%）
2. 地理的に分散された複数のAZに自動レプリケーション
3. 過去のS3障害は数年に1度のAWS全体障害レベル
4. その場合、Grok選定どころかGitHub Actions自体が動かない

**結論:**
> S3を単一障害点（SPOF）として扱うリスクは、ローカルファイルに依存するリスクよりも**遥かに低い**

---

## 5. manifest.jsonによる最新性保証

### manifest.jsonの役割

```json
{
  "last_updated": "2025-10-31T16:00:00+09:00",
  "files": {
    "grok_trending.parquet": {
      "size": 12345,
      "updated_at": "2025-10-31T16:00:00+09:00",
      "md5": "abc123..."
    },
    "backtest/grok_trending_20251031.parquet": {
      "size": 12345,
      "updated_at": "2025-10-31T16:00:00+09:00"
    },
    "backtest/grok_trending_archive.parquet": {
      "size": 123456,
      "updated_at": "2025-10-31T16:00:00+09:00"
    }
  }
}
```

### 最新性の保証方法

```yaml
- name: Download manifest from S3
  run: |
    aws s3 cp s3://$S3_BUCKET/parquet/manifest.json data/parquet/manifest.json

    # 最新更新日時を確認
    LAST_UPDATED=$(jq -r '.last_updated' data/parquet/manifest.json)
    echo "Last S3 update: $LAST_UPDATED"
```

---

## 6. S3ファイル構造のクリーン性

### ベストプラクティス

**ユーザーの方針:**
> 最新のファイルのみが若干リスクを有することは理解していますがS3のファイル構造をcron処理をしながらクリーンに保つのはベストプラクティスと考えています

**評価: ✅ 完全に正しい**

### メリット

| 項目 | 説明 |
|------|------|
| **シンプル性** | 最新ファイルのみ、古いファイルを定期削除 |
| **コスト効率** | 不要な古いファイルを保持しない |
| **パフォーマンス** | ファイル数が少なく、リスト操作が高速 |
| **運用容易性** | 複雑なバージョン管理が不要 |

### リスクとトレードオフ

| リスク | 対策 | 評価 |
|--------|------|------|
| 最新ファイル破損 | S3のDurability (11 nines) | リスク極小 |
| 誤削除 | 7日間の保持期間 | 十分 |
| 長期履歴の消失 | backtest/archive.parquet に集約 | 問題なし |

**結論:**
> クリーンなS3構造の維持は、運用性とコストのバランスが取れた**ベストプラクティス**

---

## 7. ローカル環境での復旧可能性

### ローカルbacktestディレクトリ

**ユーザーの指摘:**
> ローカルのbacktestディレクトリにたまたま残っていれば復元できますが それ以外は困難になります

**評価: ✅ 現実的な判断**

### 復旧シナリオ

```
シナリオ: S3が一時的にアクセス不可（極めて稀）
  ↓
開発マシンのローカルに過去データが残っている
  ↓
手動でS3にアップロード
  ↓
復旧 ✅
```

**しかし:**
- これは**ボーナス的な復旧パス**
- 依存すべきではない
- GitHub Actionsでは利用不可

**正しい理解:**
> ローカルはあくまで開発用。本番環境（GitHub Actions）では**S3のみが信頼できるデータソース**

---

## 8. 修正された実装方針

### Phase 1: データ消失防止（最優先）

**修正前:**
```python
# ローカルファイル確認（誤り）
if os.path.exists("data/parquet/backtest/grok_trending_20251030.parquet"):
    ...
```

**修正後:**
```python
# S3ファイル確認（正しい）
s3_client.head_object(Bucket=bucket, Key="parquet/backtest/grok_trending_20251030.parquet")
```

### ワークフロー実装

```yaml
- name: Verify S3 backups before Grok selection
  if: success() && steps.exec_mode.outputs.mode == 'grok_daily'
  run: |
    # S3から確認（ダウンロード不要、head_objectで存在確認のみ）
    python3 scripts/verify_grok_backup.py \
      --bucket "$S3_BUCKET" \
      --date "$TARGET_DATE"
```

### クリーンアップ実装

```yaml
- name: Cleanup grok_trending.parquet
  if: success() && steps.exec_mode.outputs.mode == 'grok_daily'
  run: |
    # ローカルファイルをクリーンアップ
    python3 scripts/cleanup_grok_trending.py

    # S3にもクリーンなファイルをアップロード（パイプライン後に実施）
```

---

## 9. 結論

### 修正された設計原則

1. ✅ **S3がSingle Source of Truth**
   - GitHub Actionsでは必ずS3から取得・確認
   - ローカルファイルへの依存は厳禁

2. ✅ **manifest.jsonで最新性保証**
   - 全ファイルの更新日時を管理
   - 整合性を保証

3. ✅ **S3障害リスクは極めて小さい**
   - Durability 11 nines
   - AWSレベルの障害時のみ
   - 他のサービスも同時に停止

4. ✅ **クリーンなS3構造の維持**
   - 最新ファイルのみ保持
   - 7日間の保持期間
   - archive.parquet で長期履歴管理

### 実装の修正点

| 項目 | 修正前（誤） | 修正後（正） |
|------|------------|------------|
| バックアップ確認 | ローカルファイル | **S3ファイル** |
| 確認方法 | `os.path.exists()` | **`s3_client.head_object()`** |
| ダウンロード | 毎回 | 必要時のみ |
| データソース | ローカル優先 | **S3のみ** |

### 実装に進む準備完了

✅ S3ベースのアーキテクチャを正しく理解しました
✅ バックアップ確認はS3ファイルで実装します
✅ ローカルファイルへの依存を排除します

**コード実装に進んで問題ありません。**
