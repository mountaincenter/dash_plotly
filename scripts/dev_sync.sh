#!/bin/bash
# 開発環境：本番S3から最新データを同期
# manifest.jsonのタイムスタンプを比較して、S3の方が新しい場合のみ同期

set -e

BUCKET="dash-plotly"
PREFIX="parquet/"
LOCAL_DIR="data/parquet"
LOCAL_MANIFEST="${LOCAL_DIR}/manifest.json"
S3_MANIFEST_URL="s3://${BUCKET}/${PREFIX}manifest.json"
TEMP_MANIFEST="/tmp/s3_manifest.json"

echo "📥 Checking for updates from S3..."
echo "   Bucket: s3://${BUCKET}/${PREFIX}"
echo "   Local:  ${LOCAL_DIR}"
echo ""

# data/parquetディレクトリが存在しない場合は作成
mkdir -p "${LOCAL_DIR}"

# S3のmanifest.jsonをダウンロード
if ! aws s3 cp "${S3_MANIFEST_URL}" "${TEMP_MANIFEST}" 2>/dev/null; then
  echo "❌ Failed to download manifest.json from S3"
  exit 1
fi

# ローカルのmanifest.jsonが存在しない場合は必ず同期
if [[ ! -f "${LOCAL_MANIFEST}" ]]; then
  echo "ℹ️ Local manifest.json not found. Syncing all data..."
  aws s3 sync "s3://${BUCKET}/${PREFIX}" "${LOCAL_DIR}" \
    --exclude "*" \
    --include "*.parquet" \
    --include "manifest.json"
  echo ""
  echo "✅ Development environment initialized with production data"
  ls -lh "${LOCAL_DIR}"
  exit 0
fi

# タイムスタンプを比較（generated_atフィールド）
LOCAL_TIME=$(python3 -c "import json; print(json.load(open('${LOCAL_MANIFEST}'))['generated_at'])" 2>/dev/null || echo "1970-01-01T00:00:00+00:00")
S3_TIME=$(python3 -c "import json; print(json.load(open('${TEMP_MANIFEST}'))['generated_at'])")

echo "Local timestamp: ${LOCAL_TIME}"
echo "S3 timestamp:    ${S3_TIME}"
echo ""

# ISO 8601形式の文字列比較（辞書順で比較可能）
if [[ "${S3_TIME}" > "${LOCAL_TIME}" ]]; then
  echo "✅ S3 data is newer. Syncing..."
  aws s3 sync "s3://${BUCKET}/${PREFIX}" "${LOCAL_DIR}" \
    --exclude "*" \
    --include "*.parquet" \
    --include "manifest.json"
  echo ""
  echo "✅ Development environment updated with production data"
  echo ""
  echo "Updated files:"
  ls -lh "${LOCAL_DIR}"
elif [[ "${S3_TIME}" == "${LOCAL_TIME}" ]]; then
  echo "ℹ️ Local data is up to date. No sync needed."
else
  echo "⚠️ Local data is NEWER than S3 (development changes detected)"
  echo "   Keeping local data. Run with --force to overwrite."

  if [[ "$1" == "--force" ]]; then
    echo ""
    echo "🔄 Force syncing from S3..."
    aws s3 sync "s3://${BUCKET}/${PREFIX}" "${LOCAL_DIR}" \
      --exclude "*" \
      --include "*.parquet" \
      --include "manifest.json"
    echo "✅ Forced sync completed"
  fi
fi

# クリーンアップ
rm -f "${TEMP_MANIFEST}"
