#!/bin/bash
# PreToolUse Hook: pipeline/ 配下の Edit 時に前後関係を表示
# Claude が pipeline スクリプトを編集する前に、
# 前段の出力・後段の入力・実行順序を自動表示する

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

# pipeline/ 配下でなければ何もしない
if [[ "$FILE_PATH" != *"/scripts/pipeline/"* ]]; then
  exit 0
fi

# .py ファイル以外は対象外
if [[ "$FILE_PATH" != *.py ]]; then
  exit 0
fi

# スクリプト名を抽出
SCRIPT_NAME=$(basename "$FILE_PATH" .py)
HOOK_DIR="$(cd "$(dirname "$0")" && pwd)"
DEP_FILE="$HOOK_DIR/pipeline_dependencies.json"

if [[ ! -f "$DEP_FILE" ]]; then
  echo "⚠️ pipeline_dependencies.json が見つかりません: $DEP_FILE"
  exit 0
fi

# scripts セクションに登録されているか確認
REGISTERED=$(jq -r --arg name "$SCRIPT_NAME" '.scripts[$name] // empty' "$DEP_FILE")

if [[ -z "$REGISTERED" ]]; then
  echo "⚠️ $SCRIPT_NAME は pipeline_dependencies.json に未登録"
  exit 0
fi

# 全実行順序から該当スクリプトの位置を検索
# 複数のスケジュールに含まれる場合があるので全て表示
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 Pipeline Context: $SCRIPT_NAME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# スクリプトの説明
DESC=$(jq -r --arg name "$SCRIPT_NAME" '.scripts[$name].description // "N/A"' "$DEP_FILE")
echo "📝 $DESC"
echo ""

# 各スケジュールでの位置を表示
for SCHEDULE in $(jq -r '.execution_order | keys[]' "$DEP_FILE"); do
  INDEX=$(jq -r --arg name "$SCRIPT_NAME" --arg sched "$SCHEDULE" \
    '.execution_order[$sched] | to_entries[] | select(.value == $name) | .key' \
    "$DEP_FILE" 2>/dev/null)

  if [[ -n "$INDEX" ]]; then
    TOTAL=$(jq --arg sched "$SCHEDULE" '.execution_order[$sched] | length' "$DEP_FILE")
    PREV=$(jq -r --argjson idx "$INDEX" --arg sched "$SCHEDULE" \
      'if $idx > 0 then .execution_order[$sched][$idx - 1] else "なし" end' \
      "$DEP_FILE")
    NEXT=$(jq -r --argjson idx "$INDEX" --arg sched "$SCHEDULE" \
      'if $idx < (.execution_order[$sched] | length) - 1 then .execution_order[$sched][$idx + 1] else "なし" end' \
      "$DEP_FILE")

    echo "🕐 $SCHEDULE: Step $((INDEX + 1))/$TOTAL"

    # 前段
    if [[ "$PREV" != "なし" ]]; then
      PREV_OUTPUTS=$(jq -r --arg name "$PREV" \
        '.scripts[$name].outputs // [] | join(", ")' "$DEP_FILE")
      echo "  ⬆ 前段: $PREV → 出力: $PREV_OUTPUTS"
    else
      echo "  ⬆ 前段: なし（最初のステップ）"
    fi

    # 後段
    if [[ "$NEXT" != "なし" ]]; then
      NEXT_INPUTS=$(jq -r --arg name "$NEXT" \
        '.scripts[$name].inputs // [] | join(", ")' "$DEP_FILE")
      echo "  ⬇ 後段: $NEXT → 入力: $NEXT_INPUTS"
    else
      echo "  ⬇ 後段: なし（最後のステップ）"
    fi
    echo ""
  fi
done

# 当該スクリプトの入出力
echo "🔧 現在のスクリプト:"
INPUTS=$(jq -r --arg name "$SCRIPT_NAME" \
  '.scripts[$name].inputs // [] | join(", ")' "$DEP_FILE")
OUTPUTS=$(jq -r --arg name "$SCRIPT_NAME" \
  '.scripts[$name].outputs // [] | join(", ")' "$DEP_FILE")
echo "  入力: $INPUTS"
echo "  出力: $OUTPUTS"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
exit 0
