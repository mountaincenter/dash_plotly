# dash_plotly

株式データのダッシュボードアプリケーション。

## 技術スタック

- Python 3.12+
- Dash / Plotly
- Docker / Docker Compose
- AWS (S3, ECR, App Runner)

## 主要ディレクトリ

```
server/          # FastAPI サーバー
scripts/         # データパイプライン
  └── pipeline/  # 自動実行スクリプト
frontend/        # Dash フロントエンド
docs/            # 要件定義・設計書
```

## 開発コマンド

```bash
docker compose up -d    # 開発環境起動
python3 scripts/...     # パイプライン実行
```

## 重要ドキュメント

変更前に必ず確認：
- `docs/requirements_definition.md`
- `docs/workflow_execution_flow.md`
- `.github/workflows/data-pipeline.yml`

---

※ 行動制約ルール (Behavioral Constraints) は親ディレクトリの CLAUDE.md を参照
