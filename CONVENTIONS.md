# CONVENTIONS.md

本リポジトリの**ディレクトリ構成・命名規則・運用ルール**を定義する。  
SRP/DRY/MECE を満たし、ローカル検証〜S3同期〜ECR/App Runner デプロイまでを一貫運用する。

---

## 1. ディレクトリ構成（責務）

```text
.
├── app/                         # Dash（UIアプリ：レイアウト/コールバック/生成）
│   ├── __init__.py
│   ├── app.py                   # create_app()
│   ├── callbacks.py
│   ├── config.py                # UI固有（タイトル等）
│   ├── data_loader.py           # データ読込（common_cfgを参照）
│   └── layout.py
├── common_cfg/                  # 共通設定・共通処理（副作用なし）
│   ├── __init__.py
│   ├── env.py                   # .envローダ（明示呼び出し）
│   ├── flags.py                 # PIPELINE_NO_* などのフラグ
│   ├── manifest.py              # sha256, write_manifest_atomic
│   ├── paths.py                 # data/parquet 標準パス
│   ├── s3cfg.py                 # S3関連ENV（BUCKET/PREFIX/REGION/PROFILE）
│   └── s3io.py                  # S3アップロード共通（endpoint_url対応）
├── analyze/                     # 解析・実験（壊してOK）
│   ├── fetch_core30_yf.ipynb
│   ├── anomaly.ipynb
│   └── README.md（任意）
├── pipelines/                   # 本番想定のデータ生成スクリプト
│   └── run_daily_pipeline.py    # 同時実行・manifest集約・S3アップロード
├── data/
│   ├── csv/
│   └── parquet/
│       ├── _state/              # 入力CSVのハッシュ等
│       ├── core30_anomaly.parquet
│       ├── core30_meta.parquet
│       ├── core30_prices_1y_1d.parquet
│       └── manifest.json
├── docker-compose.yml           # web-dev(ローカル), web-stg(S3) を同居
├── Dockerfile
├── requirements.txt             # 依存は1ファイルに集約
├── server.py                    # エントリ（app.runのみ）
├── .env.dev                     # dev用（ローカルparquetを参照）
├── .env.stg                     # stg用（S3を参照）
└── .gitignore
```

### 依存方向の原則

- `app/*` と `pipelines/*` は **`common_cfg/*` を参照**してよい。
- **逆依存（common_cfg → app/pipelines）は禁止**。
- `analyze/*` は自由だが、再利用コードは common_cfg へ還元する。

---

## 2. 環境変数・.env 運用

- `.env.dev`（ローカル開発: **DATA_BUCKET 空** → ローカル `data/parquet` を読む）
- `.env.stg`（擬似ステージング: **S3 を参照**）
- `.env*` は **Git追跡しない**（`.gitignore`）。雛形を作る場合は `env.example`。

### docker-compose 起動例

```bash
# dev（ローカルParquet）
docker compose up -d web-dev

# stg（S3参照）
docker compose up -d web-stg

# 同時起動
docker compose up -d web-dev web-stg
```

---

## 3. 命名規則

### 3.1 ファイル・モジュール

- Python: `snake_case.py`
- Notebook: 説明的な `snake_case.ipynb`
- ディレクトリ: `kebab-case` ではなく基本は単語（`analyze`, `pipelines`）

### 3.2 変数・関数・クラス

- 変数/関数: `snake_case`（関数は動詞+目的語：`load_anomaly()`, `write_manifest_atomic()`）
- クラス: `PascalCase`
- 定数: `UPPER_SNAKE_CASE`
- Dash コンポーネント ID: `block-purpose`（例：`ticker-dd`, `price-chart`）

### 3.3 S3 キー

- `PARQUET_PREFIX/key.parquet`（例：`parquet/core30_meta.parquet`）
- manifest は `parquet/manifest.json`

---

## 4. データ＆manifest のルール

- `data/parquet/manifest.json` 形式（共通）：

```json
{
  "generated_at": "UTC ISO8601",
  "items": [
    { "key": "file.parquet", "bytes": 123, "sha256": "...", "mtime": "UTC ISO8601" }
  ],
  "note": "Auto-generated. Do not edit by hand."
}
```

- **manifest は “現在あるべき S3 の一覧”** を表し、GitHub Actions（s3-sync）で **manifest 以外の余剰を削除**。
- **ローカル → S3** はパイプライン側で実施（`common_cfg.s3io.maybe_upload_to_s3`）。

---

## 5. CI/CD（サマリ）

### 5.1 S3 Reconcile（manifest に基づくクリーンアップ）

- 手動 or `manifest.json` 変更時に実行（`s3-sync.yml`）
- `items[].key` を基準に **S3上の余剰を削除**（manifest.json 自身は保護）
- ローカルparquetはGit無追跡のため、PUTは「ローカルに存在する場合のみ（best-effort）」

### 5.2 ECR Push & App Runner Deploy

- `ecr-push.yml`（main push など）で Docker build → ECR push
- App Runner は **イメージの digest 指定**で更新
- OIDC ロールに `ecr:BatchCheckLayerAvailability, ecr:PutImage, ecr:InitiateLayerUpload, ecr:UploadLayerPart, ecr:CompleteLayerUpload, ecr:DescribeRepositories` 等を付与  
  *CreateRepository* は必要時のみ

---

## 6. Dash アプリの責務分離

- `server.py` は **起動のみ**（`from app.app import create_app` → `app.run(...)`）
- `app/app.py`：データ読み込み・型安全化・レイアウト・コールバック登録を **合成**（UIの Composition Root）
- `app/data_loader.py`：S3/ローカル読込（`common_cfg.paths/s3cfg` を参照）
- `app/layout.py`：UI 構造
- `app/callbacks.py`：グラフ・テーブル生成ロジック
- `app/config.py`：UI 固有（タイトル等）  
  ※ **S3やパスは common_cfg 側に置く**（UIと設定の分離）

---

## 7. 解析コード（`analyze/`）の扱い

- “壊してOK”な実験場。Notebookや単発スクリプトはここに置く。
- 再利用価値のある処理は **`common_cfg/` に抽出して共有**。
- 実験で生成したファイルは、必要に応じて `data/` 配下へ。

---

## 8. パイプライン（`pipelines/`）

- 本番想定のデータ生成・アップロード・manifest 更新を行うスクリプト群。
- **`run_daily_pipeline.py`** は複数処理（CSV→Parquet、価格取得、アノマリー計算）を**直列実行**し、  
  **最終的な manifest を1回だけ**書き、S3アップロードも**1回で集約**。
- 環境変数フラグ  
  - `PIPELINE_NO_MANIFEST=1` → manifest 更新をスキップ  
  - `PIPELINE_NO_S3=1` → S3 アップロードをスキップ

---

## 9. コーディング方針

- **DRY**：共通化できるものは `common_cfg` へ。
- **SRP**：1モジュール1責務（UI/データ入出力/設定/可視化を分離）。
- **MECE**：モジュール境界は重複させない（依存は一方向）。
- 例外処理は**呼び出し側で意味のあるログ**を残す（`[WARN] ... fallback to local` 等）。
- 時刻は**UTC**基準で記録（manifest `generated_at`/`mtime`）。

---

## 10. 命名とバージョニング・コミット

- ブランチ：`feat/…`, `fix/…`, `chore/…`, `ops/…`
- コミットメッセージ：英語 or 日本語可、**命令形/簡潔**に（例：`feat: add anomaly heatmap tab`）
- タグ運用は必要になったタイミングで定義（App Runner のロールバック戦略に合わせる）

---

## 11. 秘密情報

- `.env*` は Git 管理しない
- AWS 認証は  
  - ローカル：`~/.aws/credentials`（必要なら compose で read-only マウント）  
  - CI：OIDC + GitHub Secrets/Variables  
- アプリからは**必要最小限の権限**（S3 読み取りのみ等）

---

## 12. ステージング戦略（コスト最小）

- **ローカル2系統**（`web-dev`:8050=ローカルParquet, `web-stg`:8051=S3）で擬似 stg を運用
- 必要に応じて **LocalStack/MinIO** を `AWS_ENDPOINT_URL` で切替
- App Runner の stg 常設はしない（コスト対効果低）  
  → 必要時だけ **エフェメラル起動**（CLI/Actionsで作成→テスト→削除）

---

## 13. チェックリスト

### 開発時

- [ ] `docker compose up -d web-dev web-stg` で dev/stg 同時起動
- [ ] `http://localhost:8050`（dev）と `8051`（stg）で UI/挙動一致
- [ ] `pipelines/run_daily_pipeline.py` 実行で `manifest.json` 更新

### S3整合

- [ ] `s3-sync.yml` を `workflow_dispatch` で実行し、**manifest 非掲載のS3ファイルが削除**されること

### デプロイ

- [ ] `ecr-push.yml` で ECR push 成功（digest 記録）
- [ ] App Runner が該当 digest を pull して更新完了

---

必要に応じて本ファイルをアップデートし、**“この規約が最上位”**で運用する。
