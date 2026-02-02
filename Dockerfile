# 軽量ベース（ECR/App Runner 想定）
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_NO_CACHE_DIR=on \
    PORT=8000

# 依存に必要な最小ランタイムライブラリ（numpy/pandas/pyarrow向け）
# ※ wheels利用前提。ビルドが必要な場合は build-essential 追加を検討。
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# ★ セキュリティ：openssl のみ最小アップグレード（CVE-2025-9230 対応）
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends --only-upgrade openssl && \
    rm -rf /var/lib/apt/lists/*

# 作業ディレクトリ
WORKDIR /app

# 依存インストール（キャッシュ効果のため requirements だけ先にコピー）
COPY requirements.txt ./
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# アプリ本体
COPY . .

# 非rootユーザ（App Runner/ECRでも動作可）
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# コンテナの公開ポート（App Runner は PORT を見てくれる）
EXPOSE 8000

# FastAPI(Uvicorn) 起動
# - 環境変数 PORT を利用（App Runner 側から上書き可能）
# - --proxy-headers: リバースプロキシ環境でのヘッダ考慮
# - keep-alive短縮＆アクセスログ無効化（追加費用なしの微調整）
CMD ["sh", "-c", "uvicorn server.main:app --host 0.0.0.0 --port ${PORT} --proxy-headers --timeout-keep-alive 15 --no-access-log"]
