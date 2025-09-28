# App Runner 近似の軽量 Python ベース
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_NO_CACHE_DIR=on

WORKDIR /app

# 依存インストール
COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt

# アプリ本体
COPY . .

# 既定ポート（composeの環境変数で上書き可能）
ENV PORT=8080

# 厳守: app.run を呼ぶ server.py を実行
CMD ["python", "-m", "server.main"]
