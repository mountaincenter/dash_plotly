# server.py （互換ラッパー）
from server.main import create_dash_app, run

# Gunicorn 等が "app" を探す場合に備えて公開
_dash = create_dash_app()
app = _dash.server  # Flask インスタンス

if __name__ == "__main__":
    run()