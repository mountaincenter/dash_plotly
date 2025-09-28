# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from flask_cors import CORS

from app import create_app           # 既存の Dash アプリファクトリを利用
from app.config import load_app_config
from . import register_blueprints

def configure_json_utf8(flask_app):
    try:
        flask_app.json.ensure_ascii = False  # Flask 2.3+
    except Exception:
        flask_app.config["JSON_AS_ASCII"] = False

def configure_cors(flask_app):
    allow = os.getenv("CORS_ALLOW_ORIGINS", "http://localhost:3000")
    origins = [o.strip() for o in allow.split(",") if o.strip()]
    CORS(flask_app, resources={r"/*": {"origins": origins}}, supports_credentials=False)

def main():
    cfg = load_app_config()
    dash_app = create_app()      # Dash アプリ
    flask_app = dash_app.server  # 下の Flask

    configure_json_utf8(flask_app)
    configure_cors(flask_app)
    register_blueprints(flask_app)

    # 既存方針どおり Dash 側の app.run を使う
    dash_app.run(host="0.0.0.0", port=cfg.port, debug=cfg.debug)

if __name__ == "__main__":
    main()
