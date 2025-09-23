# -*- coding: utf-8 -*-
"""
server.py
- エントリポイント。Dashアプリの生成と app.run のみ担当（SRP）
"""
from app import create_app
from app.config import load_app_config


if __name__ == "__main__":
    cfg = load_app_config()
    app = create_app()
    app.run(host="0.0.0.0", port=cfg.port, debug=cfg.debug)
