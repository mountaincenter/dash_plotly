# -*- coding: utf-8 -*-
from __future__ import annotations

from .routes.health import health_bp
from .routes.core30 import core30_bp
from .routes.demo import demo_bp

def register_blueprints(flask_app) -> None:
    """Flask Blueprints を一括登録"""
    flask_app.register_blueprint(health_bp)
    flask_app.register_blueprint(core30_bp)
    flask_app.register_blueprint(demo_bp)
