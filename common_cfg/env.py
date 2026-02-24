# -*- coding: utf-8 -*-
"""
common_cfg.env: .env 系ファイルを必要に応じてロード
  - 優先順: .env.stg → .env.dev → .env → （後方互換）.env.s3
  - CWD が analyze/ でも、親ディレクトリを遡って探索する
"""
from pathlib import Path

_ENV_NAMES = (".env.jquants", ".env.s3", ".env.xai", ".env.edinet", ".env.estat", ".env.slack", ".env.dev")

def _iter_search_dirs(max_up: int = 5):
    """CWD から親に向かって max_up 階層まで探索"""
    p = Path.cwd().resolve()
    yield p
    for _ in range(max_up):
        if p.parent == p:
            break
        p = p.parent
        yield p

def load_dotenv_cascade() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    loaded = set()
    # 親方向に遡りながら、優先順でロード（override=False で既存値は上書きしない）
    for base in _iter_search_dirs():
        for name in _ENV_NAMES:
            f = (base / name)
            if f.exists():
                ap = str(f.resolve())
                if ap in loaded:
                    continue
                load_dotenv(dotenv_path=f, override=False)
                loaded.add(ap)

def load_dotenv_if_exists() -> None:
    # 後方互換
    load_dotenv_cascade()
