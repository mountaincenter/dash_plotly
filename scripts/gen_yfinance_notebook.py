#!/usr/bin/env python3
"""
Generate yfinance-smoke-test.ipynb capturing the same intervals used by
the parquet pipeline. Intended for short-lived diagnostics in CI.
"""

from __future__ import annotations

from pathlib import Path

import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell

NOTEBOOK_PATH = Path("yfinance-smoke-test.ipynb")
TICKER = "7203.T"
SPECS = [
    ("max", "1d"),
    ("max", "1wk"),
    ("max", "1mo"),
    ("730d", "1h"),
    ("60d", "5m"),
    ("60d", "15m"),
]


def build_notebook() -> nbformat.NotebookNode:
    nb = new_notebook()
    nb.cells.append(
        new_markdown_cell(
            f"""# yfinance smoke test

This notebook fetches **{TICKER}** with the same period/interval pairs
used by the parquet pipeline, primarily for connectivity checks on CI.
"""
        )
    )
    nb.cells.append(
        new_code_cell(
            "import yfinance as yf\nimport pandas as pd\n"
            "from datetime import datetime\n\n"
            "print(f'Generated at {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC')"
        )
    )
    for period, interval in SPECS:
        var = f"df_{period}_{interval}".replace("d", "d").replace("m", "m")
        nb.cells.append(
            new_markdown_cell(f"## period={period}, interval={interval}")
        )
        nb.cells.append(
            new_code_cell(
                f"{var} = yf.download(\n"
                f"    '{TICKER}',\n"
                f"    period='{period}',\n"
                f"    interval='{interval}',\n"
                f"    auto_adjust=True,\n"
                f"    progress=False,\n"
                ")\n"
                f"print('rows:', {var}.shape[0])\n"
                f"display({var}.head())"
            )
        )
    return nb


def main() -> int:
    nb = build_notebook()
    NOTEBOOK_PATH.write_text(nbformat.writes(nb), encoding="utf-8")
    print(f"[OK] notebook generated: {NOTEBOOK_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
