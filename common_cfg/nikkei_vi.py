"""楽天証券スマホ版から日経VI(.JNIV)当日値をスクレイピング取得する共通モジュール。

- エンドポイント: /smartphone/market/info/pagecontent?pid=4001&sym=.JNIV (SSRでHTMLに数値直書き)
- 20分ディレイだが大引後（16:30以降）に当日確定値が反映される
- 過去時系列は返さない。当日1点のみ
"""
from __future__ import annotations

import re
import time
from typing import Any

import requests

URL = "https://www.rakuten-sec.co.jp/smartphone/market/info/pagecontent?pid=4001&sym=.JNIV"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
}


class NikkeiViFetchError(RuntimeError):
    """楽天証券からの日経VI取得に失敗した場合の例外"""


def fetch_nikkei_vi(retries: int = 3, timeout: int = 20) -> dict[str, Any]:
    """楽天証券スマホ版から日経VI(.JNIV)当日値を取得。

    戻り値: {"close", "open", "high", "low", "prev_close", "change", "change_pct", "source"}

    失敗時は NikkeiViFetchError を送出。呼び出し側は判断用途なら fail-fast、
    表示用途なら try/except で None 代替に倒すこと。
    """
    last_error: str | None = None
    with requests.Session() as session:
        for attempt in range(1, retries + 1):
            try:
                resp = session.get(URL, headers=HEADERS, timeout=timeout)
                if resp.status_code != 200:
                    last_error = f"http_{resp.status_code}"
                else:
                    html = resp.text
                    m_close = re.search(r'class="base">\s*([\d.]+)', html)
                    m_change = re.search(r'class="change">\s*<span[^>]*>([+\-]?[\d.]+)</span>', html)
                    m_pct = re.search(r'class="percent"><span[^>]*>([+\-]?[\d.]+)%</span>', html)
                    m_prev = re.search(r'前終<span class="update">\(([\d/]+)\)</span>\s*</th>\s*<td>([\d.]+)</td>', html)
                    m_high = re.search(r'高値<span class="update">\(([\d:]+)\)</span>\s*</th>\s*<td>([\d.]+)</td>', html)
                    m_open = re.search(r'始値<span class="update">\(([\d:]+)\)</span>\s*</th>\s*<td>([\d.]+)</td>', html)
                    m_low = re.search(r'安値<span class="update">\(([\d:]+)\)</span>\s*</th>\s*<td>([\d.]+)</td>', html)
                    if m_close and m_change and m_pct and m_prev and m_high and m_open and m_low:
                        return {
                            "close": float(m_close.group(1)),
                            "open": float(m_open.group(2)),
                            "high": float(m_high.group(2)),
                            "low": float(m_low.group(2)),
                            "prev_close": float(m_prev.group(2)),
                            "change": float(m_change.group(1)),
                            "change_pct": float(m_pct.group(1)),
                            "source": "rakuten-sec",
                        }
                    # 場中前・場中停止中はcloseが空になる。前終値が取れれば大引確定値として返す
                    if m_prev and not m_close:
                        prev = float(m_prev.group(2))
                        return {
                            "close": prev,
                            "open": prev,
                            "high": prev,
                            "low": prev,
                            "prev_close": prev,
                            "change": 0.0,
                            "change_pct": 0.0,
                            "source": "rakuten-sec-prev",
                        }
                    missing = [k for k, v in {
                        "close": m_close, "change": m_change, "pct": m_pct,
                        "prev": m_prev, "high": m_high, "open": m_open, "low": m_low,
                    }.items() if v is None]
                    last_error = f"parse_miss: missing={missing}"
            except requests.RequestException as e:
                last_error = f"{type(e).__name__}: {e}"
            if attempt < retries:
                time.sleep(attempt)
    raise NikkeiViFetchError(f"nikkei VI fetch failed after {retries} attempts: {last_error}")
