"""楽天証券スマホ版から日経VI(.JNIV)当日値をスクレイピング取得する共通モジュール。

- エンドポイント: /smartphone/market/info/pagecontent?pid=4001&sym=.JNIV (SSRでHTMLに数値直書き)
- 20分ディレイだが大引後（16:30以降）に当日確定値が反映される
- 過去時系列は返さない。当日1点のみ
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests

_JST = timezone(timedelta(hours=9))
LATEST_JSON_PATH = Path(__file__).resolve().parents[1] / "data" / "analysis" / "nikkei_vi_latest.json"

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


def _atomic_json_write(path: Path, data: dict) -> None:
    """tmp → os.replace で書込中断時の破損を防止。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def save_vi_latest(data: dict[str, Any]) -> Path:
    """fetch_nikkei_vi() の戻り値を JSON に保存。"""
    payload = {
        "generated_at": datetime.now(_JST).isoformat(),
        **data,
    }
    _atomic_json_write(LATEST_JSON_PATH, payload)
    return LATEST_JSON_PATH


def _is_fresh(data: dict, max_age_hours: float = 72) -> bool:
    """72h = 金曜16:45→月曜07:00(~63h)を許容。3連休超は stale。"""
    gen = data.get("generated_at")
    if not gen:
        print("  [WARN] nikkei_vi_latest.json has no generated_at, treating as stale")
        return False
    try:
        dt = datetime.fromisoformat(gen)
        age_h = (datetime.now(_JST) - dt).total_seconds() / 3600
        if age_h > max_age_hours:
            print(f"  [WARN] nikkei_vi_latest.json is {age_h:.0f}h old, treating as stale")
            return False
    except (ValueError, TypeError):
        pass
    return True


def _fetch_from_s3() -> dict[str, Any] | None:
    try:
        from common_cfg.s3cfg import load_s3_config
        from common_cfg.s3io import _init_s3_client
        cfg = load_s3_config()
        if not cfg or not cfg.bucket:
            return None
        s3 = _init_s3_client(cfg)
        if s3 is None:
            return None
        obj = s3.get_object(Bucket=cfg.bucket, Key="analysis/nikkei_vi_latest.json")
        data = json.loads(obj["Body"].read().decode("utf-8"))
        _atomic_json_write(LATEST_JSON_PATH, data)
        return data
    except Exception as e:
        print(f"  [WARN] VI S3 fallback failed: {e}")
        return None


def load_vi_latest() -> dict[str, Any] | None:
    """保存済みの当日VI値を読み込む。ローカルなければ S3 フォールバック。"""
    if LATEST_JSON_PATH.exists():
        try:
            with open(LATEST_JSON_PATH, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  [WARN] nikkei_vi_latest.json corrupted: {e}, falling back to S3")
            return _fetch_from_s3()
        if _is_fresh(data):
            return data
        s3_data = _fetch_from_s3()
        if s3_data and _is_fresh(s3_data):
            return s3_data
        # S3も古いかNone → ローカル(古くてもないよりまし)
        return data
    s3_data = _fetch_from_s3()
    if s3_data and not _is_fresh(s3_data):
        print("  [WARN] S3 VI data is stale, using anyway (no local alternative)")
    return s3_data
