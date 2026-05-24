from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# The exporter must read local pipeline outputs. Production/local separation is
# handled by the API at request time, not while generating artifacts.
os.environ["SEMICON_DATA_SOURCE"] = "local"

from server.routers import dev_semicon  # noqa: E402


OUT_DIR = ROOT / "data" / "analysis"


def json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def build_payload(mode: str) -> dict[str, Any]:
    payload = dev_semicon._build_payload_from_report() or dev_semicon.build_payload()
    payload = dict(payload)
    payload["artifact_mode"] = mode
    payload["artifact_generated_at"] = datetime.now().astimezone().isoformat()

    if mode == "domestic":
        payload["us_pending"] = True
        payload["market"] = {
            "state": "US_PENDING",
            "label": "米国判定待ち",
            "previous_snapshot": payload.get("market"),
        }
    else:
        payload["us_pending"] = False

    return payload


def output_path(mode: str) -> Path:
    name = "semicon_domestic_candidates.json" if mode == "domestic" else "semicon_entry_decisions.json"
    return OUT_DIR / name


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate semicon API JSON artifacts from local semicon report outputs.")
    parser.add_argument("--mode", choices=["domestic", "entry"], required=True)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = build_payload(args.mode)
    path = output_path(args.mode)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")

    counts = payload.get("counts") or {}
    print(
        " ".join(
            [
                f"WROTE {path}",
                f"mode={args.mode}",
                f"source={payload.get('source')}",
                f"data_date={payload.get('data_date')}",
                f"signals={counts.get('total', len(payload.get('signals') or []))}",
            ]
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
