from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DASH_ROOT = Path(__file__).resolve().parents[4]
SOURCE_ROOT = (
    DASH_ROOT
    / "data/research/grok_session_handoffs_20260718/04_technical_entry_training/output"
)
SOURCE_HTML = SOURCE_ROOT / "technical_entry_training.html"
SOURCE_RESULTS = SOURCE_ROOT / "technical_entry_training_results.js"
OUTPUT_ROOT = SOURCE_ROOT / "nextjs"
OUTPUT_PUBLIC = OUTPUT_ROOT / "public.json"
OUTPUT_RESULTS = OUTPUT_ROOT / "results.json"
SUPPLEMENTAL_PUBLIC = OUTPUT_ROOT / "supplemental_2459_public.json"
SUPPLEMENTAL_RESULTS = OUTPUT_ROOT / "supplemental_2459_results.json"

PUBLIC_START = "const TRAINING = "
PUBLIC_END = ";\n    const STORAGE_KEY"
RESULT_START = "window.__TECHNICAL_ENTRY_RESULTS__="
RESULT_END = ";\nwindow.dispatchEvent"


def extract_json_between(
    text: str,
    *,
    start_marker: str,
    end_marker: str | None = None,
) -> dict[str, Any]:
    start = text.find(start_marker)
    if start < 0:
        raise RuntimeError(f"開始マーカーが見つかりません: {start_marker}")
    start += len(start_marker)

    if end_marker is None:
        payload = text[start:].strip()
        if payload.endswith(";"):
            payload = payload[:-1]
    else:
        end = text.find(end_marker, start)
        if end < 0:
            raise RuntimeError(f"終了マーカーが見つかりません: {end_marker}")
        payload = text[start:end]

    value = json.loads(payload)
    if not isinstance(value, dict):
        raise RuntimeError("抽出したpayloadがobjectではありません")
    return value


def validate_payloads(
    public_data: dict[str, Any],
    result_data: dict[str, Any],
) -> None:
    public_cases = public_data.get("cases")
    result_cases = result_data.get("cases")
    if not isinstance(public_cases, list) or not isinstance(result_cases, list):
        raise RuntimeError("cases配列がありません")

    public_ids = {str(item["id"]) for item in public_cases}
    result_ids = {str(item["id"]) for item in result_cases}
    if public_ids != result_ids:
        raise RuntimeError("公開payloadと結果payloadのcase IDが一致しません")

    for item in public_cases:
        if "guidance" in item or "outcomes" in item or "fullIntraday" in item:
            raise RuntimeError(f"公開payloadへ未来情報が混入しています: {item['id']}")
        cutoff = str(item.get("cutoff", ""))
        if cutoff != "09:30":
            raise RuntimeError(f"判断時刻が09:30ではありません: {item['id']}={cutoff}")


def merge_supplemental(
    public_data: dict[str, Any],
    result_data: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not SUPPLEMENTAL_PUBLIC.exists() and not SUPPLEMENTAL_RESULTS.exists():
        return public_data, result_data
    if not SUPPLEMENTAL_PUBLIC.exists() or not SUPPLEMENTAL_RESULTS.exists():
        raise RuntimeError("2459 supplemental public/result payloadの片方がありません")

    supplemental_public = json.loads(SUPPLEMENTAL_PUBLIC.read_text(encoding="utf-8"))
    supplemental_results = json.loads(
        SUPPLEMENTAL_RESULTS.read_text(encoding="utf-8")
    )
    validate_payloads(supplemental_public, supplemental_results)

    public_ids = {str(item["id"]) for item in public_data["cases"]}
    supplemental_ids = {
        str(item["id"]) for item in supplemental_public["cases"]
    }
    duplicates = sorted(public_ids.intersection(supplemental_ids))
    if duplicates:
        raise RuntimeError(f"case IDが重複しています: {duplicates}")

    public_data = {
        **public_data,
        "generatedAt": supplemental_public.get(
            "generatedAt", public_data.get("generatedAt")
        ),
        "sources": {
            **public_data.get("sources", {}),
            "supplemental2459": supplemental_public.get("sources", {}),
        },
        "cases": [*public_data["cases"], *supplemental_public["cases"]],
    }
    public_data["caseCount"] = len(public_data["cases"])
    result_data = {
        **result_data,
        "generatedAt": supplemental_results.get(
            "generatedAt", result_data.get("generatedAt")
        ),
        "cases": [*result_data["cases"], *supplemental_results["cases"]],
    }
    result_data["caseCount"] = len(result_data["cases"])
    return public_data, result_data


def main() -> None:
    public_data = extract_json_between(
        SOURCE_HTML.read_text(encoding="utf-8"),
        start_marker=PUBLIC_START,
        end_marker=PUBLIC_END,
    )
    result_data = extract_json_between(
        SOURCE_RESULTS.read_text(encoding="utf-8"),
        start_marker=RESULT_START,
        end_marker=RESULT_END,
    )
    public_data, result_data = merge_supplemental(public_data, result_data)
    validate_payloads(public_data, result_data)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    OUTPUT_PUBLIC.write_text(
        json.dumps(public_data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    OUTPUT_RESULTS.write_text(
        json.dumps(result_data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    print(f"generated: {OUTPUT_PUBLIC}")
    print(f"generated: {OUTPUT_RESULTS}")
    print(f"cases: {len(public_data['cases'])}")
    print("validation: public_future_fields=absent, result_case_ids=matched, cutoff=09:30")


if __name__ == "__main__":
    main()
