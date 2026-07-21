#!/usr/bin/env python3
"""MarketSpeed CSVへの過去営業日データ混入を軽量に検出する。"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


JST = ZoneInfo("Asia/Tokyo")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data" / "marketspeed" / "raw"
DEFAULT_EVIDENCE_ROOT = PROJECT_ROOT / "data" / "marketspeed" / "validation"
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env.jquants"

MARKETS = ("prime", "standard", "growth")
MARKET_NAMES = {
    "プライム": "prime",
    "スタンダード": "standard",
    "グロース": "growth",
}
FAMILIES = (
    "stop_high",
    "stop_low",
    "special_buy",
    "special_sell",
    "new_high",
    "new_low",
    "large_deal",
)
EXPECTED_MARKET_FILES = tuple(
    f"{family}_{market}.csv" for family in FAMILIES for market in MARKETS
)
ACCOUNT_FILES = (
    "order.csv",
    "order_results.csv",
    "stock_results__today.csv",
    "stock_results__month.csv",
    "hold_stocks.csv",
)
PRICE_COLUMNS = {
    "stop_high": ("価格",),
    "stop_low": ("価格",),
    "special_buy": ("約定値", "特別買気配"),
    "special_sell": ("約定値", "特別売気配"),
    "new_high": ("新高値",),
    "new_low": ("新安値",),
    "large_deal": ("価格",),
}
TIME_COLUMNS = {
    "stop_high": ("時刻",),
    "stop_low": ("時刻",),
    "special_buy": ("特買気配時刻", "気配値時刻", "約定時刻"),
    "special_sell": ("特売気配時刻", "気配値時刻", "約定時刻"),
    "new_high": ("時刻",),
    "new_low": ("時刻",),
    "large_deal": ("時刻",),
}
ACCOUNT_DATE_RULES = {
    "order.csv": ("発注/受注日時", "all_target"),
    "order_results.csv": ("約定日", "all_target"),
    "stock_results__today.csv": ("約定日", "all_target"),
    "stock_results__month.csv": ("約定日", "max_target"),
}
TIME_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$")
REFERENCE_COLUMNS = {
    "master": {"Date", "Code", "CoName", "MktNm"},
    "daily": {"Date", "Code", "O", "H", "L", "C", "UL", "LL", "Vo", "Va"},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--input-dir",
        type=Path,
        help="検証対象CSVディレクトリ。省略時はdata/marketspeed/raw/対象日",
    )
    parser.add_argument(
        "--reference-dir",
        type=Path,
        help="既存のmaster.csvとdaily.csvを読むディレクトリ",
    )
    parser.add_argument(
        "--evidence-root", type=Path, default=DEFAULT_EVIDENCE_ROOT
    )
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument(
        "--fetch-jquants",
        action="store_true",
        help="不足する対象日1日分の銘柄マスターと日足だけを取得する",
    )
    parser.add_argument(
        "--persist-evidence",
        action="store_true",
        help="検証に使用したJ-Quants CSVとreport.jsonを日付別に保存する",
    )
    parser.add_argument("--report", type=Path, help="検証JSONの明示出力先")
    parser.add_argument("--request-timeout", type=int, default=45)
    parser.add_argument("--total-timeout", type=int, default=90)
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=DEFAULT_RAW_ROOT,
        help="過去営業日manifestとのSHA-256照合に使うrawルート",
    )
    parser.add_argument(
        "--market-only",
        action="store_true",
        help="過去の市場イベントだけの保存日を再検証する。通常の日次同期では使わない",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_record(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp"
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "wb", dir=destination.parent, delete=False, suffix=".tmp"
    ) as handle:
        temporary = Path(handle.name)
    try:
        shutil.copy2(source, temporary)
        if sha256_file(source) != sha256_file(temporary):
            raise RuntimeError(f"証跡コピーのSHA-256が一致しません: {source}")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def load_env(path: Path) -> dict[str, str]:
    env = os.environ.copy()
    if not path.is_file():
        raise FileNotFoundError(f"J-Quants環境ファイルがありません: {path}")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in env:
            env[key] = value.strip().strip('"').strip("'")
    if "JQUANTS_BASE_URL" not in env and "JQUANTS_API_BASE_URL" in env:
        env["JQUANTS_BASE_URL"] = env["JQUANTS_API_BASE_URL"]
    return env


def run_jquants_csv(
    command: list[str],
    output: Path,
    env: dict[str, str],
    timeout: int,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    result = subprocess.run(
        ["jquants", "--output", "csv", "--save", str(temporary), *command],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        temporary.unlink(missing_ok=True)
        detail = result.stderr.strip() or result.stdout.strip() or "unknown CLI error"
        raise RuntimeError(f"J-Quants CLI失敗: {detail}")
    if not temporary.is_file() or temporary.stat().st_size == 0:
        temporary.unlink(missing_ok=True)
        raise RuntimeError("J-Quants CLIが空のCSVを返しました")
    temporary.replace(output)


def validate_reference_csv(path: Path, kind: str, target: str) -> pd.DataFrame:
    if not path.is_file() or path.stat().st_size == 0:
        raise FileNotFoundError(f"J-Quants {kind}証跡がありません: {path}")
    frame = pd.read_csv(path, dtype={"Code": "string"})
    missing = sorted(REFERENCE_COLUMNS[kind] - set(frame.columns))
    if missing:
        raise RuntimeError(f"{path.name}の必須列不足: {missing}")
    if frame.empty:
        raise RuntimeError(f"{path.name}が空です")
    dates = pd.to_datetime(frame["Date"], errors="raise").dt.strftime("%Y-%m-%d")
    actual = sorted(dates.dropna().unique().tolist())
    if actual != [target]:
        raise RuntimeError(f"{path.name}の日付不一致: {actual} != [{target}]")
    codes = frame["Code"].astype("string").str.strip()
    if codes.isna().any() or frame.assign(Code=codes)["Code"].duplicated().any():
        if kind != "minute":
            raise RuntimeError(f"{path.name}に空または重複コードがあります")
    frame["Code"] = codes
    return frame


def normalize_code(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    text = text.upper()
    if len(text) == 4 and text.isalnum():
        return text + "0"
    if len(text) == 5 and text.isalnum():
        return text
    return None


def parse_number(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "--"}:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def parse_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def market_file_parts(filename: str) -> tuple[str, str]:
    stem = filename.removesuffix(".csv")
    family, market = stem.rsplit("_", 1)
    return family, market


def row_price(row: dict[str, str], family: str) -> tuple[str | None, float | None]:
    for column in PRICE_COLUMNS[family]:
        value = parse_number(row.get(column))
        if value is not None:
            return column, value
    return None, None


def load_prior_archive_hashes(
    archive_root: Path, target: str
) -> tuple[dict[tuple[str, str], list[str]], list[str], list[str]]:
    index: dict[tuple[str, str], list[str]] = {}
    scanned_dates: list[str] = []
    warnings: list[str] = []
    if not archive_root.is_dir():
        return index, scanned_dates, warnings
    for day_dir in sorted(archive_root.iterdir()):
        if not day_dir.is_dir() or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_dir.name):
            continue
        if day_dir.name >= target:
            continue
        manifest = day_dir / "manifest.json"
        if not manifest.is_file():
            warnings.append(f"{day_dir.name}: 過去manifestなし")
            continue
        try:
            payload = json.loads(manifest.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as error:
            warnings.append(f"{day_dir.name}: 過去manifest読込失敗: {error}")
            continue
        manifest_date = str(payload.get("trade_date", ""))
        if manifest_date != day_dir.name:
            warnings.append(
                f"{day_dir.name}: 過去manifest日付不一致 ({manifest_date})"
            )
            continue
        scanned_dates.append(day_dir.name)
        for item in payload.get("files", []):
            filename = item.get("file")
            digest = item.get("sha256")
            if isinstance(filename, str) and isinstance(digest, str):
                index.setdefault((filename, digest), []).append(day_dir.name)
    return index, scanned_dates, warnings


def prior_archive_hash_check(
    input_dir: Path,
    archive_root: Path,
    target: str,
    account_checks: dict[str, Any],
) -> tuple[dict[str, Any], list[str], list[str]]:
    prior_hashes, scanned_dates, warnings = load_prior_archive_hashes(
        archive_root, target
    )
    files: dict[str, Any] = {}
    blocking: list[str] = []
    matched_files: list[str] = []
    for filename in (*EXPECTED_MARKET_FILES, *ACCOUNT_FILES):
        path = input_dir / filename
        if not path.is_file():
            continue
        digest = sha256_file(path)
        matched_dates = sorted(prior_hashes.get((filename, digest), []))
        if not matched_dates:
            files[filename] = {
                "status": "not_seen_in_prior_archives",
                "matched_trade_dates": [],
            }
            continue
        matched_files.append(filename)
        embedded_dates = account_checks.get(filename, {}).get("embedded_dates", [])
        if target in embedded_dates:
            status = "target_date_embedded_despite_same_hash"
        elif filename == "hold_stocks.csv":
            status = "same_content_as_prior_snapshot_date_not_embedded"
            warnings.append(
                "hold_stocks.csv: 過去日と同一内容。対象日は更新時刻と同時出力された口座CSVでのみ確認"
            )
        else:
            status = "matches_prior_trade_date_archive"
            blocking.append(
                f"{filename}: SHA-256が過去営業日rawと一致 ({','.join(matched_dates)})"
            )
        files[filename] = {
            "status": status,
            "matched_trade_dates": matched_dates,
        }
    if blocking:
        status = "prior_trade_date_match_detected"
    elif matched_files:
        status = "no_blocking_prior_trade_date_match"
    else:
        status = "no_prior_trade_date_hash_match"
    return {
        "status": status,
        "archive_root": str(archive_root),
        "scanned_trade_dates": scanned_dates,
        "matched_files": sorted(matched_files),
        "files": files,
    }, blocking, warnings


def resolve_reference_root(args: argparse.Namespace, target: str) -> Path:
    if args.reference_dir is not None:
        return args.reference_dir
    return args.evidence_root / target / "jquants"


def fetch_reference_if_needed(
    args: argparse.Namespace, target: str, reference_root: Path
) -> dict[str, str]:
    commands: dict[str, str] = {}
    started = time.monotonic()
    if not args.fetch_jquants:
        return commands
    env = load_env(args.env_file)
    for name, command in (
        ("master.csv", ["eq", "master", "--date", target]),
        ("daily.csv", ["eq", "daily", "--date", target]),
    ):
        path = reference_root / name
        if path.is_file():
            continue
        remaining = args.total_timeout - (time.monotonic() - started)
        if remaining <= 1:
            raise TimeoutError("J-Quants検証の合計時間上限に達しました")
        run_jquants_csv(command, path, env, min(args.request_timeout, int(remaining)))
        commands[name] = "jquants " + " ".join(command)
    return commands


def reference_frames(
    reference_root: Path, target: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    master = validate_reference_csv(reference_root / "master.csv", "master", target)
    daily = validate_reference_csv(reference_root / "daily.csv", "daily", target)
    if set(master["Code"]) != set(daily["Code"]):
        raise RuntimeError("J-Quants masterとdailyの銘柄集合が一致しません")
    return master, daily


def frame_index(
    master: pd.DataFrame, daily: pd.DataFrame
) -> tuple[dict[str, str | None], dict[str, dict[str, Any]]]:
    market_by_code = {
        str(row.Code): MARKET_NAMES.get(str(row.MktNm))
        for row in master[["Code", "MktNm"]].itertuples(index=False)
    }
    daily_by_code = {
        str(row.Code): row._asdict() for row in daily.itertuples(index=False)
    }
    return market_by_code, daily_by_code


def row_consistency_check(
    input_dir: Path,
    market_by_code: dict[str, str | None],
    daily_by_code: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], list[str], list[str]]:
    results: dict[str, dict[str, Any]] = {}
    blocking: list[str] = []
    warnings: list[str] = []
    for filename in EXPECTED_MARKET_FILES:
        family, market = market_file_parts(filename)
        path = input_dir / filename
        if not path.is_file():
            results[filename] = {
                "status": "missing",
                "rows": 0,
                "verification_level": "inventory_only",
                "errors": [],
            }
            if family not in {"stop_high", "stop_low"}:
                blocking.append(
                    f"{filename}: 対象日の物理CSVがなく、当日不在か出力漏れかを判別できない"
                )
            continue
        rows = parse_csv(path)
        errors: list[str] = []
        notes: list[str] = []
        evidence_rows = 0
        strong_evidence_rows = 0
        price_source_counts: dict[str, int] = {}
        if not rows:
            errors.append(f"{filename}: 物理CSVが0行で対象日の指紋を確認できない")
        for row_number, row in enumerate(rows, start=2):
            code = normalize_code(row.get("コード"))
            prefix = f"{filename}:{row_number}"
            if code is None:
                errors.append(f"{prefix} 銘柄コード不正")
                continue
            if code not in daily_by_code or code not in market_by_code:
                errors.append(f"{prefix} {code}が対象日のJ-Quantsに存在しない")
                continue
            jquants_market = market_by_code[code]
            if jquants_market is not None and jquants_market != market:
                errors.append(
                    f"{prefix} {code}の市場不一致 ({jquants_market} != {market})"
                )
            elif jquants_market is None:
                notes.append(
                    f"{prefix} {code}はJ-QuantsのPrime/Standard/Growth分類外"
                )
            for column in TIME_COLUMNS[family]:
                value = str(row.get(column, "")).strip()
                if (
                    family in {"special_buy", "special_sell"}
                    and column == "約定時刻"
                    and value in {"", ":", "-", "--"}
                ):
                    continue
                if not TIME_PATTERN.fullmatch(value):
                    errors.append(f"{prefix} {column}の時刻形式不正: {value!r}")
            price_column, price = row_price(row, family)
            if price_column is not None:
                price_source_counts[price_column] = (
                    price_source_counts.get(price_column, 0) + 1
                )
            daily_row = daily_by_code[code]
            low = parse_number(daily_row.get("L"))
            high = parse_number(daily_row.get("H"))
            if price is None:
                if family in {"special_buy", "special_sell"}:
                    notes.append(f"{prefix} 特別気配後の約定なし")
                else:
                    errors.append(
                        f"{prefix} {'/'.join(PRICE_COLUMNS[family])}が数値でない"
                    )
            elif (
                family in {"special_buy", "special_sell"}
                and price_column != "約定値"
            ):
                notes.append(f"{prefix} 約定なしのため気配値は日付指紋に不使用")
            elif low is None or high is None or not (low - 0.011 <= price <= high + 0.011):
                errors.append(
                    f"{prefix} 価格{price:g}が対象日日足レンジ[{low}, {high}]外"
                )
            else:
                evidence_rows += 1
            if family in {"stop_high", "new_high"} and price is not None and high is not None:
                if not np.isclose(price, high, atol=0.011, rtol=1e-10):
                    errors.append(f"{prefix} 高値系価格{price:g} != J-Quants H {high:g}")
                else:
                    strong_evidence_rows += 1
            if family in {"stop_low", "new_low"} and price is not None and low is not None:
                if not np.isclose(price, low, atol=0.011, rtol=1e-10):
                    errors.append(f"{prefix} 安値系価格{price:g} != J-Quants L {low:g}")
                else:
                    strong_evidence_rows += 1
            if family == "large_deal":
                volume = parse_number(row.get("出来高"))
                daily_volume = parse_number(daily_row.get("Vo"))
                if volume is None or volume <= 0:
                    errors.append(f"{prefix} 出来高が正数でない")
                elif daily_volume is None or volume > daily_volume + 0.5:
                    errors.append(
                        f"{prefix} 大口出来高{volume:g}が日次出来高{daily_volume}を超える"
                    )
                close = parse_number(daily_row.get("C"))
                if str(row.get("時刻", "")).strip() == "15:30":
                    if close is None or price is None or not np.isclose(
                        price, close, atol=0.011, rtol=1e-10
                    ):
                        errors.append(
                            f"{prefix} 15:30価格{price} != J-Quants C {close}"
                        )
                    else:
                        strong_evidence_rows += 1
        if rows and evidence_rows == 0:
            errors.append(f"{filename}: 対象日日足に一致する価格指紋が0行")
        results[filename] = {
            "status": (
                "target_date_consistent" if not errors else "target_date_inconsistent"
            ),
            "rows": len(rows),
            "verification_level": "target_trade_date_price_fingerprint",
            "target_date_evidence_rows": evidence_rows,
            "strong_target_date_evidence_rows": strong_evidence_rows,
            "price_source_counts": price_source_counts,
            "sha256": sha256_file(path),
            "errors": errors[:50],
            "error_count": len(errors),
            "notes": notes[:50],
            "note_count": len(notes),
        }
        if errors:
            blocking.append(f"{filename}: 対象日整合エラー{len(errors)}件")
    return results, blocking, warnings


def flagged_sets(
    daily: pd.DataFrame,
    market_by_code: dict[str, str | None],
    flag: str,
) -> dict[str, set[str]]:
    numeric = pd.to_numeric(daily[flag], errors="coerce").fillna(0)
    codes = daily.loc[numeric.eq(1), "Code"].astype(str).tolist()
    result = {market: set() for market in MARKETS}
    for code in codes:
        market = market_by_code.get(code)
        if market in result:
            result[market].add(code)
    return result


def actual_stop_sets(
    input_dir: Path, family: str
) -> dict[str, set[str]]:
    result = {market: set() for market in MARKETS}
    for market in MARKETS:
        path = input_dir / f"{family}_{market}.csv"
        if not path.is_file():
            continue
        for row in parse_csv(path):
            code = normalize_code(row.get("コード"))
            if code is not None:
                result[market].add(code)
    return result


def exact_stop_check(
    input_dir: Path,
    daily: pd.DataFrame,
    market_by_code: dict[str, str | None],
) -> tuple[dict[str, Any], list[str], list[str]]:
    result: dict[str, Any] = {}
    blocking: list[str] = []
    verified_absent: list[str] = []
    for family, flag in (("stop_high", "UL"), ("stop_low", "LL")):
        expected = flagged_sets(daily, market_by_code, flag)
        actual = actual_stop_sets(input_dir, family)
        flag_values = pd.to_numeric(daily[flag], errors="coerce").fillna(0)
        flagged_codes = set(
            daily.loc[flag_values.eq(1), "Code"].astype(str).tolist()
        )
        expected_unclassified = {
            code for code in flagged_codes if market_by_code.get(code) not in MARKETS
        }
        expected_all = set().union(*expected.values(), expected_unclassified)
        actual_all = set().union(*actual.values())
        global_missing = sorted(expected_all - actual_all)
        global_extra = sorted(actual_all - expected_all)
        duplicate_actual = sorted(
            code
            for code in actual_all
            if sum(code in actual[market] for market in MARKETS) > 1
        )
        markets: dict[str, Any] = {}
        family_passed = not global_missing and not global_extra and not duplicate_actual
        for market in MARKETS:
            missing = sorted(expected[market] - actual[market])
            explicit_wrong_market = sorted(
                code
                for code in actual[market]
                if market_by_code.get(code) in MARKETS
                and market_by_code.get(code) != market
            )
            filename = f"{family}_{market}.csv"
            file_exists = (input_dir / filename).is_file()
            passed = not missing and not explicit_wrong_market
            family_passed = family_passed and passed
            if not passed:
                blocking.append(
                    f"{filename}: J-Quants {flag}との市場別不一致 "
                    f"missing={missing} wrong_market={explicit_wrong_market}"
                )
            zero_provable = not expected[market] and (
                market != "prime" or not expected_unclassified
            )
            if not file_exists and passed and zero_provable:
                verified_absent.append(filename)
            markets[market] = {
                "status": "passed" if passed else "failed",
                "file_exists": file_exists,
                "expected_codes": sorted(expected[market]),
                "actual_codes": sorted(actual[market]),
                "missing_codes": missing,
                "wrong_market_codes": explicit_wrong_market,
                "accepted_unclassified_codes": sorted(
                    actual[market] & expected_unclassified
                ),
            }
        if global_missing or global_extra or duplicate_actual:
            blocking.append(
                f"{family}: J-Quants {flag}との全市場集合不一致 "
                f"missing={global_missing} extra={global_extra} duplicates={duplicate_actual}"
            )
        result[family] = {
            "status": "passed" if family_passed else "failed",
            "jquants_flag": flag,
            "verification_level": "exact_global_set_and_explicit_market_set",
            "jquants_unclassified_codes": sorted(expected_unclassified),
            "global_missing_codes": global_missing,
            "global_extra_codes": global_extra,
            "duplicate_actual_codes": duplicate_actual,
            "markets": markets,
        }
    return result, blocking, sorted(verified_absent)


def parse_market_speed_dates(values: pd.Series) -> list[str]:
    parsed = pd.to_datetime(values, errors="coerce")
    if parsed.isna().any():
        return []
    return sorted(parsed.dt.strftime("%Y-%m-%d").unique().tolist())


def account_check(input_dir: Path, target: str) -> tuple[dict[str, Any], list[str]]:
    results: dict[str, Any] = {}
    blocking: list[str] = []
    for filename in ACCOUNT_FILES:
        path = input_dir / filename
        if not path.is_file():
            results[filename] = {"status": "missing"}
            blocking.append(f"{filename}: 口座スナップショットがありません")
            continue
        rows = parse_csv(path)
        if filename == "hold_stocks.csv":
            results[filename] = {
                "status": "snapshot_only",
                "rows": len(rows),
                "sha256": sha256_file(path),
                "verification_level": "file_hash_only_no_embedded_date",
            }
            continue
        column, rule = ACCOUNT_DATE_RULES[filename]
        values = pd.Series([row.get(column, "") for row in rows], dtype="string")
        dates = parse_market_speed_dates(values)
        passed = bool(dates) and (
            dates == [target] if rule == "all_target" else max(dates) == target
        )
        if not passed:
            blocking.append(f"{filename}: CSV内日付{dates}が対象日{target}と不整合")
        results[filename] = {
            "status": "passed" if passed else "failed",
            "rows": len(rows),
            "embedded_dates": dates,
            "rule": rule,
            "sha256": sha256_file(path),
            "verification_level": "embedded_date",
        }
    return results, blocking


def persist_reference(
    source_root: Path,
    destination_root: Path,
) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    relative_paths = [Path("master.csv"), Path("daily.csv")]
    for relative in relative_paths:
        source = source_root / relative
        if not source.is_file():
            continue
        destination = destination_root / relative
        if source.resolve() != destination.resolve():
            atomic_copy(source, destination)
        records[str(relative)] = file_record(destination)
    return records


def build_report(args: argparse.Namespace) -> tuple[dict[str, Any], Path, Path]:
    target = args.date.isoformat()
    input_dir = args.input_dir or DEFAULT_RAW_ROOT / target
    reference_root = resolve_reference_root(args, target)
    commands = fetch_reference_if_needed(args, target, reference_root)
    master, daily = reference_frames(reference_root, target)
    market_by_code, daily_by_code = frame_index(master, daily)
    row_checks, row_blocking, row_warnings = row_consistency_check(
        input_dir, market_by_code, daily_by_code
    )
    stop_checks, stop_blocking, verified_absent = exact_stop_check(
        input_dir, daily, market_by_code
    )
    if args.market_only:
        account_checks: dict[str, Any] = {"status": "not_in_scope"}
        account_blocking: list[str] = []
        expected_files = EXPECTED_MARKET_FILES
    else:
        account_checks, account_blocking = account_check(input_dir, target)
        expected_files = (*EXPECTED_MARKET_FILES, *ACCOUNT_FILES)
    prior_hash_checks, prior_hash_blocking, prior_hash_warnings = (
        prior_archive_hash_check(
            input_dir, args.archive_root, target, account_checks
        )
    )
    blocking = (
        row_blocking
        + stop_blocking
        + account_blocking
        + prior_hash_blocking
    )
    warnings = row_warnings + prior_hash_warnings
    inventory_present = sorted(
        filename
        for filename in expected_files
        if (input_dir / filename).is_file()
    )
    inventory_missing = sorted(
        set(expected_files) - set(inventory_present)
    )
    unresolved_missing = sorted(set(inventory_missing) - set(verified_absent))
    trade_date_status = (
        "target_date_aligned" if not blocking else "target_date_not_confirmed"
    )
    report: dict[str, Any] = {
        "schema_version": 2,
        "trade_date": target,
        "created_at": datetime.now(JST).isoformat(timespec="seconds"),
        "input_dir": str(input_dir),
        "validation_purpose": "prior_trade_date_contamination_detection",
        "archive_gate": "passed" if not blocking else "blocked",
        "trade_date_alignment_status": trade_date_status,
        "validation_status": trade_date_status,
        "inventory": {
            "present_files": inventory_present,
            "missing_files": inventory_missing,
            "physical_file_count": len(inventory_present),
            "expected_file_count": len(expected_files),
            "market_only": args.market_only,
        },
        "category_coverage": {
            "status": (
                "all_categories_accounted_for"
                if not unresolved_missing
                else "unresolved_missing_categories"
            ),
            "physical_present_count": len(inventory_present),
            "expected_count": len(expected_files),
            "verified_absent_files": verified_absent,
            "unresolved_missing_files": unresolved_missing,
        },
        "reference": {
            "provider": "J-Quants CLI",
            "reference_dir": str(reference_root),
            "master": {**file_record(reference_root / "master.csv"), "rows": len(master)},
            "daily": {**file_record(reference_root / "daily.csv"), "rows": len(daily)},
            "commands_executed": commands,
            "reproduction_commands": {
                "master.csv": f"jquants eq master --date {target}",
                "daily.csv": f"jquants eq daily --date {target}",
            },
            "network_fetch_performed": bool(commands),
            "data_usage": {
                "all_codes_master": "used",
                "all_codes_daily": "used",
                "targeted_addon_minute": "not_used",
                "all_market_minute": "not_used",
                "all_market_ticks": "not_used",
            },
        },
        "verified_absent_categories": verified_absent,
        "stop_limit_exact_checks": stop_checks,
        "prior_archive_hash_check": prior_hash_checks,
        "market_file_checks": row_checks,
        "account_file_checks": account_checks,
        "blocking_issues": blocking,
        "warnings": warnings,
        "out_of_scope_checks": [
            "MarketSpeedの各イベント抽出ルールが市場実態を完全に表すこと",
            "特別気配・新高値/安値・大口約定の全件完全性",
            "大口約定の50万株抽出仕様と個別ティックの再現",
        ],
    }
    destination_root = args.evidence_root / target / "jquants"
    if args.persist_evidence:
        report["persisted_reference"] = persist_reference(
            reference_root, destination_root
        )
        report["reference"]["reference_dir"] = str(destination_root)
        report["reference"]["master"] = {
            **file_record(destination_root / "master.csv"),
            "rows": len(master),
        }
        report["reference"]["daily"] = {
            **file_record(destination_root / "daily.csv"),
            "rows": len(daily),
        }
        reference_root = destination_root
    report_path = args.report or args.evidence_root / target / "report.json"
    if args.persist_evidence or args.report is not None:
        atomic_write_json(report_path, report)
    return report, report_path, reference_root


def print_human(report: dict[str, Any], report_path: Path) -> None:
    print("MarketSpeed 対象営業日検証")
    print(f"対象日: {report['trade_date']}")
    print(f"保存可否: {report['archive_gate']}")
    print(
        "物理ファイル: "
        f"{report['inventory']['physical_file_count']}/"
        f"{report['inventory']['expected_file_count']}"
    )
    print(
        "確認済み当日不在: "
        + (", ".join(report["verified_absent_categories"]) or "なし")
    )
    print(
        "過去raw同一SHA: "
        + report["prior_archive_hash_check"]["status"]
    )
    if report["blocking_issues"]:
        print("要確認:")
        for issue in report["blocking_issues"]:
            print(f"  - {issue}")
    if report_path.is_file():
        print(f"検証JSON: {report_path}")


def main() -> int:
    args = parse_args()
    try:
        report, report_path, _ = build_report(args)
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print_human(report, report_path)
        return 0 if report["archive_gate"] == "passed" else 2
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
