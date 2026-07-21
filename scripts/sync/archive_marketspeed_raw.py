#!/usr/bin/env python3
"""MarketSpeed CSVを営業日別rawへ安全に保存する。"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


JST = ZoneInfo("Asia/Tokyo")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_DIR = PROJECT_ROOT / "data" / "csv"
DEFAULT_ARCHIVE_ROOT = PROJECT_ROOT / "data" / "marketspeed" / "raw"
DEFAULT_VALIDATION_ROOT = PROJECT_ROOT / "data" / "marketspeed" / "validation"
DEFAULT_CALENDAR = PROJECT_ROOT / "data" / "parquet" / "calendar.parquet"
VALIDATOR = PROJECT_ROOT / "scripts" / "sync" / "validate_marketspeed_raw.py"

MARKETS = ("prime", "standard", "growth")
MARKET_FAMILIES = (
    "stop_high",
    "stop_low",
    "special_buy",
    "special_sell",
    "new_high",
    "new_low",
    "large_deal",
)
MARKET_FILES = tuple(
    f"{family}_{market}.csv"
    for family in MARKET_FAMILIES
    for market in MARKETS
)
ACCOUNT_FILES = (
    "order.csv",
    "order_results.csv",
    "stock_results__today.csv",
    "stock_results__month.csv",
    "hold_stocks.csv",
)
ACCOUNT_DATE_COLUMNS = {
    "order.csv": "発注/受注日時",
    "order_results.csv": "約定日",
    "stock_results__today.csv": "約定日",
    "stock_results__month.csv": "約定日",
}


@dataclass(frozen=True)
class FileSnapshot:
    path: str
    exists: bool
    bytes: int | None = None
    data_rows: int | None = None
    modified_at: str | None = None
    sha256: str | None = None


@dataclass
class FileDecision:
    file: str
    scope: str
    family: str | None
    status: str
    reason: str
    source: FileSnapshot
    archived: FileSnapshot
    verification_evidence: str | None = None


@dataclass
class PreflightPlan:
    mode: str
    target_date: str
    output_window_start: str
    output_window_end: str
    source_dir: str
    archive_dir: str
    date_evidence: dict[str, str]
    date_warnings: list[str]
    decisions: list[FileDecision]
    validation_report: dict[str, Any] | None
    blocking_issues: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MarketSpeed CSVの対象日・鮮度・欠損を確認し、rawへ保存する"
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        help="例外時だけ指定する取引日 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="事前判定に問題がなければ保存する。省略時はdry-run",
    )
    parser.add_argument("--json", action="store_true", help="判定結果をJSONで表示する")
    parser.add_argument(
        "--verified-absent",
        "--verified-zero",
        dest="verified_absent",
        action="append",
        default=[],
        metavar="FILE=EVIDENCE",
        help="外部照合済みの当日不在カテゴリーと証跡。例外時のみ使用し、複数指定可",
    )
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--calendar", type=Path, default=DEFAULT_CALENDAR)
    parser.add_argument(
        "--validation-report",
        type=Path,
        help="validate_marketspeed_raw.pyが作成した対象日別report.json",
    )
    parser.add_argument(
        "--validate-jquants",
        action="store_true",
        help="保存計画の当日CSVだけを一時領域でJ-Quants照合する",
    )
    parser.add_argument("--jquants-total-timeout", type=int, default=90)
    return parser.parse_args()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_data_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return max(sum(1 for _ in csv.reader(handle)) - 1, 0)


def snapshot(path: Path) -> FileSnapshot:
    if not path.is_file():
        return FileSnapshot(path=str(path), exists=False)
    stat = path.stat()
    modified_at = datetime.fromtimestamp(stat.st_mtime, JST).isoformat(timespec="seconds")
    return FileSnapshot(
        path=str(path),
        exists=True,
        bytes=stat.st_size,
        data_rows=csv_data_rows(path),
        modified_at=modified_at,
        sha256=file_sha256(path),
    )


def load_trading_days(calendar_path: Path) -> list[date]:
    if not calendar_path.is_file():
        raise FileNotFoundError(f"営業日カレンダーがありません: {calendar_path}")
    frame = pd.read_parquet(calendar_path, columns=["date"])
    days = sorted({pd.Timestamp(value).date() for value in frame["date"].dropna()})
    if not days:
        raise RuntimeError(f"営業日カレンダーが空です: {calendar_path}")
    return days


def automatic_target_date(now: datetime, trading_days: list[date]) -> date:
    cutoff = now.date() if now.time() >= time(16, 0) else now.date() - timedelta(days=1)
    candidates = [day for day in trading_days if day <= cutoff]
    if not candidates:
        raise RuntimeError(f"{cutoff}以前の営業日がカレンダーにありません")
    return candidates[-1]


def output_window(target: date, trading_days: list[date]) -> tuple[datetime, datetime]:
    start = datetime.combine(target, time(15, 30), JST)
    next_days = [day for day in trading_days if day > target]
    if next_days:
        end = datetime.combine(next_days[0], time(15, 30), JST)
    else:
        end = start + timedelta(days=7)
    return start, end


def parse_market_speed_date(value: str) -> date | None:
    normalized = value.strip().replace("/", "-")
    if len(normalized) < 10:
        return None
    try:
        return date.fromisoformat(normalized[:10])
    except ValueError:
        return None


def max_embedded_date(path: Path, column: str) -> date | None:
    if not path.is_file():
        return None
    values: list[date] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            parsed = parse_market_speed_date(row.get(column, ""))
            if parsed is not None:
                values.append(parsed)
    return max(values) if values else None


def collect_date_evidence(source_dir: Path) -> dict[str, str]:
    evidence: dict[str, str] = {}
    for filename, column in ACCOUNT_DATE_COLUMNS.items():
        embedded = max_embedded_date(source_dir / filename, column)
        if embedded is not None:
            evidence[filename] = embedded.isoformat()
    return evidence


def parse_verified_absent_specs(specs: list[str]) -> dict[str, str]:
    verified: dict[str, str] = {}
    for spec in specs:
        if "=" not in spec:
            raise ValueError(
                f"--verified-absentはFILE=EVIDENCE形式で指定してください: {spec}"
            )
        filename, evidence = spec.split("=", 1)
        filename = filename.strip()
        evidence = evidence.strip()
        if filename not in MARKET_FILES:
            raise ValueError(f"確認済み0件の対象外ファイルです: {filename}")
        if not evidence:
            raise ValueError(f"確認証跡が空です: {filename}")
        verified[filename] = evidence
    return verified


def load_manifest_hashes(manifest_path: Path) -> dict[str, str]:
    if not manifest_path.is_file():
        return {}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    hashes: dict[str, str] = {}
    for item in payload.get("files", []):
        filename = item.get("file")
        sha256 = item.get("sha256")
        if isinstance(filename, str) and isinstance(sha256, str):
            hashes[filename] = sha256
    return hashes


def load_manifest_verified_absent(manifest_path: Path) -> dict[str, str]:
    if not manifest_path.is_file():
        return {}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    verified: dict[str, str] = {}
    items = payload.get(
        "verified_absent_categories",
        payload.get("verified_zero_categories", []),
    )
    for item in items:
        filename = item.get("file")
        evidence = item.get("evidence")
        if filename in MARKET_FILES and isinstance(evidence, str) and evidence:
            verified[filename] = evidence
    return verified


def load_validation_report(
    path: Path | None, target_date: str
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[str]]:
    if path is None:
        return None, None, []
    issues: list[str] = []
    if not path.is_file():
        return None, None, [f"検証reportが存在しません: {path}"]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        return None, None, [f"検証reportを読めません: {path}: {error}"]
    if payload.get("trade_date") != target_date:
        issues.append(
            f"検証reportの対象日不一致: {payload.get('trade_date')} != {target_date}"
        )
    if payload.get("archive_gate") != "passed":
        issues.append(
            f"検証reportの保存ゲートがpassedではありません: {payload.get('archive_gate')}"
        )
    if payload.get("blocking_issues"):
        issues.append("検証reportにblocking_issuesが残っています")
    metadata = {
        "path": str(path),
        "sha256": file_sha256(path),
        "schema_version": payload.get("schema_version"),
        "created_at": payload.get("created_at"),
        "validation_purpose": payload.get("validation_purpose"),
        "trade_date_alignment_status": payload.get(
            "trade_date_alignment_status"
        ),
        "validation_status": payload.get("validation_status"),
        "archive_gate": payload.get("archive_gate"),
        "reference_data_usage": payload.get("reference", {}).get("data_usage"),
        "prior_archive_hash_status": payload.get(
            "prior_archive_hash_check", {}
        ).get("status"),
        "category_coverage": payload.get("category_coverage"),
        "verified_absent_categories": payload.get(
            "verified_absent_categories", []
        ),
        "out_of_scope_checks": payload.get("out_of_scope_checks", []),
    }
    return payload, metadata, issues


def validation_file_hashes(payload: dict[str, Any]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for section_name in ("market_file_checks", "account_file_checks"):
        section = payload.get(section_name, {})
        if not isinstance(section, dict):
            continue
        for filename, item in section.items():
            if not isinstance(item, dict):
                continue
            sha256 = item.get("sha256")
            if isinstance(filename, str) and isinstance(sha256, str):
                hashes[filename] = sha256
    return hashes


def is_fresh(snapshot_value: FileSnapshot, start: datetime, end: datetime) -> bool:
    if not snapshot_value.exists or snapshot_value.modified_at is None:
        return False
    modified_at = datetime.fromisoformat(snapshot_value.modified_at)
    return start <= modified_at < end


def decide_existing_or_source(
    filename: str,
    scope: str,
    family: str | None,
    source: FileSnapshot,
    archived: FileSnapshot,
    manifest_hash: str | None,
    start: datetime,
    end: datetime,
    embedded_date: str | None,
    target_date: str,
) -> FileDecision:
    archived_verified = (
        archived.exists
        and archived.sha256 is not None
        and manifest_hash == archived.sha256
    )
    archived_current = archived_verified or is_fresh(archived, start, end)
    source_current = is_fresh(source, start, end)

    if scope == "account" and embedded_date is not None and embedded_date != target_date:
        source_current = False

    if archived_current:
        if source_current and source.sha256 != archived.sha256:
            return FileDecision(
                filename,
                scope,
                family,
                "update",
                "当日更新された元ファイルが保存済みrawと異なる",
                source,
                archived,
            )
        reason = "manifestのSHA-256と一致" if archived_verified else "保存先の更新時刻が当日出力窓内"
        return FileDecision(filename, scope, family, "archived", reason, source, archived)

    if archived.exists:
        return FileDecision(
            filename,
            scope,
            family,
            "ambiguous",
            "保存済みrawをmanifestまたは更新時刻で当日分と確認できない",
            source,
            archived,
        )

    if source_current:
        return FileDecision(
            filename,
            scope,
            family,
            "copy",
            "元ファイルの更新時刻が当日出力窓内",
            source,
            archived,
        )

    if scope == "account":
        if source.exists and embedded_date is not None and embedded_date != target_date:
            reason = f"CSV内の最新日付が対象日と不一致 ({embedded_date})"
        elif source.exists:
            reason = "口座CSVの更新時刻が当日出力窓外"
        else:
            reason = "必須の口座CSVが存在しない"
        return FileDecision(filename, scope, family, "ambiguous", reason, source, archived)

    stale_reason = "元ファイルが存在しない"
    if source.exists:
        stale_reason = f"元ファイルが古い ({source.modified_at})"
    return FileDecision(filename, scope, family, "pending_missing", stale_reason, source, archived)


def build_plan(args: argparse.Namespace) -> PreflightPlan:
    now = datetime.now(JST)
    trading_days = load_trading_days(args.calendar)
    target = args.date or automatic_target_date(now, trading_days)
    if target not in trading_days:
        raise RuntimeError(f"対象日は営業日カレンダーにありません: {target}")

    start, end = output_window(target, trading_days)
    target_text = target.isoformat()
    archive_dir = args.archive_root / target_text
    manifest_path = archive_dir / "manifest.json"
    manifest_hashes = load_manifest_hashes(manifest_path)
    date_evidence = collect_date_evidence(args.source_dir)
    validation_payload, validation_metadata, validation_issues = load_validation_report(
        args.validation_report, target_text
    )
    verified_absent_specs = (
        {} if validation_payload is not None else load_manifest_verified_absent(manifest_path)
    )
    if validation_payload is not None:
        evidence = (
            f"{args.validation_report}#verified_absent_categories"
        )
        for filename in validation_payload.get("verified_absent_categories", []):
            if filename in MARKET_FILES:
                verified_absent_specs[filename] = evidence
    verified_absent_specs.update(
        parse_verified_absent_specs(args.verified_absent)
    )
    date_warnings = [
        f"{filename}の最新日付が対象日と不一致: {embedded}"
        for filename, embedded in date_evidence.items()
        if embedded != target_text
    ]

    decisions: list[FileDecision] = []
    for filename in MARKET_FILES:
        family = filename.rsplit("_", 1)[0]
        decisions.append(
            decide_existing_or_source(
                filename=filename,
                scope="market",
                family=family,
                source=snapshot(args.source_dir / filename),
                archived=snapshot(archive_dir / filename),
                manifest_hash=manifest_hashes.get(filename),
                start=start,
                end=end,
                embedded_date=None,
                target_date=target_text,
            )
        )

    for filename in ACCOUNT_FILES:
        decisions.append(
            decide_existing_or_source(
                filename=filename,
                scope="account",
                family=None,
                source=snapshot(args.source_dir / filename),
                archived=snapshot(archive_dir / filename),
                manifest_hash=manifest_hashes.get(filename),
                start=start,
                end=end,
                embedded_date=date_evidence.get(filename),
                target_date=target_text,
            )
        )

    for family in MARKET_FAMILIES:
        members = [item for item in decisions if item.family == family]
        family_has_current = any(
            item.status in {"archived", "copy", "update"} for item in members
        )
        for item in members:
            if item.status != "pending_missing":
                continue
            if family_has_current:
                item.status = "inferred_absent"
                item.reason += "; 同系列の他市場は当日分を確認済み"
            else:
                item.status = "ambiguous"
                item.reason += "; 同系列が全市場未確認のため0件と断定できない"

    verification_conflicts: list[str] = []
    if validation_payload is not None:
        report_hashes = validation_file_hashes(validation_payload)
        current_files: set[str] = set()
        for item in decisions:
            if item.status in {"archived", "copy", "update"}:
                current_files.add(item.file)
                actual_hash = (
                    item.source.sha256
                    if item.status in {"copy", "update"}
                    else item.archived.sha256
                )
                report_hash = report_hashes.get(item.file)
                if report_hash is None:
                    verification_conflicts.append(
                        f"{item.file}: 検証reportに物理CSVのSHA-256がありません"
                    )
                elif actual_hash != report_hash:
                    verification_conflicts.append(
                        f"{item.file}: 検証後にCSVが変化しています"
                    )
        unexpected_hashes = sorted(set(report_hashes) - current_files)
        if unexpected_hashes:
            verification_conflicts.append(
                f"検証reportの物理CSVと保存計画が不一致: {unexpected_hashes}"
            )
    for filename, evidence in verified_absent_specs.items():
        item = next(decision for decision in decisions if decision.file == filename)
        if item.status not in {"inferred_absent", "ambiguous"}:
            verification_conflicts.append(
                f"{filename}: 当日ファイルが確認されているため0件指定と矛盾 ({item.status})"
            )
            continue
        item.status = "verified_absent"
        item.reason = (
            "当日物理ファイルなしをJ-Quants日足のストップ高安フラグと市場別に照合済み"
            if validation_payload is not None
            else "当日ファイルなしを外部の当日ストップ高安一覧と照合済み"
        )
        item.verification_evidence = evidence

    blocking_issues = list(date_warnings)
    blocking_issues.extend(validation_issues)
    blocking_issues.extend(verification_conflicts)
    blocking_issues.extend(
        f"{item.file}: {item.reason}"
        for item in decisions
        if item.status in {"ambiguous", "inferred_absent"}
    )
    return PreflightPlan(
        mode="commit" if args.commit else "dry-run",
        target_date=target_text,
        output_window_start=start.isoformat(timespec="minutes"),
        output_window_end=end.isoformat(timespec="minutes"),
        source_dir=str(args.source_dir),
        archive_dir=str(archive_dir),
        date_evidence=date_evidence,
        date_warnings=date_warnings,
        decisions=decisions,
        validation_report=validation_metadata,
        blocking_issues=blocking_issues,
    )


def decision_counts(plan: PreflightPlan) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in plan.decisions:
        counts[item.status] = counts.get(item.status, 0) + 1
    return counts


def print_human(plan: PreflightPlan) -> None:
    counts = decision_counts(plan)
    print("MarketSpeed raw 事前確認")
    print(f"モード: {'保存' if plan.mode == 'commit' else 'dry-run（変更なし）'}")
    print(f"対象営業日: {plan.target_date}")
    print(f"出力判定窓: {plan.output_window_start} ～ {plan.output_window_end}")
    print(f"保存先: {plan.archive_dir}")
    if plan.validation_report is not None:
        print(
            "対象営業日検証: "
            f"{plan.validation_report.get('trade_date_alignment_status')} "
            f"({plan.validation_report.get('path')})"
        )
    else:
        print("J-Quants検証: 未指定")
    print()
    print("判定件数:")
    for status in (
        "archived",
        "copy",
        "update",
        "verified_absent",
        "inferred_absent",
        "ambiguous",
    ):
        print(f"  {status}: {counts.get(status, 0)}")
    print()
    print("確認済み当日不在:")
    verified = [item for item in plan.decisions if item.status == "verified_absent"]
    if verified:
        for item in verified:
            print(f"  - {item.file}: {item.verification_evidence}")
    else:
        print("  なし")
    print()
    print("推定0件:")
    inferred = [item for item in plan.decisions if item.status == "inferred_absent"]
    if inferred:
        for item in inferred:
            print(f"  - {item.file}: {item.reason}")
    else:
        print("  なし")
    print()
    print("要確認:")
    if plan.blocking_issues:
        for issue in plan.blocking_issues:
            print(f"  - {issue}")
    else:
        print("  なし")


def plan_to_dict(plan: PreflightPlan) -> dict[str, Any]:
    return {
        **asdict(plan),
        "counts": decision_counts(plan),
    }


def manifest_payload(plan: PreflightPlan, archive_dir: Path) -> dict[str, Any]:
    now = datetime.now(JST).isoformat(timespec="seconds")
    existing_manifest = archive_dir / "manifest.json"
    archived_at = now
    if existing_manifest.is_file():
        try:
            previous = json.loads(existing_manifest.read_text(encoding="utf-8"))
            archived_at = previous.get("archived_at", now)
        except (json.JSONDecodeError, OSError):
            pass

    files: list[dict[str, Any]] = []
    for filename in (*MARKET_FILES, *ACCOUNT_FILES):
        archived = snapshot(archive_dir / filename)
        if not archived.exists:
            continue
        item: dict[str, Any] = {
            "file": filename,
            "bytes": archived.bytes,
            "data_rows": archived.data_rows,
            "sha256": archived.sha256,
        }
        if filename in ACCOUNT_FILES:
            item["scope"] = {
                "order.csv": "account_order_snapshot",
                "order_results.csv": "account_execution_snapshot",
                "stock_results__today.csv": "account_daily_result_snapshot",
                "stock_results__month.csv": "account_month_result_snapshot",
                "hold_stocks.csv": "account_position_snapshot",
            }[filename]
        files.append(item)

    inferred_absent = [
        {
            "file": item.file,
            "reason": item.reason,
            "stale_source_modified_at": item.source.modified_at,
        }
        for item in plan.decisions
        if item.status == "inferred_absent"
    ]
    verified_absent = [
        {
            "file": item.file,
            "reason": item.reason,
            "verification_method": (
                "jquants_daily_stop_flag_exact_market_set"
                if plan.validation_report is not None
                else "external_market_list_cross_check"
            ),
            "evidence": item.verification_evidence,
            "verified_at": now,
        }
        for item in plan.decisions
        if item.status == "verified_absent"
    ]
    market_count = sum(item["file"] in MARKET_FILES for item in files)
    account_count = sum(item["file"] in ACCOUNT_FILES for item in files)
    unresolved_missing = [
        item.file for item in plan.decisions if item.status == "ambiguous"
    ]
    market_accounted_for = market_count + len(verified_absent)
    category_coverage_status = (
        "all_categories_accounted_for"
        if market_accounted_for == len(MARKET_FILES)
        and account_count == len(ACCOUNT_FILES)
        and not unresolved_missing
        else "unresolved_missing_categories"
    )
    trade_date_alignment_status = (
        plan.validation_report.get("trade_date_alignment_status")
        if plan.validation_report is not None
        else "not_run"
    )
    return {
        "schema_version": 4,
        "trade_date": plan.target_date,
        "source_application": "MarketSpeed",
        "archive_policy": "raw_copy_no_transformation",
        "validation_purpose": "prior_trade_date_contamination_detection",
        "archived_at": archived_at,
        "updated_at": now,
        "file_count": len(files),
        "expected_file_count": len(MARKET_FILES) + len(ACCOUNT_FILES),
        "market_file_count": market_count,
        "expected_market_file_count": len(MARKET_FILES),
        "account_file_count": account_count,
        "expected_account_file_count": len(ACCOUNT_FILES),
        "physical_file_status": {
            "status": f"{len(files)}_of_{len(MARKET_FILES) + len(ACCOUNT_FILES)}_present",
            "present_count": len(files),
            "expected_count": len(MARKET_FILES) + len(ACCOUNT_FILES),
            "market_present_count": market_count,
            "market_expected_count": len(MARKET_FILES),
            "account_present_count": account_count,
            "account_expected_count": len(ACCOUNT_FILES),
        },
        "category_coverage_status": {
            "status": category_coverage_status,
            "physical_present_count": len(files),
            "verified_absent_count": len(verified_absent),
            "unresolved_missing_files": unresolved_missing,
        },
        "trade_date_alignment_status": trade_date_alignment_status,
        "validation": plan.validation_report,
        "verified_absent_categories": verified_absent,
        "inferred_absent_categories": inferred_absent,
        "unresolved_missing_files": unresolved_missing,
        "date_evidence": plan.date_evidence,
        "files": files,
    }


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp"
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def commit_plan(plan: PreflightPlan) -> None:
    if plan.blocking_issues:
        raise RuntimeError("要確認項目があるため保存できません")

    archive_dir = Path(plan.archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    update_items = [item for item in plan.decisions if item.status == "update"]
    manifest_path = archive_dir / "manifest.json"
    if update_items or manifest_path.is_file():
        revision_stamp = datetime.now(JST).strftime("%Y%m%dT%H%M%S%f%z")
        revision_dir = archive_dir / "revisions" / revision_stamp
        revision_dir.mkdir(parents=True, exist_ok=False)
        if manifest_path.is_file():
            shutil.copy2(manifest_path, revision_dir / "manifest.json")
        for item in update_items:
            archived_path = archive_dir / item.file
            if archived_path.is_file():
                shutil.copy2(archived_path, revision_dir / item.file)

    for item in plan.decisions:
        if item.status not in {"copy", "update"}:
            continue
        source_path = Path(item.source.path)
        destination = archive_dir / item.file
        shutil.copy2(source_path, destination)
        if file_sha256(source_path) != file_sha256(destination):
            raise RuntimeError(f"コピー後のSHA-256が一致しません: {item.file}")

    atomic_write_json(archive_dir / "manifest.json", manifest_payload(plan, archive_dir))


def run_jquants_validation(
    args: argparse.Namespace, preliminary: PreflightPlan
) -> Path:
    if args.validation_report is not None:
        raise ValueError(
            "--validate-jquantsと--validation-reportは同時指定できません"
        )
    target = preliminary.target_date
    validation_root = DEFAULT_VALIDATION_ROOT / target
    attempt_stamp = datetime.now(JST).strftime("%Y%m%dT%H%M%S%f%z")
    report_path = validation_root / "attempts" / f"{attempt_stamp}.json"
    with tempfile.TemporaryDirectory(prefix=f"marketspeed-{target}-") as temporary:
        staged = Path(temporary)
        for item in preliminary.decisions:
            if item.status not in {"archived", "copy", "update"}:
                continue
            selected = (
                Path(item.source.path)
                if item.status in {"copy", "update"}
                else Path(item.archived.path)
            )
            if not selected.is_file():
                raise RuntimeError(f"検証用CSVが存在しません: {selected}")
            destination = staged / item.file
            shutil.copy2(selected, destination)
            if file_sha256(selected) != file_sha256(destination):
                raise RuntimeError(f"検証用コピーのSHA-256不一致: {item.file}")

        command = [
            sys.executable,
            "-B",
            str(VALIDATOR),
            "--date",
            target,
            "--input-dir",
            str(staged),
            "--fetch-jquants",
            "--persist-evidence",
            "--archive-root",
            str(args.archive_root),
            "--total-timeout",
            str(args.jquants_total_timeout),
            "--report",
            str(report_path),
            "--json",
        ]
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=args.jquants_total_timeout + 30,
            check=False,
        )
        if result.returncode not in {0, 2}:
            detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
            raise RuntimeError(f"MarketSpeed J-Quants検証に失敗: {detail}")
        if not report_path.is_file():
            raise RuntimeError(f"MarketSpeed検証reportが作成されませんでした: {report_path}")
        report_payload = json.loads(report_path.read_text(encoding="utf-8"))
        if report_payload.get("archive_gate") == "passed":
            atomic_write_json(validation_root / "report.json", report_payload)
    return report_path


def main() -> int:
    args = parse_args()
    try:
        plan = build_plan(args)
        if args.validate_jquants:
            args.validation_report = run_jquants_validation(args, plan)
            args.validate_jquants = False
            plan = build_plan(args)
        if args.json:
            print(json.dumps(plan_to_dict(plan), ensure_ascii=False, indent=2))
        else:
            print_human(plan)

        if not args.commit:
            return 2 if plan.blocking_issues else 0
        commit_plan(plan)
        print(f"保存完了: {plan.archive_dir}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
