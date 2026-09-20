from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import collect_kospi_mcap_quarterly_c8_dart_viewer_shadow as shadow
from tools.analyze_kospi_mcap_quarterly_c8_dart_viewer_policy_scope import (
    parse_official_family_references,
)


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_lineage_landing_collection_contract_v1.json"
)
Fetcher = shadow.Fetcher


class LineageLandingError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail or code


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LineageLandingError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(payload, dict):
        raise LineageLandingError("JSON_OBJECT_REQUIRED", str(path))
    return payload


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _load_context(
    root: Path, contract_path: Path
) -> tuple[dict[str, Any], dict[str, Any], list[str], str]:
    contract = _load_json(contract_path)
    if contract.get("strategy_id") != STRATEGY_ID or contract.get("mode") != "SHADOW_ONLY":
        raise LineageLandingError("LINEAGE_LANDING_CONTRACT_INVALID")
    inputs = contract.get("input")
    if not isinstance(inputs, Mapping):
        raise LineageLandingError("LINEAGE_LANDING_INPUT_CONTRACT_INVALID")
    report_path = _resolve(root, str(inputs.get("lineage_report_path", "")))
    report_bytes = report_path.read_bytes()
    report_hash = hashlib.sha256(report_bytes).hexdigest()
    if report_hash != str(inputs.get("lineage_report_sha256", "")).lower():
        raise LineageLandingError("LINEAGE_REPORT_HASH_MISMATCH", report_hash)
    report = json.loads(report_bytes.decode("utf-8"))
    for key, expected_key in (
        ("status", "required_report_status"),
        ("verdict", "required_report_verdict"),
        ("selection_as_of", "required_selection_as_of"),
    ):
        if report.get(key) != inputs.get(expected_key):
            raise LineageLandingError(f"LINEAGE_REPORT_{key.upper()}_MISMATCH")
    source = str(inputs["required_source"])
    receipts = sorted(
        {
            str(row.get("receipt_no") or "")
            for row in report.get("rows", [])
            if row.get("source") == source and row.get("authority_resolved") is False
        }
    )
    pattern = re.compile(str(inputs["receipt_number_pattern"]))
    if any(not pattern.fullmatch(receipt) for receipt in receipts):
        raise LineageLandingError("LINEAGE_RECEIPT_FORMAT_INVALID")
    if len(receipts) != int(inputs["required_unresolved_count"]):
        raise LineageLandingError(
            "LINEAGE_RECEIPT_COUNT_MISMATCH",
            f"expected={inputs['required_unresolved_count']}, actual={len(receipts)}",
        )
    return contract, report, receipts, report_hash


def _paths(root: Path, contract: Mapping[str, Any]) -> dict[str, Path]:
    output = contract["output"]
    output_root = _resolve(root, output["root"])
    return {
        "root": output_root,
        "raw": output_root / output["raw_dir"],
        "pass": output_root / output["pass_journal_dir"],
        "fail": output_root / output["failure_journal_dir"],
        "checkpoint": output_root / output["checkpoint_path"],
        "report": _resolve(root, output["report_path"]),
    }


def _safe_artifact(root: Path, relative_path: object) -> Path:
    candidate = (root / str(relative_path or "")).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise LineageLandingError("JOURNAL_ARTIFACT_PATH_ESCAPE", str(candidate)) from exc
    return candidate


def _verify_completed(
    root: Path, pass_dir: Path, receipts: list[str]
) -> tuple[list[str], list[str]]:
    completed: list[str] = []
    issues: list[str] = []
    for receipt in receipts:
        journal_path = pass_dir / f"{receipt}.json"
        if not journal_path.exists():
            continue
        try:
            journal = _load_json(journal_path)
            if (
                journal.get("status") != "SHADOW_PASS"
                or journal.get("promotion_allowed") is not False
                or journal.get("receipt_no") != receipt
            ):
                raise LineageLandingError("PASS_JOURNAL_CONTRACT_INVALID", receipt)
            landing = journal.get("landing") or {}
            raw_path = _safe_artifact(root, landing.get("relative_path"))
            raw = raw_path.read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            if digest != landing.get("sha256") or len(raw) != landing.get("size_bytes"):
                raise LineageLandingError("PASS_JOURNAL_ARTIFACT_MISMATCH", receipt)
            if int(landing.get("http_status", 0)) != 200 or not landing.get("title"):
                raise LineageLandingError("PASS_JOURNAL_LANDING_INVALID", receipt)
            if parse_official_family_references(raw) != journal.get("official_family_references"):
                raise LineageLandingError("PASS_JOURNAL_FAMILY_PARSE_MISMATCH", receipt)
            completed.append(receipt)
        except (LineageLandingError, OSError, ValueError, TypeError) as exc:
            issues.append(f"{receipt}:{getattr(exc, 'code', type(exc).__name__)}")
    return completed, issues


def _collect_one(
    *,
    root: Path,
    receipt: str,
    contract: Mapping[str, Any],
    paths: Mapping[str, Path],
    fetcher: Fetcher,
    sleep_fn: Callable[[float], None],
    timeout: float,
    retries: int,
    backoff: float,
    request_budget: int,
    clock: Callable[[], datetime],
) -> tuple[dict[str, Any], int]:
    route = contract["official_route"]
    execution = contract["execution"]
    status, payload, headers, attempts = shadow._fetch_with_retry(
        endpoint=str(route["landing_endpoint"]),
        params={str(route["landing_query_key"]): receipt},
        headers={
            "User-Agent": str(execution["user_agent"]),
            "Accept": "text/html,application/xhtml+xml",
        },
        timeout=timeout,
        retries=min(retries, max(0, request_budget - 1)),
        backoff=backoff,
        fetcher=fetcher,
        sleep_fn=sleep_fn,
    )
    if status != 200:
        raise LineageLandingError("LANDING_HTTP_STATUS_INVALID", f"{receipt}:{status}")
    title = shadow._clean_title(shadow._decode_html(payload))
    if not payload or not title:
        raise LineageLandingError("LANDING_CONTENT_INVALID", receipt)
    digest = hashlib.sha256(payload).hexdigest()
    raw_path = paths["raw"] / receipt / f"landing_{digest[:16]}.html"
    shadow._write_immutable(raw_path, payload)
    family = parse_official_family_references(payload)
    journal = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_DART_LINEAGE_LANDING_SHADOW",
        "status": "SHADOW_PASS",
        "promotion_allowed": False,
        "receipt_no": receipt,
        "captured_at": clock().astimezone(timezone.utc).isoformat(),
        "landing": {
            "http_status": status,
            "title": title,
            "server_date_header": str(headers.get("Date", "")),
            "relative_path": raw_path.relative_to(root).as_posix(),
            "sha256": digest,
            "size_bytes": len(payload),
        },
        "official_family_reference_count": len(family),
        "official_family_references": family,
        "network_requests": attempts,
    }
    shadow._write_immutable(paths["pass"] / f"{receipt}.json", _json_bytes(journal))
    return journal, attempts


def _summary(
    *,
    root: Path,
    contract: Mapping[str, Any],
    report_hash: str,
    receipts: list[str],
    paths: Mapping[str, Path],
    mode: str,
    generated_at: str,
    network_requests: int,
    run_receipts: list[str],
    stop_reason: str,
) -> dict[str, Any]:
    completed, issues = _verify_completed(root, paths["pass"], receipts)
    completed_set = set(completed)
    pending = [receipt for receipt in receipts if receipt not in completed_set]
    return {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_DART_LINEAGE_LANDING_SHADOW_STATUS",
        "generated_at": generated_at,
        "mode": mode,
        "status": "SHADOW_COMPLETE" if not pending and not issues else "SHADOW_INCOMPLETE",
        "promotion_allowed": False,
        "input_lineage_report_sha256": report_hash,
        "target_receipt_count": len(receipts),
        "completed_receipt_count": len(completed),
        "pending_receipt_count": len(pending),
        "completed_receipts": completed,
        "pending_receipts": pending,
        "integrity_pass": not issues,
        "integrity_issues": issues,
        "network_requests": network_requests,
        "run_receipts": run_receipts,
        "stop_reason": stop_reason,
        "capture_status_modified": False,
        "adapter_modified": False,
        "downstream_opened": False,
        "contract_version": contract.get("contract_version"),
    }


def run_collection(
    *,
    root: Path = ROOT,
    contract_path: Path | None = None,
    apply: bool = False,
    confirm_shadow_only: bool = False,
    max_receipts: int | None = None,
    max_network_requests: int | None = None,
    timeout: float | None = None,
    delay: float | None = None,
    retries: int | None = None,
    fetcher: Fetcher = shadow._default_fetcher,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, Any]:
    root = root.resolve()
    contract_path = contract_path or root / CONTRACT_RELATIVE
    contract, _report, receipts, report_hash = _load_context(root, contract_path)
    paths = _paths(root, contract)
    execution = contract["execution"]
    max_receipts = int(execution["default_max_receipts"] if max_receipts is None else max_receipts)
    max_network_requests = int(
        execution["default_max_network_requests"]
        if max_network_requests is None
        else max_network_requests
    )
    timeout = float(execution["default_timeout_seconds"] if timeout is None else timeout)
    delay = float(execution["default_delay_seconds"] if delay is None else delay)
    retries = int(execution["default_retries"] if retries is None else retries)
    backoff = float(execution["retry_backoff_seconds"])
    if min(max_receipts, max_network_requests) <= 0 or timeout <= 0 or delay < 0 or retries < 0:
        raise LineageLandingError("EXECUTION_ARGUMENT_INVALID")
    if apply and not confirm_shadow_only:
        raise LineageLandingError("SHADOW_CONFIRMATION_REQUIRED")
    completed, issues = _verify_completed(root, paths["pass"], receipts)
    if issues:
        raise LineageLandingError("EXISTING_SHADOW_INTEGRITY_FAILED", ";".join(issues))
    pending = [receipt for receipt in receipts if receipt not in set(completed)]
    selected = pending[: min(max_receipts, max_network_requests)]
    generated_at = clock().astimezone(timezone.utc).isoformat()
    if not apply:
        return _summary(
            root=root,
            contract=contract,
            report_hash=report_hash,
            receipts=receipts,
            paths=paths,
            mode="DRY_RUN",
            generated_at=generated_at,
            network_requests=0,
            run_receipts=selected,
            stop_reason="DRY_RUN_NO_NETWORK_NO_WRITE",
        )
    total_requests = 0
    run_receipts: list[str] = []
    stop_reason = "NO_PENDING_RECEIPTS" if not selected else "MAX_RECEIPTS_REACHED"
    for index, receipt in enumerate(selected):
        remaining_budget = max_network_requests - total_requests
        if remaining_budget <= 0:
            stop_reason = "NETWORK_REQUEST_BUDGET_EXHAUSTED"
            break
        if index and delay:
            sleep_fn(delay)
        try:
            _journal, used = _collect_one(
                root=root,
                receipt=receipt,
                contract=contract,
                paths=paths,
                fetcher=fetcher,
                sleep_fn=sleep_fn,
                timeout=timeout,
                retries=retries,
                backoff=backoff,
                request_budget=remaining_budget,
                clock=clock,
            )
            total_requests += used
            run_receipts.append(receipt)
        except (LineageLandingError, shadow.ShadowCollectionError) as exc:
            stop_reason = exc.code
            failure = {
                "strategy_id": STRATEGY_ID,
                "scope": "M1_C8_DART_LINEAGE_LANDING_SHADOW_FAILURE",
                "status": "SHADOW_FAIL",
                "promotion_allowed": False,
                "receipt_no": receipt,
                "failed_at": clock().astimezone(timezone.utc).isoformat(),
                "reason_code": exc.code,
                "detail": exc.detail,
            }
            stamp = clock().strftime("%Y%m%dT%H%M%S%fZ")
            shadow._write_immutable(paths["fail"] / receipt / f"{stamp}.json", _json_bytes(failure))
            break
    result = _summary(
        root=root,
        contract=contract,
        report_hash=report_hash,
        receipts=receipts,
        paths=paths,
        mode="APPLY_SHADOW_ONLY",
        generated_at=clock().astimezone(timezone.utc).isoformat(),
        network_requests=total_requests,
        run_receipts=run_receipts,
        stop_reason=stop_reason,
    )
    _atomic_write(paths["checkpoint"], _json_bytes(result))
    _atomic_write(paths["report"], _json_bytes(result))
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect official DART lineage landing pages")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--contract", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-shadow-only", action="store_true")
    parser.add_argument("--max-receipts", type=int)
    parser.add_argument("--max-network-requests", type=int)
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--delay", type=float)
    parser.add_argument("--retries", type=int)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        result = run_collection(
            root=args.root,
            contract_path=args.contract,
            apply=args.apply,
            confirm_shadow_only=args.confirm_shadow_only,
            max_receipts=args.max_receipts,
            max_network_requests=args.max_network_requests,
            timeout=args.timeout,
            delay=args.delay,
            retries=args.retries,
        )
    except (LineageLandingError, shadow.ShadowCollectionError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "reason_code": getattr(exc, "code", type(exc).__name__),
                    "detail": str(exc),
                },
                ensure_ascii=False,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
