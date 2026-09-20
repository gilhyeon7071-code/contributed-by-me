from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_kind_stock_issue_shadow_validation_contract_v1.json"
)


class KindCaptureError(RuntimeError):
    pass


class KindFeasibilityBlocked(KindCaptureError):
    def __init__(self, report: Mapping[str, Any]) -> None:
        self.report = dict(report)
        super().__init__(str(self.report.get("verdict") or "KIND_FEASIBILITY_BLOCKED"))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise KindCaptureError(f"KIND_REDIRECT_REJECTED:{code}:{newurl}")


class KindIssueTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[dict[str, Any]] = []
        self._row_key = ""
        self._cells: list[str] = []
        self._cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if tag == "tr":
            onclick = values.get("onclick") or ""
            match = re.search(r"fnDetailView\('([^']+)'\)", onclick)
            self._row_key = match.group(1) if match else ""
            self._cells = []
        elif tag == "td" and self._row_key:
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._cell_parts is not None:
            self._cells.append(" ".join("".join(self._cell_parts).split()))
            self._cell_parts = None
        elif tag == "tr" and self._row_key:
            if len(self._cells) != 6:
                raise KindCaptureError(
                    f"KIND_ROW_COLUMN_COUNT_INVALID:{self._row_key}:{len(self._cells)}"
                )
            self.rows.append(
                {
                    "process_key": self._row_key,
                    "company_name": self._cells[0],
                    "listing_date": self._cells[1].replace("-", ""),
                    "listing_type": self._cells[2],
                    "issued_shares": self._cells[3],
                    "par_value": self._cells[4],
                    "issue_reason": self._cells[5],
                }
            )
            self._row_key = ""
            self._cells = []


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise KindCaptureError(f"JSON_OBJECT_REQUIRED:{path}")
    return value


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def load_contract(root: Path = ROOT) -> dict[str, Any]:
    contract = _load_json(root / CONTRACT_RELATIVE)
    if contract.get("mode") != "VALIDATION_ONLY":
        raise KindCaptureError("KIND_CONTRACT_MODE_INVALID")
    permissions = contract.get("execution_permissions")
    if not isinstance(permissions, Mapping):
        raise KindCaptureError("KIND_PERMISSIONS_INVALID")
    if permissions.get("network_capture_allowed") is not True:
        raise KindCaptureError("KIND_NETWORK_CAPTURE_NOT_AUTHORIZED")
    for key, value in permissions.items():
        if key != "network_capture_allowed" and value is not False:
            raise KindCaptureError(f"KIND_OPERATION_PERMISSION_ENABLED:{key}")
    acquisition = contract.get("acquisition")
    if not isinstance(acquisition, Mapping):
        raise KindCaptureError("KIND_ACQUISITION_INVALID")
    if acquisition.get("redirects_allowed") is not False:
        raise KindCaptureError("KIND_REDIRECT_POLICY_INVALID")
    if acquisition.get("retries_allowed") is not False:
        raise KindCaptureError("KIND_RETRY_POLICY_INVALID")
    return contract


def frozen_stock_codes(root: Path, contract: Mapping[str, Any]) -> list[str]:
    source = contract["input"]
    path = root / source["timeline_report_path"]
    if _sha256_file(path) != source["timeline_report_sha256"]:
        raise KindCaptureError("TIMELINE_REPORT_HASH_MISMATCH")
    report = _load_json(path)
    codes = sorted(
        {
            str(row["stock_code"])
            for section in ("blind_gold", "unresolved_completion_projection")
            for row in report[section]["rows"]
        }
    )
    population = contract["population"]
    payload = ("\n".join(codes) + "\n").encode("ascii")
    if len(codes) != int(population["required_unique_stock_code_count"]):
        raise KindCaptureError("KIND_STOCK_CODE_COUNT_MISMATCH")
    if _sha256_bytes(payload) != population["stock_code_set_sha256"]:
        raise KindCaptureError("KIND_STOCK_CODE_SET_HASH_MISMATCH")
    suffix = str(population["common_stock_suffix"])
    if any(len(code) != 6 or not code.isdigit() or not code.endswith(suffix) for code in codes):
        raise KindCaptureError("KIND_STOCK_CODE_FORMAT_INVALID")
    return codes


def parse_issue_rows(payload: bytes) -> list[dict[str, Any]]:
    text = payload.decode("utf-8")
    parser = KindIssueTableParser()
    parser.feed(text)
    return parser.rows


def _capture_one(
    *, root: Path, contract: Mapping[str, Any], stock_code: str
) -> dict[str, Any]:
    acquisition = contract["acquisition"]
    values = {
        "method": "searchStockIssueList",
        "forward": "searchStockIssueList",
        "pageIndex": "1",
        "currentPageSize": str(acquisition["page_size"]),
        "marketType": "all",
        "comAbbrv": "",
        "listingType": "",
        "fromDate": acquisition["from_date"],
        "toDate": acquisition["to_date"],
        "orderMode": "1",
        "orderStat": "D",
        "searchCodeType": "char",
        "isurCd": stock_code[:5],
        "repIsuSrtCd": stock_code,
    }
    request = urllib.request.Request(
        acquisition["endpoint"],
        data=urllib.parse.urlencode(values).encode("ascii"),
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "RootA-C8-validation/1.0",
        },
        method="POST",
    )
    opener = urllib.request.build_opener(_NoRedirect())
    started = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        with opener.open(request, timeout=int(acquisition["timeout_seconds"])) as response:
            status = int(response.status)
            final_url = response.geturl()
            payload = response.read()
            server_date = response.headers.get("Date", "")
    except (urllib.error.URLError, TimeoutError, KindCaptureError) as exc:
        raise KindCaptureError(f"KIND_REQUEST_FAILED:{stock_code}:{type(exc).__name__}") from exc
    if status != 200:
        raise KindCaptureError(f"KIND_HTTP_STATUS_INVALID:{stock_code}:{status}")
    if final_url.rstrip("/") != str(acquisition["endpoint"]).rstrip("/"):
        raise KindCaptureError(f"KIND_FINAL_URL_INVALID:{stock_code}:{final_url}")
    rows = parse_issue_rows(payload)
    if len(rows) >= int(acquisition["page_size"]):
        raise KindCaptureError(f"KIND_RESPONSE_POSSIBLY_TRUNCATED:{stock_code}:{len(rows)}")
    raw_sha = _sha256_bytes(payload)
    raw_dir = root / acquisition["raw_directory"]
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"{stock_code}_{raw_sha[:16]}.html"
    if raw_path.exists() and _sha256_file(raw_path) != raw_sha:
        raise KindCaptureError(f"KIND_EXISTING_RAW_HASH_MISMATCH:{stock_code}")
    if not raw_path.exists():
        temporary = raw_path.with_suffix(".html.tmp")
        temporary.write_bytes(payload)
        temporary.replace(raw_path)
    return {
        "stock_code": stock_code,
        "issuer_code": stock_code[:5],
        "request_started_at": started,
        "response_received_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "http_status": status,
        "server_date_header": server_date,
        "raw_path": str(raw_path.relative_to(root)).replace("\\", "/"),
        "raw_sha256": raw_sha,
        "raw_size_bytes": len(payload),
        "row_count": len(rows),
        "merger_row_count": sum(
            str(contract["matcher"]["issue_reason_required_substring"])
            in row["issue_reason"]
            for row in rows
        ),
    }


def _require_pre_capture_feasibility(*, root: Path) -> dict[str, Any]:
    from tools import analyze_kospi_mcap_quarterly_c8_kind_stock_issue_shadow as analyzer

    report = analyzer.evaluate_pre_capture_feasibility(root=root)
    if report.get("status") != "PASS":
        raise KindFeasibilityBlocked(report)
    return report


def capture(*, root: Path = ROOT) -> dict[str, Any]:
    root = root.resolve()
    contract = load_contract(root)
    codes = frozen_stock_codes(root, contract)
    acquisition = contract["acquisition"]
    if len(codes) > int(acquisition["maximum_requests"]):
        raise KindCaptureError("KIND_REQUEST_BUDGET_EXCEEDED")
    feasibility = _require_pre_capture_feasibility(root=root)
    captured: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=int(acquisition["maximum_workers"])) as pool:
        futures = {
            pool.submit(_capture_one, root=root, contract=contract, stock_code=code): code
            for code in codes
        }
        for future in as_completed(futures):
            code = futures[future]
            try:
                captured.append(future.result())
            except Exception as exc:  # evidence must preserve every failed code
                failures.append({"stock_code": code, "error": str(exc)})
    captured.sort(key=lambda row: row["stock_code"])
    failures.sort(key=lambda row: row["stock_code"])
    status = "PASS" if len(captured) == len(codes) and not failures else "FAIL"
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    manifest = {
        "strategy_id": contract["strategy_id"],
        "scope": "C8_KIND_STOCK_ISSUE_OFFICIAL_CAPTURE",
        "status": status,
        "verdict": "KIND_STOCK_ISSUE_CAPTURE_COMPLETE" if status == "PASS" else "KIND_STOCK_ISSUE_CAPTURE_INCOMPLETE",
        "generated_at": generated_at,
        "contract_sha256": _sha256_file(root / CONTRACT_RELATIVE),
        "endpoint": acquisition["endpoint"],
        "from_date": acquisition["from_date"],
        "to_date": acquisition["to_date"],
        "requested_code_count": len(codes),
        "network_request_count": len(codes),
        "captured_code_count": len(captured),
        "failure_count": len(failures),
        "total_row_count": sum(row["row_count"] for row in captured),
        "merger_row_count": sum(row["merger_row_count"] for row in captured),
        "pre_capture_feasibility": feasibility,
        "captures": captured,
        "failures": failures,
        "operational_change": False,
        "adapter_changed": False,
        "capture_status_changed": False,
        "orders_allowed": False,
    }
    checkpoint_dir = root / Path(acquisition["latest_manifest_path"]).parent
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    versioned = checkpoint_dir / f"{acquisition['versioned_manifest_prefix']}{stamp}.json"
    latest = root / acquisition["latest_manifest_path"]
    encoded = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    versioned.write_bytes(encoded)
    latest.write_bytes(encoded)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture", action="store_true")
    args = parser.parse_args()
    if not args.capture:
        raise SystemExit("--capture is required for the official KIND network capture")
    try:
        manifest = capture()
    except KindFeasibilityBlocked as exc:
        print(json.dumps(exc.report, ensure_ascii=False, indent=2))
        return 3
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0 if manifest["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
