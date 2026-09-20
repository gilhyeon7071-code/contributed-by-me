from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ROOTA = ROOT.parent
LOGS_ROOTA = ROOTA / "2_Logs"
ROOTB_RUNS = Path("E:/vibe/buffett/runs")
TEMPLATE_PATH = ROOT / "99_Prompts" / "templates" / "daily_experiment_llm_wiki.md"
OUT_DIR = ROOT / "80_Daily_Review"
TZ = ZoneInfo("Asia/Seoul")


def newest_file(paths: list[Path]) -> Path | None:
    existing = [p for p in paths if p.exists()]
    if not existing:
        return None
    return max(existing, key=lambda p: p.stat().st_mtime)


def latest_by_glob(base: Path, pattern: str) -> Path | None:
    return newest_file(sorted(base.glob(pattern))) if base.exists() else None


def pick_roota_log() -> Path | None:
    prioritized = [
        latest_by_glob(LOGS_ROOTA, "freshness_source_*.json"),
        latest_by_glob(LOGS_ROOTA, "*_latest.json"),
        latest_by_glob(LOGS_ROOTA, "*.json"),
    ]
    for item in prioritized:
        if item is not None:
            return resolve_roota_log(item)
    return None


def pick_rootb_log() -> Path | None:
    preferred = ROOTB_RUNS / "observer_state_last.json"
    if preferred.exists():
        return preferred
    return latest_by_glob(ROOTB_RUNS, "*.json")


def resolve_cli_log(path_text: str, *, roota: bool) -> Path:
    path = Path(path_text.strip())
    if not path.exists():
        raise FileNotFoundError(f"log file not found: {path}")
    if roota:
        return resolve_roota_log(path)
    return path


def to_posix(path: Path) -> str:
    return path.as_posix()


def load_json(path: Path | None) -> dict:
    if path is None or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def resolve_roota_log(path: Path) -> Path:
    data = load_json(path)
    pointer = str(data.get("last") or "").strip()
    if pointer:
        resolved = Path(pointer)
        if resolved.exists():
            return resolved
    return path


def normalize_signal(item: object) -> str:
    if isinstance(item, str):
        return item.strip()
    if isinstance(item, dict):
        code = str(item.get("code") or "").strip()
        detail = str(item.get("detail") or "").strip()
        if code and detail:
            return f"{code}: {detail}"
        if code:
            return code
        if detail:
            return detail
    return str(item).strip()


def signal_priority(text: str) -> int:
    upper = text.upper()
    if "HARD_FAIL" in upper:
        return 0
    if "FAIL" in upper:
        return 1
    if "WARN" in upper:
        return 2
    return 3


def pick_failing_signal(roota_reasons: list, rootb_reasons: list) -> str:
    candidates: list[str] = []
    candidates.extend(f"[RootA] {normalize_signal(x)}" for x in roota_reasons if normalize_signal(x))
    candidates.extend(f"[RootB] {normalize_signal(x)}" for x in rootb_reasons if normalize_signal(x))
    if not candidates:
        return "오늘 기준 주요 FAIL 신호가 명시되지 않음(상세는 체크 항목 수동 확인)"
    return sorted(candidates, key=lambda x: (signal_priority(x), x))[0]


def map_regime(value: str) -> str:
    token = value.strip().upper()
    if token in {"NORMAL", "BULL", "BEAR", "RISK"}:
        return token
    return "UNKNOWN"


def derive_context(roota_log: Path | None, rootb_log: Path | None) -> tuple[str, bool, str]:
    roota = load_json(roota_log)
    rootb = load_json(rootb_log)
    state = rootb.get("state") if isinstance(rootb.get("state"), dict) else {}
    regime_raw = str(state.get("strategy_state_label") or "")
    regime = map_regime(regime_raw)

    roota_verdict = str(roota.get("verdict") or "").upper()
    rootb_status = str(rootb.get("status") or "").upper()
    account_label = str(state.get("account_state_label") or "").upper()
    exposure_policy = str(state.get("exposure_policy") or "").upper()
    reasons = []
    if roota_verdict in {"HARD_FAIL", "FAIL"}:
        reasons.append(f"roota_verdict={roota_verdict}")
    if rootb_status in {"FAIL", "BLOCK", "HARD_FAIL"}:
        reasons.append(f"rootb_status={rootb_status}")
    if "RISK_OFF" in account_label:
        reasons.append(f"account_state_label={account_label}")
    if exposure_policy in {"OFF", "NONE", "ZERO"}:
        reasons.append(f"exposure_policy={exposure_policy}")
    risk_off = len(reasons) > 0
    reason_text = "; ".join(reasons) if reasons else "no risk-off trigger"
    return regime, risk_off, reason_text


def build_action_tomorrow(
    roota_verdict: str,
    rootb_status: str,
    failing_signal: str,
    expected: str,
) -> str:
    signal = failing_signal.upper()
    if "ROOTA" in signal or "FRESHNESS" in signal or "CAND" in signal or "KRX" in signal or "PRICES" in signal:
        return (
            f"cand/krx_clean/prices 기준일을 D({expected})에 맞춰 갱신 후 "
            "freshness_source 재실행으로 HARD_FAIL/FAIL 해소 여부 검증"
        )
    if "ROOTB" in signal or rootb_status.upper() != "PASS":
        return "observer_state_last 기준 reasons 우선순위 원인 제거 후 상태 재검증"
    if roota_verdict.upper() in {"HARD_FAIL", "FAIL"}:
        return (
            f"RootA freshness 기준일을 D({expected})로 맞추고 "
            "freshness_source 재실행 결과 비교"
        )
    return "내일 동일 로그 소스 기준으로 D/STOP/6개 검증 항목을 우선 재확인"


def lag_for_signal(roota: dict, failing_signal: str) -> tuple[str, int | None]:
    signal = failing_signal.upper()
    if "KRX_CLEAN" in signal or "KRX" in signal:
        node = roota.get("krx_clean") if isinstance(roota.get("krx_clean"), dict) else {}
        lag = node.get("lag_days")
        return "krx_clean", lag if isinstance(lag, int) else None
    if "PRICES" in signal:
        node = roota.get("prices") if isinstance(roota.get("prices"), dict) else {}
        lag = node.get("lag_days")
        return "prices", lag if isinstance(lag, int) else None
    node = roota.get("cand") if isinstance(roota.get("cand"), dict) else {}
    lag = node.get("lag_days")
    return "cand", lag if isinstance(lag, int) else None


def auto_texts(roota_log: Path | None, rootb_log: Path | None) -> tuple[str, str, str]:
    roota = load_json(roota_log)
    rootb = load_json(rootb_log)

    roota_verdict = str(roota.get("verdict") or "NA")
    rootb_status = str(rootb.get("status") or "NA")
    roota_reasons = roota.get("reasons") if isinstance(roota.get("reasons"), list) else []
    rootb_reasons = rootb.get("reasons") if isinstance(rootb.get("reasons"), list) else []
    failing = pick_failing_signal(roota_reasons, rootb_reasons)
    lag_label, lag = lag_for_signal(roota, failing)
    expected = str(roota.get("expected_date") or "")

    if roota_verdict == "HARD_FAIL":
        lag_text = f" ({lag_label} lag={lag}일)" if isinstance(lag, int) else ""
        outcome = f"RootB 상태 {rootb_status} 유지, RootA 신선도는 {roota_verdict}{lag_text}"
        action = build_action_tomorrow(roota_verdict, rootb_status, failing, expected)
        return outcome, failing, action

    if rootb_status != "PASS":
        outcome = f"RootA 신선도 {roota_verdict}, RootB 상태 {rootb_status}"
        action = build_action_tomorrow(roota_verdict, rootb_status, failing, expected)
        return outcome, failing, action

    outcome = f"RootA 신선도 {roota_verdict}, RootB 상태 {rootb_status}로 기본 체인 이상 신호 없음"
    action = build_action_tomorrow(roota_verdict, rootb_status, failing, expected)
    return outcome, failing, action


def yaml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_frontmatter(
    today: datetime,
    d_value: str,
    regime: str,
    risk_off: bool,
    fact_text: str,
    roota_log: Path | None,
    rootb_log: Path | None,
    outcome_one_line: str,
    failing_signal: str,
    action_tomorrow: str,
) -> str:
    logs = []
    if roota_log is not None:
        logs.append(to_posix(roota_log))
    if rootb_log is not None:
        logs.append(to_posix(rootb_log))

    log_lines = "\n".join(f"    - {item}" for item in logs) if logs else "    - E:/1_Data/2_Logs/..."

    return (
        "---\n"
        f"date: {today.strftime('%Y-%m-%d')}\n"
        "project: trading-lab\n"
        "context:\n"
        f"  D: {d_value}\n"
        f"  regime: {regime}\n"
        f"  risk_off: {'true' if risk_off else 'false'}\n"
        "\n"
        f"outcome_one_line: \"{yaml_escape(outcome_one_line)}\"\n"
        f"failing_signal: \"{yaml_escape(failing_signal)}\"\n"
        f"action_tomorrow: \"{yaml_escape(action_tomorrow)}\"\n"
        "\n"
        "evidence:\n"
        "  logs:\n"
        f"{log_lines}\n"
        "  outputs: []\n"
        "\n"
        "scope_verdict: PASS   # PASS / FAIL / NA\n"
        "ops_verdict: NA       # PASS / FAIL / NA\n"
        "\n"
        "stop_conditions:\n"
        "  orders_exec_present: NA\n"
        "  exec_date_matches_D: NA\n"
        "  asof_runid_match: NA\n"
        "  paper_broker_date_mixed: NA\n"
        "\n"
        "checks:\n"
        "  functional: NA\n"
        "  consistency: NA\n"
        "  ops_reflect: NA\n"
        "  policy: NA\n"
        "  fail_closed: NA\n"
        "  regression: NA\n"
        "\n"
        f"fact: \"{yaml_escape(fact_text)}\"\n"
        "interpretation: \"\"\n"
        "\n"
        "tested:\n"
        "  - \"\"\n"
        "not_tested:\n"
        "  - \"\"\n"
        "\n"
        "tags: [daily, experiment, trading, obsidian, llm-wiki]\n"
        "---\n"
        "\n"
        "# Daily Experiment\n"
        "\n"
        "## Outcome (One Line)\n"
        "\n"
        "## Failing Signal\n"
        "\n"
        "## Action Tomorrow\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a daily experiment note with auto-filled evidence logs.")
    parser.add_argument("--d", dest="d_value", default="", help="D value in YYYYMMDD. Default: today's date in Asia/Seoul.")
    parser.add_argument("--output", default="", help="Output markdown path. Default: 80_Daily_Review/<date>_daily_experiment.md")
    parser.add_argument("--freeze-log", default="", help="Use a fixed RootA log path instead of auto-selecting latest.")
    parser.add_argument("--freeze-rootb-log", default="", help="Use a fixed RootB log path instead of auto-selecting latest.")
    args = parser.parse_args()

    now = datetime.now(TZ)
    d_value = args.d_value.strip() or now.strftime("%Y%m%d")

    output = Path(args.output) if args.output else OUT_DIR / f"{now.strftime('%Y-%m-%d')}_daily_experiment.md"
    output.parent.mkdir(parents=True, exist_ok=True)

    try:
        roota_log = resolve_cli_log(args.freeze_log, roota=True) if args.freeze_log.strip() else pick_roota_log()
        rootb_log = resolve_cli_log(args.freeze_rootb_log, roota=False) if args.freeze_rootb_log.strip() else pick_rootb_log()
    except FileNotFoundError as exc:
        print(f"ERROR={exc}")
        return 2
    regime, risk_off, risk_reason = derive_context(roota_log, rootb_log)
    outcome, failing, action = auto_texts(roota_log, rootb_log)
    roota = load_json(roota_log)
    rootb = load_json(rootb_log)
    cand = roota.get("cand") if isinstance(roota.get("cand"), dict) else {}
    krx_clean = roota.get("krx_clean") if isinstance(roota.get("krx_clean"), dict) else {}
    prices = roota.get("prices") if isinstance(roota.get("prices"), dict) else {}
    expected = str(roota.get("expected_date") or "")
    max_date = str(cand.get("max_date") or "")
    lag_days = cand.get("lag_days")
    krx_lag_days = krx_clean.get("lag_days")
    prices_lag_days = prices.get("lag_days")
    rootb_d = str(rootb.get("D") or "")
    d_mismatch = "NA"
    if expected and rootb_d:
        d_mismatch = "true" if expected != rootb_d else "false"
    lag_text = str(lag_days) if isinstance(lag_days, int) else "NA"
    krx_lag_text = str(krx_lag_days) if isinstance(krx_lag_days, int) else "NA"
    prices_lag_text = str(prices_lag_days) if isinstance(prices_lag_days, int) else "NA"
    fact_text = (
        f"context auto: regime={regime}, risk_off={'true' if risk_off else 'false'}, reason={risk_reason}; "
        f"expected_date={expected or 'NA'}, cand_max_date={max_date or 'NA'}, cand_lag_days={lag_text}, "
        f"krx_clean_lag_days={krx_lag_text}, prices_lag_days={prices_lag_text}, "
        f"rootb_D={rootb_d or 'NA'}, D_mismatch={d_mismatch}"
    )

    text = build_frontmatter(now, d_value, regime, risk_off, fact_text, roota_log, rootb_log, outcome, failing, action)
    output.write_text(text, encoding="utf-8")

    print(f"CREATED={output}")
    print(f"D={d_value}")
    print(f"LOG_ROOTA={to_posix(roota_log) if roota_log else 'NA'}")
    print(f"LOG_ROOTB={to_posix(rootb_log) if rootb_log else 'NA'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
