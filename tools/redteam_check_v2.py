from __future__ import annotations

import json, subprocess, sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
LOGS.mkdir(parents=True, exist_ok=True)

REDTEAM_V1 = ROOT / "tools" / "redteam_check_v1.py"
FRESHNESS = ROOT / "tools" / "freshness_check_v1.py"


def _latest(pattern: str) -> Path | None:
    hits = list(LOGS.glob(pattern))
    if not hits:
        return None
    hits.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return hits[0]


def _run_py(py: str, script: Path) -> int:
    if not script.exists():
        print(f"[REDTEAM_V2] missing: {script}")
        return 2
    p = subprocess.run([py, str(script)], capture_output=True, text=True)
    if p.stdout:
        print(p.stdout.rstrip())
    if p.stderr:
        print(p.stderr.rstrip(), file=sys.stderr)
    return p.returncode


def _norm_verdict(v: object) -> str:
    return (str(v) if v is not None else "").strip().upper()


def main() -> int:
    py = sys.executable

    rc1 = _run_py(py, REDTEAM_V1)
    v1_json = _latest("redteam_check_[0-9]*.json")

    rc2 = _run_py(py, FRESHNESS)
    fr_json = _latest("freshness_source_[0-9]*.json")

    verdict = "PASS"
    hard_fails = 0
    warns = 0
    reasons: list[str] = []

    if rc1 != 0:
        verdict = "HARD_FAIL"
        hard_fails += 1
        reasons.append(f"redteam_v1 exit={rc1}")

    if v1_json and v1_json.exists():
        try:
            v1 = json.loads(v1_json.read_text(encoding="utf-8"))
            hard_fails += int(v1.get("hard_fail", 0) or 0)
            warns += int(v1.get("warn", 0) or 0)
            v1_verdict = _norm_verdict(v1.get("verdict"))
            if v1_verdict and v1_verdict != "PASS":
                verdict = "HARD_FAIL"
                if int(v1.get("hard_fail", 0) or 0) == 0:
                    hard_fails += 1
                reasons.append(f"redteam_v1 verdict={v1_verdict}")
        except Exception:
            pass

    if rc2 != 0:
        verdict = "HARD_FAIL"
        hard_fails += 1
        reasons.append(f"freshness exit={rc2}")

    freshness_obj = None
    if fr_json and fr_json.exists():
        try:
            freshness_obj = json.loads(fr_json.read_text(encoding="utf-8"))
            fr_v = _norm_verdict(freshness_obj.get("verdict"))
            fr_reasons = (freshness_obj.get("reasons") or [])
            if fr_v and fr_v != "PASS":
                verdict = "HARD_FAIL"
                if rc2 == 0:
                    hard_fails += 1
                if fr_reasons:
                    reasons += fr_reasons[:10]
                else:
                    reasons.append(f"freshness verdict={fr_v}")
        except Exception:
            reasons.append("freshness json parse error")
            verdict = "HARD_FAIL"
            hard_fails += 1

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = {
        "run_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "verdict": verdict,
        "hard_fail": hard_fails,
        "warn": warns,
        "reasons": reasons[:20],
        "redteam_v1_json": str(v1_json) if v1_json else None,
        "freshness_json": str(fr_json) if fr_json else None,
        "freshness": freshness_obj,
    }

    out_path = LOGS / f"redteam_check_v2_{ts}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    (LOGS / "redteam_check_last.json").write_text(
        json.dumps({"last": str(out_path), "ts": ts}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[REDTEAM_V2] wrote: {out_path}")
    verdict = "PASS" if hard_fails == 0 else "HARD_FAIL"

    print(f"[REDTEAM_V2] verdict={verdict} HARD_FAIL={hard_fails} WARN={warns}")
    if hard_fails > 0 or verdict != "PASS":
        print("[REDTEAM_V2] HARD_FAIL reasons:")
        for r in reasons[:10]:
            print(f"- {r}")

    return 0 if hard_fails == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())


