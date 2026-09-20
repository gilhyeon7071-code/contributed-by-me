# -*- coding: utf-8 -*-
"""HPO 자동 발동 원장을 **다중검정 원장에 반영**한다.

## 왜 필요한가

`optimize_if_due_ledger.jsonl` 에 발동을 남기는 것(2026-09-10 신설)만으로는 부족하다.
그 숫자가 `2_Logs/research_trial_ledger.json` 에 들어가야 DSR 의 `n_trials` 가 맞는다.

`project_1data_multiple_testing_correction_broken`: `n_trials` 가 **4** 였을 때
DSR 이 `0.5928 PASS` 였고, 실제 시도 수를 넣으니 `0.0055 FAIL` 로 뒤집혔다.
**세는 것과 쓰는 것은 다른 일이다.** 이 도구가 그 사이를 잇는다.

## 무엇을 시도로 세나

발동 1회 = HPO 라운드 1회다. 파라미터 격자를 몇 개 훑었는지는 그 라운드의 설정에 달렸고
여기서는 알 수 없으므로 **보수적으로 라운드당 1건**으로 센다.
실제 시도는 더 많았을 수 있다 - 원장의 `_rule` 이 말하는 "기록으로 방어 가능한 하한" 이다.

## 멱등이다

이미 반영한 발동은 `ref` 로 식별해 다시 더하지 않는다.
`--apply` 없이는 무엇을 더할지만 출력한다.

[2026-09-10] 신설.
"""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LED = ROOT / "2_Logs" / "optimize_if_due_ledger.jsonl"
TRIAL = ROOT / "2_Logs" / "research_trial_ledger.json"
REF_PREFIX = "optimize_if_due:"


def main() -> int:
    ap = argparse.ArgumentParser(description="HPO 발동 -> 다중검정 원장 반영")
    ap.add_argument("--apply", action="store_true", help="없으면 무엇을 더할지만 출력해요")
    ap.add_argument("--ledger", default=str(LED))
    ap.add_argument("--trial", default=str(TRIAL))
    a = ap.parse_args()

    led = Path(a.ledger)
    if not led.exists():
        print("[INFO] 발동 원장이 아직 없어요: %s" % led)
        print("       optimize_if_due 가 한 번도 안 돌았거나 신설(2026-09-10) 이전이에요.")
        return 0

    firings = []
    for line in io.open(led, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            firings.append(json.loads(line))
        except Exception:
            continue
    print("발동 원장 %d건" % len(firings))

    trial = json.load(io.open(a.trial, encoding="utf-8"))
    done = {e.get("ref") for e in trial.get("entries", []) if str(e.get("ref", "")).startswith(REF_PREFIX)}
    print("이미 반영된 발동 %d건" % len(done))

    new = []
    for f in firings:
        ref = REF_PREFIX + str(f.get("ts") or f.get("as_of") or "")
        if ref in done:
            continue
        new.append((ref, f))

    if not new:
        print("추가할 것이 없어요. (멱등)")
        return 0

    print()
    print("추가 대상 %d건:" % len(new))
    for ref, f in new:
        print("  %s  decision=%s status=%s reason=%s"
              % (f.get("ts"), f.get("decision"), f.get("status"), str(f.get("reason"))[:60]))

    before = sum(e["n"] for e in trial["entries"])
    if not a.apply:
        print()
        print("  누적 시도 %d -> %d 이 될 거예요. **--apply 를 주면 반영해요.**"
              % (before, before + len(new)))
        return 0

    for ref, f in new:
        trial["entries"].append({
            "n": 1,
            "when": str(f.get("ts") or "")[:10].replace("-", "-"),
            "source": "HPO 자동 발동 1회 (optimize_if_due). status=%s" % f.get("status"),
            "ref": ref,
        })
    trial["updated"] = max(str(f.get("ts") or "")[:10] for _, f in new)
    io.open(a.trial, "w", encoding="utf-8", newline="").write(
        json.dumps(trial, ensure_ascii=False, indent=2))
    after = sum(e["n"] for e in trial["entries"])
    print()
    print("  반영 완료. 누적 시도 **%d -> %d**" % (before, after))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
