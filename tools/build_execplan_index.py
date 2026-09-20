"""ExecPlan 상태 인덱스 생성기.

왜 필요한가 (2026-08-24):
    `docs/exec-plans/active/` 가 76건인데 04월 이후 `completed/` 로 옮겨진 것이 0건이다.
    상태줄이 있는 것은 15건뿐이고 그중에도 실제와 어긋나는 것이 있다 -
    `20260820_config_lock_enforcement.md` 는 "승인 전"이라 적혀 있는데
    바로 그 잠금이 08-23~24 배치를 이틀 죽였다.
    => **active/ 를 미완 목록으로 쓸 수 없다.**

이 도구는 파일을 옮기지 않는다. 옮기면 PLANS 의 경로 참조가 깨지고,
한 번 옮긴 결과는 다시 낡는다. 대신 **다시 돌리면 갱신되는 인덱스**를 만든다.

판정 규칙 (근거가 강한 순):
    적용완료      상태줄에 "적용 완료 / 적용됨 / 반영 완료"
    철회          상태줄에 "철회 / 무효 / 폐기"
    초안(승인 전)  상태줄에 "초안 / 승인 전"          <- 이것이 진짜 대기 목록이다
    PLANS 참조됨  파일 stem 이 PLANS 본문에 그대로 있음
    PLANS 유사언급 제목 키워드 다수가 PLANS 본문에 있음 (약한 증거)
    미판정        위 어느 것도 아님

**"PLANS 유사언급"은 적용됐다는 뜻이 아니다.** 논의 흔적이 있다는 뜻일 뿐이며,
적용 여부를 알려면 코드를 봐야 한다. 인덱스는 그 구분을 흐리지 않는다.

사용: python tools/build_execplan_index.py
"""
from __future__ import annotations

import collections
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ACTIVE = ROOT / "docs" / "exec-plans" / "active"
COMPLETED = ROOT / "docs" / "exec-plans" / "completed"
OUT = ROOT / "docs" / "exec-plans" / "INDEX.md"
PLANS_A = ROOT / ".agent" / "PLANS.md"
PLANS_B = Path(r"E:\vibe\buffett\PLANS.md")

STOP_WORDS = {"execplan", "exec", "plan", "수정", "적용", "변경", "추가"}


def _read(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def classify(path: Path, plans: str, plans_lower: str) -> dict:
    text = _read(path)
    title_m = re.search(r"^#\s+(.+)$", text, re.M)
    title = title_m.group(1).strip() if title_m else ""
    status_m = re.search(
        r"(?:^|\n)\s*[-*]?\s*\**(?:상태|status)\**\s*[:：]\s*(.{0,80})", text[:2500], re.I
    )
    status = re.sub(r"\*", "", status_m.group(1)).strip() if status_m else ""
    ymd_m = re.match(r"(\d{8})", path.name)
    ymd = ymd_m.group(1) if ymd_m else ""

    strict = path.stem in plans
    kws = [
        w for w in re.findall(r"[A-Za-z_]{5,}|[가-힣]{3,}", title)
        if w.lower() not in STOP_WORDS
    ][:4]
    loose = bool(kws) and sum(1 for w in kws if w.lower() in plans_lower) >= max(2, len(kws) - 1)

    if re.search(r"적용\s*완료|적용됨|반영\s*완료", status):
        verdict = "적용완료"
    elif re.search(r"철회|무효|폐기", status):
        verdict = "철회"
    elif re.search(r"초안|승인\s*전", status):
        verdict = "초안(승인 전)"
    elif strict:
        verdict = "PLANS 참조됨"
    elif loose:
        verdict = "PLANS 유사언급"
    else:
        verdict = "미판정"
    return {"file": path.name, "ymd": ymd, "title": title, "status": status, "verdict": verdict}


def main() -> int:
    plans = _read(PLANS_A) + _read(PLANS_B)
    plans_lower = plans.lower()
    rows = [
        classify(p, plans, plans_lower)
        for p in sorted(ACTIVE.glob("*.md"))
        if p.name != "README.md"
    ]
    counts = collections.Counter(r["verdict"] for r in rows)
    by_month = collections.Counter(r["ymd"][:6] for r in rows)
    n_completed = len(list(COMPLETED.glob("*.md"))) if COMPLETED.exists() else 0

    order = ["초안(승인 전)", "적용완료", "철회", "PLANS 참조됨", "PLANS 유사언급", "미판정"]
    lines = []
    lines.append("# ExecPlan 상태 인덱스")
    lines.append("")
    lines.append(f"- 생성: {datetime.now().strftime('%Y-%m-%d %H:%M')} · `tools/build_execplan_index.py`")
    lines.append(f"- active **{len(rows)}건** / completed **{n_completed}건**")
    lines.append("- 파일은 옮기지 않는다. 옮기면 PLANS 의 경로 참조가 깨지고, 한 번 옮긴 결과는 다시 낡는다")
    lines.append("")
    lines.append("## 판정 분포")
    lines.append("")
    lines.append("| 판정 | 건수 | 뜻 |")
    lines.append("|---|---:|---|")
    meaning = {
        "초안(승인 전)": "**진짜 대기 목록.** 여기부터 본다",
        "적용완료": "상태줄에 적용 완료가 적혀 있다",
        "철회": "상태줄에 철회/무효가 적혀 있다",
        "PLANS 참조됨": "파일명이 PLANS 본문에 있다",
        "PLANS 유사언급": "제목 키워드가 PLANS 에 있다. **적용됐다는 뜻이 아니다**",
        "미판정": "파일에도 PLANS 에도 근거가 없다",
    }
    for k in order:
        if counts.get(k):
            lines.append(f"| {k} | {counts[k]} | {meaning[k]} |")
    lines.append("")
    lines.append(f"월별 분포: {dict(sorted(by_month.items()))}")
    lines.append("")
    lines.append("> **`PLANS 유사언급`을 완료로 읽지 말 것.** 논의 흔적이 있다는 뜻이고,")
    lines.append("> 적용 여부는 코드를 봐야 안다. 이 인덱스는 그 구분을 흐리지 않는다.")
    lines.append("")
    for k in order:
        sel = [r for r in rows if r["verdict"] == k]
        if not sel:
            continue
        lines.append(f"## {k} — {len(sel)}건")
        lines.append("")
        lines.append("| 날짜 | 파일 | 상태줄 |")
        lines.append("|---|---|---|")
        for r in sorted(sel, key=lambda x: x["ymd"]):
            st = r["status"].replace("|", "/") if r["status"] else "—"
            lines.append(f"| {r['ymd']} | `{r['file']}` | {st} |")
        lines.append("")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[INDEX] {OUT}")
    for k in order:
        if counts.get(k):
            print(f"   {k:<16} {counts[k]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
