# -*- coding: utf-8 -*-
"""철회 색인 — **인용하기 전에 여기부터 본다.**

## 왜 필요한가 (2026-09-10)

철회가 세 곳에 흩어져 있었다.
```
CONCLUSION_REGISTER.md §5   전용 표. 그런데 2026-09-10 것은 **2건뿐**이었다
PLANS 항목 안               정정·철회 표지가 든 항목이 그날만 **18개**
BROKEN_WINDOW_REGISTER.md   C6·C7 에 [정정] 블록
```
결론 등록부는 "인용 전 필수 대조" 문서인데 그날 철회 대부분이 거기 없었다.
누가 PLANS (321) 의 "14파일 전부 고쳤다" 를 인용하면 **경고를 못 받는다.**

그리고 방식이 일관되지 않았다. 대부분은 원문 뒤에 `[정정]` 을 덧붙였는데
(318) 만 항목을 통째로 다시 써서 **원문 그대로는 남지 않았다.**

## 이 도구가 하는 일

```
--add     철회 하나를 append-only 색인에 등록한다
--audit   PLANS 에서 정정·철회 표지가 든 항목을 훑어
          **색인에 없는 것**을 보여준다 (빠뜨림을 드러낸다)
--list    등록된 것을 성격별로 본다
```

## 성격을 갈라 적는다

```
research_conclusion  연구 결론.  **CONCLUSION_REGISTER 에도 넣어야 한다**
operational_fact     운영 사실 오판 (예: "그 런처가 띄웠다")
process_error        과정 오류 (예: "전부 고쳤다", 검산기 정규식)
```
이 구분이 없으면 등록부에 무엇이 들어가야 하는지 규칙이 없어진다.

## 산출물

`2_Logs/retraction_index.jsonl` — append-only, **이름에 날짜 없음**
(`tools/log_cleanup_30d.py` 대상이 아니다)

[2026-09-10] 신설. PLANS (339)
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import re
from pathlib import Path

# 콘솔이 cp949 라 em-dash 같은 글자에서 죽는다. 죽지 말고 치환한다.
try:
    import sys as _sys
    _sys.stdout.reconfigure(errors="replace")
    _sys.stderr.reconfigure(errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "2_Logs" / "retraction_index.jsonl"
PLANS = ROOT / ".agent" / "PLANS.md"

KINDS = ("research_conclusion", "operational_fact", "process_error")

# PLANS 항목 안에서 "여기 철회가 있다" 를 가리키는 표지
MARKERS = re.compile(
    r"\[정정\]|\[철회\]|철회한다|철회합니다|철회해야|철회했다|철회됐|"
    r"틀렸다|틀렸어요|과했다|오판|내 오류"
)
HEAD = re.compile(r"^## (\d{4}-\d{2}-\d{2}) \((\d+)\)")


def _load() -> list[dict]:
    if not INDEX.exists():
        return []
    out = []
    for line in io.open(INDEX, encoding="utf-8"):
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except Exception:
                pass
    return out


def _plans_entries_with_markers() -> list[tuple[str, str, str]]:
    """(날짜, 항목번호, 제목) 중 정정·철회 표지가 든 것."""
    if not PLANS.exists():
        return []
    cur = None
    body: list[str] = []
    found = []
    for line in io.open(PLANS, encoding="utf-8"):
        m = HEAD.match(line)
        if m:
            if cur and MARKERS.search("".join(body)):
                found.append(cur)
            cur = (m.group(1), m.group(2), line.strip()[:110])
            body = []
        else:
            body.append(line)
    if cur and MARKERS.search("".join(body)):
        found.append(cur)
    return found


def cmd_add(a) -> int:
    if a.kind not in KINDS:
        print("[ERR] kind 는 %s 중 하나여야 해요" % ", ".join(KINDS))
        return 2
    rec = {
        "ts": dt.datetime.now().isoformat(timespec="seconds"),
        "when": a.when or dt.date.today().strftime("%Y-%m-%d"),
        "kind": a.kind,
        "claim": a.claim,
        "why_wrong": a.why,
        "replaced_by": a.replaced_by,
        "source": a.source,
        "plans": a.plans,
        "also_in_conclusion_register": bool(a.in_register),
    }
    with io.open(INDEX, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + chr(10))
    print("[RETRACTION] 등록: %s" % (a.claim[:70]))
    if a.kind == "research_conclusion" and not a.in_register:
        print("  ** 연구 결론이에요. CONCLUSION_REGISTER.md §5 에도 넣으세요. **")
    return 0


def cmd_audit(a) -> int:
    idx = _load()
    registered = set()
    for r in idx:
        for tok in re.findall(r"\((\d+)\)", str(r.get("plans") or "")):
            registered.add(tok)

    entries = _plans_entries_with_markers()
    if a.since:
        entries = [e for e in entries if e[0] >= a.since]
    missing = [e for e in entries if e[1] not in registered]

    scope = ("%s 이후" % a.since) if a.since else "전체 이력"
    print("범위: %s" % scope)
    print("PLANS 에서 정정·철회 표지가 든 항목: %d" % len(entries))
    print("색인에 등록된 항목 번호: %d" % len(registered))
    print("**색인에 없는 항목: %d**" % len(missing))
    print()
    if missing:
        for d, n, t in missing[-a.limit:]:
            print("  (%s) %s" % (n, t))
        print()
        print("  표지가 있다고 반드시 철회는 아니에요 - 사람이 가려야 해요.")
        print("  진짜 철회면 --add 로 등록하세요.")
    else:
        print("  빠뜨린 것이 없어요.")
    return 0


def cmd_list(a) -> int:
    idx = _load()
    if not idx:
        print("색인이 비어 있어요.")
        return 0
    by = {}
    for r in idx:
        by.setdefault(r.get("kind", "?"), []).append(r)
    print("총 %d건" % len(idx))
    for k in KINDS:
        rows = by.get(k, [])
        if not rows:
            continue
        print()
        print("== %s (%d) ==" % (k, len(rows)))
        for r in rows:
            flag = "" if (k != "research_conclusion" or r.get("also_in_conclusion_register")) \
                else "   << 등록부 미등록"
            print("  [%s] %s%s" % (r.get("when"), str(r.get("claim"))[:78], flag))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="철회 색인 (append-only)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("add", help="철회 하나 등록")
    p1.add_argument("--claim", required=True, help="철회된 주장 (원문에 가깝게)")
    p1.add_argument("--why", required=True, help="왜 틀렸나")
    p1.add_argument("--replaced-by", default="", help="대체 사실 / 현재 상태")
    p1.add_argument("--kind", required=True, choices=KINDS)
    p1.add_argument("--source", default="", help="원 주장이 있던 곳")
    p1.add_argument("--plans", default="", help="관련 PLANS 항목 예: (318)(328)")
    p1.add_argument("--when", default="", help="철회 시점 YYYY-MM-DD")
    p1.add_argument("--in-register", action="store_true",
                    help="CONCLUSION_REGISTER 에도 넣었으면 지정")
    p1.set_defaults(func=cmd_add)

    p2 = sub.add_parser("audit", help="PLANS 표지 대비 색인 누락 확인")
    p2.add_argument("--limit", type=int, default=25)
    p2.add_argument("--since", default="", help="YYYY-MM-DD 이후만 (기본: 전체)")
    p2.set_defaults(func=cmd_audit)

    p3 = sub.add_parser("list", help="등록된 철회 보기")
    p3.set_defaults(func=cmd_list)

    a = ap.parse_args()
    return a.func(a)


if __name__ == "__main__":
    raise SystemExit(main())
