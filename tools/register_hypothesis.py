# -*- coding: utf-8 -*-
"""가설의 출처를 **재기 전에** 기록한다.

## 왜 만드나

`project_1data_methodology_diagnosis` 의 5번 결함이 이것이다.

> **Layer 0 이 없다.** V1/V2/V3 셋 다 "가설이 있다고 치고 엄밀히 검증한다" 로 시작한다.
> **왜 이 전략인지를 기록하는 곳이 없다. 그래서 틀린 `rs` 정의가 살아남았다.**

`rs` 가 수익률의 **비**로 정의돼 하락장에서 부호가 뒤집히는 것을 아무도 못 잡은 이유는,
"이 축이 무엇을 주장하는가" 를 문장으로 적어둔 곳이 없었기 때문이다.
`rs = 이 종목이 시장보다 강하다` 라고 한 줄만 적혀 있었어도 비율 정의의 부호 문제가 보인다.

## 무겁게 만들지 않는다

V3(`STRATEGY_VALIDATION_METHOD_V3_DRAFT.md`)는 feature_dictionary_id, SHA-256 동결,
parameter_registry, 관찰 원장, CPCV/PBO/DSR 를 요구했고 **한 달간 0라운드**였다.
혼자 실행할 수 없는 규격은 규격이 아니다.

여기서 요구하는 것은 **여섯 칸**이고, 그중 둘은 비워두면 등록이 거부된다.

    python tools/register_hypothesis.py --new "외국인 순매수 상위가 10일 뒤 더 오른다"
    python tools/register_hypothesis.py --list

## 규칙

- **측정 전에 쓴다.** 작성 시각이 파일에 박힌다. 측정 후 작성은 그 시각으로 드러난다
- 작성 후 **본문을 고치지 않는다.** 생각이 바뀌면 새 가설을 등록한다(그것도 시도 1건이다)
- 등록 자체가 **다중검정 시도 1건**이다. 재보지 않고 버린 가설도 남긴다 -
  버린 것을 안 세면 남은 것의 유의성이 부풀려진다

[2026-09-09] 신설. 사용자 지시.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "docs" / "research" / "hypotheses"
INDEX = DIR / "INDEX.md"

TEMPLATE = """# {hid} — {title}

- 등록 시각: **{ts}**  (이 시각 이후의 측정만 사전등록으로 인정돼요)
- 상태: `DRAFT`  (측정 시작 시 `MEASURING`, 끝나면 `DONE` / `DROPPED`)

> **작성 후 본문을 고치지 마세요.** 생각이 바뀌면 새 가설을 등록해요 — 그것도 시도 1건이에요.

---

## 1. 어디서 왔나 (필수)

<!-- 관찰 / 문헌 / 기존결과에서 파생 / 우연 중 무엇이고, **구체적으로 무엇을 보고** 떠올렸는지.
     "그냥 해봤다" 도 정직한 답이에요. 그렇게 적으면 나중에 그 사실을 알 수 있어요. -->

출처 종류:
근거:

## 2. 기계적 주장 (필수)

<!-- 부호까지 포함해 한 문장으로. 이 칸이 `rs` 사건을 막는 자리예요.
     예: "외국인 순매수 **금액이 큰** 종목이 10거래일 뒤 유니버스보다 **더 오른다**"
     정의도 적어요 - 무엇을 어떻게 계산하는지. "비"인지 "차"인지가 부호를 바꿔요. -->

주장:
축의 정의:
지평(h):

## 3. 왜 그래야 하나 (기전)

<!-- 시장에서 무슨 일이 일어나서 그 결과가 나오는지. 기전이 없으면 우연과 구별되지 않아요. -->

## 4. 기존 축과의 관계

<!-- 이미 있는 축과 겹치나요? 겹치면 새 시도가 아니라 재측정이에요.
     상관을 재봤다면 값을, 안 재봤으면 "미확인" 이라고 적어요. -->

## 5. 틀렸다면 어떻게 아나 (반증 조건)

<!-- 어떤 결과가 나오면 이 가설을 버리는지. 미리 적지 않으면 나중에 해석이 늘어나요. -->

## 6. 잴 수 있나 (검정력)

<!-- **이 칸이 비면 측정하지 마세요.** 결함 4번(검정력 부재)이 여기서 막혀요.
     필요 표본 = (2.8 x sd / 목표알파)^2 블록.  겹침 보정: 블록 1개 = h 거래일.
     sd 를 모르면 "소표본으로 sd 부터 추정" 이라고 적고 그 결과를 여기에 채워요. -->

목표 알파:
관측 sd:
필요 블록:
필요 거래일:
현재 보유:
-> 잴 수 있나:

---

## 결과 (측정 후에만 채워요)

<!-- 측정 전에는 비워둬요. 여기가 채워졌는데 등록 시각보다 이른 측정이면 사전등록이 아니에요. -->

측정 시각:
결과:
판정:
"""


def _slug(s: str) -> str:
    s = re.sub(r"[^\w가-힣]+", "_", s.strip())
    return re.sub(r"_+", "_", s).strip("_")[:40]


def _next_id() -> str:
    DIR.mkdir(parents=True, exist_ok=True)
    n = 0
    for p in DIR.glob("H*.md"):
        m = re.match(r"H(\d{3})", p.name)
        if m:
            n = max(n, int(m.group(1)))
    return "H%03d" % (n + 1)


def cmd_new(title: str) -> int:
    hid = _next_id()
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path = DIR / ("%s_%s.md" % (hid, _slug(title)))
    if path.exists():
        print("[STOP] 이미 있어요: %s" % path)
        return 2
    path.write_text(TEMPLATE.format(hid=hid, title=title, ts=ts), encoding="utf-8")

    if not INDEX.exists():
        INDEX.write_text(
            "# 가설 등록부\n\n"
            "**재기 전에** 등록해요. 등록 자체가 다중검정 시도 1건이에요 - "
            "재보지 않고 버린 가설도 남겨요. 버린 것을 안 세면 남은 것의 유의성이 부풀려져요.\n\n"
            "규격과 도구: `tools/register_hypothesis.py`\n\n"
            "| ID | 등록 시각 | 제목 | 상태 |\n|---|---|---|---|\n",
            encoding="utf-8",
        )
    with INDEX.open("a", encoding="utf-8") as f:
        f.write("| %s | %s | %s | DRAFT |\n" % (hid, ts, title))

    print("[NEW] %s" % path)
    print("      등록 시각 %s" % ts)
    print()
    print("  1·2·6 번 칸을 채우세요. **6번(검정력)이 비면 측정하지 마세요.**")
    print("  다 쓰면:  python tools/register_hypothesis.py --check %s" % hid)
    return 0


REQUIRED = {
    "1. 어디서 왔나": ["출처 종류:", "근거:"],
    "2. 기계적 주장": ["주장:", "축의 정의:", "지평(h):"],
    "6. 잴 수 있나": ["목표 알파:", "필요 거래일:", "-> 잴 수 있나:"],
}


def cmd_check(hid: str) -> int:
    hits = sorted(DIR.glob("%s_*.md" % hid))
    if not hits:
        print("[STOP] 못 찾았어요: %s" % hid)
        return 2
    p = hits[0]
    text = io.open(p, encoding="utf-8").read()
    missing = []
    for sec, keys in REQUIRED.items():
        for k in keys:
            m = re.search(re.escape(k) + r"[ \t]*(.*)", text)
            if not m or not m.group(1).strip():
                missing.append("%s / %s" % (sec, k))
    print("[CHECK] %s" % p.name)
    if missing:
        print("  **비어 있어요 - 측정을 시작하면 안 돼요**")
        for x in missing:
            print("    - %s" % x)
        return 3
    print("  필수 칸이 모두 찼어요. 측정을 시작해도 돼요.")
    print("  상태를 MEASURING 으로 바꾸고, 결과는 **측정 후에만** 채우세요.")
    return 0


def cmd_list() -> int:
    if not INDEX.exists():
        print("등록된 가설이 없어요.")
        return 0
    print(io.open(INDEX, encoding="utf-8").read())
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="가설 출처 등록기 (Layer 0)")
    ap.add_argument("--new", metavar="제목", help="새 가설을 등록해요")
    ap.add_argument("--check", metavar="ID", help="필수 칸이 찼는지 봐요 (예: H001)")
    ap.add_argument("--list", action="store_true", help="등록부를 봐요")
    a = ap.parse_args()
    if a.new:
        return cmd_new(a.new)
    if a.check:
        return cmd_check(a.check)
    if a.list:
        return cmd_list()
    ap.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
