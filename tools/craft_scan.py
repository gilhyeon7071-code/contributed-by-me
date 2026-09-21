# -*- coding: utf-8 -*-
"""내가 반복해서 내는 **기계로 잡히는 실수**를 매일 훑는다.

[2026-09-13] 사용자: *"왜 그런보고를 하는지. 지시의 잘못인지, 너의습성인지,
재발방지 방법은있는지"* / *"또 다른 문제들도 체크해 같이넣을수있는것이 있는지"*

**스킬로는 안 막힌다.** 2026-09-13 에 `before-you-touch` 를 만들고 10분 뒤에 어겼다.
내가 불러야 뜨는 것은 안 된다. 작동한 것은 전부 탐지였다.
그래서 **기계로 잡히는 것만** 여기 모은다.

무엇을 잡나 — 전부 이 세션에서 실제로 낸 것이다

  1. 제어문자 손상    `E:\\1_Data` 의 `\\1` 이 0x01 로 먹힌다. 2026-09-13 하루에 5회.
                     주석에 있으면 무해해 보이지만 경로 문자열이면 파일이 안 열린다
  2. `or` 낙장 트랩   `_to_int(x, 5) or 5` - 0(끄기)이 falsy 라 5로 덮인다.
                     **기본값이 0 이 아닐 때만** 트랩이다(0.0 이면 무해).
                     2026-07-24 에 10건 고쳤는데 2026-09-13 에 내가 또 썼다
  4. 죽은 패키지 참조   `pe.<이름>` 이 paper_engine 에 실제로 없다.
                     2026-08-07 패키지 분리 때 __init__ 이 재수출을 멈췄고,
                     도구 6개가 그대로 끊겼다. 배치는 rc=1 을 'advisory' 로
                     흘려서 **37일간 조용히** 죽어 있었다 (2026-09-13 발견)
  5. bat 화살표 리다이렉션  `echo ... -> X` 의 `>` 는 **리다이렉션**이다. 문장이 거기서
                     잘리고 나머지는 X 라는 쓰레기 파일로 들어간다. 로그가 조용히
                     사라진다. run_paper_daily.bat 7줄이 몇 달째 그랬다(2026-09-13)
  6. bat 의 BOM         cmd 는 BOM 을 첫 명령의 일부로 읽어 매 기동마다 오류를 낸다.
                     run_intraday_paper.bat 이 그랬다 - 돌긴 도는데 오류 한 줄이
                     늘 섞여 진짜 오류를 가린다 (2026-09-13)
  3. 시험이 진짜 산출물에  tests/ 가 monkeypatch 없이 2_Logs 실제 경로에 쓰면
                     시험 한 번이 공유 SSOT 를 덮는다. 2026-09-11 에 실제로 냈다

무엇을 안 잡나 — 여기 넣지 않는다
  "행동을 목표 달성으로 보고" 같은 것은 기계로 못 잡는다. 그건 보고 형식으로 다룬다
  (한 일 / 잰 것 / 안 잰 것 - 숫자 없는 문장은 '잰 것' 에 못 들어간다)

    python tools/craft_scan.py
    python tools/craft_scan.py --json
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUT_NAME = "craft_scan_latest.json"

SCAN_DIRS = [ROOT, ROOT / "tools", ROOT / "paper_engine", ROOT / "tests",
             ROOT / "docs" / "references"]
SCAN_EXT = (".py", ".bat", ".ps1", ".md")
# 문서도 넣는다. 원장·등록부는 **판단 근거**라, 거기 심긴 깨진 경로는
#   코드의 깨진 경로보다 오래 살아남는다 (2026-09-13 에 실제로 한 줄 냈다)

# 기본값이 0 이 아닌 경우만 진짜 트랩이다
_OR_TRAP = re.compile(r"_to_(?:int|float)\([^()]*,\s*([0-9]*\.?[0-9]+)\s*\)\s*or\s*\1")
# 시험이 실제 로그 경로 문자열을 직접 쓰는가
_REAL_PATH_IN_TEST = re.compile(r"""["'][^"']*2_Logs[/\\][A-Za-z0-9_]+\.(?:json|csv)["']""")


def _files() -> List[Path]:
    out: List[Path] = []
    for base in SCAN_DIRS:
        if not base.is_dir():
            continue
        try:
            with os.scandir(base) as it:
                for e in it:
                    if (e.is_file() and e.name.endswith(SCAN_EXT)
                            and ".bak" not in e.name and "backup" not in e.name):
                        out.append(Path(e.path))
        except Exception:
            continue
    return out


def _dead_pkg_refs(files: List[Path]) -> List[Dict[str, Any]]:
    """`pe.<name>` / `from paper_engine import <name>` 가 실제로 있는지 확인한다.

    **패키지를 못 불러오면 통과로 접지 않는다.** 확인 못 한 것을 '없음' 으로
    적으면 이 검사가 있는 것이 없는 것보다 나쁘다.
    """
    try:
        import paper_engine as _pe
    except Exception as exc:
        return [{"file": "-", "line": 0,
                 "text": "paper_engine import 실패 - 확인불가 (%s)" % type(exc).__name__}]
    out: List[Dict[str, Any]] = []
    for p in files:
        if p.suffix != ".py" or p.name == "craft_scan.py":
            continue
        try:
            s = io.open(str(p), encoding="utf-8", errors="strict").read()
        except Exception:
            continue
        rel = str(p.relative_to(ROOT))
        alias = re.search(r"import\s+paper_engine\s+as\s+(\w+)", s)
        names = set()
        if alias:
            names |= set(re.findall(r"\b%s\.(\w+)" % alias.group(1), s))
        for grp in re.findall(r"from\s+paper_engine\s+import\s+\(?([^)]+)\)?", s):
            for tok in grp.split(","):
                b = tok.strip().split(" as ")[0].strip()
                if b.isidentifier():
                    names.add(b)
        for n in sorted(names):
            if not hasattr(_pe, n):
                out.append({"file": rel, "line": 0, "text": "paper_engine.%s 가 없다" % n})
    return out


_BAT_ECHO = re.compile(r"^\s*(?:@?echo\s|call\s+:LOG)", re.I)
_BAT_TAIL = re.compile(r"\s*>>?\s*\"[^\"]*\"\s*(?:2>&1)?\s*$")


def _bat_arrow_redirects(files: List[Path]) -> List[Dict[str, Any]]:
    """echo/LOG 줄의 메시지 안에 남은 `->` 나 `&` 를 찾는다.

    끝의 `>> "%LOG%"` 는 **의도한** 리다이렉션이라 떼고 본다. 남은 것만 사고다.
    """
    out: List[Dict[str, Any]] = []
    for p in files:
        if p.suffix != ".bat":
            continue
        try:
            s = io.open(str(p), encoding="utf-8", errors="replace").read()
        except Exception:
            continue
        rel = str(p.relative_to(ROOT))
        for i, line in enumerate(s.splitlines(), 1):
            if not _BAT_ECHO.match(line):
                continue
            msg = _BAT_TAIL.sub("", line)
            if "|" in msg:
                continue          # 파이프라인 줄은 메시지가 아니다
            # `^&` 는 이스케이프, `&&` 는 의도한 연결, `2>&1` 은 표준 관용구다.
            #   사고가 되는 것은 **맨 &** 하나와 화살표 `->` 뿐이다.
            _m = msg.replace("^&", "").replace("&&", "").replace("2>&1", "")
            if "->" in _m or "&" in _m:
                out.append({"file": rel, "line": i, "text": line.strip()[:90]})
    return out


def _bat_bom(files: List[Path]) -> List[Dict[str, Any]]:
    """.bat/.ps1 맨 앞의 UTF-8 BOM. cmd 가 첫 명령에 붙여 읽는다."""
    out: List[Dict[str, Any]] = []
    for p in files:
        if p.suffix not in (".bat", ".cmd"):
            continue
        try:
            head = io.open(str(p), "rb").read(3)
        except Exception:
            continue
        if head == bytes((0xEF, 0xBB, 0xBF)):
            out.append({"file": str(p.relative_to(ROOT)), "line": 1,
                        "text": "맨 앞에 UTF-8 BOM 이 있다"})
    return out


# [2026-09-21] 코드에 박힌 자격증명. 2026-06-27 에 들어간 텔레그램 봇 토큰이 **석 달 뒤**
#   "전수 시험 돌려봐" 요청에서야 나왔다. 추적 중이었고 이 저장소에는 GitHub 원격이 붙어 있다 —
#   push 한 번이면 공개된다. '누가 보면 걸린다' 에 맡기지 않고 매일 도는 스캔에 넣는다.
_SECRET_PATTERNS = (
    (re.compile(r"\b\d{9,10}:AA[A-Za-z0-9_-]{30,}"), "텔레그램 봇 토큰"),
    (re.compile(r"""(?i)\bapp_?secret\s*[:=]\s*["'][A-Za-z0-9+/=]{30,}"""), "API 시크릿"),
    (re.compile(r"""(?i)\bapp_?key\s*[:=]\s*["'][A-Za-z0-9]{24,}"""), "API 키"),
    (re.compile(r"""(?i)\bpassword\s*=\s*["'][^"']{6,}"""), "평문 비밀번호"),
)
# 설명·예시는 잡지 않는다. 오탐이 남으면 스캔 전체가 무시된다
_SECRET_ALLOW = re.compile(r"(?i)dummy|example|sample|your_|xxxx|placeholder|<[a-z_]+>|_SECRET_PATTERNS")


def _plaintext_secrets(files: List[Path]) -> List[Dict[str, Any]]:
    """`.secrets/` 와 백업은 대상이 아니다 — 거기가 정식 보관처다."""
    out: List[Dict[str, Any]] = []
    for p in files:
        rel = str(p.relative_to(ROOT)).replace("\\", "/")
        if rel.startswith(".secrets/") or rel.startswith("backup/") or "/backup" in rel:
            continue
        try:
            s = io.open(str(p), encoding="utf-8", errors="ignore").read()
        except Exception:
            continue
        for i, line in enumerate(s.splitlines(), 1):
            if _SECRET_ALLOW.search(line):
                continue
            for rx, label in _SECRET_PATTERNS:
                if rx.search(line):
                    out.append({"file": rel, "line": i, "text": label})
                    break
    return out


# [2026-09-21] 왕복 비용 상수가 코드마다 갈렸다 — 0.358%(12개 파일) vs 0.400%(8개 파일).
#   09-10 에 브로커 실측으로 갱신했는데 전수 반영이 안 됐다. 비용은 모든 손익 판정이 서는 값이라
#   갈라지면 결론이 갈린다. 지금 값(tools/cost_model.py 재계산)과 **다른 리터럴**만 잡는다.
# 서술과 상수를 가른다. 첫 구현은 "왕복" 이 들어간 **모든 줄**을 봐서 37건을 냈는데
#   전부 과거 사건을 적은 주석이었다("엔진은 왕복 1.400% 를 청구하고 있었다").
#   오탐이 남으면 스캔 전체가 무시된다 — **실행되는 대입/기본값만** 본다.
_COST_ASSIGN = re.compile(
    r"(?:^|[^#\w])(?:cost|COST|round_trip|ROUND_TRIP|fee_total|비용)\w*\s*=\s*(0\.0\d{1,4})\b"
    r"|default\s*=\s*(0\.0\d{1,4})\s*,\s*help\s*=\s*[\"'][^\"']*왕복")


def _cost_constant_drift(files: List[Path]) -> List[Dict[str, Any]]:
    """코드가 **실제로 쓰는** 왕복 비용이 지금 모델과 다른가.

    주석·문서의 서술은 잡지 않는다(과거 기록은 그대로 둬야 한다).
    """
    try:
        sys.path.insert(0, str(ROOT / "tools"))
        from cost_model import model_round_trip
        m = model_round_trip()
        if not m.get("ok"):
            return [{"file": "-", "line": 0, "text": "비용 모델을 못 읽음: %s" % m.get("reason")}]
        cur = round(m["round_trip"], 5)
    except Exception as e:
        return [{"file": "-", "line": 0, "text": "비용 모델 로드 실패: %s" % type(e).__name__}]
    out: List[Dict[str, Any]] = []
    for p in files:
        rel = str(p.relative_to(ROOT)).replace("\\", "/")
        # tests/ 는 고정값을 일부러 쓴다 — 여기 오탐이 남으면 스캔 전체가 무시된다
        if (rel.startswith("backup/") or "/backup" in rel or rel.startswith("tests/")
                or rel in ("tools/cost_model.py", "tools/craft_scan.py")):
            continue
        if p.suffix != ".py":
            continue
        try:
            s = io.open(str(p), encoding="utf-8", errors="ignore").read()
        except Exception:
            continue
        for i, line in enumerate(s.splitlines(), 1):
            code = line.split("#", 1)[0]
            if not code.strip():
                continue
            mm = _COST_ASSIGN.search(code)
            if not mm:
                continue
            val = float(mm.group(1) or mm.group(2))
            if 0.001 <= val <= 0.02 and round(val, 5) != cur:
                out.append({"file": rel, "line": i,
                            "text": "쓰는 값 %.3f%% (지금 모델 %.3f%%)" % (100 * val, 100 * cur)})
    return out


def scan() -> Dict[str, Any]:
    ctrl: List[Dict[str, Any]] = []
    ortrap: List[Dict[str, Any]] = []
    testwrite: List[Dict[str, Any]] = []
    files = _files()

    for p in files:
        try:
            s = io.open(str(p), encoding="utf-8", errors="strict").read()
        except Exception:
            continue
        rel = str(p.relative_to(ROOT))
        is_test = rel.replace("\\", "/").startswith("tests/")
        # **독스트링 구간을 건너뛴다.** 이 파일 자신의 설명이 잡혀 오탐 1건을 냈다.
        #   오탐이 남으면 스캔 전체가 무시된다 - 그게 감시를 죽이는 길이다.
        _in_doc = False
        for i, line in enumerate(s.splitlines(), 1):
            _q3 = line.count('"""') + line.count("'''")
            _was_doc = _in_doc
            if _q3 % 2 == 1:
                _in_doc = not _in_doc
            _skip_code = _was_doc or _in_doc
            # 1) 제어문자
            if any(ord(c) < 32 and c != "\t" for c in line):
                ctrl.append({"file": rel, "line": i, "text": repr(line[:70])})
            # 2) or 낙장 (기본값 != 0)
            #    **주석·문서 줄은 뺀다.** 이 파일 자신의 설명 예시를 잡아
            #    첫 실행에서 오탐 1건을 냈다. 오탐이 남으면 스캔이 무시된다
            _code = line.split("#", 1)[0]
            _stripped = line.strip()
            _is_doc = _stripped.startswith(("#", "\"\"\"", "'''", "*", "-"))
            if p.suffix == ".py" and not _is_doc and not _skip_code:
                m = _OR_TRAP.search(_code)
                if m:
                    try:
                        if float(m.group(1)) != 0.0:
                            ortrap.append({"file": rel, "line": i, "text": line.strip()[:90]})
                    except Exception:
                        pass
            # 3) 시험이 실제 산출물 경로를 직접
            if is_test and _REAL_PATH_IN_TEST.search(line):
                if "monkeypatch" not in s[max(0, s.find(line) - 400):s.find(line) + 400]:
                    testwrite.append({"file": rel, "line": i, "text": line.strip()[:90]})

    return {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "files_scanned": len(files),
        "control_chars": ctrl,
        "falsy_or_traps": ortrap,
        "tests_touching_real_artifacts": testwrite,
        "dead_package_refs": _dead_pkg_refs(files),
        "bat_arrow_redirects": _bat_arrow_redirects(files),
        "bat_bom": _bat_bom(files),
        "plaintext_secrets": _plaintext_secrets(files),
        "cost_constant_drift": _cost_constant_drift(files),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    rep = scan()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / OUT_NAME).write_text(json.dumps(rep, ensure_ascii=False, indent=2),
                                    encoding="utf-8")
    if args.json:
        print(json.dumps(rep, ensure_ascii=False, indent=2))
        return 0

    n1 = len(rep["control_chars"])
    n2 = len(rep["falsy_or_traps"])
    n3 = len(rep["tests_touching_real_artifacts"])
    print("=" * 74)
    print(" 반복 실수 스캔   파일 %d개   %s" % (rep["files_scanned"], rep["generated_at"]))
    print("=" * 74)
    print("  [1] 제어문자 손상 (백슬래시 먹힘)   %d건" % n1)
    for x in rep["control_chars"][:5]:
        print("        %s:%s  %s" % (x["file"], x["line"], x["text"]))
    print("  [2] or 낙장 트랩 (기본값 != 0)      %d건" % n2)
    for x in rep["falsy_or_traps"][:5]:
        print("        %s:%s  %s" % (x["file"], x["line"], x["text"]))
    print("  [3] 시험이 진짜 산출물 경로         %d건" % n3)
    for x in rep["tests_touching_real_artifacts"][:5]:
        print("        %s:%s  %s" % (x["file"], x["line"], x["text"]))
    n4 = len(rep["dead_package_refs"])
    print("  [4] 죽은 패키지 참조 (pe.<없는 이름>)  %d건" % n4)
    for x in rep["dead_package_refs"][:5]:
        print("        %s  %s" % (x["file"], x["text"]))
    n5 = len(rep["bat_arrow_redirects"])
    print("  [5] bat echo 안의 -> / & (리다이렉션)  %d건" % n5)
    for x in rep["bat_arrow_redirects"][:5]:
        print("        %s:%s  %s" % (x["file"], x["line"], x["text"]))
    n6 = len(rep["bat_bom"])
    print("  [6] bat 맨 앞 BOM                     %d건" % n6)
    for x in rep["bat_bom"][:5]:
        print("        %s  %s" % (x["file"], x["text"]))
    n7 = len(rep["plaintext_secrets"])
    print("  [7] 코드에 박힌 자격증명              %d건" % n7)
    for x in rep["plaintext_secrets"][:5]:
        print("        %s:%s  %s" % (x["file"], x["line"], x["text"]))
    n8 = len(rep["cost_constant_drift"])
    print("  [8] 왕복 비용 상수 불일치            %d건" % n8)
    for x in rep["cost_constant_drift"][:5]:
        print("        %s:%s  %s" % (x["file"], x["line"], x["text"]))
    total = n1 + n2 + n3 + n4 + n5 + n6 + n7 + n8
    print("-" * 74)
    if total == 0:
        print("  없음")
    else:
        print("  합계 %d건 - **내가 낸 것이다.** 고치고 왜 또 났는지 본다" % total)
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
