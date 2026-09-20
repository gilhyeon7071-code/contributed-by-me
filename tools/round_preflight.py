# -*- coding: utf-8 -*-
"""측정 라운드 사전등록·동결·점검 (기준서 §4 / §4.1 을 모든 라운드에 적용).

왜 필요한가
    docs/references/NEW_SIGNAL_VALIDATION_STANDARD.md v1.0 은 §4 등록표와
    §4.1 시작 금지 규칙(MDE > MES 면 시작 안 함)을 이미 갖고 있다.
    그런데 §1 적용 범위가 "RootA 새 후보 생성 신호" 로만 한정돼 있어서,
    2026-08-28~31 의 측정(94조합 재측정 / Track B 재현 / ETF 대조 / 수급 MDE)은
    전부 그 밖에서 돌았고 §4.1 을 한 번도 통과하지 않았다.
    그 결과 41.6년·67.6년·326년·1,237년이 **측정 후에** 계산됐다.

이 도구가 강제하는 것
    (1) 판정 규칙을 재기 전에 쓴다   - 필수 필드 미기재면 동결 거부
    (4) 기준선을 동결한다           - 기준선 파일 sha256 기록·대조
    (5) 검정력을 재기 전에 계산한다   - MDE > MES 면 NO-GO

사용
    python tools/round_preflight.py --new <slug> --kind EXPLORATION
    python tools/round_preflight.py --freeze RD_YYYYMMDD_<slug>
    python tools/round_preflight.py --check RD_YYYYMMDD_<slug>
    python tools/round_preflight.py --list
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RDIR = ROOT / "docs" / "research" / "rounds"

REQUIRED = [
    "round_id", "kind", "가설", "기준선", "기준선_파일", "주지표",
    "표본단위", "클러스터단위", "n", "MDE", "MES", "판정규칙", "중단조건",
]

TEMPLATE = """# 사전등록 - {rid}

**{one_line}**

- 등록 시각: {ts}
- 상태: **DRAFT** (동결 전. --freeze 를 통과해야 측정을 시작할 수 있다)
- 규격: docs/references/NEW_SIGNAL_VALIDATION_STANDARD.md
- 이 문서는 **동결 후 수정하지 않는다.** 바꿔야 하면 새 round_id 를 연다.

## 등록표 (기준서 §4)

| 항목 | 값 |
|---|---|
| round_id | {rid} |
| kind | {kind} |
| 가설 | (한 문장. 반증 가능하게. 무엇이 참이면 무엇이 거짓인가) |
| 기준선 | (무엇과 비교하는가. 실제로 살 수 있는 것 + 스타일 일치 둘 다) |
| 기준선_파일 | (기준선을 만드는 파일/데이터 경로. 쉼표 구분. 동결 대상) |
| 주지표 | (하나만. 보조 지표는 아래 별도 절에) |
| 표본단위 | (관측 하나가 무엇인가: 종목-일 / 신호일 / 비겹침블록) |
| 클러스터단위 | (유의성을 무엇 단위로 잡는가: 일 / 블록. 거래 단위 금지) |
| n | (확보 가능한 관측 수. 숫자) |
| MDE | (양측 5% / 검정력 80% 최소검출효과. 숫자 + 산출근거) |
| MES | (운영상 의미 있는 최소효과. 숫자 + 근거. 비용을 넘는가) |
| 판정규칙 | (이 값이 이렇게 나오면 무엇을 한다. SUPPORTED/NOT_SUPPORTED 조건) |
| 중단조건 | (무엇이 관측되면 이 라운드를 접는가) |
| 검정조합수 | (훑을 조합 수. 1이 아니면 다중비교 통제를 아래에 쓴다) |
| 다중비교 | (조합수>1 이면 Bonferroni 등. 시도 원장에도 추가할 것) |
| 운영분리 | 운영 Gate/LOCK/주문/브로커/paper runtime 미변경 |

## 이 가설의 출처

(사후 가설인가 사전 가설인가. 사후면 어느 탐색에서 나왔는지 PLANS 번호로 명시)

## 한계 (함께 인용할 것)

(1)
(2)
"""


def _parse(md: str) -> dict:
    out = {}
    for line in md.splitlines():
        m = re.match(r"^\|\s*([^|]+?)\s*\|\s*(.*?)\s*\|\s*$", line)
        if m:
            k, v = m.group(1).strip(), m.group(2).strip()
            if k and k != "항목" and not set(k) <= set("-: "):
                out[k] = v
    return out


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


# [2026-08-31 수리] 매일 자라는 데이터 파일에 전체 sha256 을 걸어두면
#   정상 수집만으로 --check 가 매일 FAIL 하고, **진짜 변조와 구별할 수 없다.**
#   실제로 2_Logs/index_daily_history.csv 가 16:05 수집 후 FAIL 로 바뀌었다
#   (8,673 -> 8,676행. 동결 라운드의 exempt 는 "수집은 계속한다" 이므로 이 증가는 정상이다).
#   그리고 이 파일은 append 가 아니라 **매일 전체를 다시 쓴다** - 바이트 프리픽스로도 안 된다.
#   -> 날짜 컬럼이 있는 CSV 는 **동결 시점 이하 행의 내용 해시**로 본다.
#      과거 행이 하나라도 바뀌면 FAIL, 뒤에 붙는 것은 통과. 그것이 원래 지키려던 계약이다.
def _is_growing_csv(p: Path) -> bool:
    if p.suffix.lower() != ".csv":
        return False
    try:
        with open(p, "r", encoding="utf-8-sig", errors="ignore") as f:
            head = f.readline()
    except Exception:
        return False
    return "date" in [c.strip().lower() for c in head.split(",")]


def _csv_prefix(p: Path, cutoff: str | None = None, ignore_columns: list[str] | None = None):
    """(cutoff 이하 행의 내용 sha256, 행수, 그 파일의 date_max) 를 돌려준다.

    cutoff 가 None 이면 현재 date_max 를 쓴다(동결 시점 기록용).
    행은 원문 그대로 이어 붙여 해시한다 - 열 순서/서식이 바뀌어도 잡히게 한다.
    """
    import csv as _csv
    with open(p, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        rows = list(_csv.reader(f))
    if not rows:
        return "EMPTY", 0, ""
    hdr = [c.strip().lower() for c in rows[0]]
    try:
        di = hdr.index("date")
    except ValueError:
        return "NO_DATE_COL", 0, ""
    body = [r for r in rows[1:] if len(r) > di]
    # [2026-09-09] ignore_columns 는 **진단 전용**이다. 계약(prefix_sha256)은 전 열로 계산한다.
    #   불일치가 났을 때 "시장 자료가 바뀐 것인가, 기록용 열만 바뀐 것인가" 를 가르려고 쓴다.
    #   기준선 정의를 바꾸는 것이 아니다 - 그건 사실상 재동결이라 사용자 결정 사항이다
    #   (docs/research/rounds/RD_20260831_flow_h10/exceptions.md 참조).
    drop = sorted({hdr.index(c.strip().lower()) for c in (ignore_columns or [])
                   if c.strip().lower() in hdr}, reverse=True)
    if drop:
        def _cut(r):
            r = list(r)
            for i in drop:
                if i < len(r):
                    del r[i]
            return r
        rows = [_cut(rows[0])] + rows[1:]
        body = [_cut(r) for r in body]
        di = di - sum(1 for i in drop if i < hdr.index("date"))
    if cutoff is None:
        cutoff = max((str(r[di]).strip() for r in body), default="")
    sel = [r for r in body if str(r[di]).strip() <= cutoff]
    sel.sort(key=lambda r: tuple(str(x) for x in r))
    h = hashlib.sha256()
    h.update(("\x1f".join(rows[0]) + "\x1e").encode("utf-8"))
    for r in sel:
        h.update(("\x1f".join(r) + "\x1e").encode("utf-8"))
    return h.hexdigest(), len(sel), cutoff


def _num(s):
    m = re.search(r"-?\d+(?:\.\d+)?", str(s).replace(",", ""))
    return float(m.group(0)) if m else None


def cmd_new(slug: str, kind: str) -> int:
    rid = "RD_%s_%s" % (dt.date.today().strftime("%Y%m%d"), slug)
    d = RDIR / rid
    if d.exists():
        print("[ERR] 이미 있다: %s" % d)
        return 2
    d.mkdir(parents=True)
    (d / "registration.md").write_text(
        TEMPLATE.format(rid=rid, kind=kind,
                        ts=dt.datetime.now().isoformat(timespec="seconds"),
                        one_line="(한 줄 제목)"),
        encoding="utf-8")
    print("[OK] 생성: %s" % (d / "registration.md"))
    print("     필수 필드를 채운 뒤 --freeze %s" % rid)
    return 0


def cmd_freeze(rid: str) -> int:
    d = RDIR / rid
    reg = d / "registration.md"
    if not reg.exists():
        print("[ERR] 없다: %s" % reg)
        return 2
    md = reg.read_text(encoding="utf-8")
    f = _parse(md)

    miss = [k for k in REQUIRED if not f.get(k) or f[k].startswith("(")]
    if miss:
        print("[NO-GO] 필수 필드 미기재 %d개: %s" % (len(miss), ", ".join(miss)))
        print("        기준서 §4. 재기 전에 채워야 한다.")
        return 3

    mde, mes = _num(f["MDE"]), _num(f["MES"])
    if mde is None or mes is None:
        print("[NO-GO] MDE/MES 에서 숫자를 읽지 못했다: MDE=%r MES=%r" % (f["MDE"], f["MES"]))
        return 3
    if mde > mes:
        print("[NO-GO] §4.1 시작 금지 규칙   MDE %.6g > MES %.6g" % (mde, mes))
        print("        이 표본으로 검출 가능한 최소효과가 운영상 의미 있는 최소효과보다 크다.")
        print("        결과는 시작 전에 정해져 있다. 표본을 늘리거나 질문을 바꿔라.")
        return 4

    frozen = {
        "round_id": rid,
        "frozen_at": dt.datetime.now().isoformat(timespec="seconds"),
        "registration_sha256": hashlib.sha256(md.encode("utf-8")).hexdigest(),
        "MDE": mde, "MES": mes, "margin": mes - mde, "baselines": {},
        "draft_status_line": next((l for l in md.splitlines() if l.startswith("- 상태:")), ""),
    }
    for raw in str(f["기준선_파일"]).split(","):
        key = raw.strip()
        if not key:
            continue
        p = Path(key) if Path(key).is_absolute() else ROOT / key
        if not p.exists():
            frozen["baselines"][key] = "MISSING"
        elif _is_growing_csv(p):
            # 자라는 데이터 파일: 동결 시점 이하 행만 계약 대상
            ph, n, cut = _csv_prefix(p, None)
            rec_b = {"mode": "growing_csv", "prefix_sha256": ph,
                     "rows_upto": n, "cutoff": cut,
                     "full_sha256_at_freeze": _sha(p)}
            # [2026-09-09] **계약은 위 prefix_sha256 하나다.** 아래는 진단용 보조 증거다.
            #   기록용 열(fetched_at 등)을 뺀 해시를 함께 남겨 두면, 나중에 불일치가 났을 때
            #   "시장 자료가 바뀌었나 / 수집 장부만 바뀌었나" 를 그 자리에서 가를 수 있다.
            #   실제로 RD_20260831_flow_h10 은 이 값이 없어서 대조가 불가능했다(exceptions.md).
            try:
                with open(p, "r", encoding="utf-8-sig", errors="ignore") as _f:
                    _hdr = [c.strip().lower() for c in _f.readline().split(",")]
            except Exception:
                _hdr = []
            for _mc in ("fetched_at", "updated_at", "collected_at", "ingested_at"):
                if _mc in _hdr:
                    try:
                        _ph2, _, _ = _csv_prefix(p, cut, ignore_columns=[_mc])
                        rec_b["prefix_sha256_excl_%s" % _mc] = _ph2
                    except Exception:
                        pass
            # [2026-09-09] **원본 바이트를 라운드 폴더에 보존한다.**
            #   해시만 남기면 나중에 불일치가 났을 때 "무엇이 달라졌는가" 를 답할 수 없다.
            #   실측: RD_20260831_flow_h10 의 index_daily_history.csv 는 저장소 전체에
            #   사본이 **하나도 없었고**(backup/ 전수 검색 0건), 그래서 재수집된 과거 57행 중
            #   동결 때 우연히 적어둔 앵커 3행만 검증할 수 있었다. 나머지 54행은 지금도 확인 불가다.
            #   680KB 다. 답을 못 하는 것보다 싸다.
            try:
                snap_dir = d / "baseline_snapshots"
                snap_dir.mkdir(parents=True, exist_ok=True)
                snap = snap_dir / Path(key).name
                snap.write_bytes(p.read_bytes())
                rec_b["snapshot_path"] = str(snap.relative_to(ROOT)).replace("\\", "/")
                rec_b["snapshot_sha256"] = _sha(snap)
            except Exception as _e:
                rec_b["snapshot_error"] = "%s: %s" % (type(_e).__name__, _e)
            frozen["baselines"][key] = rec_b
        else:
            frozen["baselines"][key] = _sha(p)
    missb = [k for k, v in frozen["baselines"].items() if v == "MISSING"]
    if missb:
        print("[NO-GO] 기준선 파일을 찾을 수 없다: %s" % ", ".join(missb))
        return 3

    (d / "frozen.json").write_text(json.dumps(frozen, ensure_ascii=False, indent=2),
                                   encoding="utf-8")
    reg.write_text(re.sub(r"^- 상태: \*\*DRAFT\*\*.*$",
                          "- 상태: **FROZEN** (%s 동결. 이 문서는 수정하지 않는다)" % frozen["frozen_at"][:19],
                          md, count=1, flags=re.M), encoding="utf-8")
    print("[GO] 동결 완료  MDE %.6g <= MES %.6g  (여유 %.6g)" % (mde, mes, mes - mde))
    print("     %s" % (d / "frozen.json"))
    print("     이제 측정을 시작할 수 있다. 등록표와 기준선은 --check 로 대조된다.")
    return 0


def cmd_check(rid: str) -> int:
    d = RDIR / rid
    fz = d / "frozen.json"
    if not fz.exists():
        print("[FAIL] 동결되지 않았다: %s  -> 이 라운드의 결과는 확증으로 인용할 수 없다" % rid)
        return 3
    fr = json.loads(fz.read_text(encoding="utf-8"))
    md = (d / "registration.md").read_text(encoding="utf-8")
    cur = hashlib.sha256(re.sub(r"^- 상태: \*\*FROZEN\*\*.*$", fr.get("draft_status_line", ""),
                                md, count=1, flags=re.M).encode("utf-8")).hexdigest()
    ok = True
    if cur != fr["registration_sha256"]:
        print("[FAIL] 등록표가 동결 후 변경됐다. 이 라운드는 확증이 아니라 탐색이다.")
        ok = False
    for raw, rec in fr["baselines"].items():
        p = Path(raw) if Path(raw).is_absolute() else ROOT / raw
        if not p.exists():
            print("[FAIL] 기준선 없음: %s" % raw)
            ok = False
            continue
        if isinstance(rec, dict) and rec.get("mode") == "growing_csv":
            # 동결 시점 이하 행만 계약이다. 뒤에 붙는 것은 정상 수집이다
            #
            # [2026-09-09] **계약의 대상은 자료 열이지 파일 바이트가 아니다.**
            #   동결이 지키려는 것은 "내가 측정한 숫자가 안 바뀌었다" 이지
            #   "파일이 안 바뀌었다" 가 아니다. fetched_at 같은 수집 장부 열은 측정 대상이
            #   아니면서 매일 바뀌므로, 포함하면 **변조와 재수집을 구별할 수 없다**
            #   (RD_20260831_flow_h10 이 그래서 09-07부터 매일 FAIL 했다).
            #
            #   그래서 동결 기록에 prefix_sha256_excl_<열> 이 있으면 **그쪽을 계약으로 쓴다.**
            #   전 열 해시는 참고로 남긴다.
            #   **소급 적용하지 않는다** - 그 값이 없는 기존 동결(flow_h10 등)은 종전대로
            #   전 열 해시가 계약이다. 지금 값으로 채워 넣는 것은 사용자가 2026-09-08 에
            #   거부한 재동결이다(rounds/RD_20260831_flow_h10/exceptions.md).
            _excl_col = next((c for c in ("fetched_at", "updated_at", "collected_at", "ingested_at")
                              if rec.get("prefix_sha256_excl_%s" % c)), None)
            if _excl_col:
                ph, n, _ = _csv_prefix(p, rec.get("cutoff"), ignore_columns=[_excl_col])
                _expect = rec.get("prefix_sha256_excl_%s" % _excl_col)
                if ph != _expect:
                    print("[FAIL] 기준선 과거 자료 변경: %s  (%s 이하, %s 제외 기준, %s -> %s)"
                          % (raw, rec.get("cutoff"), _excl_col, str(_expect)[:12], ph[:12]))
                    ok = False
                elif n != rec.get("rows_upto"):
                    print("[FAIL] 기준선 과거 행수 변경: %s  (%s -> %s)"
                          % (raw, rec.get("rows_upto"), n))
                    ok = False
                else:
                    print("[ok] %s  %s 이하 %d행 자료 불변 (%s 제외. 수집 장부 변화는 계약 밖)"
                          % (raw, rec.get("cutoff"), n, _excl_col))
                continue
            ph, n, _ = _csv_prefix(p, rec.get("cutoff"))
            if ph != rec.get("prefix_sha256"):
                print("[FAIL] 기준선 과거 행 변경: %s  (%s 이하, %s -> %s)"
                      % (raw, rec.get("cutoff"), str(rec.get("prefix_sha256"))[:12], ph[:12]))
                # [2026-09-09] 해시 두 개만 찍으면 진단이 불가능하다. 무엇이 바뀐 것인지 함께 낸다.
                #   판정은 바꾸지 않는다 - 아래는 전부 출력일 뿐이고 ok 는 이미 False 다.
                if n != rec.get("rows_upto"):
                    print("       [DIAG] 행 수가 바뀌었다: %s -> %s  **자료 변경이다**"
                          % (rec.get("rows_upto"), n))
                else:
                    print("       [DIAG] 행 수는 불변: %s" % n)
                    try:
                        with open(p, "r", encoding="utf-8-sig", errors="ignore") as _f:
                            _hdr = [c.strip().lower() for c in _f.readline().split(",")]
                    except Exception:
                        _hdr = []
                    _metas = [c for c in ("fetched_at", "updated_at", "collected_at", "ingested_at")
                              if c in _hdr]
                    if not _metas:
                        print("       [DIAG] 기록용 열이 없다 -> 차이는 자료 컬럼에서 났다")
                    for meta_col in _metas:
                        try:
                            ph2, n2, _ = _csv_prefix(p, rec.get("cutoff"), ignore_columns=[meta_col])
                        except Exception:
                            continue
                        base = rec.get("prefix_sha256_excl_%s" % meta_col)
                        if base:
                            print("       [DIAG] %s 제외 해시 %s (동결값 %s) %s"
                                  % (meta_col, ph2[:12], str(base)[:12],
                                     "일치 -> 기록용 열만 바뀌었다" if ph2 == base else "불일치 -> 자료가 바뀌었다"))
                        else:
                            print("       [DIAG] %s 제외 해시 %s (동결 시 기록 없음 - 대조 불가)"
                                  % (meta_col, ph2[:12]))
                    print("       [DIAG] 판정에는 영향 없음. 예외가 기록돼 있으면 그 유효범위를 확인할 것"
                          " (rounds/<id>/exceptions.md)")
                ok = False
            elif n != rec.get("rows_upto"):
                print("[FAIL] 기준선 과거 행수 변경: %s  (%s -> %s)" % (raw, rec.get("rows_upto"), n))
                ok = False
            else:
                print("[ok] %s  %s 이하 %d행 불변 (이후 추가는 정상 수집)" % (raw, rec.get("cutoff"), n))
            continue
        now = _sha(p)
        if now != rec:
            print("[FAIL] 기준선 변경: %s  (%s -> %s)" % (raw, str(rec)[:12], now[:12]))
            ok = False
    print("[OK] 동결 상태 유지" if ok else "[FAIL] 동결 위반")
    return 0 if ok else 3


def cmd_list() -> int:
    if not RDIR.exists():
        print("(라운드 없음)  %s" % RDIR)
        return 0
    n = 0
    for d in sorted(RDIR.iterdir()):
        if not d.is_dir():
            continue
        n += 1
        fz = d / "frozen.json"
        st = "FROZEN" if fz.exists() else "DRAFT"
        extra = ""
        if fz.exists():
            fr = json.loads(fz.read_text(encoding="utf-8"))
            extra = "  MDE %.4g / MES %.4g  동결 %s" % (fr["MDE"], fr["MES"], fr["frozen_at"][:10])
        print("  %-7s %s%s" % (st, d.name, extra))
    if n == 0:
        print("(라운드 없음)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="측정 라운드 사전등록·동결·점검")
    ap.add_argument("--new", metavar="SLUG")
    ap.add_argument("--kind", default="EXPLORATION", choices=["EXPLORATION", "CONFIRMATION"])
    ap.add_argument("--freeze", metavar="ROUND_ID")
    ap.add_argument("--check", metavar="ROUND_ID")
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    if a.new:
        return cmd_new(a.new, a.kind)
    if a.freeze:
        return cmd_freeze(a.freeze)
    if a.check:
        return cmd_check(a.check)
    if a.list:
        return cmd_list()
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
