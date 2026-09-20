# -*- coding: utf-8 -*-
"""bat/cmd 파일의 줄바꿈을 CRLF 로 정규화한다. 내용은 바꾸지 않는다.

왜 필요한가 (2026-08-24 실증):
  cmd.exe 는 배치 파일을 **바이트 오프셋으로 재읽기**한다. LF 단독 줄이 있으면
  오프셋이 어긋나 줄 중간에 착지하고, 긴 줄 뒤일수록 잘 깨진다.
  run_ops_sanity_quick.bat(LF 129줄, 128행이 829자)에서 실패 경로의
  `call :write_summary` 가 깨져 JSON 을 3일간 못 썼고, 그 낡은 JSON 때문에
  인시던트 리포트가 "PASS ... exit_code=5" 라는 거짓 문장을 매일 만들었다.
  동일 내용을 LF/CRLF 두 판본으로 돌려 재현 확인했다.

안전장치:
  - 실행 중인 파일은 건드리지 않는다(호출자가 걸러서 넘겨야 한다).
  - 변환 후 **줄바꿈만 다르고 나머지 바이트가 동일한지** 검증하고, 아니면 되돌린다.
  - --apply 없이는 아무것도 쓰지 않는다.
"""
import argparse, pathlib, shutil, sys, datetime as dt


def normalize(raw: bytes) -> bytes:
    # 기존 CRLF 를 LF 로 내린 뒤 전부 CRLF 로 올린다(중복 \r 방지)
    return raw.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")


def same_content(a: bytes, b: bytes) -> bool:
    """줄바꿈을 제거한 나머지가 같은가."""
    return a.replace(b"\r\n", b"\n").replace(b"\r", b"") == b.replace(b"\r\n", b"\n").replace(b"\r", b"")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=r"E:\1_Data")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--exclude", default="", help="쉼표로 구분한 파일명(실행 중인 것)")
    args = ap.parse_args()

    root = pathlib.Path(args.root)
    skip = {x.strip().lower() for x in args.exclude.split(",") if x.strip()}
    bk = root / "backup" / "20260825_bat_eol_normalize" / dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    targets = []
    for pat in ("*.bat", "*.cmd"):
        for p in sorted(root.glob(pat)):
            raw = p.read_bytes()
            lone = raw.count(b"\n") - raw.count(b"\r\n")
            if lone <= 0:
                continue
            if p.name.lower() in skip:
                print("  SKIP(실행중) %s  LF단독 %d" % (p.name, lone))
                continue
            targets.append((p, raw, lone))

    print("대상 %d개 (LF 단독 줄 보유)" % len(targets))
    if not args.apply:
        print("dry-run. --apply 를 주면 변환한다.")
        for p, raw, lone in targets[:10]:
            print("  %-46s LF단독 %4d -> CRLF" % (p.name, lone))
        if len(targets) > 10:
            print("  ... 외 %d개" % (len(targets) - 10))
        return 0

    bk.mkdir(parents=True, exist_ok=True)
    ok = fail = 0
    for p, raw, lone in targets:
        shutil.copy2(p, bk / p.name)
        out = normalize(raw)
        if not same_content(raw, out):
            print("  [SKIP] %s 내용 불일치 - 건드리지 않는다" % p.name)
            fail += 1
            continue
        p.write_bytes(out)
        chk = p.read_bytes()
        if chk != out or (chk.count(b"\n") - chk.count(b"\r\n")) != 0:
            shutil.copy2(bk / p.name, p)
            print("  [ROLLBACK] %s 검증 실패 - 원복함" % p.name)
            fail += 1
            continue
        ok += 1
    print("변환 %d개 성공, %d개 건너뜀/원복" % (ok, fail))
    print("백업: %s" % bk)
    return 0


if __name__ == "__main__":
    sys.exit(main())
