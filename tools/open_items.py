# -*- coding: utf-8 -*-
"""미결 작업 대장의 개수를 **표에서 계산한다.** 손으로 쓴 숫자는 낡는다.

[2026-09-12] PLANS (370). 근거 대화:
  *"처리하면 체크해서 갯수에서 빼고 또 나오면 목록에 포함시키고"*

대장: docs/references/OPEN_ITEMS_REGISTER.md
상태: DECISION(사용자 결정) / BLOCKED(차단사유 명시) / OPEN(지금 가능) / DONE

    python tools/open_items.py                 # 현황
    python tools/open_items.py --open          # 지금 할 수 있는 것만
    python tools/open_items.py --done C1 --note "고친 근거"    # 닫기
    python tools/open_items.py --add "항목" --status OPEN --why "차단없음" --ref "PLANS 371"
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
REG = ROOT / "docs" / "references" / "OPEN_ITEMS_REGISTER.md"
STATES = ["DECISION", "BLOCKED", "OPEN", "DONE"]
PREFIX_OF = {"DECISION": "A", "BLOCKED": "B", "OPEN": "C", "DONE": "E"}


def load() -> List[Dict[str, str]]:
    if not REG.is_file():
        return []
    out: List[Dict[str, str]] = []
    for line in REG.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 5:
            continue
        if cells[0] in ("ID", "---") or set(cells[0]) <= set("-: "):
            continue
        if not re.match(r"^[A-Z]\d+$", cells[0]):
            continue
        out.append({"id": cells[0], "state": cells[1], "item": cells[2],
                    "why": cells[3], "ref": cells[4]})
    return out


CARD_MARK = "## 처리 카드"
STAGES = [("verify", "검증"), ("cause", "원인"), ("judge", "판단"),
          ("fix", "조치"), ("report", "보고")]


def save(rows: List[Dict[str, str]]) -> None:
    """표만 다시 쓰고 **뒤의 카드 절은 보존한다.**

    [2026-09-13] 예전 save() 는 표 이후를 통째로 버렸다. 카드를 붙이면서
    그대로 뒀으면 카드가 매 저장마다 사라졌을 것이다.
    """
    txt = REG.read_text(encoding="utf-8")
    head = txt.split("| ID | 상태 |", 1)[0]
    tail = ""
    if CARD_MARK in txt:
        tail = "\n---\n\n" + CARD_MARK + txt.split(CARD_MARK, 1)[1]
    body = "| ID | 상태 | 항목 | 차단사유·트리거 | 근거 |\n|---|---|---|---|---|\n"
    for r in rows:
        body += "| %s | %s | %s | %s | %s |\n" % (r["id"], r["state"], r["item"], r["why"], r["ref"])
    REG.write_text(head + body + tail, encoding="utf-8")


def load_cards() -> Dict[str, Dict[str, str]]:
    """카드 절을 읽는다. ID -> {stage: text}"""
    if not REG.is_file():
        return {}
    txt = REG.read_text(encoding="utf-8")
    if CARD_MARK not in txt:
        return {}
    ko2key = {ko: key for key, ko in STAGES}
    out: Dict[str, Dict[str, str]] = {}
    cur = ""
    for line in txt.split(CARD_MARK, 1)[1].splitlines():
        m = re.match(r"^###\s+([A-Z]\d+)\s*(.*)$", line.strip())
        if m:
            cur = m.group(1)
            out[cur] = {"_title": m.group(2).strip()}
            continue
        if cur:
            m2 = re.match(r"^-\s*(검증|원인|판단|조치|보고)\s*:\s*(.*)$", line.strip())
            if m2:
                out[cur][ko2key[m2.group(1)]] = m2.group(2).strip()
    return out


def save_card(item_id: str, title: str, card: Dict[str, str]) -> None:
    txt = REG.read_text(encoding="utf-8")
    if CARD_MARK not in txt:
        raise SystemExit("카드 절이 없다: %s" % REG)
    head, cards = txt.split(CARD_MARK, 1)
    block = ["### %s %s" % (item_id, title)]
    for key, ko in STAGES:
        block.append("- %s: %s" % (ko, card.get(key, "")))
    new_block = chr(10).join(block)
    pat = re.compile(r"^###\s+" + re.escape(item_id) + r"\s.*?(?=^###\s|\Z)", re.M | re.S)
    if pat.search(cards):
        cards = pat.sub(new_block + chr(10) * 2, cards, count=1)
    else:
        cards = cards.rstrip(chr(10)) + chr(10) * 2 + new_block + chr(10)
    REG.write_text(head + CARD_MARK + cards, encoding="utf-8")

def report(rows: List[Dict[str, str]], only_open: bool) -> None:
    counts = {s: sum(1 for r in rows if r["state"] == s) for s in STATES}
    other = [r for r in rows if r["state"] not in STATES]
    live = counts["DECISION"] + counts["BLOCKED"] + counts["OPEN"]
    print("=" * 72)
    print(" 미결 작업 대장   %s" % dt.datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 72)
    print("  미결 %d건  =  결정대기 %d + 차단 %d + 지금가능 %d     (완료 %d)"
          % (live, counts["DECISION"], counts["BLOCKED"], counts["OPEN"], counts["DONE"]))
    if other:
        print("  [경고] 알 수 없는 상태 %d건: %s" % (len(other), ", ".join(r["id"] for r in other)))
    print("-" * 72)
    show = [r for r in rows if r["state"] == "OPEN"] if only_open else \
           [r for r in rows if r["state"] != "DONE"]
    for r in show:
        print("  %-4s %-9s %s" % (r["id"], r["state"], r["item"][:46]))
        if r["why"]:
            print("       %s" % r["why"][:78])
    if not only_open and counts["OPEN"]:
        print("-" * 72)
        print("  * 지금 가능 %d건 - 차단 사유가 없다. 미룬 게 아니라 안 한 것이다" % counts["OPEN"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--open", action="store_true", help="지금 할 수 있는 것만")
    ap.add_argument("--done", default="", help="닫을 항목 ID")
    ap.add_argument("--note", default="", help="--done 과 함께: 닫은 근거")
    ap.add_argument("--add", default="", help="새 항목 본문")
    ap.add_argument("--status", default="OPEN", choices=STATES)
    ap.add_argument("--why", default="", help="차단사유·트리거")
    ap.add_argument("--ref", default="", help="근거")
    ap.add_argument("--card", default="", help="처리 카드 보기/쓰기 대상 ID")
    ap.add_argument("--cards", action="store_true", help="카드가 비어 있는 항목을 찾는다")
    ap.add_argument("--verify", default="", help="검증: 무엇을 재서 실재를 확인했나")
    ap.add_argument("--cause", default="", help="원인: 증상이 아니라 기전")
    ap.add_argument("--judge", default="", help="판단: 고칠까/둘까/사용자결정. 그 이유")
    ap.add_argument("--fix", default="", help="조치: 실제로 무엇을 바꿨나")
    ap.add_argument("--report", default="", help="보고: 무엇을 봐야 검증되나 + 언제")
    args = ap.parse_args()

    rows = load()
    if not rows:
        print("[ERR] 대장을 읽지 못했다: %s" % REG)
        return 2

    if args.cards:
        cards = load_cards()
        live = [r for r in rows if r["state"] != "DONE"]
        empty = [r for r in live
                 if not any((cards.get(r["id"]) or {}).get(k) for k, _ in STAGES)]
        print("미결 %d건 중 카드 없는 것 %d건" % (len(live), len(empty)))
        for r in empty:
            print("  %-4s %-9s %s" % (r["id"], r["state"], r["item"][:52]))
        part = [r for r in live if r["id"] not in [x["id"] for x in empty]]
        if part:
            print("-" * 60)
            print("카드 있는 것 %d건" % len(part))
            for r in part:
                c = cards.get(r["id"]) or {}
                miss = [ko for k, ko in STAGES if not c.get(k)]
                print("  %-4s %s%s" % (r["id"], r["item"][:44],
                                       ("  [빈 단계 %s]" % ",".join(miss)) if miss else ""))
        return 0

    if args.card:
        cid = args.card.strip().upper()
        tgt = [r for r in rows if r["id"] == cid]
        if not tgt:
            print("[ERR] 없는 ID: %s" % cid)
            return 3
        cards = load_cards()
        cur = dict(cards.get(cid) or {})
        changed = False
        for key, _ko in STAGES:
            v = getattr(args, key if key != "report" else "report")
            if v:
                cur[key] = v.strip()
                changed = True
        if changed:
            save_card(cid, tgt[0]["item"][:60], cur)
            print("[CARD] %s 갱신" % cid)
            cards = load_cards()
            cur = cards.get(cid) or {}
        print("=" * 72)
        print(" %s  %s" % (cid, tgt[0]["item"]))
        print(" 상태 %s | %s" % (tgt[0]["state"], tgt[0]["why"][:60]))
        print("=" * 72)
        for key, ko in STAGES:
            val = cur.get(key) or ""
            print("  %-4s %s" % (ko, val if val else "**비어 있음**"))
        return 0

    if args.done:
        tgt = [r for r in rows if r["id"] == args.done.strip().upper()]
        if not tgt:
            print("[ERR] 없는 ID: %s" % args.done)
            return 3
        r = tgt[0]
        if r["state"] == "DONE":
            print("[SKIP] 이미 DONE: %s" % r["id"])
            return 0
        r["state"] = "DONE"
        stamp = dt.datetime.now().strftime("%Y-%m-%d")
        r["why"] = ("DONE %s. %s" % (stamp, args.note)).strip()
        save(rows)
        print("[DONE] %s  %s" % (r["id"], r["item"]))

    if args.add:
        pre = PREFIX_OF.get(args.status, "C")
        nums = [int(r["id"][1:]) for r in rows if r["id"].startswith(pre)]
        new_id = "%s%d" % (pre, (max(nums) + 1) if nums else 1)
        rows.append({"id": new_id, "state": args.status, "item": args.add.strip(),
                     "why": args.why.strip(), "ref": args.ref.strip()})
        save(rows)
        print("[ADD] %s  %s" % (new_id, args.add.strip()))
        rows = load()

    report(rows, only_open=args.open)
    return 0


if __name__ == "__main__":
    sys.exit(main())
