"""H2 시장조치 사건 만들기 — KIND 원본에서 거래소 조치를 뽑아 H1 과 같은 형식의 사건 목록으로.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.collect_market_actions --events-dir <폴더>

가설 H2
  거래소 조치는 **그 종목을 사고파는 방식 자체를 제도로 바꾼다** — 신용거래 금지, 위탁증거금 100%,
  단일가매매, 거래정지. 그러면 팔 수밖에 없는 쪽과 못 사는 쪽이 생긴다(= 가격과 무관한 매매).
  그 제약이 걸릴 때 눌리고, 풀릴 때 되돌아오는가.

  H1(자금이 사야 해서 미는 압력)과 방향이 반대인 자매 가설이에요. 둘 다 "가격이 아닌 이유로 생기는 매매" 라는
  같은 뿌리에서 나와요.

무엇을 쓰나
  `kind_disclosures.jsonl` (이미 쌓고 있는 KIND 원본) — 새 수집 없음. 제목으로 분류만 한다.
  종목코드는 KIND 에 없다 → 회사명으로 붙인다. **우선주·이름 불일치로 못 붙인 건은 버리지 않고 남긴다.**

기록: `h2_events.jsonl` (H1 과 같은 형식이라 대조군·측정 도구를 그대로 쓴다)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

STRATEGY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RULES = STRATEGY_ROOT / "config" / "h2_market_action_rules_v1.json"


def classify(title: str, rules: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    name = str(title or "").replace(" ", "")
    out = []
    for r in rules:
        if not any(k.replace(" ", "") in name for k in r["any"]):
            continue
        if any(k.replace(" ", "") in name for k in r.get("none", [])):
            continue
        if any(k.replace(" ", "") not in name for k in r.get("require", [])):
            continue
        out.append({"event_type": r["event_type"], "direction": r["direction"]})
    return out


def name_to_code(dart_rows: List[Dict[str, Any]], universe_names: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """회사명 → 종목코드. DART 원본이 1차(같은 시장·같은 기간), 유니버스 이름이 2차."""
    m: Dict[str, str] = {}
    for r in dart_rows:
        nm, code = str(r.get("corp_name") or "").strip(), str(r.get("stock_code") or "").strip()
        if nm and code:
            m.setdefault(nm, code)
    for nm, code in (universe_names or {}).items():
        m.setdefault(nm, code)
    return m


def build(kind_rows: List[Dict[str, Any]], rules: List[Dict[str, Any]], codes: Dict[str, str],
          now: datetime) -> Dict[str, Any]:
    events, unmapped = [], []
    seen = set()
    for r in kind_rows:
        hits = classify(r.get("title"), rules)
        if not hits:
            continue
        code = codes.get(str(r.get("corp_name") or "").strip())
        for h in hits:
            eid = f"{r['rcept_no']}|{h['event_type']}"
            if eid in seen:
                continue
            seen.add(eid)
            row = {"event_id": eid, "rcept_no": r["rcept_no"], "rcept_dt": r["ymd"],
                   "stock_code": code or "", "corp_name": r.get("corp_name"), "report_nm": r.get("title"),
                   "event_type": h["event_type"], "direction": h["direction"], "is_amendment": False,
                   "session": r.get("session"), "time": r.get("time"), "rules_version": "h2",
                   "observed_at": now.isoformat(timespec="seconds")}
            (events if code else unmapped).append(row)
    return {"events": events, "unmapped": unmapped}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    ap.add_argument("--universe", type=Path, help="input_YYYYMMDD.csv (name·code) — 2차 이름 사전")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    ed = args.events_dir
    kind = [json.loads(x) for x in (ed / "kind_disclosures.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    dart = [json.loads(x) for x in (ed / "dart_disclosures.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    uni_names = {}
    if args.universe and args.universe.exists():
        import pandas as pd
        u = pd.read_csv(args.universe, dtype={"code": str})
        if "name" in u.columns:
            uni_names = {str(n).strip(): str(c) for n, c in zip(u["name"], u["code"])}
    rules = json.loads(args.rules.read_text(encoding="utf-8"))["rules"]
    now = datetime.now()
    out = build(kind, rules, name_to_code(dart, uni_names), now)
    path = ed / "h2_events.jsonl"
    with path.open("w", encoding="utf-8") as fh:      # 파생 파일 — 원본에서 언제든 다시 만든다
        for r in out["events"]:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    (ed / "h2_unmapped.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in out["unmapped"]), encoding="utf-8")
    import collections
    print(json.dumps({"events": len(out["events"]), "unmapped_no_code": len(out["unmapped"]),
                      "by_type": collections.Counter(r["event_type"] for r in out["events"]),
                      "kind_rows": len(kind)}, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
