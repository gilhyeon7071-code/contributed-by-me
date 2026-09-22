"""아침 배치가 '할 일 파일' 3종으로 **실제로 분기하는지** CLI 경로로 확인한다 (2026-09-21 신설).

가부 기준 8번 증거 4: *"아침 배치는 `--no-submit` 으로 할 일 파일 3종(없음·노출 변경·래치) 각각 분기 확인"*

왜 시험으로 충분하지 않나: 시험 27건은 `morning()` **함수**를 부른다.
배치가 실패한 이력은 함수가 아니라 **배선**에서 났다 — 인자 파싱, 설정 로드, 분기 dispatch.
2026-09-04 에 "실측 PASS" 라고 적은 것이 09-07 까지 미검증이었던 이유도 배치가 그 지점에 도달 못 해서였다.
그래서 여기서는 `daily_ops.main()` 을 **CLI 인자로** 부른다.

왜 가짜 클라이언트인가: 장중에 실계좌로 돌리면 재현이 안 되고, 래치 분기는 합성 원장과 실제 계좌 보유가
어긋나 STOP 으로 끝난다 — 분기가 도는지를 못 본다. 실계좌 경로는 증거 2(예약 작업 자체 실행)가 덮는다.
**이 파일이 덮지 않는 것: `K.make_client()` 자체.** 기록에 그렇게 적는다.

설정은 **진짜 `config/daily_ops_v1.json`** 을 쓴다(`auto_submit=false`). 그게 그림자 모드의 증거다.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

V2 = Path(__file__).resolve().parents[1]
ROOT = V2.parents[2]
sys.path.insert(0, str(ROOT))          # daily_ops 가 paper.strategies... 절대 경로로 import 한다
sys.path.insert(0, str(V2 / "src"))
sys.path.insert(0, str(ROOT / "tests"))

import daily_ops as D  # noqa: E402
# [2026-09-21] **`import kis_adapter` 로 바꿔치기하면 안 된다.** daily_ops 는
#   `paper.strategies...src.kis_adapter` 로 import 하므로 **다른 모듈 객체**다 —
#   처음에 그렇게 짰다가 예행이 실계좌 클라이언트로 돌았다(발주는 auto_submit=false 가 막아 0건).
#   daily_ops 가 실제로 들고 있는 것을 바꾼다.
K = D.K  # noqa: E402
# 가짜 클라이언트는 시험이 쓰는 것을 그대로 쓴다 — 두 벌이 되면 갈라진다
from test_kospi_mcap_quarterly_v2_runner import SimKIS  # noqa: E402

import pandas as pd  # noqa: E402

CODES = ["005930", "000660", "035420", "051910", "005380"]
# 래치는 **발동 다음 거래일**에 판다. 그래서 두 경우를 모두 본다 —
# 발동 당일이면 멈춰야 하고(규칙), 다음 거래일이면 팔아야 한다(기능).
SCENARIOS = (
    ("NONE", "할 일 없음", None, None),
    ("SCALE", "노출 변경", {"status": "OK", "exposure": 0.5, "regime": "below"}, None),
    ("LIQUIDATE", "래치 — 발동 당일(멈춰야 한다)", None, "same_day"),
    ("LIQUIDATE", "래치 — 다음 거래일(팔아야 한다)", None, "prev_day"),
)


def _seed(base: Path, action: str, regime, for_date: str, latch_when=None, tag="") -> Path:
    st = base / (action + tag) / "state"
    st.mkdir(parents=True, exist_ok=True)
    w = [1.0 / len(CODES)] * len(CODES)
    tgt = base / (action + tag) / "t.csv"
    pd.DataFrame({"code": CODES, "market_cap": [1e14 - i for i in range(len(CODES))],
                  "basket_weight": w, "account_target_weight": w,
                  "target_qty": [9] * len(CODES), "price": [1] * len(CODES)}).to_csv(tgt, index=False)
    summ = base / (action + tag) / "s.json"
    summ.write_text(json.dumps({"status": "OK", "stage": "D7", "selection_date": "20260930",
                                "d3": {"status": "OK", "exposure": 1.0, "regime": "above"}}), encoding="utf-8")
    D.set_current(state_dir=st, target_csv=tgt, summary_json=summ)
    na = {"for_date": for_date, "action": action, "reasons": ["REHEARSAL"]}
    if regime:
        na["regime"] = regime
    (st / D.NEXT_ACTION).write_text(json.dumps(na), encoding="utf-8")
    if action == "LIQUIDATE":
        # 발동일: same_day = 오늘(멈춰야 함) / prev_day = 어제(오늘이 집행일)
        trig = for_date if latch_when == "same_day" else _prev_ymd(for_date)
        (st / "latch.json").write_text(json.dumps({"active": True, "triggered_as_of": trig}), encoding="utf-8")
        # 팔 것이 있어야 분기가 실제로 도는지 보인다 — 합성 체결 1건
        (st / "fills.jsonl").write_text(json.dumps(
            {"fill_id": "seed|5", "order_id": "seed", "code": CODES[0], "side": "BUY", "qty": 5,
             "amount": 5 * 70000, "trade_date": trig, "observed_at": f"{trig[:4]}-{trig[4:6]}-{trig[6:]}T10:00:00"}
        ) + chr(10), encoding="utf-8")
    return st


def _parse_report(text: str, state_dir: Path) -> tuple:
    """`main()` 이 찍은 보고서를 꺼낸다. **stdout 전체를 통째로 파싱하지 않는다.**

    [2026-09-22 실측] 종전에는 `json.loads(stdout)` 이었다. 잡소리 한 줄만 섞이면
      네 분기가 전부 `JSONDecodeError` 로 FAIL 이 됐다 — 실제로 한 번 그렇게 났고
      상태 로그 4개는 전부 정상이었다(= 분기는 옳았는데 판정이 틀렸다).
      stdout 은 우리 것만 나오는 통로가 아니다. 그래서 두 겹으로 읽는다.
    """
    text = text or ""
    try:
        return json.loads(text), None                    # 잡소리가 없으면 이게 정상 경로다
    except ValueError:
        pass
    lines = text.splitlines()                            # 보고서는 여러 줄로 예쁘게 찍힌다.
    for i in range(len(lines) - 1, -1, -1):              # 마지막 '{' 부터 끝까지 다시 붙여 본다
        if lines[i].lstrip().startswith("{"):
            try:
                return json.loads("\n".join(lines[i:])), "stdout 앞에 잡소리가 섞였다"
            except ValueError:
                continue
    try:                                                 # 그래도 못 읽으면 배치가 남긴 기록에서
        rows = [json.loads(x) for x in (state_dir / "daily_log.jsonl").read_text(
            encoding="utf-8").splitlines() if x.strip()]
        if rows:
            return rows[-1], "stdout 파싱 실패 — daily_log 마지막 줄로 대체"
    except OSError:
        pass
    return {}, "stdout·daily_log 둘 다 못 읽었다"


def _prev_ymd(ymd: str) -> str:
    d = dt.datetime.strptime(ymd, "%Y%m%d") - dt.timedelta(days=1)
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d.strftime("%Y%m%d")


def run(out_dir: Path, for_date: str) -> dict:
    results = []
    for action, label, regime, latch_when in SCENARIOS:
        tag = f"_{latch_when}" if latch_when else ""
        st = _seed(out_dir, action, regime, for_date, latch_when, tag)
        kis = SimKIS()
        if latch_when:
            kis.hold = {CODES[0]: 5}
        real_make = K.make_client
        K.make_client = lambda *a, **k: kis            # daily_ops 가 들고 있는 모듈을 바꾼다
        assert D.K.make_client is not real_make, "패치가 안 먹었다 — 실계좌로 돌 뻔했다"
        argv = sys.argv
        sys.argv = ["daily_ops.py", "morning", "--state-dir", str(st)]
        buf = io.StringIO()
        rc, rep, note = None, {}, None
        try:
            with redirect_stdout(buf):
                rc = D.main()
            rep, note = _parse_report(buf.getvalue(), st)
        except SystemExit as e:                         # main() 이 SystemExit 로 끝나는 경우
            rc = int(getattr(e, "code", 1) or 0)
            rep, note = _parse_report(buf.getvalue(), st)
        except Exception as e:                          # 터지면 통과가 아니다
            rep = {"status": "EXCEPTION", "reasons": [f"{type(e).__name__}: {e}"]}
        finally:
            K.make_client = real_make
            sys.argv = argv

        submitted = len(getattr(kis, "orders", []))
        # 발동 당일은 STOP 이 정답, 다음 거래일은 OK 가 정답
        want_stop = latch_when == "same_day"
        results.append({
            "next_action": action, "label": label, "rc": rc,
            "status": rep.get("status"), "action_taken": rep.get("action"),
            "reasons": rep.get("reasons"),
            "planned_sell": (rep.get("sell") or {}).get("orders"),
            "planned_buy": (rep.get("buy") or {}).get("orders"),
            "submitted_to_broker": submitted,
            "branch_ok": rep.get("action") == action,
            "expected": "STOP" if want_stop else "OK",
            "outcome_ok": (rep.get("status") == "STOP") if want_stop else (rep.get("status") in ("OK", "STANDBY")),
            "no_submit_ok": submitted == 0,
        })
        if note:                                        # 대체 경로를 썼으면 감추지 않는다
            results[-1]["report_note"] = note
        if not results[-1]["branch_ok"] or not results[-1]["outcome_ok"]:
            results[-1]["stdout_raw"] = buf.getvalue()[-2000:]   # 실패는 진단 가능해야 한다

    ok = all(r["branch_ok"] and r["no_submit_ok"] and r["outcome_ok"] for r in results)
    return {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "for_date": for_date,
        "entry_point": "daily_ops.main() — CLI 인자 경로 (함수 직접 호출 아님)",
        "ops_config": str(D.DEFAULT_OPS_CFG),
        "client": "SimKIS (가짜). 실계좌 경로는 증거 2(예약 작업 자체 실행)가 덮는다",
        "not_covered": ["kis_adapter.make_client() 자체", "실제 호가·잔고"],
        "scenarios": results,
        "verdict": "PASS" if ok else "FAIL",
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="아침 배치 분기 3종 예행(발주 없음)")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--for-date", default=dt.datetime.now().strftime("%Y%m%d"))
    ap.add_argument("--write-evidence", type=Path, help="증거 JSON 경로")
    a = ap.parse_args(argv)
    a.out_dir.mkdir(parents=True, exist_ok=True)
    rec = run(a.out_dir, a.for_date)
    print(json.dumps(rec, ensure_ascii=False, indent=2))
    if a.write_evidence:
        a.write_evidence.parent.mkdir(parents=True, exist_ok=True)
        a.write_evidence.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        print("\n증거:", a.write_evidence)
    return 0 if rec["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
