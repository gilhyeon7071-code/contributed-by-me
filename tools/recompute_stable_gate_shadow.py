# -*- coding: utf-8 -*-
"""현직 파라미터로 게이트 입력을 재계산해 **그림자 판정**을 낸다 (O5-2, 2026-08-31 신설).

왜 있나
    utils/stable_params_gate.py 는 가격 데이터를 받지 않아 현직을 재계산할 수 없고,
    stable_params_v41_1.json 에 **저장된** windows 를 읽어 판정한다.
    그래서 파라미터가 나중에 바뀌면 옛 파라미터의 성과로 새 파라미터를 심사한다.
    실제로 windows as_of=2026-08-14 인데 파라미터는 2026-08-20 에 손편집됐고
    그 사이 windows 는 재계산되지 않았다.
    이것은 2026-08-20 에 "다섯 겹 중 1번: 게이트가 현직을 재계산하지 않는다" 로
    기록된 뒤 미해결로 남아 있던 결함이다.

무엇을 하는가
    현직 파라미터 + 현재 데이터로 windows 를 재계산하고,
    **같은 게이트 함수**에 그 입력을 넣어 "현직 기준 판정" 을 산출한다.
    저장 입력 기준 판정과 나란히 적어 차이를 매일 보이게 한다.

무엇을 하지 않는가 - 중요
    **stable_params_v41_1.json 을 쓰지 않는다.** 읽기만 한다.
    재계산 결과를 그 파일에 반영하면 게이트가 즉시 FAIL 이 되고
    후보 0 -> 매매 정지가 된다. 그때 무엇을 기본 동작으로 할지는 아직 결정되지 않았다
    (docs/references/GATE_JUDGMENT_FORM.md R1: (a)차단 /(b)통과+경고 /(c)축소가동).
    그 결정이 내려지기 전까지 이 도구는 **관측만** 한다.
    optimize_params_v41_1 의 승격·영속화 경로는 호출하지 않는다
    (eval_params / simulate_window 에 쓰기 호출이 없음을 2026-08-31 에 확인했다).

한계 - 결과를 인용할 때 함께 적을 것
    optimizer 세계는 생산과 다르다. hold=10 / max_pos=20 / fee=optimize_params_v41_1.DEFAULT_FEE
    (2026-09-10 현재 0.00400. 2026-09-09 에 0.00358 에서 바뀌었다) 로 시뮬레이션하며
    생산은 hold=13 / max_pos=6 이다. 따라서 이 숫자는 **실제 성과가 아니라**
    "게이트가 쓰는 잣대 위에서 현직 파라미터가 받는 점수" 다.
    저장값도 같은 시뮬레이터에서 나왔으므로 저장값과의 비교는 정당하다.

사용
    python tools/recompute_stable_gate_shadow.py
    python tools/recompute_stable_gate_shadow.py --pinned      저장된 창 경계에 고정(비교용)
    python tools/recompute_stable_gate_shadow.py --out DIR     출력 경로 지정
반환 코드
    0 정상(판정 일치/불일치 무관)  2 데이터 부족  3 실행 실패
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import io
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

STABLE = ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json"
CONFIG = ROOT / "paper" / "paper_engine_config.json"
DEFAULT_OUT = ROOT / "2_Logs"

DROPNA = [
    "rs", "rs_slope", "v_accel", "stretch", "atr14_pct",
    "gap_next", "rsi14", "vol_close_corr20", "high_52w_gap", "listing_days",
]


def _optimizer_fee() -> float:
    """optimizer 가 실제로 쓰는 왕복 비용. 문자열에 박지 않는다."""
    try:
        import optimize_params_v41_1 as _O
        return float(_O.DEFAULT_FEE)
    except Exception:
        return float("nan")


def _log(t0, *a):
    print("[%6.1fs]" % (time.time() - t0), *a, flush=True)


def _live_params(O):
    """optimize_params_v41_1.main() 의 base 딕셔너리를 원문 그대로 실행해 만든다.

    전사 오류를 막기 위해 소스에서 해당 블록을 그대로 읽어 실행한다.
    이 블록이 곧 "옵티마이저가 현직 파라미터를 어떻게 읽는가" 의 정의다.
    """
    src = io.open(ROOT / "optimize_params_v41_1.py", encoding="utf-8").read().split("\n")
    i0 = next(i for i, l in enumerate(src) if l.strip().startswith("base = {"))
    i1 = next(i for i in range(i0, len(src)) if src[i] == "    }")
    block = "\n".join(l[4:] if l.startswith("    ") else l for l in src[i0:i1 + 1])
    ns = {
        "prev_stable": O._jload(O.OUT_DIR / "stable_params_v41_1.json"),
        "_safe_float": O._safe_float,
    }
    ns["gu_cfg"], ns["gd_cfg"] = O._load_paper_gap_policy()
    exec(block, ns)
    return ns["base"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pinned", action="store_true",
                    help="저장된 창 경계에 고정한다(저장값과 직접 비교할 때)")
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="출력 디렉터리")
    a = ap.parse_args()

    t0 = time.time()
    out_dir = Path(a.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    import pandas as pd
    import optimize_params_v41_1 as O
    from utils.stable_params_gate import evaluate_stable_params, load_stable_quality_gate

    live = json.load(io.open(STABLE, encoding="utf-8"))
    gate_cfg = load_stable_quality_gate(CONFIG)

    _log(t0, "load_data ...")
    df = O.load_data(O.BASE_DIR)
    df = df[df["date"] >= pd.Timestamp("2020-01-01")].copy()
    _log(t0, "compute_factors ...")
    df = O.compute_factors(df)
    df = df.dropna(subset=DROPNA).copy()
    if df.empty:
        print("[FAIL] 팩터 계산 후 유효 데이터가 없다")
        return 2
    _log(t0, "rows=%d codes=%d" % (len(df), df["price_history_key"].nunique()))

    if a.pinned:
        windows = [(pd.Timestamp(w["start"]), pd.Timestamp(w["end"]))
                   for w in live.get("windows", [])]
        wmode = "pinned_to_stored"
    else:
        windows = O.build_windows(df, O.YEARS_BACK)
        wmode = "rebuilt_today"
    if not windows:
        print("[FAIL] 창 구성 실패")
        return 2
    _log(t0, "windows=%d mode=%s" % (len(windows), wmode))

    params = _live_params(O)
    _log(t0, "eval_params ...")
    score, results = O.eval_params(df, windows, params, label="gate_shadow")
    rows = []
    for r in results:
        d = r.__dict__ if hasattr(r, "__dict__") else dict(r._asdict())
        rows.append({k: v for k, v in d.items() if k != "daily_rets"})
    metrics = O._fold_selection_metrics(results, score)

    # 같은 게이트 함수에 저장 입력 / 재계산 입력을 각각 넣는다
    stored_verdict = evaluate_stable_params(live, gate_cfg)
    shadow_src = copy.deepcopy(live)
    shadow_src["windows"] = rows
    shadow_src["selection_metrics"] = metrics
    shadow_src["best_score"] = score
    shadow_verdict = evaluate_stable_params(shadow_src, gate_cfg)

    def _wsummary(ws):
        """창 집합을 한 줄로 요약한다. 두 판정이 같은 질문에 답하는지 보려면 이게 필요하다."""
        ws = list(ws or [])
        if not ws:
            return {"n": 0, "splits": "", "first_start": None, "last_end": None}
        comp = []
        for w in ws:
            sp = str(w.get("split") or "")[:1]
            comp.append(sp)
        return {
            "n": len(ws),
            "splits": "".join(comp),
            "split_counts": {k: comp.count(k) for k in sorted(set(comp))},
            "first_start": str(ws[0].get("start"))[:10],
            "last_end": str(ws[-1].get("end"))[:10],
        }

    stored_ws = _wsummary(live.get("windows") or [])
    shadow_ws = _wsummary(rows)

    def pick(v):
        return {k: v.get(k) for k in (
            "ok", "reason", "oos_n_total", "oos_pf_weighted", "oos_worst_pf",
            "mean_pf_weighted", "all_n_total", "windows_stale",
            "windows_as_of", "params_edited_at")}

    payload = {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "window_mode": wmode,
        "elapsed_sec": round(time.time() - t0, 1),
        # [2026-09-10] fee 를 문자열에 박아두면 상수가 바뀌어도 산출물이 옛 값을 주장한다.
        #   실제로 그랬다 - 2026-09-09 에 DEFAULT_FEE 가 0.00358 -> 0.00400 으로 바뀌었는데
        #   이 줄은 0.00358 을 계속 적고 있었다. 실행 시점의 상수를 읽는다.
        "note": ("그림자 판정이다. stable_params 를 쓰지 않는다. "
                 "optimizer 세계(hold=10/max_pos=20/fee=%.5f)의 숫자이며 "
                 "생산(hold=13/max_pos=6)과 다르다. 실제 성과로 인용하지 말 것."
                 % _optimizer_fee()),
        "stored_verdict": pick(stored_verdict),
        "shadow_verdict": pick(shadow_verdict),
        "agree": bool(stored_verdict.get("ok")) == bool(shadow_verdict.get("ok")),
        # [2026-09-09] agree 만 보면 안 된다. 두 판정은 **같은 질문에 답하고 있지 않다.**
        #   실측 2026-09-09: 저장 창 경계는 08-12/08-13 앵커(2026-08-14 산출),
        #   그림자는 그날 앵커로 매일 밀린다(08-27 -> 09-07 로 이동한 것이 일별 산출물에 남아 있다).
        #   더 큰 차이는 **분할 정책**이다 - 저장 IIIIVOO(4IS/1VAL/2OOS) vs 현직 IIIVVOO(3IS/2VAL/2OOS).
        #   --pinned 는 경계만 고정하고 분할은 현직 코드가 다시 정하므로
        #   **어떤 모드로도 저장 판정을 재현할 수 없다.** 즉 agree=False 를
        #   "저장 게이트가 틀렸다" 의 증거로 쓰면 안 된다. 원인 귀속이 불가능하다.
        #   comparable 이 False 면 두 verdict 의 차이는 파라미터/데이터 때문만이 아니다.
        "stored_windows": stored_ws,
        "shadow_windows_summary": shadow_ws,
        "comparable": bool(
            stored_ws.get("splits") == shadow_ws.get("splits")
            and stored_ws.get("first_start") == shadow_ws.get("first_start")
            and stored_ws.get("last_end") == shadow_ws.get("last_end")
        ),
        "shadow_windows": rows,
        "shadow_selection_metrics": metrics,
        "shadow_score": score,
    }
    ymd = dt.datetime.now().strftime("%Y%m%d")
    p1 = out_dir / ("stable_gate_shadow_%s.json" % ymd)
    p2 = out_dir / "stable_gate_shadow_latest.json"
    for p in (p1, p2):
        io.open(p, "w", encoding="utf-8").write(
            json.dumps(payload, ensure_ascii=False, indent=1, default=str))

    sv, hv = payload["stored_verdict"], payload["shadow_verdict"]
    print()
    print("=== 게이트 판정 대조 (%s) ===" % wmode)
    print("  저장 입력  ok=%-5s oos_pf=%.4f mean_pf=%.4f oos_worst=%s"
          % (sv["ok"], sv["oos_pf_weighted"], sv["mean_pf_weighted"], sv["oos_worst_pf"]))
    print("  현직 재계산 ok=%-5s oos_pf=%.4f mean_pf=%.4f oos_worst=%s"
          % (hv["ok"], hv["oos_pf_weighted"], hv["mean_pf_weighted"], hv["oos_worst_pf"]))
    print("  창 집합    저장 %s %s~%s / 현직 %s %s~%s"
          % (stored_ws["splits"], stored_ws["first_start"], stored_ws["last_end"],
             shadow_ws["splits"], shadow_ws["first_start"], shadow_ws["last_end"]))
    if not payload["comparable"]:
        print("  [NOT_COMPARABLE] 두 판정은 같은 창·같은 분할을 쓰지 않는다.")
        print("                   판정 차이를 파라미터/데이터 탓으로 귀속할 수 없다.")
    if not payload["agree"]:
        print("  [DIVERGE] 저장 입력과 현직 재계산의 판정이 다르다")
        print("            shadow reason: %s" % hv["reason"])
    else:
        print("  [AGREE] 두 입력의 판정이 같다")
    print("  windows_stale(저장) =", sv["windows_stale"])
    print("  saved:", p1)

    # 안전 확인 - 이 도구는 stable_params 를 쓰지 않는다
    assert STABLE.exists(), "stable_params 가 사라졌다"
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        print("[FAIL] %s" % exc)
        raise SystemExit(3)
