# -*- coding: utf-8 -*-
"""2026-09-10 비용·사다리 수리의 **불변식을 재실행 가능한 형태로** 검증한다.

## 왜 필요한가

그날 검증은 전부 터미널 출력으로만 존재했다. PLANS 에는 결과 문장만 남고
**재현할 수 있는 산출물이 없었다.** 그러면 나중에 "정말 맞았나" 를 다시 물을 때
처음부터 다시 손으로 확인해야 한다.

이 도구는 그날 손으로 한 확인을 **하나씩 assert 로 바꾼 것**이다.
읽기 전용이고 주문을 내지 않는다. 패널을 읽지 않아 몇 초면 끝난다.

## 무엇을 검증하나

```
COST-1  생산 경로(load_config -> build_cost_profile) 왕복 = 0.00400
COST-2  DEFAULT_CONFIG 단독도 0.00400          (설정에서 키가 빠져도 안전한가)
COST-3  빈 설정 {} / None 값도 0.00400          (최후 기본값)
COST-4  fee_pct 는 0.0 이어야 한다 (브로커 실측: 수수료 0)
COST-5  `or` 함정 부재 - 설정의 0.0 이 삼켜지지 않는가 (C7)
COST-6  DEFAULT_FEE 와 라이브 실효 왕복이 일치 (COST_DRIFT 무경고 조건)
TIER-1  티어 슬리피지가 설정 실측치를 그대로 쓴다 (C8)
TIER-2  티어 값이 없으면 **평탄 슬리피지로** 떨어진다 (코드 상수 0.005/0.010 아님)
LAD-1   완화 사다리 rs_lim 이 단조 감소 (D6)
LAD-2   사다리가 단일 출처 - 세 모듈이 같은 함수 객체
LAD-3   rs_lim 이 양수여도 아래로 간다 (부호 무관)
PARAM-1 value_min / v_accel_lim 이 코드 선언 기본값과 같다 (2026-09-10 되돌림 유지)
PARAM-2 HPO 자동 덮어쓰기가 promoted AND gate_ok 로 막혀 있다
LAD-4   완화 방향이 선언으로 존재한다 (수식에 숨지 않는다)
LAD-5   결함을 되살리면 사다리 불변식이 잡는다
GUARD-1 결함을 되살리면 COST_DRIFT 가 잡는다 (재발 방지가 실제로 작동)
D4-1    지표 진단 기본 단계 = auto (생산 단계를 잰다)
D4-2    산출물이 어느 단계에서 쟀는지 스스로 밝힌다
```

## 저장

```
2_Logs/verify_cost_ladder_invariants_latest.json   최신 1회분 (덮어씀)
2_Logs/verification_evidence_ledger.jsonl          **append-only, 이름에 날짜 없음**
```
날짜 없는 이름이라 `tools/log_cleanup_30d.py` 대상이 아니다
(`[[feedback_dated_artifacts_get_deleted]]`).

[2026-09-10] 신설. PLANS (322)(324)(326)(327)(328)
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

LOG = ROOT / "2_Logs"
LATEST = LOG / "verify_cost_ladder_invariants_latest.json"
LEDGER = LOG / "verification_evidence_ledger.jsonl"

EXPECT_ROUNDTRIP = 0.00400
EPS = 1e-9


class Check:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def add(self, cid: str, desc: str, ok: bool, got, want=None, note: str = "") -> None:
        self.rows.append({
            "id": cid, "desc": desc, "ok": bool(ok),
            "got": got, "want": want, "note": note,
        })

    def run(self, cid: str, desc: str, fn, want=None, note: str = "") -> None:
        try:
            ok, got = fn()
        except Exception as exc:  # 검증 자체가 죽는 것도 실패다
            self.add(cid, desc, False, "%s: %s" % (type(exc).__name__, exc), want, note)
            return
        self.add(cid, desc, ok, got, want, note)


def _rt(profile) -> float:
    return float(profile.fee_pct) * 2.0 + float(profile.slippage_pct) * 2.0 + float(profile.sell_tax_pct)


def main() -> int:
    ap = argparse.ArgumentParser(description="비용·사다리 불변식 검증 (읽기 전용)")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    from paper_engine.config import load_config, DEFAULT_CONFIG
    from pricing_engine import build_cost_profile, resolve_slippage_pct_tiered
    import pricing_engine as PE
    import generate_candidates_v41_1 as GEN
    import optimize_params_v41_1 as OPT
    import report_backtest_v41_1 as RBT

    cfg = load_config()
    c = Check()

    # --- COST ---
    c.run("COST-1", "생산 경로 왕복 = 0.400%",
          lambda: (abs(_rt(build_cost_profile(cfg)) - EXPECT_ROUNDTRIP) < EPS,
                   round(_rt(build_cost_profile(cfg)), 6)), EXPECT_ROUNDTRIP)
    c.run("COST-2", "DEFAULT_CONFIG 단독 왕복 = 0.400%",
          lambda: (abs(_rt(build_cost_profile(DEFAULT_CONFIG)) - EXPECT_ROUNDTRIP) < EPS,
                   round(_rt(build_cost_profile(DEFAULT_CONFIG)), 6)), EXPECT_ROUNDTRIP,
          "설정 파일에서 비용 키가 빠져도 맞는 값이 나와야 한다")
    c.run("COST-3", "빈 설정 / None 값도 0.400%",
          lambda: (abs(_rt(build_cost_profile({})) - EXPECT_ROUNDTRIP) < EPS
                   and abs(_rt(build_cost_profile({"fee_pct": None, "sell_tax_pct": None})) - EXPECT_ROUNDTRIP) < EPS,
                   [round(_rt(build_cost_profile({})), 6),
                    round(_rt(build_cost_profile({"fee_pct": None, "sell_tax_pct": None})), 6)]),
          EXPECT_ROUNDTRIP)
    c.run("COST-4", "fee_pct = 0.0 (브로커 실측: 수수료 0)",
          lambda: (abs(float(build_cost_profile(cfg).fee_pct)) < EPS,
                   float(build_cost_profile(cfg).fee_pct)), 0.0,
          "tr_id TTTC8715R: 매도대금 88,730 / 수수료 0 / 제세금 175")
    c.run("COST-5", "`or` 함정 부재 - 설정의 0.0 이 삼켜지지 않는다 (C7)",
          lambda: (abs(float(build_cost_profile({"fee_pct": 0.0, "slippage_pct": 0.001,
                                                 "sell_tax_pct": 0.002}).fee_pct)) < EPS,
                   float(build_cost_profile({"fee_pct": 0.0, "slippage_pct": 0.001,
                                             "sell_tax_pct": 0.002}).fee_pct)), 0.0,
          "종전 `float(cfg.get('fee_pct',0.005) or 0.005)` 는 0.005 를 냈다")
    c.run("COST-6", "DEFAULT_FEE == 라이브 실효 왕복 (COST_DRIFT 무경고 조건)",
          lambda: (abs(float(OPT.DEFAULT_FEE) - _rt(build_cost_profile(cfg))) < EPS,
                   [float(OPT.DEFAULT_FEE), round(_rt(build_cost_profile(cfg)), 6)]))

    # --- TIER (C8) ---
    base_slip = float(build_cost_profile(cfg).slippage_pct)
    tier_expect = {"large": (2e12, 0.00139), "mid": (5e11, 0.00178), "small": (1e10, 0.00238)}
    def _tier_live():
        got = {k: round(resolve_slippage_pct_tiered(mc, cfg, base_slip), 6) for k, (mc, _e) in tier_expect.items()}
        ok = all(abs(got[k] - e) < EPS for k, (_m, e) in tier_expect.items())
        return ok, got
    c.run("TIER-1", "티어가 설정 실측치를 그대로 쓴다 (C8)", _tier_live,
          {k: e for k, (_m, e) in tier_expect.items()})

    def _tier_fallback():
        bare = {"tiered_slippage": {"enabled": True}}
        got = {k: round(resolve_slippage_pct_tiered(mc, bare, 0.001), 6) for k, (mc, _e) in tier_expect.items()}
        return all(abs(v - 0.001) < EPS for v in got.values()), got
    c.run("TIER-2", "티어 값이 없으면 평탄 슬리피지로 떨어진다 (코드 상수 아님)", _tier_fallback, 0.001,
          "종전엔 0.003 / 0.005 / 0.010 이라는 미보정 코드 상수로 떨어졌다")

    # --- LADDER (D6) ---
    stable = GEN._normalize_params(
        json.load(io.open(ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json", encoding="utf-8-sig")))

    def _ladder_mono():
        lad = GEN._relax_ladder(dict(stable))
        vals = [(n, float(p["rs_lim"])) for n, p in lad]
        ok = all(vals[i][1] <= vals[i - 1][1] + EPS for i in range(1, len(vals)))
        return ok, [[n, round(v, 5)] for n, v in vals]
    c.run("LAD-1", "완화 사다리 rs_lim 단조 감소 (D6)", _ladder_mono, "비증가",
          "종전엔 L0 -0.0400 -> L6 -0.0278 로 **올라갔다**(더 엄격)")

    c.run("LAD-2", "사다리 단일 출처 - 세 모듈이 같은 함수 객체",
          lambda: (GEN._relax_ladder is OPT._relax_ladder is RBT._relax_ladder,
                   "generate/optimize/report 동일 객체"), True)

    def _ladder_sign():
        # rs_lim 이 양수여도 아래로 가야 한다
        pos = dict(stable); pos["rs_lim"] = 0.30
        vals = [float(p["rs_lim"]) for _n, p in GEN._relax_ladder(pos)]
        return all(vals[i] <= vals[i - 1] + EPS for i in range(1, len(vals))), [round(v, 5) for v in vals]
    c.run("LAD-3", "rs_lim 이 양수여도 아래로 간다 (부호 무관)", _ladder_sign, "비증가")

    # --- GUARD: 결함을 되살려 잡히는지 ---
    def _guard_catches():
        orig = PE.build_cost_profile
        captured: list[str] = []

        def broken(cfg_in, profile_name=None):            # C7 당시 코드
            return PE.CostProfile(
                fee_pct=float(cfg_in.get("fee_pct", 0.005) or 0.005),
                slippage_pct=float(cfg_in.get("slippage_pct", 0.001) or 0.001),
                sell_tax_pct=float(cfg_in.get("sell_tax_pct", 0.0) or 0.0),
            )
        import builtins
        real_print = builtins.print
        try:
            PE.build_cost_profile = broken
            builtins.print = lambda *aa, **kk: captured.append(" ".join(str(x) for x in aa))
            OPT._warn_if_fee_diverges_from_live()
        finally:
            PE.build_cost_profile = orig
            builtins.print = real_print
        hit = [x for x in captured if "COST_DRIFT" in x]
        return (len(hit) >= 1), hit[:2]
    c.run("GUARD-1", "결함을 되살리면 COST_DRIFT 가 잡는다 (재발 방지 작동)", _guard_catches, ">=1건 경고")

    def _guard_silent():
        captured: list[str] = []
        import builtins
        real_print = builtins.print
        try:
            builtins.print = lambda *aa, **kk: captured.append(" ".join(str(x) for x in aa))
            OPT._warn_if_fee_diverges_from_live()
        finally:
            builtins.print = real_print
        hit = [x for x in captured if "COST_DRIFT" in x]
        return (len(hit) == 0), hit
    c.run("GUARD-2", "정상 상태에서는 COST_DRIFT 가 조용하다", _guard_silent, "0건")

    # --- PARAM: 되돌린 값이 유지되는가 (2026-09-10) ---
    def _reverted_params():
        got = {"value_min": float(stable["value_min"]), "v_accel_lim": float(stable["v_accel_lim"])}
        want = {"value_min": float(GEN.DEFAULT_PARAMS["value_min"]),
                "v_accel_lim": float(GEN.DEFAULT_PARAMS["v_accel_lim"])}
        ok = all(abs(got[k] - want[k]) < 1e-9 for k in want)
        return ok, {"got": got, "want": want}
    c.run("PARAM-1", "value_min / v_accel_lim 이 **코드 선언 기본값**과 같다", _reverted_params,
          "DEFAULT_PARAMS 와 일치",
          "2026-09-10 되돌림. 종전 1,550억 / 6.6 은 promoted=False 인 HPO 산물이었고 "
          "as_of 2026-08-14 는 섹터 유니온 폴백이 죽은 채 튜닝된 시점이다. "
          "다시 어긋나면 누가 되돌렸거나 HPO 가 덮어쓴 것이다")

    def _promotion_write_blocked():
        # optimize 가 stable 을 덮어쓰려면 promoted AND gate_ok 여야 한다.
        src = io.open(ROOT / "optimize_params_v41_1.py", encoding="utf-8").read()
        has = "write_allowed = bool(promoted and gate_ok)" in src
        guard = "if not promoted:" in src and "_persist_promoted_stable" in src
        return (has and guard), {"write_allowed_cond": has, "persist_guard": guard}
    c.run("PARAM-2", "HPO 자동 덮어쓰기가 promoted AND gate_ok 로 막혀 있다",
          _promotion_write_blocked, True,
          "이 조건이 사라지면 되돌린 값이 조용히 덮어써질 수 있다")

    # --- B: 사다리 불변식이 실제로 작동하는가 ---
    def _ladder_guard_present():
        has = hasattr(GEN, "RELAX_DIRECTION") and hasattr(GEN, "_assert_ladder_monotone")
        n = len(getattr(GEN, "RELAX_DIRECTION", {}) or {})
        return (has and n >= 9), {"RELAX_DIRECTION_n": n, "assert_fn": has}
    c.run("LAD-4", "완화 방향이 **선언**으로 존재한다 (수식에 숨지 않는다)",
          _ladder_guard_present, ">=9개 선언 + 검사 함수",
          "종전엔 방향이 각 수식 안에 있어 rs 역전이 6주간 안 보였다")

    def _ladder_guard_catches():
        # D6 결함을 되살려 불변식이 잡는지 본다. 평소 조용한 것만으로는 검증이 안 된다.
        orig = GEN._relax_rs_lim
        try:
            GEN._relax_rs_lim = lambda v, frac, floor=-0.10: max(float(v) * (1.0 - frac), floor)
            try:
                GEN._relax_ladder(dict(stable))
                return False, "결함을 넣었는데 통과했다 - 불변식이 무력하다"
            except RuntimeError as e:
                return True, str(e).replace(chr(10), " ")[:160]
        finally:
            GEN._relax_rs_lim = orig
    c.run("LAD-5", "결함을 되살리면 사다리 불변식이 잡는다", _ladder_guard_catches, "RuntimeError")

    # --- D4: 진단이 생산 단계를 재는가 ---
    def _diag_default_auto():
        import ast
        src = io.open(ROOT / "tools" / "indicator_factor_diagnostic.py", encoding="utf-8-sig").read()
        tree = ast.parse(src)
        # ap.add_argument("--gate-level", ..., default="auto")
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "add_argument"
                    and node.args and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == "--gate-level"):
                for kw in node.keywords:
                    if kw.arg == "default" and isinstance(kw.value, ast.Constant):
                        return (kw.value.value == "auto"), kw.value.value
        return False, "--gate-level 인자를 못 찾음"
    c.run("D4-1", "지표 진단 기본 단계 = auto (생산 단계를 잰다)", _diag_default_auto, "auto",
          "종전엔 stable_params 그대로 = L0 만 쟀다. 생산은 L0 을 한 번도 안 쓴다")

    def _diag_reports_level():
        latest = sorted((ROOT / "2_Logs").glob("indicator_diag_summary_*.json"))
        if not latest:
            return False, "요약 파일 없음"
        d = json.loads(io.open(latest[-1], encoding="utf-8-sig").read())
        gl = d.get("gate_level")
        ok = isinstance(gl, dict) and bool(gl.get("levels_used"))
        return ok, {"file": latest[-1].name, "gate_level": gl}
    c.run("D4-2", "산출물이 **어느 단계에서 쟀는지** 스스로 밝힌다", _diag_reports_level,
          "gate_level.levels_used 존재",
          "종전 산출물은 L0 로 쟀으면서 그 사실을 어디에도 적지 않았다")

    n_ok = sum(1 for r in c.rows if r["ok"])
    n_bad = len(c.rows) - n_ok
    payload = {
        "checked_at": dt.datetime.now().isoformat(timespec="seconds"),
        "expect_roundtrip": EXPECT_ROUNDTRIP,
        "passed": n_ok, "failed": n_bad, "total": len(c.rows),
        "all_ok": n_bad == 0,
        "checks": c.rows,
    }
    LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with io.open(LEDGER, "a", encoding="utf-8") as f:
        f.write(json.dumps({k: payload[k] for k in
                            ("checked_at", "passed", "failed", "total", "all_ok")} |
                           {"failed_ids": [r["id"] for r in c.rows if not r["ok"]]},
                           ensure_ascii=False) + chr(10))

    if not a.quiet:
        for r in c.rows:
            print("  [%s] %-8s %s" % ("OK" if r["ok"] else "!!", r["id"], r["desc"]))
            if not r["ok"]:
                print("        got=%r want=%r" % (r["got"], r["want"]))
        print()
        print("통과 %d / 실패 %d / 전체 %d" % (n_ok, n_bad, len(c.rows)))
        print("저장: %s" % LATEST.name)
        print("      %s  (append-only, 이름에 날짜 없음)" % LEDGER.name)
    return 0 if n_bad == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
