# -*- coding: utf-8 -*-
"""with_final_score 사이드카를 날짜별로 보존하고, 격리된 축의 값을 매니페스트에 남긴다.

왜 필요한가 (2026-08-22, PLANS 57):
  2026-08-20 (80) 이 final_score 를 8축 -> 2축으로 줄이면서 나머지 6축을
  **제거가 아니라 격리**했다. 이유가 "제거하면 나중에 검정할 데이터도 사라진다" 였다.

  그런데 격리한 축들이 실려 있는 파일 `candidates_latest_data.with_final_score.csv` 는
  **최신 1개만 존재한다.** 매일 덮어써진다. 즉 검정할 데이터를 남기겠다고 컬럼은 남겼는데
  **그 컬럼이 담긴 파일에는 이력이 없다.**

  실측(2026-08-22): base 후보는 `.bak_<YMD>_<HHMMSS>` 로 1,252개 / 160일치가 쌓여 있는데,
  그 파일들은 40컬럼이라 `final_score_w_*` 와 `candidate_origin_hybrid` 가 아예 없다.
  축별 기여도를 시계열로 잴 수 있는 데이터가 **하루치도 없다.**

무엇을 하나
  1. 내용이 바뀌었을 때만 스냅샷을 뜬다(해시 비교). 매 사이클 불러도 파일이 안 불어난다
  2. 매니페스트에 축 요약을 남긴다 - CSV 가 정리돼도 축 시계열은 남는다
  3. 선언된 축과 실제 반영 축이 어긋나면 표시한다

읽기 전용 소스, 로컬 쓰기만 한다.

사용
  python tools/archive_final_score_snapshot.py
  python tools/archive_final_score_snapshot.py --force     # 내용 같아도 뜬다
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "2_Logs"
SRC = LOG_DIR / "candidates_latest_data.with_final_score.csv"
ARCHIVE = LOG_DIR / "final_score_archive"
MANIFEST = ARCHIVE / "manifest.csv"

# (80) 이 격리한 축들. 값은 계속 산출되지만 final_score 에는 반영되지 않는다.
AXIS_COLS = [
    "news_score", "policy_score", "forecast_score", "fundamental_score_01",
    "fundamental_prereflection_score", "fundamental_prereflection_adjustment",
    "execution_lob_adjustment", "sector_score", "medium_news_adjustment",
]
WEIGHT_COLS = [
    "final_score_w_news", "final_score_w_policy", "final_score_w_forecast",
    "final_score_w_fundamental_quality", "final_score_w_fundamental_prereflection",
    "final_score_w_execution_lob_adjustment",
]

MANIFEST_COLS = [
    "snapshot_ts", "signal_date", "rows", "rows_news_only", "cols", "sha256_16",
    "axis_mode", "axis_source", "final_score_min", "final_score_max",
    "base_min", "base_max", "reconstruct_max_err", "reconstruct_match_rows",
    "archived_file",
] + [f"{c}__nonnull" for c in AXIS_COLS] + [f"{c}__median" for c in AXIS_COLS] + WEIGHT_COLS


# [2026-08-24] 중복제거를 무력화하던 시각 파생 컬럼.
#
# 파일 전체를 해싱하면 이 세 컬럼 때문에 매 실행 해시가 달라져 [SKIP] 이 한 번도 걸리지 않았다.
# 실측: manifest 288행의 sha256_16 이 288개 전부 다른데, 연속 두 스냅샷을 비교하면
# 다른 컬럼이 아래 셋뿐이고 final_score / final_score_base / 축 값은 전부 동일했다.
# 그 결과 휴장일(2026-08-23 일요일)에만 235건이 쌓였다.
#
# 제외는 **해시 판정에서만** 한다. 파일에는 그대로 저장된다.
# 상세: docs/exec-plans/active/20260824_archive_dedup_time_derived.md
TIME_DERIVED_COLS = (
    "news_freshest_age_hours",     # 경과 시간. 매 실행 증가한다
    "news_reason",                 # 위 값이 문자열 안에 박혀 있다
    "news_topic_l3_judgment_id",   # 실행 시각이 ID 에 박혀 있다
)


def _sha16(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _content_sha16(path: Path) -> str:
    """시각 파생 컬럼을 제외한 내용으로 해시를 만든다.

    실패하면 파일 전체 해시로 폴백한다 - 그 경우 동작은 종전과 같다(과잉 저장).
    **저장을 건너뛰는 쪽으로는 절대 폴백하지 않는다.**
    """
    try:
        import pandas as pd

        df = pd.read_csv(path, dtype=str, low_memory=False)
        drop = [c for c in TIME_DERIVED_COLS if c in df.columns]
        if drop:
            df = df.drop(columns=drop)
        payload = df.to_csv(index=False).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()[:16]
    except Exception as e:  # pragma: no cover - 폴백 경로
        print(f"[WARN] content hash 실패({type(e).__name__}), 파일 전체 해시로 폴백")
        return _sha16(path)


def _last_hash() -> str:
    if not MANIFEST.exists():
        return ""
    try:
        rows = list(csv.DictReader(MANIFEST.open("r", encoding="utf-8-sig", newline="")))
        return rows[-1].get("sha256_16", "") if rows else ""
    except Exception:
        return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    if not SRC.exists():
        print(f"[SKIP] 소스 없음: {SRC}")
        return 0

    # [2026-08-24] 시각 파생 컬럼을 뺀 내용 해시로 비교한다. 그 전에는 시계 때문에 한 번도 안 걸렸다.
    digest = _content_sha16(SRC)
    if not args.force and digest == _last_hash():
        print(f"[SKIP] 내용 동일 (content sha {digest}) - 시각 파생 컬럼 제외 비교")
        return 0

    import pandas as pd  # 무거우므로 실제로 뜰 때만 로드

    df = pd.read_csv(SRC, low_memory=False)
    ts = datetime.now()
    sig_date = ""
    for col in ("date_yyyymmdd", "date"):
        if col in df.columns:
            s = df[col].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
            s = s[s.str.len() == 8]
            if len(s):
                sig_date = str(s.max())
                break

    ARCHIVE.mkdir(parents=True, exist_ok=True)
    out = ARCHIVE / f"final_score_{sig_date or ts.strftime('%Y%m%d')}_{ts.strftime('%H%M%S')}.csv"
    shutil.copy2(SRC, out)

    def num(col: str):
        return pd.to_numeric(df[col], errors="coerce") if col in df.columns else pd.Series(dtype=float)

    # 선언된 축 vs 실제 반영 - 생산 공식을 그대로 재현해 확인한다.
    # [2026-08-24] 재현식이 08-20 의 가산->승산 변경을 따라가지 않아 스냅샷 286개 내내
    # 20/22 로 실패하고 있었다. 불일치 2행은 medium_news_adjustment 비영 행이었다.
    # 생산 공식은 final_score_merge_daily.py:2264 -
    #   final_score = clip((base + execution_lob_adjustment) * (1 + medium_news), 0, 1)
    base, fs = num("final_score_base").fillna(0.0), num("final_score").fillna(0.0)
    lob = num("execution_lob_adjustment").fillna(0.0)
    mn = num("medium_news_adjustment").fillna(0.0).clip(lower=-0.95, upper=0.95)
    recon = ((base + lob) * (1.0 + mn)).clip(lower=0.0, upper=1.0)
    err = (recon - fs).abs() if len(base) and len(fs) else pd.Series([float("nan")])
    max_err = float(err.max()) if len(err) else float("nan")
    match_rows = int((err < 1e-4).sum()) if len(err) else 0

    origin = df["candidate_origin_hybrid"].astype(str) if "candidate_origin_hybrid" in df.columns else pd.Series([], dtype=str)
    rec: Dict[str, Any] = {
        "snapshot_ts": ts.isoformat(timespec="seconds"),
        "signal_date": sig_date,
        "rows": int(len(df)),
        "rows_news_only": int((origin == "NEWS_ONLY").sum()) if len(origin) else "",
        "cols": int(len(df.columns)),
        "sha256_16": digest,
        "axis_mode": (df["final_score_axis_mode"].dropna().iloc[0] if "final_score_axis_mode" in df.columns and df["final_score_axis_mode"].notna().any() else ""),
        "axis_source": (df["final_score_source"].dropna().iloc[0] if "final_score_source" in df.columns and df["final_score_source"].notna().any() else ""),
        "final_score_min": float(fs.min()) if len(fs) else "",
        "final_score_max": float(fs.max()) if len(fs) else "",
        "base_min": float(base.min()) if len(base) else "",
        "base_max": float(base.max()) if len(base) else "",
        "reconstruct_max_err": round(max_err, 8) if max_err == max_err else "",
        "reconstruct_match_rows": f"{match_rows}/{len(df)}",
        "archived_file": out.name,
    }
    for c in AXIS_COLS:
        s = num(c)
        rec[f"{c}__nonnull"] = int(s.notna().sum()) if len(s) else 0
        rec[f"{c}__median"] = float(s.median()) if len(s) and s.notna().any() else ""
    for c in WEIGHT_COLS:
        s = num(c)
        rec[c] = float(s.dropna().iloc[0]) if len(s) and s.notna().any() else ""

    new_file = not MANIFEST.exists()
    with MANIFEST.open("a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=MANIFEST_COLS)
        if new_file:
            w.writeheader()
        w.writerow({k: rec.get(k, "") for k in MANIFEST_COLS})

    print(f"[SNAP] {out.name}  {rec['rows']}행 {rec['cols']}컬럼  sha {digest}")
    print(f"       signal_date={sig_date}  NEWS_ONLY={rec['rows_news_only']}")

    # [2026-08-24] asof 정체를 알린다. 차단하지 않고 로그로만 남긴다.
    #
    # 후보가 2026-08-20 에 멈춘 것을 아무도 몰랐고, 08-24 장중 매매 차단으로야 드러났다
    # (.agent/PLANS.md (80)B). 같은 signal_date 가 며칠째 이어지면 그 자체가 신호다.
    try:
        stale_days = int(str(os.environ.get("FINAL_SCORE_ASOF_STALE_DAYS", "2")).strip() or "2")
        if sig_date and len(sig_date) == 8:
            asof_d = datetime.strptime(sig_date, "%Y%m%d").date()
            age = (ts.date() - asof_d).days
            if age >= stale_days:
                print(
                    f"       [ASOF_STALE] signal_date={sig_date} 가 {age}일째다 "
                    f"(임계 {stale_days}일). 후보 생성이 멈춰 있을 수 있다 - 배치를 확인할 것"
                )
    except Exception:
        pass
    print(f"       axis_mode={rec['axis_mode']}")
    print(f"       재현 final_score = (base + exec_lob) x (1 + medium_news) : "
          f"{rec['reconstruct_match_rows']} (최대오차 {rec['reconstruct_max_err']})")

    # 선언과 실제가 어긋나면 표시한다. (80) 은 6축을 격리했는데 source 문자열은 갱신되지 않았다.
    src_txt = str(rec["axis_source"]).upper()
    claimed_on = [k for k in ("NEWS_ON", "FORECAST_ON", "PREREF_ON") if k in src_txt]
    if claimed_on and match_rows >= max(1, int(len(df) * 0.9)):
        print(f"       [NOTE] source 는 {claimed_on} 이라 하지만 산술상 반영되지 않는다 "
              f"(2축 격리, PLANS 80). 문자열이 갱신되지 않은 것이다")
    return 0


if __name__ == "__main__":
    sys.exit(main())
