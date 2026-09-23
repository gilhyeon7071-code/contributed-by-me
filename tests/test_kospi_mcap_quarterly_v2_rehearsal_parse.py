"""예행의 **판정 경로**가 stdout 잡소리에 무너지지 않는지 (2026-09-22 실측).

배경: 2026-09-22 11:19 에 예행이 FAIL 로 났는데 상태 로그 4개는 전부 정답이었다.
원인은 분기가 아니라 **판정 방법**이었다 — `json.loads(stdout 전체)` 라서
stdout 에 한 줄만 섞이면 네 분기가 전부 JSONDecodeError 로 실패했다.
33회 재실행이 전부 PASS 라 재현이 안 됐다(기계 단위 일회성 출력으로 보인다).
그래서 여기서는 **잡소리를 직접 넣어** 판정이 버티는지 고정한다.
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

V2 = Path(__file__).resolve().parents[1] / "paper" / "strategies" / "kospi_mcap_quarterly_v2"
sys.path.insert(0, str(V2 / "src"))
import morning_branch_rehearsal as R  # noqa: E402

REP = {"job": "morning", "status": "OK", "action": "NONE", "reasons": []}
PRETTY = json.dumps(REP, ensure_ascii=False, indent=2)



# [2026-09-23] 날짜를 **오늘로** 준다. 예행은 `next_action.for_date` 를 그 날짜로 심는데
#   `daily_ops` 는 그것을 **오늘과 견준다** — 과거 날짜면 `NO_ACTION_FOR_TODAY` /
#   `STALE_ACTION_NOT_EXECUTED` 로 막는 것이 **설계대로**다(묵은 지시는 집행하지 않는다).
#   "20260922" 로 박아두니 09-23 에 3건이 빨갛게 됐다 — 분기는 멀쩡했고 시험이 그날만 통과하는 물건이었다.
TODAY = dt.datetime.now().strftime("%Y%m%d")

def test_clean_stdout_uses_the_primary_path(tmp_path):
    rep, note = R._parse_report(PRETTY, tmp_path)
    assert rep == REP and note is None          # 대체 경로를 조용히 타면 주 경로가 죽은 것이다


def test_noise_before_the_report_is_survived(tmp_path):
    rep, note = R._parse_report("[경고] 캐시를 만들었다\n" + PRETTY, tmp_path)
    assert rep == REP and "잡소리" in note


def test_falls_back_to_daily_log_when_stdout_is_unusable(tmp_path):
    (tmp_path / "daily_log.jsonl").write_text(json.dumps(REP) + "\n", encoding="utf-8")
    rep, note = R._parse_report("아무 JSON 도 없다", tmp_path)
    assert rep == REP and "daily_log" in note


def test_no_source_is_reported_not_guessed(tmp_path):
    rep, note = R._parse_report("", tmp_path)
    assert rep == {} and "둘 다 못 읽었다" in note


def test_junk_is_recorded_so_the_cause_can_be_found(tmp_path, monkeypatch):
    """잡소리가 섞이면 **무엇이 찍었는지**를 남긴다.

    [2026-09-22] 노트만 남기던 판으로는 원인을 못 찾았다 — 40회에 1회쯤 나는데
    그때마다 '섞였다' 만 알고 무엇이 섞였는지는 몰랐다."""
    import daily_ops as D
    real = D.main
    monkeypatch.setattr(D, "main", lambda: (print("[표식] 이 줄이 범인이다"), real())[1])
    rec = R.run(tmp_path, TODAY)
    noisy = [s for s in rec["scenarios"] if s.get("report_note")]
    assert noisy, "잡소리를 넣었는데 알아채지 못했다"
    assert any("[표식] 이 줄이 범인이다" in j for s in noisy for j in s.get("stdout_junk", []))
    assert rec["verdict"] == "PASS"          # 잡소리가 판정을 뒤집지 않는다
