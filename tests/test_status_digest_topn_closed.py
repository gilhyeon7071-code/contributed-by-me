"""아침 상태판이 **종결된 라운드를 진행 중처럼 보이지 않는지** (2026-09-21).

배경: topn 은 09-16 종결 / 09-17 보유 전량 청산(브로커 보유 0)인데,
09-21 아침 상태판이 09-15 자 낡은 산출물을 읽어 **"보유 6/6"** 을 띄웠다.
같은 마커(`2_Logs/topn/ROUND_CLOSED.json`)를 신선도 감시·원장 대조는 09-19 에 읽게 고쳤는데
이 화면만 빠져 있었다 — 같은 결함의 세 번째 복사본.

화면이 사실을 단정하면, 그 단정을 검증하는 코드가 있어야 한다."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import build_status_digest as B  # noqa: E402

MARKER = {"round_id": "RD_20260901_topn", "closed_at": "2026-09-16T20:4x",
          "why": "라운드 종결. 09-17 보유 전량 청산(6종목)"}


def _topn(tmp_path, monkeypatch, *, closed=None, exec_doc=None):
    tp = tmp_path / "topn"
    tp.mkdir(parents=True)
    (tp / "forward_ledger.csv").write_text("date\n20260915\n", encoding="utf-8")
    if exec_doc is not None:
        (tp / "exec_20260915.json").write_text(json.dumps(exec_doc), encoding="utf-8")
    if closed is not None:
        (tp / "ROUND_CLOSED.json").write_text(json.dumps(closed, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(B, "LOGS", tmp_path)
    return B.section_topn()


# ---------------------------------------------------------------- 종결이면 진행 중처럼 보이지 않는다
def test_closed_round_shows_closed_not_positions(tmp_path, monkeypatch):
    lines = _topn(tmp_path, monkeypatch, closed=MARKER,
                  exec_doc={"A3_positions": 6, "A3_max_pos": 6, "date": "20260915"})
    body = "\n".join(lines)
    assert "[종결]" in body and "RD_20260901_topn" in body
    assert "보유 6/6" not in body            # 낡은 숫자를 띄우지 않는다
    assert "브로커 보유 0" in body            # 무엇이 정본인지 말한다


def test_closed_round_hides_stale_batch_logs(tmp_path, monkeypatch):
    """종결되면 배치 로그 나이도 경보 대상이 아니다 — 안 도는 게 정상이다."""
    tp = tmp_path / "topn"
    lines = _topn(tmp_path, monkeypatch, closed=MARKER, exec_doc={"A3_positions": 6, "A3_max_pos": 6})
    (tp / "run_topn_evening.log").write_text("[END] rc=0\n", encoding="utf-8")
    assert not any("저녁" in ln or "[주의]" in ln for ln in lines)


# ---------------------------------------------------------------- 종결이 아니면 종전대로
def test_open_round_still_shows_positions(tmp_path, monkeypatch):
    lines = _topn(tmp_path, monkeypatch, closed=None,
                  exec_doc={"A3_positions": 4, "A3_max_pos": 6, "date": "20260915"})
    body = "\n".join(lines)
    assert "보유 4/6" in body and "[종결]" not in body


def test_open_round_warns_on_missing_batch_log(tmp_path, monkeypatch):
    """진행 중인데 배치 로그가 없으면 여전히 경보해야 한다 — 이걸 죽이면 감시가 사라진다."""
    lines = _topn(tmp_path, monkeypatch, closed=None, exec_doc={"A3_positions": 4, "A3_max_pos": 6})
    assert any("[주의]" in ln for ln in lines)


def test_no_topn_dir_is_silent(tmp_path, monkeypatch):
    monkeypatch.setattr(B, "LOGS", tmp_path)
    assert B.section_topn() == []
