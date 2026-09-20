"""H1 수집기 5 — 공시 시각. 가짜 HTML 로. 네트워크 없음."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import collect_disclosure_times as T

NOW = datetime(2026, 9, 18, 13, 0, 0)
HTML = """<tbody>
<tr id="parkman" class="first"> <td class="first txc">20:00</td>
 <td><a title='영흥'> 영흥</a></td>
 <td><a href="#viewer" onclick="openDisclsViewer('20260706000746','')" title='[투자주의]'>x</a></td></tr>
<tr> <td class="first txc">10:31</td>
 <td><a title='신송홀딩스'>신송홀딩스</a></td>
 <td><a href="#viewer" onclick="openDisclsViewer('20260706000243','')" title='자기주식취득결정'>y</a></td></tr>
</tbody>"""


def test_parse_time_and_receipt_number():
    rows = T.parse_rows(HTML)
    assert [(r["time"], r["rcept_no"], r["corp_name"]) for r in rows] == [
        ("20:00", "20260706000746", "영흥"), ("10:31", "20260706000243", "신송홀딩스")]
    assert rows[1]["title"] == "자기주식취득결정"


@pytest.mark.parametrize("hhmm,expect", [("08:30", "BEFORE_OPEN"), ("09:00", "INTRADAY"), ("15:30", "INTRADAY"),
                                         ("15:31", "AFTER_CLOSE"), ("20:00", "AFTER_CLOSE")])
def test_session_classification(hhmm, expect):
    assert T.classify_time(hhmm) == expect


class FakeSession:
    def __init__(self, html, status=200):
        self.html, self.status, self.calls = html, status, 0

    def post(self, url, headers, timeout, data):
        self.calls += 1

        class R:
            status_code = self.status
            text = self.html
        return R()


def _events(tmp_path, rows):
    (tmp_path / "h1_events.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")


def _ev(rc="20260706000243"):
    return {"event_id": f"{rc}|BUYBACK_DIRECT", "rcept_no": rc, "rcept_dt": "20260706", "stock_code": "004770",
            "corp_name": "신송홀딩스", "event_type": "BUYBACK_DIRECT"}


def test_matches_only_by_receipt_number(tmp_path):
    _events(tmp_path, [_ev()])
    r = T.run(tmp_path, NOW, session=FakeSession(HTML))
    assert r["matched"] == 1
    row = json.loads((tmp_path / "disclosure_times.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["time"] == "10:31" and row["session"] == "INTRADAY"   # 같은 날 20:00 짜리를 가져오지 않는다


def test_rerun_does_not_refetch(tmp_path):
    _events(tmp_path, [_ev()])
    s = FakeSession(HTML)
    T.run(tmp_path, NOW, session=s)
    calls = s.calls
    r2 = T.run(tmp_path, NOW, session=s)
    assert r2["days"] == 0 and s.calls == calls


def test_empty_page_is_failure_not_zero(tmp_path):
    _events(tmp_path, [_ev()])
    r = T.run(tmp_path, NOW, session=FakeSession("<tbody></tbody>"))
    assert r["status"] == "WARN" and r["day_fail"][0]["status"] == "NO_ROWS" and r["matched"] == 0


def test_http_error_is_recorded(tmp_path):
    _events(tmp_path, [_ev()])
    r = T.run(tmp_path, NOW, session=FakeSession(HTML, status=500))
    assert r["status"] == "WARN" and r["day_fail"][0]["status"] == "HTTP_500"


# ---------------- 09-18 오후: 보조 연결 + 페이지 넘김

HTML2 = """<tbody>
<tr> <td class="first txc">13:05</td>
 <td><a title='대한제당'>대한제당</a></td>
 <td><a href="#viewer" onclick="openDisclsViewer('20260706009999','')" title='자기주식취득신탁계약체결결정'>y</a></td></tr>
<tr> <td class="first txc">09:40</td>
 <td><a title='다른회사'>다른회사</a></td>
 <td><a href="#viewer" onclick="openDisclsViewer('20260706008888','')" title='자기주식취득신탁계약체결결정'>y</a></td></tr>
</tbody>"""


def test_name_title_fallback_when_receipt_missing(tmp_path):
    ev = {"event_id": "E|BUYBACK_TRUST", "rcept_no": "20260706000447", "rcept_dt": "20260706",
          "stock_code": "001790", "corp_name": "대한제당", "event_type": "BUYBACK_TRUST"}
    _events(tmp_path, [ev])
    r = T.run(tmp_path, NOW, session=FakeSession(HTML2))
    assert r["by_method"] == {"NAME_TITLE": 1}
    row = json.loads((tmp_path / "disclosure_times.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["time"] == "13:05" and row["kind_rcept_no"] == "20260706009999" and row["match_method"] == "NAME_TITLE"


def test_fallback_does_not_guess_when_two_candidates(tmp_path):
    html = HTML2.replace("다른회사", "대한제당")     # 같은 회사 같은 제목이 둘
    ev = {"event_id": "E|BUYBACK_TRUST", "rcept_no": "20260706000447", "rcept_dt": "20260706",
          "stock_code": "001790", "corp_name": "대한제당", "event_type": "BUYBACK_TRUST"}
    _events(tmp_path, [ev])
    assert T.run(tmp_path, NOW, session=FakeSession(html))["matched"] == 0


def test_paging_uses_raw_row_count(tmp_path):
    """파서가 일부 행을 놓쳐도 페이지 넘김이 멈추면 안 된다(100행 중 97행만 잡히던 실측)."""
    page = "<tbody>" + "".join(
        f"<tr><td class=\"first txc\">10:0{i%10}</td><td><a title='회사{i}'>회사{i}</a></td>"
        f"<td><a onclick=\"openDisclsViewer('2026070600{i:04d}','')\" title='자기주식취득결정'>t</a></td></tr>"
        for i in range(100)) + "</tbody>"
    broken = page.replace("<td><a title='회사5'>회사5</a></td>", "<td>회사5</td>")  # 한 행을 못 잡게
    s = FakeSession(broken)
    day = T.fetch_day("20260706", s)
    assert day["parse_lost"] >= 1 and s.calls > 1   # 놓쳤어도 다음 페이지로 넘어갔다


def test_raw_kind_rows_are_stored_even_when_unrelated(tmp_path):
    """우리 사건과 무관한 공시도 저장해야 한다 — 시장조치 같은 다른 가설의 재료이고, 버리면 다시 못 구한다."""
    _events(tmp_path, [_ev()])
    T.run(tmp_path, NOW, session=FakeSession(HTML))
    raw = [json.loads(x) for x in (tmp_path / "kind_disclosures.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {r["rcept_no"] for r in raw} == {"20260706000746", "20260706000243"}
    assert [r["session"] for r in raw if r["rcept_no"] == "20260706000746"] == ["AFTER_CLOSE"]
    assert all("ymd" in r and "title" in r for r in raw)


def test_raw_store_is_deduped_on_rerun(tmp_path):
    _events(tmp_path, [_ev()])
    s = FakeSession(HTML)
    T.run(tmp_path, NOW, session=s)
    n1 = len((tmp_path / "kind_disclosures.jsonl").read_text(encoding="utf-8").strip().splitlines())
    T.run(tmp_path, NOW, session=s)
    n2 = len((tmp_path / "kind_disclosures.jsonl").read_text(encoding="utf-8").strip().splitlines())
    assert n1 == n2 == 2
