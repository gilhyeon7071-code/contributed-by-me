from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path


def _load_module():
    mod_path = Path(r"E:\1_Data\tools\orderflow_hawkes_glr.py")
    spec = importlib.util.spec_from_file_location("orderflow_hawkes_glr", mod_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_glr_detects_clustered_trade_flow():
    mod = _load_module()
    base = datetime(2026, 4, 29, 9, 0, 0)
    quiet = [base + timedelta(seconds=i * 30) for i in range(20)]
    burst_start = base + timedelta(minutes=11)
    burst = [burst_start + timedelta(milliseconds=i * 250) for i in range(50)]
    rows, summary = mod._evaluate_code(
        "005930",
        quiet + burst,
        quiet_minutes=10,
        tau=1.0,
        consecutive_required=2,
        eta_delta_threshold=0.15,
        eta_high_threshold=0.90,
        min_baseline_events=30,
        # [2026-09-09] max_windows 는 뒤에 필수 인자로 추가됐는데 이 테스트가 안 따라와
        #   TypeError 로 죽어 있었다. 0 = 캡 해제로, **파라미터가 생기기 전 동작**과 같다.
        #   생산 기본값은 720(ORDERFLOW_GLR_MAX_WINDOWS_PER_CODE)이지만, 여기서 720을 쓰면
        #   테스트가 검증하던 의미가 조용히 바뀐다. 원래 의미를 보존한다.
        max_windows=0,
    )
    assert rows
    assert summary["max_glr"] > 1.0
    assert summary["auto_action_candidate"] is True


def test_jsonl_loader_uses_trade_frames_only(tmp_path):
    mod = _load_module()
    path = tmp_path / "ticks.jsonl"
    records = [
        {
            "ts": "2026-04-29T09:00:01",
            "event": "message",
            "tr_id": "H0STASP0",
            "fields": ["005930", "090001"],
            "normalized": {"event_type": "hoga", "tr_id": "H0STASP0", "code": "005930", "fields": ["005930", "090001"]},
        },
        {
            "ts": "2026-04-29T09:00:02",
            "event": "message",
            "tr_id": "H0STCNT0",
            "fields": ["005930", "090002"],
            "normalized": {"event_type": "trade", "tr_id": "H0STCNT0", "code": "005930", "fields": ["005930", "090002"]},
        },
    ]
    path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    events, total, malformed = mod._load_trade_events(path)
    assert total == 2
    assert malformed == 0
    assert len(events) == 1
    assert events[0].code == "005930"


def test_lambda0_does_not_include_future_quiet_minutes():
    mod = _load_module()
    base = datetime(2026, 5, 6, 9, 30, 0)
    events = [base + timedelta(minutes=i) for i in range(20)]
    lambda0, meta = mod._estimate_lambda0(events, quiet_minutes=60)
    assert lambda0 > 0.0
    assert meta["median_count_per_min"] == 1.0
