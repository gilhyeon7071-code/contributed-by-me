from pathlib import Path
import importlib.util
import sys


def _load_module(name: str, path: str):
    mod_path = Path(path)
    spec = importlib.util.spec_from_file_location(name, mod_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_news_collect_last_success_excludes_soft_skip():
    mod = _load_module("news_collect_naver_daily", r"E:\1_Data\tools\news_collect_naver_daily.py")
    assert mod._should_record_last_success({"reason": "ok"}) is True
    assert mod._should_record_last_success({"reason": "no_rows_saved"}) is True
    assert mod._should_record_last_success({"reason": "quota_budget_guard"}) is False
    assert mod._should_record_last_success({"reason": "naver_quota_exceeded"}) is False


def test_news_collect_charge_quota_updates_all_counters():
    mod = _load_module("news_collect_naver_daily_quota", r"E:\1_Data\tools\news_collect_naver_daily.py")
    quota_state = {"daily_used": 10, "hourly_used": 3}
    session_map = {"20260421:intraday": 7}
    used = mod._charge_quota(quota_state, session_map, "20260421:intraday")
    assert quota_state["daily_used"] == 11
    assert quota_state["hourly_used"] == 4
    assert session_map["20260421:intraday"] == 8
    assert used == 8


def test_news_score_zero_signal_fallback_is_fail():
    mod = _load_module("news_score_daily_quality", r"E:\1_Data\tools\news_score_daily.py")
    quality = mod._assess_news_score_quality(
        reason="candidate_article_coverage_zero_with_zero_signal_fallback",
        mapped_rate=0.0,
        nonzero_rate=0.0,
        fallback_reason="ok",
        fallback_coverage_rate=1.0,
        fallback_nonzero_rate=0.0,
        pass_mapped=0.6,
        pass_nonzero=0.1,
        warn_mapped=0.4,
    )
    assert quality == "FAIL"


def test_news_score_candidate_fallback_with_nonzero_signal_is_warn():
    mod = _load_module("news_score_daily_warn", r"E:\1_Data\tools\news_score_daily.py")
    quality = mod._assess_news_score_quality(
        reason="candidate_article_coverage_zero",
        mapped_rate=0.0,
        nonzero_rate=0.0,
        fallback_reason="ok",
        fallback_coverage_rate=1.0,
        fallback_nonzero_rate=0.2,
        pass_mapped=0.6,
        pass_nonzero=0.1,
        warn_mapped=0.4,
    )
    assert quality == "WARN"


def test_news_score_canonical_entity_tags_alias_mapping():
    """별칭 -> 정규 태그 매핑. 이것이 _split_entity_tags_canonical 의 실제 계약이다."""
    mod = _load_module("news_score_daily_tags", "E:/1_Data/tools/news_score_daily.py")
    assert mod._split_entity_tags_canonical("반도체|USD/KRW|BATTERY|AI") == [
        "반도체", "환율", "배터리", "AI"
    ]
    assert mod._split_entity_tags_canonical("SEMICON|FOMC|") == ["반도체", "금리"]
    assert mod._split_entity_tags_canonical("") == []
    assert mod._split_entity_tags_canonical(None) == []


def test_news_score_canonical_entity_tags_pass_through_unrecoverable_mojibake():
    """모지바케는 복원하지 않고 그대로 통과시킨다 - 그것이 현재의 계약이다.

    [2026-09-09 사용자 결정] 이전 테스트는 모지바케 입력에서 정상 한글을 요구했다.
      그 기대는 원리적으로 만족할 수 없다. 반도체의 UTF-8 은 9바이트(홀수)라
      cp949 로 잘못 읽으면 2바이트씩 4쌍 + 남는 1바이트가 대체문자가 된다.
      역복원하면 앞 두 글자까지가 한계이고 마지막 글자는 되살릴 바이트가 없다.
      복원 기능을 만들 근거도 없다 - 뉴스 축은 2026-08-31 에 OFF 됐고,
      news_score 는 IC 0(1/5/20일 t 전부 +-0.3 내), 78%가 빈 입력이며
      final_score 와 상관 -0.639 로 구조적으로 진입 불가다
      (PLANS 2026-08-22 (58), 2026-08-31 (148)).
      -> 기능을 만들지 않고 현재 동작을 계약으로 고정한다.
      나중에 복원을 구현하면 이 테스트가 먼저 깨져서 알려준다.
    """
    mod = _load_module("news_score_daily_tags", "E:/1_Data/tools/news_score_daily.py")
    broken = "諛섎룄泥?"
    assert mod._split_entity_tags_canonical(broken + "|USD/KRW") == [broken, "환율"]
