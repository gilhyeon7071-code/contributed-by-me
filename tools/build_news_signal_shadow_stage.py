from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"
IN_FINAL = LOGS / "candidates_latest_data.with_final_score.csv"
IN_NEWS = LOGS / "candidates_latest_data.with_news_score.csv"
OUT_CSV = LOGS / "news_signal_shadow_stage_latest.csv"
OUT_JSON = LOGS / "news_signal_shadow_stage_latest.json"
NEWS_STATUS = LOGS / "news_score_status_latest.json"
FINAL_STATUS = LOGS / "final_score_merge_status_latest.json"

VERSION = "news_signal_shadow_stage_v1"
OBSERVED_FRESH_MAX_AGE_HOURS = 72.0


def _now_kst() -> str:
    return datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _read_csv(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [dict(r) for r in reader]
        return rows, list(reader.fieldnames or [])


def _f(row: Dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        value = str(row.get(key, "")).strip()
        if value == "":
            return default
        return float(value)
    except Exception:
        return default


def _i(row: Dict[str, str], key: str, default: int = 0) -> int:
    try:
        value = str(row.get(key, "")).strip()
        if value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _s(row: Dict[str, str], key: str) -> str:
    return str(row.get(key, "") or "").strip()


def _norm_code6(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _is_invalid_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"", "nan", "none", "null", "<na>", "nat"}


def _is_direct_observed_source(code: Any, name: Any, title: Any) -> bool:
    code_s = _norm_code6(code)
    name_s = "" if _is_invalid_text(name) else str(name or "").strip()
    title_s = str(title or "").strip()
    if not title_s:
        return False
    if code_s and code_s in title_s:
        return True
    if name_s and name_s in title_s:
        return True
    return False


def _parse_dt(v: Any) -> datetime | None:
    raw = str(v or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=9)))
            return dt.astimezone(timezone(timedelta(hours=9)))
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone(timedelta(hours=9)))
        return dt.astimezone(timezone(timedelta(hours=9)))
    except Exception:
        return None


def _parse_kis_dt(date8: Any, time6: Any, fallback: Any = "") -> datetime | None:
    d = "".join(ch for ch in str(date8 or "") if ch.isdigit())[:8]
    t = "".join(ch for ch in str(time6 or "") if ch.isdigit())[:6]
    if len(d) == 8 and len(t) >= 4:
        t = t.ljust(6, "0")
        try:
            return datetime.strptime(d + t, "%Y%m%d%H%M%S").replace(tzinfo=timezone(timedelta(hours=9)))
        except Exception:
            pass
    return _parse_dt(fallback)


def _latest_observed_freshness(code_names: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    targets = sorted({_norm_code6(c) for c in code_names.keys() if _norm_code6(c)})
    if not targets or not NEWS_DB.exists():
        return {}
    now_dt = datetime.now(timezone(timedelta(hours=9)))
    out: Dict[str, Dict[str, Any]] = {}

    def consider(code: str, source: str, event_dt: datetime | None, title: Any, table: str) -> None:
        if not code or event_dt is None:
            return
        if not _is_direct_observed_source(code, code_names.get(code, ""), title):
            return
        age_hours = (now_dt - event_dt).total_seconds() / 3600.0
        if age_hours < 0:
            return
        cur = out.get(code)
        if cur is None or float(age_hours) < float(cur.get("age_hours", 999999.0)):
            out[code] = {
                "source": source,
                "source_table": table,
                "age_hours": round(float(age_hours), 6),
                "published_at": event_dt.isoformat(timespec="seconds"),
                "title": str(title or "")[:200],
                "fresh": bool(age_hours <= OBSERVED_FRESH_MAX_AGE_HOURS),
            }

    try:
        con = sqlite3.connect(f"file:{NEWS_DB.as_posix()}?mode=ro", uri=True, timeout=5)
        tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        placeholders = ",".join("?" for _ in targets)
        if "news_articles_google_rss" in tables:
            q = (
                "SELECT code, published_at, fetched_at, title "
                "FROM news_articles_google_rss "
                f"WHERE code IN ({placeholders})"
            )
            for code, published_at, fetched_at, title in con.execute(q, targets).fetchall():
                consider(str(code or "").zfill(6), "GOOGLE_NEWS_RSS", _parse_dt(published_at) or _parse_dt(fetched_at), title, "news_articles_google_rss")
        if "news_articles_kis_title" in tables:
            q = (
                "SELECT code, data_dt, data_tm, collected_at, title "
                "FROM news_articles_kis_title "
                f"WHERE code IN ({placeholders})"
            )
            for code, data_dt, data_tm, collected_at, title in con.execute(q, targets).fetchall():
                consider(str(code or "").zfill(6), "KIS_NEWS_TITLE", _parse_kis_dt(data_dt, data_tm, collected_at), title, "news_articles_kis_title")
        con.close()
    except Exception:
        return {}
    return out


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _direction(score: float, block_rows: int, penalize_rows: int, boost_rows: int) -> str:
    if block_rows > 0 or penalize_rows > 0 or score <= -0.15:
        return "negative"
    if boost_rows > 0 or score >= 0.15:
        return "positive"
    return "neutral"


def _market_context(row: Dict[str, str]) -> Tuple[float, str]:
    reasons: List[str] = []
    score = 0.0

    lob_status = _s(row, "execution_lob_status").upper()
    orderflow_tag = _s(row, "execution_orderflow_tag").upper()
    spread_bps = _f(row, "execution_spread_bps", -1.0)
    lob_score = _f(row, "execution_lob_score", 0.0)
    lob_adjustment = _f(row, "execution_lob_adjustment", 0.0)
    v_accel = _f(row, "v_accel", 0.0)
    final_score = _f(row, "final_score", 0.0)

    has_lob_quote = bool(lob_status and lob_status not in {"MISSING", "NO_LOB", "NO_HISTORY"})
    has_orderflow = bool(orderflow_tag and orderflow_tag not in {"MISSING", "NO_LOB", "NO_HISTORY"})

    if has_lob_quote:
        reasons.append(f"lob_status={lob_status}")
        score += 0.20
    else:
        reasons.append("lob_missing")

    if has_lob_quote and spread_bps >= 0:
        if spread_bps <= 80.0:
            score += 0.20
            reasons.append(f"spread_ok={spread_bps:.1f}")
        else:
            score -= 0.30
            reasons.append(f"spread_wide={spread_bps:.1f}")

    if orderflow_tag in {"PAUSE", "CAUTION"}:
        score -= 0.35
        reasons.append(f"orderflow={orderflow_tag}")
    elif has_orderflow:
        score += 0.15
        reasons.append(f"orderflow={orderflow_tag}")

    if lob_score > 0:
        score += min(0.20, lob_score * 0.20)
        reasons.append(f"lob_score={lob_score:.3f}")
    if lob_adjustment > 0:
        score += 0.10
        reasons.append(f"lob_adjustment={lob_adjustment:.3f}")
    if v_accel >= 1.2:
        score += 0.15
        reasons.append(f"v_accel={v_accel:.3f}")
    if final_score >= 0.5:
        score += 0.10
        reasons.append(f"final_score={final_score:.3f}")

    return _clip01(score), "|".join(reasons)


def _classify(row: Dict[str, str]) -> Dict[str, Any]:
    news_score = _f(row, "news_score", 0.0)
    source_signal_score = _f(row, "news_source_signal_score", 0.0)
    article_count = _i(row, "news_article_count", 0)
    implication_rows = _i(row, "news_implication_rows", 0)
    block_rows = _i(row, "news_implication_block_rows", 0)
    penalize_rows = _i(row, "news_implication_penalize_rows", 0)
    boost_rows = _i(row, "news_implication_boost_rows", 0)
    reduce_rows = _i(row, "news_implication_reduce_size_rows", 0)
    age_hours = _f(row, "news_freshest_age_hours", -1.0)
    used_lag_days = _i(row, "news_trace_used_lag_days", -1)
    observed_fresh = _s(row, "news_observed_source_fresh").lower() == "true"
    observed_source = _s(row, "news_observed_source")
    observed_age_hours = _f(row, "news_observed_freshest_age_hours", -1.0)

    text_strength = max(abs(news_score), abs(source_signal_score))
    has_text = bool(
        article_count > 0
        or implication_rows > 0
        or abs(news_score) >= 0.10
        or abs(source_signal_score) >= 0.10
    )
    stale = bool((age_hours > 72.0 and age_hours >= 0) or used_lag_days > 2)
    direction = _direction(news_score if abs(news_score) >= abs(source_signal_score) else source_signal_score, block_rows, penalize_rows, boost_rows)
    market_score, market_reason = _market_context(row)

    preliminary = bool(has_text and not stale and text_strength >= 0.10)
    blocked = bool(block_rows > 0 or (direction == "negative" and (text_strength >= 0.35 or reduce_rows > 0)))
    confirmed = bool(preliminary and not blocked and market_score >= 0.50 and text_strength >= 0.15)

    if not has_text:
        stage = "NO_TEXT_SIGNAL"
    elif stale and observed_fresh:
        stage = "SOURCE_FRESH_OBSERVED_SHADOW"
    elif stale:
        stage = "STALE_TEXT_SIGNAL"
    elif blocked:
        stage = "BLOCK_SHADOW"
    elif confirmed:
        stage = "CONFIRMED_SHADOW"
    elif preliminary:
        stage = "PRE_SIGNAL"
    else:
        stage = "WATCH"

    publisher_gate_state = _s(row, "publisher_gate_state").upper()
    publisher_gate_applied = "false"
    publisher_gate_reason = ""
    if stage == "CONFIRMED_SHADOW" and publisher_gate_state != "PUBLISHER_VERIFIED":
        stage = "PUBLISHER_UNVERIFIED_SHADOW"
        publisher_gate_applied = "true"
        publisher_gate_reason = "confirmed_shadow_blocked_by_publisher_gate"

    reason_bits = [
        f"direction={direction}",
        f"text_strength={text_strength:.3f}",
        f"market_score={market_score:.3f}",
    ]
    if stale:
        reason_bits.append("stale")
    if observed_fresh:
        reason_bits.append(f"source_fresh_observed={observed_source}:{observed_age_hours:.3f}h")
    if block_rows > 0:
        reason_bits.append(f"block_rows={block_rows}")
    if reduce_rows > 0:
        reason_bits.append(f"reduce_size_rows={reduce_rows}")
    if market_reason:
        reason_bits.append(market_reason)
    if publisher_gate_reason:
        reason_bits.append(publisher_gate_reason)

    return {
        "news_signal_stage": stage,
        "news_signal_direction": direction,
        "news_text_strength": round(text_strength, 6),
        "news_market_corroboration_score": round(market_score, 6),
        "news_signal_reason": ";".join(reason_bits)[:500],
        "publisher_gate_applied": publisher_gate_applied,
        "publisher_gate_reason": publisher_gate_reason,
        "news_signal_shadow_only": "true",
        "news_signal_trading_effect": "false",
    }


def _policy_confirm_level(row: Dict[str, Any]) -> Dict[str, str]:
    text = " ".join(
        str(row.get(k, "") or "")
        for k in (
            "news_signal_reason",
            "news_source",
            "news_source_signal_title",
            "news_implication_top_actions",
            "news_observed_title",
        )
    ).lower()
    policy_terms = (
        "\uc815\ucc45", "\uc815\ubd80", "\uad6d\ud68c", "\ubc95\uc548", "\uc608\uc0b0", "\uaddc\uc81c",
        "policy", "government", "ministry", "bill", "budget",
    )
    confirmed_terms = (
        "\ud655\uc815", "\ud1b5\uacfc", "\uacf5\uc2dc", "\uacc4\uc57d", "\uc218\uc8fc", "\uc9d1\ud589",
        "passed", "approved", "confirmed", "contract", "dart",
    )
    directional_terms = (
        "\ucd94\uc9c4", "\uacc4\ud68d", "\ubc29\ud5a5", "\ubc1c\ud45c", "\uc9c0\uc6d0", "\uc721\uc131",
        "plan", "support", "roadmap", "initiative",
    )
    has_policy = any(term in text for term in policy_terms)
    if has_policy and any(term in text for term in confirmed_terms):
        return {
            "policy_confirm_level": "3",
            "policy_confirm_action": "entry_allowed_by_policy_confirmation_shadow",
            "policy_confirm_reason": "policy_confirmed_terms",
        }
    if has_policy or any(term in text for term in directional_terms):
        return {
            "policy_confirm_level": "2",
            "policy_confirm_action": "watchlist_only_shadow",
            "policy_confirm_reason": "policy_direction_terms",
        }
    return {
        "policy_confirm_level": "1",
        "policy_confirm_action": "ignore_noise_shadow",
        "policy_confirm_reason": "no_policy_confirmation_terms",
    }


def main() -> int:
    input_path = IN_FINAL if IN_FINAL.exists() else IN_NEWS
    rows, fieldnames = _read_csv(input_path)
    LOGS.mkdir(parents=True, exist_ok=True)
    observed_by_code = _latest_observed_freshness({
        _norm_code6(_s(row, "code")): _s(row, "name")
        for row in rows
        if _norm_code6(_s(row, "code"))
    })

    output_rows: List[Dict[str, Any]] = []
    for row in rows:
        code = _s(row, "code")
        observed = observed_by_code.get(code, {})
        out = {
            "code": code,
            "name": _s(row, "name"),
            "date": _s(row, "date"),
            "news_score": _s(row, "news_score"),
            "news_source": _s(row, "news_source"),
            "news_article_count": _s(row, "news_article_count"),
            "news_freshest_age_hours": _s(row, "news_freshest_age_hours"),
            "news_trace_table": _s(row, "news_trace_table"),
            "news_trace_used_date8": _s(row, "news_trace_used_date8"),
            "news_source_signal_score": _s(row, "news_source_signal_score"),
            "news_implication_top_actions": _s(row, "news_implication_top_actions"),
            "publisher_trust": _s(row, "publisher_trust"),
            "publisher_tier": _s(row, "publisher_tier"),
            "publisher_gate_state": _s(row, "publisher_gate_state"),
            "publisher_trust_missing_metrics": _s(row, "publisher_trust_missing_metrics"),
            "publisher_trust_reason": _s(row, "publisher_trust_reason"),
            "publisher_duplicate_rate": _s(row, "publisher_duplicate_rate"),
            "publisher_p95_latency_ms": _s(row, "publisher_p95_latency_ms"),
            "publisher_historical_tp_fp_score": _s(row, "publisher_historical_tp_fp_score"),
            "publisher_crypto_auth_status": _s(row, "publisher_crypto_auth_status"),
            "publisher_crypto_auth_score": _s(row, "publisher_crypto_auth_score"),
            "final_score": _s(row, "final_score"),
            "execution_lob_status": _s(row, "execution_lob_status"),
            "execution_orderflow_tag": _s(row, "execution_orderflow_tag"),
            "news_observed_source_fresh": str(bool(observed.get("fresh", False))).lower(),
            "news_observed_source": str(observed.get("source", "")),
            "news_observed_source_table": str(observed.get("source_table", "")),
            "news_observed_freshest_age_hours": str(observed.get("age_hours", "")),
            "news_observed_published_at": str(observed.get("published_at", "")),
            "news_observed_title": str(observed.get("title", "")),
        }
        classify_row = dict(row)
        classify_row.update(out)
        out.update(_classify(classify_row))
        out.update(_policy_confirm_level(out))
        output_rows.append(out)

    out_fields = [
        "code",
        "name",
        "date",
        "news_signal_stage",
        "news_signal_direction",
        "news_text_strength",
        "news_market_corroboration_score",
        "news_signal_reason",
        "news_score",
        "news_source",
        "news_article_count",
        "news_freshest_age_hours",
        "news_trace_table",
        "news_trace_used_date8",
        "news_source_signal_score",
        "news_implication_top_actions",
        "publisher_trust",
        "publisher_tier",
        "publisher_gate_state",
        "publisher_trust_missing_metrics",
        "publisher_trust_reason",
        "publisher_duplicate_rate",
        "publisher_p95_latency_ms",
        "publisher_historical_tp_fp_score",
        "publisher_crypto_auth_status",
        "publisher_crypto_auth_score",
        "publisher_gate_applied",
        "publisher_gate_reason",
        "final_score",
        "execution_lob_status",
        "execution_orderflow_tag",
        "news_observed_source_fresh",
        "news_observed_source",
        "news_observed_source_table",
        "news_observed_freshest_age_hours",
        "news_observed_published_at",
        "news_observed_title",
        "news_signal_shadow_only",
        "news_signal_trading_effect",
        "policy_confirm_level",
        "policy_confirm_action",
        "policy_confirm_reason",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(output_rows)

    stage_counts: Dict[str, int] = {}
    for row in output_rows:
        stage = str(row.get("news_signal_stage") or "")
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
    publisher_gate_applied_rows = sum(1 for row in output_rows if str(row.get("publisher_gate_applied") or "").lower() == "true")
    policy_confirm_counts: Dict[str, int] = {}
    for row in output_rows:
        level = str(row.get("policy_confirm_level") or "")
        if level:
            policy_confirm_counts[level] = policy_confirm_counts.get(level, 0) + 1
    publisher_gate_counts: Dict[str, int] = {}
    publisher_trust_values: List[float] = []
    publisher_duplicate_measured_rows = 0
    publisher_latency_measured_rows = 0
    publisher_hist_measured_rows = 0
    publisher_crypto_status_counts: Dict[str, int] = {}
    for row in output_rows:
        gate_state = str(row.get("publisher_gate_state") or "")
        if gate_state:
            publisher_gate_counts[gate_state] = publisher_gate_counts.get(gate_state, 0) + 1
        try:
            publisher_trust_values.append(float(row.get("publisher_trust") or 0.0))
        except Exception:
            publisher_trust_values.append(0.0)
        try:
            if float(row.get("publisher_duplicate_rate") or -1.0) >= 0:
                publisher_duplicate_measured_rows += 1
        except Exception:
            pass
        try:
            if float(row.get("publisher_p95_latency_ms") or -1.0) >= 0:
                publisher_latency_measured_rows += 1
        except Exception:
            pass
        try:
            if float(row.get("publisher_historical_tp_fp_score") or -1.0) >= 0:
                publisher_hist_measured_rows += 1
        except Exception:
            pass
        crypto_status = str(row.get("publisher_crypto_auth_status") or "")
        if crypto_status:
            publisher_crypto_status_counts[crypto_status] = publisher_crypto_status_counts.get(crypto_status, 0) + 1

    status = {
        "generated_at": _now_kst(),
        "version": VERSION,
        "status": "PASS",
        "quality": "PASS" if rows else "WARN",
        "reason": "ok" if rows else "candidate_input_empty",
        "input": str(input_path),
        "output_csv": str(OUT_CSV),
        "rows": len(output_rows),
        "stage_counts": stage_counts,
        "shadow_only": True,
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "consumed_by_order_path": False,
        "publisher_trust_summary": {
            "enabled": bool(publisher_gate_counts),
            "mode": "shadow_observe_only",
            "score_min": round(min(publisher_trust_values), 6) if publisher_trust_values else 0.0,
            "score_max": round(max(publisher_trust_values), 6) if publisher_trust_values else 0.0,
            "score_avg": round(sum(publisher_trust_values) / len(publisher_trust_values), 6) if publisher_trust_values else 0.0,
            "gate_state_counts": publisher_gate_counts,
            "applied_rows": int(publisher_gate_applied_rows),
            "applied_rule": "block_confirmed_shadow_unless_publisher_verified",
            "duplicate_rate_measured_rows": int(publisher_duplicate_measured_rows),
            "p95_latency_measured_rows": int(publisher_latency_measured_rows),
            "historical_tp_fp_measured_rows": int(publisher_hist_measured_rows),
            "crypto_auth_status_counts": publisher_crypto_status_counts,
            "score_effect": False,
            "trading_effect": False,
            "policy_effect": False,
            "auto_emit_enabled": False,
        },
        "policy_confirm_level_summary": {
            "enabled": True,
            "mode": "shadow_observe_only",
            "level_counts": policy_confirm_counts,
            "trading_effect": False,
            "policy_effect": False,
        },
        "news_status": {
            "path": str(NEWS_STATUS),
            "quality": _read_json(NEWS_STATUS).get("quality"),
            "generated_at": _read_json(NEWS_STATUS).get("generated_at"),
        },
        "final_score_status": {
            "path": str(FINAL_STATUS),
            "asof_ymd": _read_json(FINAL_STATUS).get("asof_ymd"),
            "news_gate": _read_json(FINAL_STATUS).get("news_gate"),
        },
    }
    OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": status["status"], "rows": status["rows"], "stage_counts": stage_counts}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
