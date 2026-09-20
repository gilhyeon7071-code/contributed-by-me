# tools/check_entry_room.py
import json
import logging
import os
import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger("check_entry_room")

def P(*parts: str) -> str:
    return os.path.join(BASE, *parts)

def norm_code(x) -> str:
    s = "" if x is None else str(x).strip()
    s = s.replace(".0", "")
    return s.zfill(6) if s else ""

def yyyymmdd_from_any(x) -> str:
    if x is None:
        return ""
    try:
        ts = pd.to_datetime(x, errors="coerce")
        if pd.isna(ts):
            return ""
        return ts.strftime("%Y%m%d")
    except Exception:
        s = str(x).strip()
        s = s.replace("-", "").replace("/", "")
        return s[:8] if len(s) >= 8 else ""

def read_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    cfg_path = P("paper", "paper_engine_config.json")
    st_path = P("paper", "paper_state.json")
    cand_path = P("2_Logs", "candidates_latest_data.csv")

    cfg = read_json(cfg_path) or {}
    st = read_json(st_path) or {}

    max_positions = cfg.get("max_positions", None)
    max_new_per_day = cfg.get("max_new_trades_per_day", None)
    crash_cfg = cfg.get("crash_risk_off", None)

    op = st.get("open_positions", []) or []
    ps = st.get("processed_signals", []) or []

    op_codes = sorted({
        norm_code(i.get("code"))
        for i in op
        if isinstance(i, dict) and norm_code(i.get("code"))
    })

    ps_set = set(str(x).strip() for x in ps if str(x).strip())

    dfc = pd.DataFrame()
    cand_codes_set = []
    cand_keys_set = []
    if os.path.exists(cand_path):
        dfc = pd.read_csv(cand_path)
        if "code" in dfc.columns:
            cand_codes_set = sorted({norm_code(x) for x in dfc["code"].tolist() if norm_code(x)})
        if "date" in dfc.columns and "code" in dfc.columns:
            tmp_keys = []
            for _, r in dfc.iterrows():
                c = norm_code(r.get("code"))
                d = yyyymmdd_from_any(r.get("date"))
                if c and d:
                    tmp_keys.append(f"{c}:{d}")
            cand_keys_set = sorted(set(tmp_keys))

    new_keys = sorted(set(cand_keys_set) - ps_set)
    new_codes = sorted({k.split(":")[0] for k in new_keys})

    slots_left = None
    try:
        if max_positions is not None:
            mp = int(max_positions)
            if mp > 0:
                slots_left = mp - len(op_codes)
    except Exception:
        slots_left = None

    logger.info("CFG_FILE=%s EXISTS=%s", os.path.relpath(cfg_path, BASE), os.path.exists(cfg_path))
    logger.info("max_positions=%s", max_positions)
    logger.info("max_new_trades_per_day=%s", max_new_per_day)
    logger.info("crash_risk_off_cfg=%s", crash_cfg)

    logger.info("STATE_FILE=%s EXISTS=%s", os.path.relpath(st_path, BASE), os.path.exists(st_path))
    logger.info("open_positions_len=%s", len(op))
    logger.info("open_pos_codes=%s", op_codes)
    logger.info("processed_signals_len=%s", len(ps))
    logger.info("processed_signals_sample=%s", list(sorted(ps_set))[:30])

    logger.info("CANDS_FILE=%s EXISTS=%s", os.path.relpath(cand_path, BASE), os.path.exists(cand_path))
    logger.info("cand_codes=%s", cand_codes_set)
    logger.info("cand_keys_sample=%s", cand_keys_set[:30])

    logger.info("new_candidate_keys_not_processed=%s", new_keys)
    logger.info("new_candidate_codes_not_processed=%s", new_codes)

    logger.info("slots_left=%s", slots_left)

    if slots_left is not None and slots_left <= 0:
        logger.info("DECISION=NO_NEW_ENTRIES_EXPECTED (slots_full)")
    elif len(new_codes) == 0:
        logger.info("DECISION=NO_NEW_ENTRIES_EXPECTED (no_new_candidates)")
    else:
        logger.info("DECISION=NEW_ENTRIES_POSSIBLE (need risk_off OFF and engine allowed)")

if __name__ == "__main__":
    main()
