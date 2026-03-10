# tools/check_entry_room.py
import json
import os
import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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

    print("CFG_FILE=", os.path.relpath(cfg_path, BASE), "EXISTS=", os.path.exists(cfg_path))
    print("max_positions=", max_positions)
    print("max_new_trades_per_day=", max_new_per_day)
    print("crash_risk_off_cfg=", crash_cfg)

    print("STATE_FILE=", os.path.relpath(st_path, BASE), "EXISTS=", os.path.exists(st_path))
    print("open_positions_len=", len(op))
    print("open_pos_codes=", op_codes)
    print("processed_signals_len=", len(ps))
    print("processed_signals_sample=", list(sorted(ps_set))[:30])

    print("CANDS_FILE=", os.path.relpath(cand_path, BASE), "EXISTS=", os.path.exists(cand_path))
    print("cand_codes=", cand_codes_set)
    print("cand_keys_sample=", cand_keys_set[:30])

    print("new_candidate_keys_not_processed=", new_keys)
    print("new_candidate_codes_not_processed=", new_codes)

    print("slots_left=", slots_left)

    if slots_left is not None and slots_left <= 0:
        print("DECISION=NO_NEW_ENTRIES_EXPECTED (slots_full)")
    elif len(new_codes) == 0:
        print("DECISION=NO_NEW_ENTRIES_EXPECTED (no_new_candidates)")
    else:
        print("DECISION=NEW_ENTRIES_POSSIBLE (need risk_off OFF and engine allowed)")

if __name__ == "__main__":
    main()
