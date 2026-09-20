import os
import pandas as pd
import logging

p = r"paper\trades.csv"
_log_print("FILE=", p, "EXISTS=", os.path.exists(p))
if not os.path.exists(p):
    raise SystemExit(0)

df = pd.read_csv(p)
_log_print("rows=", len(df))
_log_print("cols=", list(df.columns))

open_mask = None
if "exit_date" in df.columns:
    open_mask = df["exit_date"].isna() | (df["exit_date"].astype(str).str.strip() == "") | (df["exit_date"].astype(str).str.lower() == "nan")
elif "is_open" in df.columns:
    open_mask = df["is_open"].astype(str).str.lower().isin(["1","true","yes","y"])
elif "status" in df.columns:
    open_mask = df["status"].astype(str).str.lower().isin(["open","opened","holding"])
else:
    _log_print("OPEN_DETECT=UNKNOWN (no exit_date/is_open/status columns)")
    raise SystemExit(0)

odf = df[open_mask].copy()
_log_print("open_positions=", len(odf))
cols_show = [c for c in ["code","entry_date","entry_price","shares","qty","exit_date","exit_price","pnl_pct"] if c in odf.columns]
_log_print("open_cols_show=", cols_show)
if cols_show:
    _log_print(odf[cols_show].tail(20).to_string(index=False))
else:
    _log_print(odf.tail(20).to_string(index=False))


logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
