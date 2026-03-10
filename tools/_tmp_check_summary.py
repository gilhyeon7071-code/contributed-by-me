import json, glob, os
import pandas as pd

a = json.load(open(r"2_Logs\after_close_summary_last.json", "r", encoding="utf-8"))

df = pd.read_csv(r"paper\fills.csv", encoding="utf-8-sig")
ts = pd.to_datetime(df["datetime"], errors="coerce")
df["ymd"] = ts.dt.strftime("%Y%m%d")
df["side"] = df["side"].astype(str)

d_any = (df["ymd"].dropna().max() if len(df) else "")
buy = df[df["side"] == "BUY"]
d_buy = (buy["ymd"].dropna().max() if len(buy) else "")
D = (d_buy or d_any or "")

core = rf"2_Logs\p0_live_vs_bt_core_{D}.json"
cj = json.load(open(core, "r", encoding="utf-8")) if (D and os.path.exists(core)) else {}

audit = max(glob.glob(r"2_Logs\audit_daily_*.json"), key=os.path.getmtime)
aj = json.load(open(audit, "r", encoding="utf-8"))

pn = max(glob.glob(r"2_Logs\paper_pnl_summary_*.json"), key=os.path.getmtime)
pj = json.load(open(pn, "r", encoding="utf-8"))

pend = max(glob.glob(r"2_Logs\paper_pending_report_*.json"), key=os.path.getmtime)
pk = json.load(open(pend, "r", encoding="utf-8"))

print("AFTER_LAST.as_of_ymd", a.get("as_of_ymd"), "generated_at", a.get("generated_at"))
print("D_FROM_FILLS", D, "BUY_MAX", d_buy, "ALL_MAX", d_any)
print("LIVE_VS_BT_CORE", os.path.basename(core) if D else None, "status", cj.get("status"), "status_exec", cj.get("status_exec"), "as_of", cj.get("as_of"))
print("AUDIT", os.path.basename(audit), "status", (aj.get("summary") or {}).get("status"), "lookahead_suspects", (aj.get("summary") or {}).get("lookahead_suspects"))
print("PAPER_PNL", os.path.basename(pn), "comp_ret", pj.get("comp_ret"), "sum_ret", pj.get("sum_ret"), "trades_used", pj.get("trades_used"))
print("PENDING", os.path.basename(pend), "pending", len(pk.get("pending") or []), "active", len(pk.get("active") or []), "prices_date_max", pk.get("prices_date_max"))
