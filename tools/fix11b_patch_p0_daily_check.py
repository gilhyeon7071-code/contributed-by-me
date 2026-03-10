# fix11b_patch_p0_daily_check.py
# - Ensures crash_risk_off variable is always defined before being serialized.
import io
import os
import re
from pathlib import Path

ROOT = Path(r"E:\1_Data")
P = ROOT / "p0_daily_check.py"

if not P.exists():
    raise SystemExit(f"[FATAL] not found: {P}")

src = P.read_text(encoding="utf-8", errors="replace").splitlines(True)

needle = "'crash_risk_off': crash_risk_off"
insert_block = [
    "        # --- fix11b: ensure crash_risk_off always defined ---\n",
    "        try:\n",
    "            crash_risk_off\n",
    "        except NameError:\n",
    "            crash_risk_off = _eval_crash_risk_off(str(date_max), cfg) if date_max else _eval_crash_risk_off(dt.datetime.now().strftime(\"%Y%m%d\"), cfg)\n",
    "        # --- fix11b end ---\n",
]

# If already patched, do nothing
if any("fix11b: ensure crash_risk_off" in line for line in src):
    print("OK: already patched (fix11b marker found)")
    raise SystemExit(0)

# Find the line index to insert BEFORE the dict entry that references crash_risk_off
idx = None
for i, line in enumerate(src):
    if needle in line:
        idx = i
        break

if idx is None:
    # fallback: search for "'crash_risk_off':" entry without variable name
    for i, line in enumerate(src):
        if "'crash_risk_off':" in line:
            idx = i
            break

if idx is None:
    raise SystemExit("[FATAL] could not find crash_risk_off serialization line in p0_daily_check.py")

# Insert with same indentation level as the needle line.
m = re.match(r"^(\s*)", src[idx])
indent = m.group(1) if m else ""
patched_block = []
for line in insert_block:
    # keep original indentation for block; it's written assuming 8 spaces ("        ")
    # adjust to match the target indent (likely 8 spaces) by replacing leading 8 spaces if present
    if line.startswith("        "):
        patched_block.append(indent + line[8:])
    else:
        patched_block.append(indent + line)

out_lines = src[:idx] + patched_block + src[idx:]
P.write_text("".join(out_lines), encoding="utf-8")
print("OK: patched p0_daily_check.py (fix11b)")
