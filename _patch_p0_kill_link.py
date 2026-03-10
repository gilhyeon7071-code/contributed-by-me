from pathlib import Path
import datetime, shutil, re, sys, os

p = Path(r"E:\1_Data\p0_daily_check.py")
if not p.exists():
    print("ERR_NOT_FOUND", p); raise SystemExit(2)

raw = p.read_text(encoding="utf-8", errors="replace").replace("\r\n", "\n")
if len(raw) < 1000:
    print("ERR_SRC_TOO_SMALL", len(raw)); raise SystemExit(2)

mark = "# 1.5b) Link kill_switch -> risk_off"
if mark in raw:
    print("SKIP_ALREADY_PATCHED"); raise SystemExit(0)

lines = raw.splitlines(True)  # keep line endings

# find: if kill_switch.get("enabled"):
start = None
for i, s in enumerate(lines):
    if re.search(r'^\s*if\s+kill_switch\.get\("enabled"\)\s*:\s*$', s):
        start = i; break
if start is None:
    for i, s in enumerate(lines):
        if 'kill_switch.get("enabled")' in s:
            start = i; break
if start is None:
    print("ERR_KILL_SWITCH_ENABLED_BLOCK_NOT_FOUND"); raise SystemExit(2)

base_indent = re.match(r"\s*", lines[start]).group(0)
base_n = len(base_indent)

# find end of that block by indentation drop
end = None
for j in range(start + 1, len(lines)):
    if lines[j].strip() == "":
        continue
    ind = len(re.match(r"\s*", lines[j]).group(0))
    if ind <= base_n:
        end = j
        break
if end is None:
    end = len(lines)

ins = []
ins.append(base_indent + mark + " (fail-closed)\n")
ins.append(base_indent + 'if isinstance(kill_switch, dict) and kill_switch.get("triggered"):\n')
ins.append(base_indent + '    risk_off["enabled"] = True\n')
ins.append(base_indent + '    if "kill_switch" not in risk_off["reasons"]:\n')
ins.append(base_indent + '        risk_off["reasons"].append("kill_switch")\n')
ins.append(base_indent + '    for _r in (kill_switch.get("reasons") or []):\n')
ins.append(base_indent + '        _tag = "kill_switch:" + str(_r)\n')
ins.append(base_indent + '        if _tag not in risk_off["reasons"]:\n')
ins.append(base_indent + '            risk_off["reasons"].append(_tag)\n')
ins.append("\n")

new_lines = lines[:end] + ins + lines[end:]

ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
bak = p.with_name(p.name + f".bak_link_{ts}")
shutil.copy2(p, bak)

tmp = Path(str(p) + f".tmp_{ts}")
tmp.write_text("".join(new_lines), encoding="utf-8", newline="\n")
os.replace(tmp, p)

print("PATCH_OK", "bak", str(bak), "insert_at_line", end + 1)
