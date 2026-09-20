import pathlib

root_dir = pathlib.Path(r"E:\1_Data")
exts = [".md", ".txt", ".json", ".csv"]

for p in root_dir.rglob("*"):
    if p.is_dir() or p.suffix not in exts:
        continue
    # skip backup, logs, cache, git, _tmp etc.
    if any(part.startswith(".") or part.startswith("_") or part in ["2_Logs", "node_modules", "Raw", "backup"] for part in p.parts):
        continue
    try:
        content = p.read_text(encoding='utf-8', errors='ignore')
        if "11.35%" in content or "(127)" in content:
            print(f"FOUND ACTIVE MATCH IN FILE: {p}")
            for line_idx, line in enumerate(content.splitlines()):
                if "11.35%" in line or "(127)" in line:
                    print(f"  Line {line_idx}: {line.strip()}")
            print()
    except Exception as e:
        pass
