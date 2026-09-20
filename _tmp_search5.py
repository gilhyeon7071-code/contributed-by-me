import pathlib

brain_dir = pathlib.Path(r"C:\Users\jjtop\.gemini\antigravity\brain")

for conv_dir in brain_dir.iterdir():
    if not conv_dir.is_dir():
        continue
    log_file = conv_dir / ".system_generated" / "logs" / "transcript_full.jsonl"
    if log_file.exists():
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line_idx, line in enumerate(f):
                    if "11.35" in line:
                        print(f"--- MATCH IN CONV: {conv_dir.name} at line {line_idx} ---")
                        # print first 500 characters of the line
                        print(line[:500])
                        print("\n")
        except Exception as e:
            pass
