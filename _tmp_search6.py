import pathlib
import json

brain_dir = pathlib.Path(r"C:\Users\jjtop\.gemini\antigravity\brain")

for conv_dir in brain_dir.iterdir():
    if not conv_dir.is_dir():
        continue
    log_file = conv_dir / ".system_generated" / "logs" / "transcript_full.jsonl"
    if log_file.exists():
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line_idx, line in enumerate(f):
                    if "(127)" in line or "(120)" in line:
                        print(f"--- MATCH IN CONV: {conv_dir.name} at line {line_idx} ---")
                        try:
                            data = json.loads(line)
                            content = data.get("content", "")
                            # print lines in content that contain (127) or (120)
                            for l in content.splitlines():
                                if "(127)" in l or "(120)" in l:
                                    print(f"  {l}")
                        except Exception:
                            # If not json, print line preview
                            print(line[:300])
                        print()
        except Exception as e:
            pass
