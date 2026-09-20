import pathlib
import json

log_path = pathlib.Path(r"C:\Users\jjtop\.gemini\antigravity\brain\90035e10-9988-42dc-8509-c215e9d268fe\.system_generated\logs\transcript_full.jsonl")

if log_path.exists():
    lines = log_path.read_text(encoding='utf-8').splitlines()
    print(f"Total lines in transcript_full.jsonl: {len(lines)}")
    for idx, line in enumerate(lines):
        try:
            data = json.loads(line)
            content = data.get("content", "")
            step_index = data.get("step_index", idx)
            if "127" in content or "120" in content or "11.35%" in content:
                print(f"--- MATCH AT STEP {step_index} ({data.get('type')}) ---")
                lines_in_content = content.splitlines()
                for l_idx, l in enumerate(lines_in_content):
                    if "127" in l or "120" in l or "11.35" in l:
                        start = max(0, l_idx - 5)
                        end = min(len(lines_in_content), l_idx + 6)
                        print(f"Context from line {start} to {end}:")
                        for i in range(start, end):
                            print(f"  {i}: {lines_in_content[i]}")
                        print()
        except Exception as e:
            print(f"Error parsing line {idx}: {e}")
else:
    print("Log file not found.")
