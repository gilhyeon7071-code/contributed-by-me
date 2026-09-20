import pathlib
import json

log_path = pathlib.Path(r"C:\Users\jjtop\.gemini\antigravity\brain\91465888-5733-4a90-a12a-cfa056f34a16\.system_generated\logs\transcript_full.jsonl")

if log_path.exists():
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            try:
                data = json.loads(line)
                step = data.get("step_index")
                if step in [120, 127]:
                    print(f"=== STEP {step} ({data.get('type')}) ===")
                    print(data.get("content"))
                    print("\n" + "="*50 + "\n")
            except Exception as e:
                pass
else:
    print("Not found.")
