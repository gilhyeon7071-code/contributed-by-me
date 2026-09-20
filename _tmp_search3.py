import pathlib
import json

brain_dir = pathlib.Path(r"C:\Users\jjtop\.gemini\antigravity\brain")

for conv_dir in brain_dir.iterdir():
    if not conv_dir.is_dir():
        continue
    log_file = conv_dir / ".system_generated" / "logs" / "transcript_full.jsonl"
    if log_file.exists():
        try:
            content_text = log_file.read_text(encoding='utf-8', errors='ignore')
            if "11.35%" in content_text or "127)" in content_text or "120)" in content_text:
                print(f"Matched Conversation ID: {conv_dir.name}")
                lines = content_text.splitlines()
                # Find the user input that contains 11.35% or similar
                for idx, line in enumerate(lines):
                    if "11.35" in line or "127" in line or "120" in line:
                        try:
                            data = json.loads(line)
                            if data.get("type") in ["USER_INPUT", "PLANNER_RESPONSE"]:
                                # print the first 150 chars of matching content
                                print(f"  Step {data.get('step_index')}: {data.get('content')[:200].strip()}...")
                        except Exception:
                            pass
        except Exception as e:
            pass
