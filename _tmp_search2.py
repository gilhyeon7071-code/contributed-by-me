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
            if "11.35%" in content_text or "CAGR -6.28%" in content_text:
                print(f"FOUND MATCHING CONVERSATION: {conv_dir.name}")
                # Let's find specific lines in the jsonl
                lines = content_text.splitlines()
                for idx, line in enumerate(lines):
                    if "11.35%" in line or "127" in line or "120" in line:
                        try:
                            data = json.loads(line)
                            step = data.get("step_index", idx)
                            t = data.get("type")
                            cnt = data.get("content", "")
                            print(f"  Step {step} ({t}):")
                            for l in cnt.splitlines():
                                if "11.35" in l or "127" in l or "120" in l or "CAGR" in l or "KOSPI" in l:
                                    print(f"    {l}")
                        except Exception as e:
                            pass
        except Exception as e:
            print(f"Error reading {log_file}: {e}")
