import urllib.request
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "manual"))
from telegram_secret import bot_token, chat_id  # noqa: E402

TOKEN = bot_token()
URL = f"https://api.telegram.org/bot{TOKEN}/getUpdates"

try:
    req = urllib.request.Request(URL)
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())
        if data.get("ok"):
            results = data.get("result", [])
            for r in results:
                if "message" in r:
                    chat_id = r["message"]["chat"]["id"]
                    print(f"CHAT_ID={chat_id}")
                    sys.exit(0)
            print("No messages found in getUpdates.")
        else:
            print("Failed to getUpdates:", data)
except Exception as e:
    print("Error:", e)
