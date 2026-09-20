import urllib.request
import json
import sys

TOKEN = "8931399277:AAGXaRL1meuMtgS5SC3kajES374xJ8WY-nY"
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
