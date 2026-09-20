import os
import json
import urllib.request
import urllib.parse
import argparse

SECRETS_DIR = r"E:\1_Data\.secrets"
CONFIG_FILE = r"E:\1_Data\config\notification_config.json"

def get_secret(filename):
    path = os.path.join(SECRETS_DIR, filename)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    return ""

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {"notifyBuy": False, "notifySell": False, "notifyError": True, "notifyMaintenance": True}

def send_telegram_message(message, event_type=None):
    """
    event_type can be: 'buy', 'sell', 'error', 'maintenance', 'test'
    2026-08-20: 'maintenance' 추가. 정비/보존 알림을 'error' 채널에 얹으면
    장애 알림과 섞이고, 사용자가 에러 알림을 끄면 함께 사라진다.
    """
    token = get_secret("telegram_bot_token.txt")
    chat_id = get_secret("telegram_chat_id.txt")
    
    if not token or not chat_id:
        print("[Telegram] Skip: Missing Bot Token or Chat ID.")
        return False

    config = load_config()
    
    # Check flags
    if event_type == 'buy' and not config.get('notifyBuy', False):
        print("[Telegram] Skip: notifyBuy is OFF.")
        return False
    if event_type == 'sell' and not config.get('notifySell', False):
        print("[Telegram] Skip: notifySell is OFF.")
        return False
    if event_type == 'maintenance' and not config.get('notifyMaintenance', True):
        print("[Telegram] Skip: notifyMaintenance is OFF.")
        return False
    if event_type == 'error' and not config.get('notifyError', True):
        print("[Telegram] Skip: notifyError is OFF.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": message
    }).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=5) as response:
            res = response.read()
            print("[Telegram] Sent successfully.")
            return True
    except Exception as e:
        print(f"[Telegram] Error sending message: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send Telegram Notification")
    parser.add_argument("--test", action="store_true", help="Send a test message")
    parser.add_argument("--msg", type=str, help="Message to send")
    parser.add_argument("--event", type=str, choices=['buy', 'sell', 'error', 'maintenance', 'test'], default='test')
    args = parser.parse_args()

    if args.test:
        send_telegram_message("✅ [알림 테스트] 시스템과 텔레그램이 정상적으로 연결되었습니다!", event_type='test')
    elif args.msg:
        send_telegram_message(args.msg, event_type=args.event)
    else:
        print("Provide --test or --msg.")
