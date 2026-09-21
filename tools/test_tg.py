import urllib.request
import urllib.parse
import urllib.error
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "manual"))
from telegram_secret import bot_token, chat_id  # noqa: E402

TOKEN = bot_token()
CHAT_ID = chat_id()
TEXT = '✅ 텔레그램 연동 성공!'

req = urllib.request.Request(
    f'https://api.telegram.org/bot{TOKEN}/sendMessage',
    data=urllib.parse.urlencode({'chat_id': CHAT_ID, 'text': TEXT}).encode()
)
try:
    print(urllib.request.urlopen(req).read().decode())
except urllib.error.HTTPError as e:
    print("HTTPError:", e.code)
    print(e.read().decode())
