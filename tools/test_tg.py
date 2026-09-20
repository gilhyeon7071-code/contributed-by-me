import urllib.request
import urllib.parse
import urllib.error

TOKEN = '8931399277:AAGXaRL1meuMtgS5SC3kajES374xJ8WY-nY'
CHAT_ID = '7176011988'
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
