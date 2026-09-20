import sys
import json
import urllib.request
import traceback

def get_price_naver_json(code):
    url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            price_str = data['datas'][0]['closePrice']
            return int(price_str.replace(',', ''))
    except Exception as e:
        pass
    return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({}))
        sys.exit(0)
    
    codes_arg = sys.argv[1]
    if not codes_arg.strip():
        print(json.dumps({}))
        sys.exit(0)

    codes = codes_arg.split(',')
    result = {}
    for code in codes:
        code = code.strip()
        if not code: continue
        price = get_price_naver_json(code)
        if price is not None:
            result[code] = price
    
    print(json.dumps(result))
