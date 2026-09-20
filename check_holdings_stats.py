import json
import pandas as pd

def check():
    try:
        with open('E:/vibe/buffett/runs/dashboard_state_latest.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            for code in ['011090', '012690', '033340', '093240']:
                for h in data.get('dashboard', {}).get('holdings_rows', []):
                    if h.get('code', '') == code:
                        print(f"{code}: CurPrice={h.get('curr_price')} Ret={h.get('curr_ret_pct')} Buy={h.get('buy_price')}")
    except Exception as e:
        print(e)
check()
