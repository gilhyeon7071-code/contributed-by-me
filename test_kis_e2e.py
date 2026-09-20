import sys
sys.path.insert(0, r'E:\1_Data\tools')
import kis_order_client
from unittest.mock import patch
import requests

cfg = kis_order_client.KISConfig(app_key='dummy', app_secret='dummy', cano='12345678', acnt_prdt_cd='12')
client = kis_order_client.KISOrderClient(cfg=cfg)

with patch('requests.Session.request') as mock_request:
    mock_request.side_effect = requests.exceptions.Timeout('Mocked timeout error test 2')
    try:
        client._request_json('POST', '/uapi/domestic-stock/v1/trading/order-cash')
    except Exception as e:
        print('Exception caught:', type(e).__name__)
        print('Exception details:', str(e))
        if hasattr(e, 'category'):
            print('Exception category:', e.category)

import sqlite3
import datetime
conn = sqlite3.connect(r'E:\1_Data\2_Logs\error_history.db')
c = conn.cursor()
c.execute('SELECT * FROM error_history ORDER BY id DESC LIMIT 1')
row = c.fetchone()
print('DB Row:', row)
conn.close()
