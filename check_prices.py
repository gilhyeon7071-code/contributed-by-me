import pandas as pd
try:
    df = pd.read_csv('E:/vibe/buffett/data/live/live_fills.csv')
    df = df[df['side'] == 'BUY']
    for code in ['011090', '012690', '033340', '093240']:
        match = df[df['code'].astype(str).str.zfill(6) == code]
        for idx, row in match.iterrows():
            note = row.get('note', '')
            ts = ''
            if 'signal_ts=' in note:
                ts = note.split('signal_ts=')[1].split(';')[0]
            print(f"{code} Fill: Qty={row.get('qty')} Price={row.get('price')} SignalTS={ts}")
except Exception as e:
    print(e)
