import pandas as pd
try:
    df = pd.read_csv('E:/vibe/buffett/data/live/live_fills.csv')
    for code in ['011090', '012690', '033340', '093240']:
        match = df[df['code'].astype(str).str.zfill(6) == code]
        if not match.empty:
            for idx, row in match.iterrows():
                print(f"{code} Fill: {row.get('side')} {row.get('qty')} @ {row.get('price')} reason={row.get('reason')} note={row.get('note')}")
except Exception as e:
    print(e)
