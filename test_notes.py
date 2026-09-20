import pandas as pd
df = pd.read_csv('E:/vibe/buffett/data/live/live_fills.csv')
for code in ['355390', '037440', '403870']:
    match = df[(df['side'] == 'BUY') & (df['code'].astype(str).str.zfill(6) == code)]
    for idx, row in match.iterrows():
        print(f'{code} | {row.get("note", "")}')
