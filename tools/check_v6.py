import csv, json
import urllib.request
import ssl

print(f"SSL Version: {ssl.OPENSSL_VERSION}")

try:
    with open('E:/1_Data/2_Logs/candidates_latest_data.with_news_score.csv', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        dates = set()
        scores = []
        for i, row in enumerate(reader):
            if 'date' in row and row['date']: dates.add(row['date'])
            if 'score' in row and i < 20: scores.append(row['score'])
        print('A. Date unique values:', list(dates))
        print('B. Score format (first 20):', scores)
except Exception as e: print('CSV Error:', e)

try:
    with open('E:/vibe/buffett/runs/dashboard_state_latest.json', encoding='utf-8') as f:
        data = json.load(f)
        print('C. as_of_ymd format:', data.get('as_of_ymd'))
except Exception as e: print('JSON Error:', e)

try:
    req = urllib.request.Request('https://api.openai.com/v1/chat/completions', method='OPTIONS')
    with urllib.request.urlopen(req, timeout=5) as response:
        print('OpenAI HTTPS test: Status', response.status)
except urllib.error.HTTPError as e:
    # 401 is expected for OPTIONS or without key, but means SSL and connection worked
    print('OpenAI HTTPS test HTTPError (Expected 401):', e.code)
except Exception as e:
    print('OpenAI HTTPS test error:', type(e), e)
