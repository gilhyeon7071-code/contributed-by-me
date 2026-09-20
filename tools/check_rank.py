import csv

def safe_float(v):
    try:
        s = str(v).strip().replace(',', '')
        if s in ('', '-', 'N/A', 'NA', 'None', 'null', 'nan'):
            return float('-inf')
        return float(s)
    except (ValueError, TypeError):
        return float('-inf')

with open("E:/1_Data/2_Logs/candidates_latest_data.with_news_score.csv", encoding="utf-8") as f:
    r = list(csv.DictReader(f))
    r.sort(key=lambda x: safe_float(x.get("score")), reverse=True)
    
    for i, x in enumerate(r):
        if x["name"] == "삼성전자":
            print(f"Index: {i}, Name: {x['name']}, Score: {x['score']}")
