import sqlite3
import pandas as pd

db = sqlite3.connect('E:/1_Data/news_trading/data/trading.db')
query = """
SELECT code, title, llm_score, implication_direction, llm_reasoning 
FROM news_articles_naver 
WHERE date8='20260702' AND llm_score IS NOT NULL 
LIMIT 10
"""
df = pd.read_sql(query, db)
df.to_csv('E:/1_Data/2_Logs/llm_sample.csv', index=False, encoding='utf-8-sig')
