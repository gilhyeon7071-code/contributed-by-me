import sqlite3
import pandas as pd

def check_db():
    try:
        # Connect in read-only mode using URI
        conn = sqlite3.connect('file:E:/1_Data/news_trading/data/trading.db?mode=ro', uri=True, timeout=5)
        
        tables = ['news_articles_naver', 'news_articles_google_rss', 'news_articles_kis_title', 'news_articles_global_macro', 'signals_naver_daily', 'source_signals_daily']
        
        for table in tables:
            print(f"\\n--- {table} ---")
            try:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table});")
                cols = [c[1] for c in cursor.fetchall()]
                
                date_col = 'published_at'
                if 'published_at' not in cols:
                    if 'created_at' in cols:
                        date_col = 'created_at'
                    elif 'pubDate' in cols:
                        date_col = 'pubDate'
                    elif 'date' in cols:
                        date_col = 'date'
                    elif 'fetch_date' in cols:
                        date_col = 'fetch_date'
                    else:
                        print(f"Columns: {cols}")
                        continue
                
                query = f"SELECT SUBSTR({date_col}, 1, 10) as dt, COUNT(*) FROM {table} GROUP BY dt ORDER BY dt DESC LIMIT 5;"
                cursor.execute(query)
                rows = cursor.fetchall()
                for r in rows:
                    print(f"Date: {r[0]}, Count: {r[1]}")
                    
            except Exception as e:
                print(f"Error querying {table}: {e}")
                
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

check_db()
