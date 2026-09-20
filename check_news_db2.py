import sqlite3

def check_db():
    conn = sqlite3.connect('file:E:/1_Data/news_trading/data/trading.db?mode=ro', uri=True, timeout=5)
    cursor = conn.cursor()
    
    queries = {
        'news_articles_kis_title': "SELECT data_dt, COUNT(*) FROM news_articles_kis_title GROUP BY data_dt ORDER BY data_dt DESC LIMIT 5;",
        'signals_naver_daily': "SELECT date8, COUNT(*) FROM signals_naver_daily GROUP BY date8 ORDER BY date8 DESC LIMIT 5;",
        'source_signals_daily': "SELECT date8, COUNT(*) FROM source_signals_daily GROUP BY date8 ORDER BY date8 DESC LIMIT 5;"
    }
    
    for table, query in queries.items():
        print(f"\\n--- {table} ---")
        try:
            cursor.execute(query)
            for r in cursor.fetchall():
                print(f"Date: {r[0]}, Count: {r[1]}")
        except Exception as e:
            print(e)

check_db()
