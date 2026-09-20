"""error_pattern_analyzer.py
Extracts error logs and tracks them into SQLite for auto-recovery and analytics.
"""
import sqlite3
import datetime as dt
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
LOGS_DIR = ROOT / "2_Logs"
DB_PATH = LOGS_DIR / "error_history.db"
KST = dt.timezone(dt.timedelta(hours=9))

def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS error_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            date TEXT NOT NULL,
            error_code TEXT NOT NULL,
            source_file TEXT,
            raw_message TEXT
        )
    ''')
    conn.commit()
    return conn

def log_error(error_code: str, source_file: str, raw_message: str):
    """Log an error pattern to the SQLite DB."""
    conn = init_db()
    now = dt.datetime.now(tz=KST)
    timestamp = now.isoformat(timespec='seconds')
    date = now.strftime("%Y-%m-%d")
    
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO error_history (timestamp, date, error_code, source_file, raw_message)
        VALUES (?, ?, ?, ?, ?)
    ''', (timestamp, date, error_code, source_file, raw_message))
    conn.commit()
    conn.close()

def get_error_count_today(error_code: str) -> int:
    """Returns how many times this error_code was recorded today."""
    conn = init_db()
    date = dt.datetime.now(tz=KST).strftime("%Y-%m-%d")
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM error_history WHERE date = ? AND error_code = ?', (date, error_code))
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_all_errors_today() -> list:
    """Returns a list of dictionaries with all errors logged today."""
    conn = init_db()
    date = dt.datetime.now(tz=KST).strftime("%Y-%m-%d")
    cursor = conn.cursor()
    cursor.execute('SELECT id, timestamp, error_code, source_file, raw_message FROM error_history WHERE date = ? ORDER BY id DESC', (date,))
    rows = cursor.fetchall()
    conn.close()
    
    results = []
    for row in rows:
        results.append({
            "id": row[0],
            "timestamp": row[1],
            "error_code": row[2],
            "source_file": row[3],
            "raw_message": row[4]
        })
    return results

if __name__ == "__main__":
    init_db()
    print(f"Error Database initialized at {DB_PATH}")
