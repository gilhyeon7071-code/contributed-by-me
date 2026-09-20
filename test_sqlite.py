import sqlite3
conn = sqlite3.connect(r'E:\1_Data\2_Logs\error_history.db')
c = conn.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type='table';")
print("Tables:", c.fetchall())

try:
    c.execute("SELECT * FROM error_history ORDER BY id DESC LIMIT 1;")
    print("Row:", c.fetchone())
except Exception as e:
    print(e)
conn.close()
