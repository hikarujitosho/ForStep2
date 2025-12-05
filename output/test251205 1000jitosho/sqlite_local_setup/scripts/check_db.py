import sqlite3

conn = sqlite3.connect(r'C:/Users/PC/dev/ForStep2/output/test251205 1000jitosho/sqlite_local_setup/database/analytics.db')
cursor = conn.cursor()

tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()

print("=== Tables in analytics.db ===")
for table in tables:
    count = cursor.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
    print(f"{table[0]}: {count:,} records")

conn.close()
