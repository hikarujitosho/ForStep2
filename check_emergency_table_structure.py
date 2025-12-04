import sqlite3

# データベース接続
db_path = 'database/data_lake.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# テーブル構造を確認
cursor.execute("PRAGMA table_info(gold_emergency_transportation_cost_share)")
columns = cursor.fetchall()

print("=" * 80)
print("gold_emergency_transportation_cost_share テーブル構造")
print("=" * 80)
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# サンプルデータを取得
cursor.execute("SELECT * FROM gold_emergency_transportation_cost_share LIMIT 5")
rows = cursor.fetchall()

print("\n【サンプルデータ（最初の5件）】")
col_names = [col[1] for col in columns]
print(" | ".join(col_names))
print("-" * 80)
for row in rows:
    print(" | ".join(str(x) for x in row))

conn.close()
