import sqlite3
import pandas as pd

conn = sqlite3.connect('data/bronze_data.db')

print("Bronze_p2p_procurement_headerのカラム:")
df_cols = pd.read_sql_query("PRAGMA table_info(bronze_p2p_procurement_header)", conn)
print(df_cols[['name']].to_string(index=False))

print("\n新規POのorder_dateサンプル:")
df = pd.read_sql_query("""
SELECT purchase_order_id, order_date
FROM bronze_p2p_procurement_header
WHERE purchase_order_id LIKE 'PO-SUPP-ACD%'
ORDER BY order_date
LIMIT 10
""", conn)
print(df.to_string(index=False))

conn.close()
