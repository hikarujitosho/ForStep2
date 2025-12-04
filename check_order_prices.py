import sqlite3
import pandas as pd

conn = sqlite3.connect('database/data_lake.db')

# 受注データの価格分布を確認
query = """
SELECT 
    product_id,
    product_name,
    MIN(line_total_ex_tax) as min_price,
    AVG(line_total_ex_tax) as avg_price,
    MAX(line_total_ex_tax) as max_price,
    COUNT(*) as record_count
FROM (
    SELECT 
        o.product_id,
        i.product_name,
        o.line_total_ex_tax
    FROM silver_order_data o
    JOIN silver_item_master i ON o.product_id = i.product_id
    WHERE o.line_total_ex_tax > 0
)
GROUP BY product_id, product_name
ORDER BY avg_price DESC
"""

df = pd.read_sql_query(query, conn)

print("=" * 120)
print("商品別販売価格の分布")
print("=" * 120)
for _, row in df.iterrows():
    print(f"\n{row['product_name']} ({row['product_id']})")
    print(f"  最低価格: ¥{row['min_price']:,.0f}")
    print(f"  平均価格: ¥{row['avg_price']:,.0f}")
    print(f"  最高価格: ¥{row['max_price']:,.0f}")
    print(f"  レコード数: {row['record_count']:,}件")
    
    # 適正範囲チェック (200万～500万円)
    if row['avg_price'] < 2000000:
        print(f"  ⚠️ 平均価格が200万円未満")
    elif row['avg_price'] > 5000000:
        print(f"  ⚠️ 平均価格が500万円超過")
    else:
        print(f"  ✓ 適正範囲内 (200-500万円)")

# サンプルデータを確認
print("\n" + "=" * 120)
print("サンプル受注データ（最初の20件）")
print("=" * 120)

sample_query = """
SELECT 
    o.order_id,
    o.product_id,
    i.product_name,
    o.quantity,
    o.unit_price_ex_tax,
    o.line_total_ex_tax,
    o.order_date
FROM silver_order_data o
JOIN silver_item_master i ON o.product_id = i.product_id
ORDER BY o.order_date, o.order_id
LIMIT 20
"""

sample_df = pd.read_sql_query(sample_query, conn)
print(sample_df.to_string(index=False))

# テーブル構造を確認
print("\n" + "=" * 120)
print("silver_order_dataテーブル構造")
print("=" * 120)

schema_query = """
SELECT sql FROM sqlite_master 
WHERE type='table' AND name='silver_order_data'
"""
schema = pd.read_sql_query(schema_query, conn)
print(schema['sql'].iloc[0])

conn.close()
