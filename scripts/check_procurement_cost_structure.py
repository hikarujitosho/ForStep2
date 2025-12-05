import sqlite3
import pandas as pd

conn = sqlite3.connect('database/data_lake.db')

# 受注データと調達データの関係を確認
query = """
SELECT 
    o.year_month,
    o.product_id,
    i.product_name,
    o.line_total_ex_tax as order_amount,
    p.line_total_ex_tax as procurement_amount,
    CASE 
        WHEN p.line_total_ex_tax IS NOT NULL THEN 
            ROUND((p.line_total_ex_tax * 100.0 / o.line_total_ex_tax), 2)
        ELSE NULL
    END as procurement_ratio
FROM silver_order_data o
JOIN silver_item_master i ON o.product_id = i.product_id
LEFT JOIN silver_procurement_data p ON o.product_id = p.product_id 
    AND o.year_month = p.year_month
WHERE o.year_month IN ('2022-01', '2022-02', '2023-01', '2024-01', '2025-01')
    AND o.line_total_ex_tax > 0
LIMIT 30
"""

df = pd.read_sql_query(query, conn)

print("=" * 100)
print("受注データと調達データの金額比較")
print("=" * 100)
print(df.to_string(index=False))

# 集計統計
print("\n" + "=" * 100)
print("調達比率の統計")
print("=" * 100)

有効データ = df[df['procurement_ratio'].notna()]
if len(有効データ) > 0:
    print(f"平均調達比率: {有効データ['procurement_ratio'].mean():.2f}%")
    print(f"最小調達比率: {有効データ['procurement_ratio'].min():.2f}%")
    print(f"最大調達比率: {有効データ['procurement_ratio'].max():.2f}%")
else:
    print("調達データなし")

print(f"\n調達データがあるレコード: {df['procurement_amount'].notna().sum()}件")
print(f"調達データがないレコード: {df['procurement_amount'].isna().sum()}件")

# silver_procurement_dataテーブルの構造を確認
print("\n" + "=" * 100)
print("silver_procurement_dataテーブル構造")
print("=" * 100)

schema_query = """
SELECT sql FROM sqlite_master 
WHERE type='table' AND name='silver_procurement_data'
"""
schema = pd.read_sql_query(schema_query, conn)
if len(schema) > 0:
    print(schema['sql'].iloc[0])
else:
    print("テーブルが存在しません")

# サンプルデータを確認
print("\n" + "=" * 100)
print("silver_procurement_dataサンプルデータ")
print("=" * 100)

procurement_query = """
SELECT * FROM silver_procurement_data
LIMIT 10
"""
procurement_df = pd.read_sql_query(procurement_query, conn)
print(procurement_df.to_string(index=False))

conn.close()
