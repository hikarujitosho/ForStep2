import sqlite3
import pandas as pd

conn = sqlite3.connect('database/data_lake.db')

query = """
SELECT 
    year_month,
    product_name,
    revenue,
    cost,
    gross_profit,
    gross_margin
FROM gold_monthly_product_gross_margin
LIMIT 20
"""

df = pd.read_sql_query(query, conn)

print("=" * 100)
print("year_monthカラムのフォーマット確認")
print("=" * 100)
print(df)

print("\n" + "=" * 100)
print("year_monthのデータ型とサンプル値")
print("=" * 100)
print(f"データ型: {df['year_month'].dtype}")
print(f"ユニーク値の数: {df['year_month'].nunique()}")
print(f"\nサンプル値:")
print(df['year_month'].unique()[:5])

# テーブル構造を確認
query_schema = """
SELECT sql FROM sqlite_master 
WHERE type='table' AND name='gold_monthly_product_gross_margin'
"""
schema = pd.read_sql_query(query_schema, conn)
print("\n" + "=" * 100)
print("テーブル構造")
print("=" * 100)
print(schema['sql'].iloc[0])

conn.close()
