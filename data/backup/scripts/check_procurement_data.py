import pandas as pd

# CSVファイルから調達データの期間を確認
df_header = pd.read_csv(r'C:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_header.csv')
df_item = pd.read_csv(r'C:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_item.csv')

print("=" * 100)
print("調達伝票データの期間確認（CSVファイル）")
print("=" * 100)

print("\n【調達伝票ヘッダー】")
print(f"最小日付: {df_header['order_date'].min()}")
print(f"最大日付: {df_header['order_date'].max()}")
print(f"総レコード数: {len(df_header)}")

print("\n年別レコード数:")
year_counts = df_header['order_date'].str[:4].value_counts().sort_index()
print(year_counts)

print("\n【調達伝票明細（直接材のみ）】")
df_direct = df_item[df_item['material_type'] == 'direct']
print(f"直接材レコード数: {len(df_direct)}")
print(f"商品ID数: {df_direct['product_id'].nunique()}")

print("\n商品ID一覧:")
print(sorted(df_direct['product_id'].unique()))

# データベースと比較
import sqlite3
conn = sqlite3.connect(r'C:\Users\PC\dev\ForStep2\data\bronze_data.db')

print("\n" + "=" * 100)
print("データベースとの比較")
print("=" * 100)

query = """
SELECT 
    MIN(order_date) as min_date,
    MAX(order_date) as max_date,
    COUNT(*) as count
FROM p2p_procurement_header
"""
df_db = pd.read_sql_query(query, conn)
print("\n【データベース: 調達ヘッダー】")
print(df_db)

query2 = """
SELECT 
    COUNT(*) as total_count,
    COUNT(DISTINCT product_id) as unique_products
FROM p2p_procurement_item
WHERE material_type = 'direct'
"""
df_db2 = pd.read_sql_query(query2, conn)
print("\n【データベース: 調達明細（直接材）】")
print(df_db2)

conn.close()

print("\n結論:")
if df_header['order_date'].max() > '2022-12-31':
    print("CSVファイルには2023年以降のデータも存在します！")
    print("データベースに正しくロードされていない可能性があります。")
else:
    print("CSVファイルも2022年までのデータのみです。")
