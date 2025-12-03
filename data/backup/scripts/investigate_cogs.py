import sqlite3
import pandas as pd

# データベース接続
db_path = r'C:\Users\PC\dev\ForStep2\data\bronze_data.db'
conn = sqlite3.connect(db_path)

print("=" * 100)
print("調達データ調査")
print("=" * 100)

# 1. 調達伝票itemテーブルのサンプル
print("\n=== 調達伝票itemテーブルのサンプル ===")
df = pd.read_sql_query("""
    SELECT purchase_order_id, line_number, material_id, material_name, 
           material_type, product_id, quantity, unit_price_ex_tax
    FROM p2p_procurement_item 
    LIMIT 10
""", conn)
print(df)

# 2. material_typeの分布
print("\n=== material_typeの分布 ===")
df2 = pd.read_sql_query("""
    SELECT material_type, COUNT(*) as count 
    FROM p2p_procurement_item 
    GROUP BY material_type
""", conn)
print(df2)

# 3. product_idの状態を確認
print("\n=== product_idの状態 ===")
df3 = pd.read_sql_query("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(product_id) as not_null_count,
        SUM(CASE WHEN product_id IS NULL OR product_id = '' THEN 1 ELSE 0 END) as null_or_empty_count,
        SUM(CASE WHEN material_type = 'direct' THEN 1 ELSE 0 END) as direct_material_count,
        SUM(CASE WHEN material_type = 'direct' AND (product_id IS NULL OR product_id = '') THEN 1 ELSE 0 END) as direct_without_product_id
    FROM p2p_procurement_item
""", conn)
print(df3)

# 4. 直接材でproduct_idがあるレコードのサンプル
print("\n=== 直接材でproduct_idがあるレコードのサンプル ===")
df4 = pd.read_sql_query("""
    SELECT purchase_order_id, material_id, material_name, material_type, 
           product_id, quantity, unit_price_ex_tax
    FROM p2p_procurement_item 
    WHERE material_type = 'direct' AND product_id IS NOT NULL AND product_id != ''
    LIMIT 10
""", conn)
print(df4)
print(f"\n直接材でproduct_idがあるレコード数: {len(df4)}")

# 5. 実際のクエリを実行してみる
print("\n=== 実際の売上原価クエリの結果 ===")
df5 = pd.read_sql_query("""
    SELECT 
        pi.product_id,
        strftime('%Y-%m', ph.order_date) as year_month,
        COUNT(*) as record_count,
        SUM(pi.quantity * pi.unit_price_ex_tax) as total_material_cost
    FROM p2p_procurement_item pi
    INNER JOIN p2p_procurement_header ph ON pi.purchase_order_id = ph.purchase_order_id
    WHERE pi.material_type = 'direct'
    GROUP BY pi.product_id, strftime('%Y-%m', ph.order_date)
    LIMIT 10
""", conn)
print(df5)

# 6. 受注データで使われているproduct_idのリスト
print("\n=== 受注データの商品ID ===")
df6 = pd.read_sql_query("""
    SELECT DISTINCT product_id 
    FROM erp_order_item 
    ORDER BY product_id
""", conn)
print(df6)

# 7. 調達データのproduct_idの一覧（直接材のみ）
print("\n=== 調達データの商品ID（直接材） ===")
df7 = pd.read_sql_query("""
    SELECT DISTINCT product_id 
    FROM p2p_procurement_item 
    WHERE material_type = 'direct' AND product_id IS NOT NULL AND product_id != ''
    ORDER BY product_id
""", conn)
print(df7)

# 8. 受注と調達のproduct_idの照合
print("\n=== 受注と調達のproduct_ID照合 ===")
df8 = pd.read_sql_query("""
    SELECT 
        oi.product_id,
        COUNT(DISTINCT oi.order_id) as order_count,
        SUM(oi.quantity) as total_order_quantity,
        COUNT(DISTINCT pi.purchase_order_id) as procurement_count
    FROM erp_order_item oi
    LEFT JOIN p2p_procurement_item pi ON 
        oi.product_id = pi.product_id 
        AND pi.material_type = 'direct'
    GROUP BY oi.product_id
    LIMIT 20
""", conn)
print(df8)

conn.close()
