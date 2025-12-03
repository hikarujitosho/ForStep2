import sqlite3
import pandas as pd

# データベース接続
db_path = r'C:\Users\PC\dev\ForStep2\data\bronze_data.db'
conn = sqlite3.connect(db_path)

print("=" * 100)
print("期間別データ調査")
print("=" * 100)

# 1. 受注データの期間
print("\n=== 受注データの期間 ===")
df1 = pd.read_sql_query("""
    SELECT 
        MIN(DATE(order_timestamp)) as min_date,
        MAX(DATE(order_timestamp)) as max_date,
        COUNT(*) as total_orders,
        COUNT(DISTINCT product_id) as unique_products
    FROM erp_order_header oh
    JOIN erp_order_item oi ON oh.order_id = oi.order_id
""", conn)
print(df1)

# 2. 調達データの期間
print("\n=== 調達データの期間（直接材） ===")
df2 = pd.read_sql_query("""
    SELECT 
        MIN(order_date) as min_date,
        MAX(order_date) as max_date,
        COUNT(*) as total_procurements,
        COUNT(DISTINCT product_id) as unique_products
    FROM p2p_procurement_header ph
    JOIN p2p_procurement_item pi ON ph.purchase_order_id = pi.purchase_order_id
    WHERE pi.material_type = 'direct'
""", conn)
print(df2)

# 3. 月別の受注と調達の件数比較
print("\n=== 月別の受注と調達の件数 ===")
df3 = pd.read_sql_query("""
    SELECT 
        strftime('%Y-%m', oh.order_timestamp) as year_month,
        COUNT(DISTINCT oh.order_id) as order_count,
        SUM(oi.quantity) as order_quantity
    FROM erp_order_header oh
    JOIN erp_order_item oi ON oh.order_id = oi.order_id
    GROUP BY strftime('%Y-%m', oh.order_timestamp)
    ORDER BY year_month
""", conn)
print("\n受注データ:")
print(df3)

df4 = pd.read_sql_query("""
    SELECT 
        strftime('%Y-%m', ph.order_date) as year_month,
        COUNT(DISTINCT ph.purchase_order_id) as procurement_count,
        SUM(pi.quantity * pi.unit_price_ex_tax) as total_cost
    FROM p2p_procurement_header ph
    JOIN p2p_procurement_item pi ON ph.purchase_order_id = pi.purchase_order_id
    WHERE pi.material_type = 'direct'
    GROUP BY strftime('%Y-%m', ph.order_date)
    ORDER BY year_month
""", conn)
print("\n調達データ（直接材）:")
print(df4)

# 4. 商品別・月別の受注と調達の対応状況
print("\n=== 商品別の受注と調達の対応状況（サンプル） ===")
df5 = pd.read_sql_query("""
    SELECT 
        oi.product_id,
        strftime('%Y-%m', oh.order_timestamp) as order_month,
        SUM(oi.quantity) as order_quantity,
        SUM(oi.quantity * pc.selling_price_ex_tax) as sales_amount,
        COALESCE(SUM(pi.quantity * pi.unit_price_ex_tax), 0) as procurement_cost
    FROM erp_order_item oi
    JOIN erp_order_header oh ON oi.order_id = oh.order_id
    JOIN erp_price_condition_master pc ON 
        oi.product_id = pc.product_id 
        AND oh.customer_id = pc.customer_id
        AND DATE(oh.order_timestamp) >= pc.valid_from 
        AND DATE(oh.order_timestamp) < pc.valid_to
    LEFT JOIN p2p_procurement_item pi ON 
        oi.product_id = pi.product_id 
        AND pi.material_type = 'direct'
    LEFT JOIN p2p_procurement_header ph ON 
        pi.purchase_order_id = ph.purchase_order_id
        AND strftime('%Y-%m', oh.order_timestamp) = strftime('%Y-%m', ph.order_date)
    WHERE strftime('%Y-%m', oh.order_timestamp) >= '2024-01'
    GROUP BY oi.product_id, strftime('%Y-%m', oh.order_timestamp)
    ORDER BY order_month, oi.product_id
    LIMIT 30
""", conn)
print(df5)

# 5. ACD-CV1とRDG-YF6の調達データ確認
print("\n=== ACD-CV1の調達データ確認 ===")
df6 = pd.read_sql_query("""
    SELECT COUNT(*) as count
    FROM p2p_procurement_item
    WHERE product_id = 'ACD-CV1' AND material_type = 'direct'
""", conn)
print(f"ACD-CV1の直接材調達レコード数: {df6.iloc[0]['count']}")

df7 = pd.read_sql_query("""
    SELECT COUNT(*) as count
    FROM p2p_procurement_item
    WHERE product_id = 'RDG-YF6' AND material_type = 'direct'
""", conn)
print(f"RDG-YF6の直接材調達レコード数: {df7.iloc[0]['count']}")

conn.close()
