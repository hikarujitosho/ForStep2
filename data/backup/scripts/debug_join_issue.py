"""
新規生成した調達データとJOIN結果を直接確認
"""
import sqlite3
import pandas as pd

db_path = 'data/bronze_data.db'
conn = sqlite3.connect(db_path)

TARGET_VEHICLES = ['ACD-CV1', 'RDG-YF6']

print("=" * 80)
print("Silver層の調達データ確認")
print("=" * 80)

for vehicle in TARGET_VEHICLES:
    print(f"\n【{vehicle}】")
    
    # Silver層の月次調達データ
    query1 = f"""
    SELECT year_month, 
           COUNT(*) as record_count,
           SUM(total_cost_ex_tax) as total_cost
    FROM silver_monthly_procurement
    WHERE product_id = '{vehicle}'
        AND material_type = 'direct'
    GROUP BY year_month
    ORDER BY year_month DESC
    LIMIT 5
    """
    result1 = pd.read_sql_query(query1, conn)
    print("\nSilver月次調達データ（最新5ヶ月）:")
    print(result1.to_string(index=False))
    
    # 月次受注データ
    query2 = f"""
    SELECT year_month,
           total_quantity,
           total_sales_ex_tax
    FROM silver_monthly_orders
    WHERE product_id = '{vehicle}'
    ORDER BY year_month DESC
    LIMIT 5
    """
    result2 = pd.read_sql_query(query2, conn)
    print("\nSilver月次受注データ（最新5ヶ月）:")
    print(result2.to_string(index=False))
    
    # JOINした結果
    query3 = f"""
    SELECT 
        o.year_month,
        o.total_quantity,
        o.total_sales_ex_tax,
        COALESCE(p.total_cost, 0) as total_cost,
        ROUND((o.total_sales_ex_tax - COALESCE(p.total_cost, 0)) / o.total_sales_ex_tax * 100, 2) as margin_rate
    FROM silver_monthly_orders o
    LEFT JOIN (
        SELECT year_month,
               product_id,
               SUM(total_cost_ex_tax) as total_cost
        FROM silver_monthly_procurement
        WHERE material_type = 'direct'
        GROUP BY year_month, product_id
    ) p ON o.year_month = p.year_month AND o.product_id = p.product_id
    WHERE o.product_id = '{vehicle}'
    ORDER BY o.year_month DESC
    LIMIT 5
    """
    result3 = pd.read_sql_query(query3, conn)
    print("\nJOIN結果（最新5ヶ月）:")
    print(result3.to_string(index=False))

# Bronze層の新規生成POを確認
print("\n" + "=" * 80)
print("Bronze層の新規生成PO確認")
print("=" * 80)

for vehicle in TARGET_VEHICLES:
    query4 = f"""
    SELECT COUNT(*) as po_count,
           SUM(line_subtotal_ex_tax) as total_amount,
           MIN(ship_date) as min_date,
           MAX(ship_date) as max_date
    FROM bronze_p2p_procurement_item
    WHERE purchase_order_id LIKE 'PO-SUPP-{vehicle}%'
    """
    result4 = pd.read_sql_query(query4, conn)
    print(f"\n{vehicle}:")
    print(result4.to_string(index=False))

conn.close()
