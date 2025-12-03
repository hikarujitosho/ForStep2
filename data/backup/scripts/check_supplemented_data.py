"""
補充された調達データがデータベースに正しく反映されているか確認
"""
import sqlite3
import pandas as pd

db_path = 'data/bronze_data.db'
conn = sqlite3.connect(db_path)

print("=" * 80)
print("Bronze層の調達データ確認（product_idベース）")
print("=" * 80)

target_vehicles = ['ACD-CV1', 'RDG-YF6', 'ENP-ENP1', 'NOE-JG4', 'HDE-ZC1', 'PRO-ZC5']

for vehicle in target_vehicles:
    query = f"""
    SELECT COUNT(*) as count, 
           SUM(line_subtotal_ex_tax) as total_amount
    FROM bronze_p2p_procurement_item
    WHERE product_id = '{vehicle}'
    """
    result = pd.read_sql_query(query, conn)
    print(f"\n{vehicle}:")
    print(f"  Bronze調達item件数: {result['count'].iloc[0]:,}件")
    print(f"  Bronze調達金額合計: {result['total_amount'].iloc[0]:,.0f}円")

print("\n" + "=" * 80)
print("Silver層の月次調達データ確認")
print("=" * 80)

for vehicle in target_vehicles:
    query = f"""
    SELECT year_month, 
           COUNT(*) as count,
           SUM(total_cost_ex_tax) as total_amount
    FROM silver_monthly_procurement
    WHERE product_id = '{vehicle}'
    GROUP BY year_month
    ORDER BY year_month
    """
    result = pd.read_sql_query(query, conn)
    print(f"\n{vehicle}:")
    if len(result) == 0:
        print("  [データなし]")
    else:
        print(f"  期間: {result['year_month'].min()} ～ {result['year_month'].max()}")
        print(f"  月次レコード数: {len(result)}ヶ月")
        print(f"  総調達金額: {result['total_amount'].sum():,.0f}円")
        print(f"  月平均金額: {result['total_amount'].mean():,.0f}円")

print("\n" + "=" * 80)
print("月次受注データとの突合確認")
print("=" * 80)

for vehicle in target_vehicles:
    query = f"""
    SELECT o.year_month,
           o.total_quantity as order_qty,
           o.total_sales_ex_tax as sales_amount,
           COALESCE(p.procurement_amount, 0) as procurement_amount,
           COALESCE(p.procurement_amount, 0) / NULLIF(o.total_sales_ex_tax, 0) as cost_rate
    FROM silver_monthly_orders o
    LEFT JOIN (
        SELECT year_month, 
               SUM(total_cost_ex_tax) as procurement_amount
        FROM silver_monthly_procurement
        WHERE product_id = '{vehicle}'
        GROUP BY year_month
    ) p ON o.year_month = p.year_month
    WHERE o.product_id = '{vehicle}'
    ORDER BY o.year_month
    """
    result = pd.read_sql_query(query, conn)
    print(f"\n{vehicle}:")
    if len(result) == 0:
        print("  [受注データなし]")
    else:
        print(f"  期間: {result['year_month'].min()} ～ {result['year_month'].max()}")
        print(f"  総受注台数: {result['order_qty'].sum()}台")
        print(f"  総売上: {result['sales_amount'].sum():,.0f}円")
        print(f"  総調達額: {result['procurement_amount'].sum():,.0f}円")
        has_procurement = result['procurement_amount'] > 0
        print(f"  調達データがある月: {has_procurement.sum()}/{len(result)}ヶ月")

conn.close()
print("\n処理完了")
