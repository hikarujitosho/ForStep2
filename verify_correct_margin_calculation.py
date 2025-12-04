import sqlite3
import pandas as pd

conn = sqlite3.connect('database/data_lake.db')

# 正しい集計方法での確認
query = """
WITH order_summary AS (
    SELECT 
        year_month,
        product_id,
        SUM(line_total_ex_tax) as total_revenue
    FROM silver_order_data
    WHERE year_month IN ('2022-01', '2023-01', '2024-01', '2025-01')
    GROUP BY year_month, product_id
),
procurement_summary AS (
    SELECT 
        year_month,
        product_id,
        SUM(line_total_ex_tax) as total_cost
    FROM silver_procurement_data
    WHERE year_month IN ('2022-01', '2023-01', '2024-01', '2025-01')
    GROUP BY year_month, product_id
)
SELECT 
    o.year_month,
    o.product_id,
    i.product_name,
    o.total_revenue,
    COALESCE(p.total_cost, 0) as total_cost,
    COALESCE(p.total_cost, 0) * 100.0 / o.total_revenue as cost_ratio,
    (o.total_revenue - COALESCE(p.total_cost, 0)) * 100.0 / o.total_revenue as gross_margin
FROM order_summary o
JOIN silver_item_master i ON o.product_id = i.product_id
LEFT JOIN procurement_summary p ON o.product_id = p.product_id 
    AND o.year_month = p.year_month
ORDER BY o.year_month, i.product_name
"""

df = pd.read_sql_query(query, conn)

print("=" * 120)
print("正しく集計した場合の粗利率分析")
print("=" * 120)
print(df.to_string(index=False))

print("\n" + "=" * 120)
print("統計サマリー")
print("=" * 120)
print(f"平均コスト比率: {df['cost_ratio'].mean():.2f}%")
print(f"平均粗利率: {df['gross_margin'].mean():.2f}%")
print(f"最大粗利率: {df['gross_margin'].max():.2f}% ({df.loc[df['gross_margin'].idxmax(), 'product_name']})")
print(f"最小粗利率: {df['gross_margin'].min():.2f}% ({df.loc[df['gross_margin'].idxmin(), 'product_name']})")

# コストがゼロの商品を確認
no_cost = df[df['total_cost'] == 0]
print(f"\n調達データがない商品: {len(no_cost)}件")
if len(no_cost) > 0:
    print(no_cost[['year_month', 'product_name', 'total_revenue']].to_string(index=False))

conn.close()
