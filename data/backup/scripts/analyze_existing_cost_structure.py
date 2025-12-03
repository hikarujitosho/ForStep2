"""
既存車種の原価構造を分析して、BOM単価推定の基準を作成
"""
import sqlite3
import pandas as pd
import numpy as np

db_path = 'data/bronze_data.db'
conn = sqlite3.connect(db_path)

print("=" * 80)
print("既存車種の原価構造分析")
print("=" * 80)

# 既存の調達データがある車種の粗利率を取得
query = """
SELECT 
    o.product_id,
    o.product_name,
    o.item_hierarchy,
    SUM(o.total_quantity) as total_quantity,
    SUM(o.total_sales_ex_tax) as total_sales,
    SUM(p.total_cost_ex_tax) as total_cost,
    ROUND(SUM(p.total_cost_ex_tax) * 100.0 / NULLIF(SUM(o.total_sales_ex_tax), 0), 2) as cost_rate,
    ROUND((1 - SUM(p.total_cost_ex_tax) / NULLIF(SUM(o.total_sales_ex_tax), 0)) * 100, 2) as margin_rate
FROM silver_monthly_orders o
INNER JOIN silver_monthly_procurement p 
    ON o.year_month = p.year_month 
    AND o.product_id = p.product_id
WHERE p.material_type = 'direct'
GROUP BY o.product_id, o.product_name, o.item_hierarchy
HAVING SUM(p.total_cost_ex_tax) > 0
    AND SUM(p.total_cost_ex_tax) < SUM(o.total_sales_ex_tax)
ORDER BY margin_rate DESC
"""

existing_vehicles = pd.read_sql_query(query, conn)
print("\n【既存車種の原価率（適正データのみ）】")
print(existing_vehicles.to_string(index=False))

# ICEとEV別の平均原価率
print("\n" + "=" * 80)
print("【カテゴリ別平均原価率】")
print("=" * 80)
summary = existing_vehicles.groupby('item_hierarchy').agg({
    'cost_rate': ['mean', 'median', 'min', 'max'],
    'margin_rate': ['mean', 'median', 'min', 'max'],
    'product_id': 'count'
}).round(2)
print(summary)

# BOMマスタの部品カテゴリ別分析
print("\n" + "=" * 80)
print("【BOMマスタの構成部品分析】")
print("=" * 80)

bom_query = """
SELECT 
    component_category,
    COUNT(*) as part_count,
    AVG(component_quantity_per) as avg_quantity,
    GROUP_CONCAT(DISTINCT product_id) as used_in_vehicles
FROM bronze_p2p_bom_master
GROUP BY component_category
ORDER BY part_count DESC
"""
bom_analysis = pd.read_sql_query(bom_query, conn)
print(bom_analysis.to_string(index=False))

# 既存データから部品カテゴリ別の平均単価を取得
print("\n" + "=" * 80)
print("【既存調達データから部品カテゴリ別平均単価】")
print("=" * 80)

price_query = """
SELECT 
    material_category,
    COUNT(*) as record_count,
    ROUND(AVG(unit_price_ex_tax), 0) as avg_unit_price,
    ROUND(MIN(unit_price_ex_tax), 0) as min_unit_price,
    ROUND(MAX(unit_price_ex_tax), 0) as max_unit_price,
    ROUND(AVG(unit_price_ex_tax) * 0.8, 0) as recommended_80pct,
    ROUND(AVG(unit_price_ex_tax) * 0.7, 0) as recommended_70pct,
    ROUND(AVG(unit_price_ex_tax) * 0.6, 0) as recommended_60pct
FROM bronze_p2p_procurement_item
WHERE material_type = 'direct'
    AND unit_price_ex_tax > 0
GROUP BY material_category
ORDER BY avg_unit_price DESC
"""
price_analysis = pd.read_sql_query(price_query, conn)
print(price_analysis.to_string(index=False))

# 目標原価率の推奨値を計算
print("\n" + "=" * 80)
print("【BOM単価推定の推奨方針】")
print("=" * 80)

ice_avg_cost_rate = existing_vehicles[existing_vehicles['item_hierarchy'] == 'ICE']['cost_rate'].mean()
ev_avg_cost_rate = 80.0  # EVは既存データが少ないため、やや高めの原価率を想定

print(f"\n既存ICE車種の平均原価率: {ice_avg_cost_rate:.2f}%")
print(f"EV車種の想定原価率: {ev_avg_cost_rate:.2f}%")
print("\n推奨BOM単価設定:")
print("  - 既存の部品カテゴリ別平均単価の60-70%を使用")
print("  - 車両1台あたりの総原価が売上の原価率目標に収まるよう調整")
print("  - ICE: 55-70%原価率（粗利率30-45%）")
print("  - EV: 75-85%原価率（粗利率15-25%）")

# 補充対象車種の目標原価を計算
target_vehicles = ['ACD-CV1', 'RDG-YF6', 'ENP-ENP1', 'NOE-JG4', 'HDE-ZC1', 'PRO-ZC5']

print("\n" + "=" * 80)
print("【補充対象車種の目標原価設定】")
print("=" * 80)

vehicle_info_query = """
SELECT 
    o.product_id,
    o.product_name,
    o.item_hierarchy,
    SUM(o.total_quantity) as total_quantity,
    SUM(o.total_sales_ex_tax) as total_sales,
    ROUND(SUM(o.total_sales_ex_tax) / SUM(o.total_quantity), 0) as avg_selling_price,
    COUNT(DISTINCT b.component_id) as bom_part_count
FROM silver_monthly_orders o
LEFT JOIN bronze_p2p_bom_master b ON o.product_id = b.product_id
WHERE o.product_id IN ('{}')
GROUP BY o.product_id, o.product_name, o.item_hierarchy
""".format("','".join(target_vehicles))

target_info = pd.read_sql_query(vehicle_info_query, conn)

for _, row in target_info.iterrows():
    hierarchy = row['item_hierarchy']
    target_cost_rate = ice_avg_cost_rate if hierarchy == 'ICE' else ev_avg_cost_rate
    target_cost_per_vehicle = row['avg_selling_price'] * target_cost_rate / 100
    target_cost_per_part = target_cost_per_vehicle / row['bom_part_count'] if row['bom_part_count'] > 0 else 0
    
    print(f"\n{row['product_id']} ({row['product_name']}) - {hierarchy}")
    print(f"  平均販売価格: ¥{row['avg_selling_price']:,.0f}")
    print(f"  BOM部品数: {row['bom_part_count']}個")
    print(f"  目標原価率: {target_cost_rate:.1f}%")
    print(f"  目標原価/台: ¥{target_cost_per_vehicle:,.0f}")
    print(f"  目標単価/部品: ¥{target_cost_per_part:,.0f} (単純平均)")

conn.close()
print("\n処理完了")
