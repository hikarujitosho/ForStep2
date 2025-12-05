import sqlite3
import pandas as pd

conn = sqlite3.connect('database/data_lake.db')

print("=" * 120)
print("PASSPORT 赤字原因分析")
print("=" * 120)

# 1. PASSPORT月次データの詳細
query_monthly = """
SELECT 
    g.year_month,
    g.revenue,
    g.cost,
    g.gross_profit,
    g.gross_margin,
    g.revenue / 100000000 as revenue_oku,
    g.cost / 100000000 as cost_oku,
    g.gross_profit / 100000000 as profit_oku
FROM gold_monthly_product_gross_margin g
WHERE g.product_id = 'PSP-YF7'
ORDER BY g.year_month
"""

df_monthly = pd.read_sql_query(query_monthly, conn)

print("\n【月次推移】")
print(df_monthly[['year_month', 'revenue_oku', 'cost_oku', 'profit_oku', 'gross_margin']].to_string(index=False))

# 2. 赤字月の特定
print("\n" + "=" * 120)
print("【赤字月の詳細】")
print("=" * 120)

loss_months = df_monthly[df_monthly['gross_margin'] < 0]
if len(loss_months) > 0:
    print(f"\n赤字月数: {len(loss_months)}ヶ月 / 全{len(df_monthly)}ヶ月")
    for _, row in loss_months.iterrows():
        print(f"\n{row['year_month']}: 粗利率 {row['gross_margin']:.2f}%")
        print(f"  売上: {row['revenue_oku']:.2f}億円")
        print(f"  原価: {row['cost_oku']:.2f}億円")
        print(f"  損失: {row['profit_oku']:.2f}億円")

# 3. 受注データの確認
print("\n" + "=" * 120)
print("【受注データ分析】")
print("=" * 120)

order_query = """
SELECT 
    year_month,
    COUNT(*) as order_count,
    SUM(quantity) as total_quantity,
    AVG(unit_price_ex_tax) as avg_unit_price,
    SUM(line_total_ex_tax) as total_revenue,
    SUM(line_total_ex_tax) / 100000000 as revenue_oku
FROM silver_order_data
WHERE product_id = 'PSP-YF7'
GROUP BY year_month
ORDER BY year_month
"""

df_order = pd.read_sql_query(order_query, conn)
print(df_order.to_string(index=False))

# 4. 調達データの確認
print("\n" + "=" * 120)
print("【調達データ分析】")
print("=" * 120)

procurement_query = """
SELECT 
    p.year_month,
    COUNT(DISTINCT p.purchase_order_id) as po_count,
    COUNT(*) as line_count,
    SUM(p.quantity) as total_parts,
    SUM(p.line_total_ex_tax) as total_cost,
    SUM(p.line_total_ex_tax) / 100000000 as cost_oku,
    COUNT(DISTINCT p.supplier_id) as supplier_count
FROM silver_procurement_data p
WHERE p.product_id = 'PSP-YF7'
GROUP BY p.year_month
ORDER BY p.year_month
"""

df_procurement = pd.read_sql_query(procurement_query, conn)
print(df_procurement.to_string(index=False))

# 5. コスト比率の異常月を特定
print("\n" + "=" * 120)
print("【コスト比率分析】")
print("=" * 120)

merged = df_monthly.merge(df_order[['year_month', 'total_quantity', 'order_count']], 
                          on='year_month', how='left')
merged = merged.merge(df_procurement[['year_month', 'line_count', 'supplier_count']], 
                      on='year_month', how='left')

merged['cost_ratio'] = (merged['cost'] / merged['revenue'] * 100).round(2)
merged['1台あたり売上'] = (merged['revenue'] / merged['total_quantity']).round(0)
merged['1台あたり原価'] = (merged['cost'] / merged['total_quantity']).round(0)

print(merged[['year_month', 'total_quantity', 'order_count', 'line_count', 
              '1台あたり売上', '1台あたり原価', 'cost_ratio', 'gross_margin']].to_string(index=False))

# 6. 最悪月の詳細調査 (2023-06)
print("\n" + "=" * 120)
print("【最悪月 2023-06 の詳細分析】")
print("=" * 120)

worst_month_procurement = """
SELECT 
    p.purchase_order_id,
    p.line_number,
    p.order_date,
    p.supplier_name,
    p.material_type,
    p.material_category,
    p.quantity,
    p.unit_price_ex_tax,
    p.line_total_ex_tax,
    p.line_total_ex_tax / 100000000 as cost_oku
FROM silver_procurement_data p
WHERE p.product_id = 'PSP-YF7' AND p.year_month = '2023-06'
ORDER BY p.line_total_ex_tax DESC
LIMIT 20
"""

df_worst = pd.read_sql_query(worst_month_procurement, conn)
print(f"\n2023-06の調達明細トップ20 (高額順):")
print(df_worst[['order_date', 'supplier_name', 'material_category', 
                'quantity', 'unit_price_ex_tax', 'line_total_ex_tax']].to_string(index=False))

print(f"\n2023-06 調達総額: {df_worst['line_total_ex_tax'].sum() / 100000000:.2f}億円")
print(f"平均単価: ¥{df_worst['unit_price_ex_tax'].mean():,.0f}")

# 7. 他車種との比較
print("\n" + "=" * 120)
print("【同価格帯車種との比較】")
print("=" * 120)

comparison_query = """
SELECT 
    i.product_name,
    i.product_id,
    AVG(o.unit_price_ex_tax) as avg_price,
    SUM(g.revenue) / 100000000 as total_revenue_oku,
    SUM(g.cost) / 100000000 as total_cost_oku,
    ROUND(AVG(g.gross_margin), 2) as avg_margin
FROM gold_monthly_product_gross_margin g
JOIN silver_item_master i ON g.product_id = i.product_id
JOIN silver_order_data o ON g.product_id = o.product_id AND g.year_month = o.year_month
WHERE i.item_group IN ('suv', 'premium_suv')
GROUP BY i.product_name, i.product_id
ORDER BY avg_margin DESC
"""

df_comparison = pd.read_sql_query(comparison_query, conn)
print(df_comparison.to_string(index=False))

# 8. 結論と推奨事項
print("\n" + "=" * 120)
print("【分析結果サマリー】")
print("=" * 120)

total_revenue = df_monthly['revenue'].sum() / 100000000
total_cost = df_monthly['cost'].sum() / 100000000
total_loss = df_monthly['gross_profit'].sum() / 100000000
avg_margin = df_monthly['gross_margin'].mean()

print(f"\nPASSPORT 全期間実績:")
print(f"  累計売上: {total_revenue:.2f}億円")
print(f"  累計原価: {total_cost:.2f}億円")
print(f"  累計損失: {total_loss:.2f}億円")
print(f"  平均粗利率: {avg_margin:.2f}%")
print(f"  販売月数: {len(df_monthly)}ヶ月")
print(f"  赤字月数: {len(loss_months)}ヶ月 ({len(loss_months)/len(df_monthly)*100:.1f}%)")

if len(loss_months) > 0:
    worst_month = loss_months.loc[loss_months['gross_margin'].idxmin()]
    print(f"\n最悪月: {worst_month['year_month']}")
    print(f"  売上: {worst_month['revenue_oku']:.2f}億円")
    print(f"  原価: {worst_month['cost_oku']:.2f}億円")
    print(f"  損失: {worst_month['profit_oku']:.2f}億円")
    print(f"  粗利率: {worst_month['gross_margin']:.2f}%")

print("\n" + "=" * 120)
print("【原因推定】")
print("=" * 120)

# 原価率が100%超の月を分析
high_cost_months = df_monthly[df_monthly['cost'] > df_monthly['revenue']]
print(f"\n1. 原価 > 売上 の月: {len(high_cost_months)}ヶ月")

if len(df_procurement) > 0:
    avg_parts_per_month = df_procurement['line_count'].mean()
    print(f"\n2. 月平均部品調達明細数: {avg_parts_per_month:.0f}件")
    
    # 2023-06の異常を確認
    jun_2023_proc = df_procurement[df_procurement['year_month'] == '2023-06']
    if len(jun_2023_proc) > 0:
        print(f"\n3. 2023-06の調達:")
        print(f"   部品明細数: {jun_2023_proc['line_count'].iloc[0]}件")
        print(f"   総コスト: {jun_2023_proc['cost_oku'].iloc[0]:.2f}億円")
        print(f"   → 月平均の約{jun_2023_proc['line_count'].iloc[0] / avg_parts_per_month:.1f}倍")

conn.close()

print("\n" + "=" * 120)
print("【推奨対策】")
print("=" * 120)
print("""
1. 調達コストの見直し
   - 高額部品の代替サプライヤー検討
   - 部品統合・共通化の推進
   
2. 販売価格戦略の再検討
   - 競合他車種との価格比較
   - 付加価値向上による価格引き上げ
   
3. 事業継続性の評価
   - 累計赤字車種として事業性を再評価
   - 製造中止または大幅なコスト削減の検討
""")
