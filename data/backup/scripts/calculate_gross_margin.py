import sqlite3
import pandas as pd
from datetime import datetime

# データベース接続
db_path = r'C:\Users\PC\dev\ForStep2\data\bronze_data.db'
conn = sqlite3.connect(db_path)

print("=" * 100)
print("商品別粗利率算出")
print("=" * 100)

# データ期間の確認
print("\n【データ期間の確認】")
query_check = """
SELECT 
    'Order' as data_type,
    MIN(DATE(order_timestamp)) as min_date,
    MAX(DATE(order_timestamp)) as max_date,
    COUNT(*) as record_count
FROM erp_order_header
UNION ALL
SELECT 
    'Procurement' as data_type,
    MIN(order_date) as min_date,
    MAX(order_date) as max_date,
    COUNT(*) as record_count
FROM p2p_procurement_header ph
JOIN p2p_procurement_item pi ON ph.purchase_order_id = pi.purchase_order_id
WHERE pi.material_type = 'direct'
"""
df_check = pd.read_sql_query(query_check, conn)
print(df_check)

print("\n警告: 調達データは2022年のみです。2023年以降の売上原価は商品別平均原価率を使用して推定します。")

# 1. 売上を計算
query_sales = """
SELECT 
    oi.product_id,
    pm.product_name,
    pm.item_hierarchy,
    pm.detail_category,
    DATE(oh.order_timestamp) as order_date,
    strftime('%Y-%m', oh.order_timestamp) as year_month,
    oi.quantity,
    pc.selling_price_ex_tax,
    oi.quantity * pc.selling_price_ex_tax as sales_amount
FROM erp_order_item oi
INNER JOIN erp_order_header oh ON oi.order_id = oh.order_id
INNER JOIN erp_product_master pm ON oi.product_id = pm.product_id
INNER JOIN erp_price_condition_master pc ON 
    oi.product_id = pc.product_id 
    AND oh.customer_id = pc.customer_id
    AND DATE(oh.order_timestamp) >= pc.valid_from 
    AND DATE(oh.order_timestamp) < pc.valid_to
"""

df_sales = pd.read_sql_query(query_sales, conn)
print(f"\n受注データ取得: {len(df_sales)}件")

# 2. 売上原価を計算（実績データ）
query_cogs = """
SELECT 
    pi.product_id,
    strftime('%Y-%m', ph.order_date) as year_month,
    SUM(pi.quantity * pi.unit_price_ex_tax) as total_material_cost,
    COUNT(*) as procurement_count
FROM p2p_procurement_item pi
INNER JOIN p2p_procurement_header ph ON pi.purchase_order_id = ph.purchase_order_id
WHERE pi.material_type = 'direct'
GROUP BY pi.product_id, strftime('%Y-%m', ph.order_date)
"""

df_cogs = pd.read_sql_query(query_cogs, conn)
print(f"原価データ取得: {len(df_cogs)}件")

# 3. 商品別の平均原価率を計算（2022年実績から）
query_avg_cogs_rate = """
SELECT 
    pi.product_id,
    SUM(pi.quantity * pi.unit_price_ex_tax) as total_cogs_2022
FROM p2p_procurement_item pi
INNER JOIN p2p_procurement_header ph ON pi.purchase_order_id = ph.purchase_order_id
WHERE pi.material_type = 'direct'
GROUP BY pi.product_id
"""
df_avg_cogs = pd.read_sql_query(query_avg_cogs_rate, conn)

# 2022年の売上を取得
query_sales_2022 = """
SELECT 
    oi.product_id,
    SUM(oi.quantity * pc.selling_price_ex_tax) as total_sales_2022
FROM erp_order_item oi
INNER JOIN erp_order_header oh ON oi.order_id = oh.order_id
INNER JOIN erp_price_condition_master pc ON 
    oi.product_id = pc.product_id 
    AND oh.customer_id = pc.customer_id
    AND DATE(oh.order_timestamp) >= pc.valid_from 
    AND DATE(oh.order_timestamp) < pc.valid_to
WHERE strftime('%Y', oh.order_timestamp) = '2022'
GROUP BY oi.product_id
"""
df_sales_2022 = pd.read_sql_query(query_sales_2022, conn)

# 原価率を計算
df_cogs_rate = pd.merge(df_avg_cogs, df_sales_2022, on='product_id', how='left')
df_cogs_rate['cogs_rate'] = df_cogs_rate['total_cogs_2022'] / df_cogs_rate['total_sales_2022']
df_cogs_rate['cogs_rate'] = df_cogs_rate['cogs_rate'].fillna(0)

print(f"\n【商品別平均原価率（2022年実績）】")
print(df_cogs_rate[['product_id', 'cogs_rate']])

# 4. 月次商品別の売上を集計
df_sales_monthly = df_sales.groupby(['year_month', 'product_id', 'product_name', 'item_hierarchy', 'detail_category']).agg({
    'quantity': 'sum',
    'sales_amount': 'sum'
}).reset_index()

df_sales_monthly.columns = ['year_month', 'product_id', 'product_name', 'item_hierarchy', 'detail_category', 'total_quantity', 'total_sales']

# 5. 実績原価データを結合（2022年）
df_margin = pd.merge(
    df_sales_monthly,
    df_cogs,
    on=['year_month', 'product_id'],
    how='left'
)

# 6. 平均原価率を結合
df_margin = pd.merge(
    df_margin,
    df_cogs_rate[['product_id', 'cogs_rate']],
    on='product_id',
    how='left'
)

# 7. 売上原価を計算（実績がある場合は実績、ない場合は推定）
df_margin['has_actual_cogs'] = df_margin['total_material_cost'].notna()
df_margin['total_material_cost'] = df_margin.apply(
    lambda row: row['total_material_cost'] if pd.notna(row['total_material_cost']) 
    else row['total_sales'] * row['cogs_rate'], 
    axis=1
)
df_margin['total_material_cost'] = df_margin['total_material_cost'].fillna(0)

# 8. 粗利と粗利率を計算
df_margin['gross_profit'] = df_margin['total_sales'] - df_margin['total_material_cost']
df_margin['gross_margin_rate'] = (df_margin['gross_profit'] / df_margin['total_sales'] * 100).round(2)
df_margin['gross_margin_rate'] = df_margin['gross_margin_rate'].fillna(0)

# データソースを明記
df_margin['cogs_source'] = df_margin['has_actual_cogs'].apply(lambda x: '実績' if x else '推定')

# 9. 結果を並び替え
df_margin = df_margin.sort_values(['year_month', 'product_id'])

# 10. 結果を表示（サンプル）
print("\n" + "=" * 100)
print("月次商品別粗利率（サンプル: 最新5ヶ月）")
print("=" * 100)
recent_months = df_margin['year_month'].unique()[-5:]
df_recent = df_margin[df_margin['year_month'].isin(recent_months)]
print(df_recent[['year_month', 'product_id', 'product_name', 'total_sales', 'total_material_cost', 'gross_profit', 'gross_margin_rate', 'cogs_source']].to_string(index=False))

# 11. CSVファイルに出力
output_path = r'C:\Users\PC\dev\ForStep2\data\Gold\月次商品別粗利率.csv'
df_output = df_margin[['year_month', 'product_id', 'product_name', 'item_hierarchy', 'detail_category', 
                        'total_quantity', 'total_sales', 'total_material_cost', 'gross_profit', 
                        'gross_margin_rate', 'cogs_source']]
df_output.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n結果をCSVファイルに出力しました: {output_path}")

# 12. 統計サマリー
print("\n" + "=" * 100)
print("統計サマリー")
print("=" * 100)
print(f"対象期間: {df_margin['year_month'].min()} ~ {df_margin['year_month'].max()}")
print(f"商品数: {df_margin['product_id'].nunique()}種類")
print(f"総売上高: ¥{df_margin['total_sales'].sum():,.0f}")
print(f"総売上原価: ¥{df_margin['total_material_cost'].sum():,.0f}")
print(f"  - 実績データ: ¥{df_margin[df_margin['cogs_source']=='実績']['total_material_cost'].sum():,.0f}")
print(f"  - 推定データ: ¥{df_margin[df_margin['cogs_source']=='推定']['total_material_cost'].sum():,.0f}")
print(f"総粗利: ¥{df_margin['gross_profit'].sum():,.0f}")
print(f"全体粗利率: {(df_margin['gross_profit'].sum() / df_margin['total_sales'].sum() * 100):.2f}%")

# 13. データソース別の統計
print("\n【データソース別統計】")
df_source_stat = df_margin.groupby('cogs_source').agg({
    'year_month': 'count',
    'total_sales': 'sum',
    'total_material_cost': 'sum'
}).reset_index()
df_source_stat.columns = ['データソース', 'レコード数', '売上高', '売上原価']
print(df_source_stat)

# 14. 商品別粗利率ランキング（最新月）
latest_month = df_margin['year_month'].max()
df_latest = df_margin[df_margin['year_month'] == latest_month].copy()
df_latest = df_latest.sort_values('gross_margin_rate', ascending=False)

print(f"\n【{latest_month} の商品別粗利率ランキング】")
print("=" * 100)
print(df_latest[['product_id', 'product_name', 'total_sales', 'total_material_cost', 'gross_profit', 'gross_margin_rate', 'cogs_source']].to_string(index=False))

# 15. 原価データが存在しない商品の警告
products_without_cogs = df_cogs_rate[df_cogs_rate['cogs_rate'] == 0]['product_id'].tolist()
if products_without_cogs:
    print(f"\n【警告】以下の商品は調達データが存在しないため、売上原価を0として計算しています:")
    print(f"  {', '.join(products_without_cogs)}")

conn.close()
print("\n処理完了")
