"""
Silver層データから車種別（商品別）月次粗利率を計算するスクリプト
月次にサマライズされたデータを使用して正確な粗利率を算出
"""
import sqlite3
import pandas as pd
from pathlib import Path

# データベース接続
DB_PATH = Path(r'C:\Users\PC\dev\ForStep2\data\bronze_data.db')
conn = sqlite3.connect(DB_PATH)

print("=" * 100)
print("車種別（商品別）月次粗利率の計算")
print("=" * 100)

# Silver層の月次データを結合して粗利率を計算
query = """
SELECT 
    smo.year_month,
    smo.product_id,
    smo.product_name,
    smo.item_hierarchy,
    smo.detail_category,
    smo.import_export_group,
    smo.order_count,
    smo.total_quantity,
    smo.total_sales_ex_tax,
    COALESCE(smp.total_cost_ex_tax, 0) as total_cost_ex_tax,
    (smo.total_sales_ex_tax - COALESCE(smp.total_cost_ex_tax, 0)) as gross_profit,
    CASE 
        WHEN smo.total_sales_ex_tax > 0 
        THEN ROUND((smo.total_sales_ex_tax - COALESCE(smp.total_cost_ex_tax, 0)) / smo.total_sales_ex_tax * 100, 2)
        ELSE 0 
    END as gross_margin_rate
FROM silver_monthly_orders smo
LEFT JOIN (
    SELECT 
        year_month,
        product_id,
        SUM(CASE WHEN material_type = 'direct' THEN total_cost_ex_tax ELSE 0 END) as total_cost_ex_tax
    FROM silver_monthly_procurement
    GROUP BY year_month, product_id
) smp ON smo.year_month = smp.year_month AND smo.product_id = smp.product_id
ORDER BY smo.year_month, smo.product_id
"""

print("\n月次Silver層データから車種別粗利率を計算中...")
df_gross_margin = pd.read_sql_query(query, conn)

print(f"✓ 計算完了: {len(df_gross_margin):,}件のレコード")

# 統計情報
print("\n" + "=" * 100)
print("統計サマリー")
print("=" * 100)
print(f"対象期間: {df_gross_margin['year_month'].min()} ～ {df_gross_margin['year_month'].max()}")
print(f"車種数: {df_gross_margin['product_id'].nunique()}種類")
print(f"総売上高: ¥{df_gross_margin['total_sales_ex_tax'].sum():,.0f}")
print(f"総売上原価: ¥{df_gross_margin['total_cost_ex_tax'].sum():,.0f}")
print(f"総粗利: ¥{df_gross_margin['gross_profit'].sum():,.0f}")

if df_gross_margin['total_sales_ex_tax'].sum() > 0:
    overall_margin = (df_gross_margin['gross_profit'].sum() / df_gross_margin['total_sales_ex_tax'].sum() * 100)
    print(f"全体粗利率: {overall_margin:.2f}%")

# 最新月のランキング
latest_month = df_gross_margin['year_month'].max()
df_latest = df_gross_margin[df_gross_margin['year_month'] == latest_month].copy()
df_latest = df_latest.sort_values('gross_margin_rate', ascending=False)

print(f"\n【{latest_month} 車種別粗利率ランキング】")
print("=" * 100)
print(df_latest[['product_id', 'product_name', 'total_quantity', 'total_sales_ex_tax', 
                  'total_cost_ex_tax', 'gross_profit', 'gross_margin_rate']].to_string(index=False))

# 車種別の全期間平均粗利率
print("\n" + "=" * 100)
print("車種別の全期間平均粗利率")
print("=" * 100)

df_product_avg = df_gross_margin.groupby(['product_id', 'product_name', 'item_hierarchy']).agg({
    'total_quantity': 'sum',
    'total_sales_ex_tax': 'sum',
    'total_cost_ex_tax': 'sum',
    'gross_profit': 'sum'
}).reset_index()

df_product_avg['avg_gross_margin_rate'] = (
    df_product_avg['gross_profit'] / df_product_avg['total_sales_ex_tax'] * 100
).round(2)

df_product_avg = df_product_avg.sort_values('avg_gross_margin_rate', ascending=False)

print(df_product_avg[['product_id', 'product_name', 'item_hierarchy', 'total_quantity',
                       'total_sales_ex_tax', 'total_cost_ex_tax', 'gross_profit', 
                       'avg_gross_margin_rate']].to_string(index=False))

# EV車とICE車の比較
print("\n" + "=" * 100)
print("EV車とICE車の粗利率比較")
print("=" * 100)

df_vehicle_type = df_gross_margin.groupby('item_hierarchy').agg({
    'total_quantity': 'sum',
    'total_sales_ex_tax': 'sum',
    'total_cost_ex_tax': 'sum',
    'gross_profit': 'sum'
}).reset_index()

df_vehicle_type['gross_margin_rate'] = (
    df_vehicle_type['gross_profit'] / df_vehicle_type['total_sales_ex_tax'] * 100
).round(2)

print(df_vehicle_type.to_string(index=False))

# 月次推移（最近12ヶ月）
print("\n" + "=" * 100)
print("月次粗利率推移（最近12ヶ月）")
print("=" * 100)

recent_months = sorted(df_gross_margin['year_month'].unique())[-12:]
df_recent = df_gross_margin[df_gross_margin['year_month'].isin(recent_months)]

df_monthly_trend = df_recent.groupby('year_month').agg({
    'total_sales_ex_tax': 'sum',
    'total_cost_ex_tax': 'sum',
    'gross_profit': 'sum'
}).reset_index()

df_monthly_trend['gross_margin_rate'] = (
    df_monthly_trend['gross_profit'] / df_monthly_trend['total_sales_ex_tax'] * 100
).round(2)

print(df_monthly_trend.to_string(index=False))

# CSVに出力
output_path = Path(r'C:\Users\PC\dev\ForStep2\data\Gold\月次商品別粗利率.csv')
df_gross_margin.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n✓ 結果をCSVファイルに出力: {output_path}")

# サマリーもCSVに出力
summary_path = Path(r'C:\Users\PC\dev\ForStep2\data\Gold\車種別粗利率サマリー.csv')
df_product_avg.to_csv(summary_path, index=False, encoding='utf-8-sig')
print(f"✓ 車種別サマリーを出力: {summary_path}")

conn.close()
print("\n処理完了")
print("=" * 100)
