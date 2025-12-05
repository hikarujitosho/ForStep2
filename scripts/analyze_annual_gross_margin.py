import sqlite3
import pandas as pd

# データベース接続
conn = sqlite3.connect('database/data_lake.db')

# 月次粗利データを取得
query = """
SELECT 
    product_id,
    product_name,
    substr(year_month, 1, 4) as year,
    SUM(revenue) as total_revenue,
    SUM(cost) as total_cost,
    SUM(gross_profit) as total_gross_profit,
    CASE 
        WHEN SUM(revenue) > 0 THEN (SUM(gross_profit) * 100.0 / SUM(revenue))
        ELSE 0
    END as gross_margin_pct
FROM gold_monthly_product_gross_margin
GROUP BY product_id, product_name, substr(year_month, 1, 4)
ORDER BY year, product_id
"""

df = pd.read_sql_query(query, conn)

print("=" * 100)
print("年次別・商品別 粗利率分析")
print("=" * 100)

for year in sorted(df['year'].unique()):
    year_data = df[df['year'] == year]
    print(f"\n【{year}年】")
    print(f"商品数: {len(year_data)}")
    print(f"平均粗利率: {year_data['gross_margin_pct'].mean():.2f}%")
    print(f"最高粗利率: {year_data['gross_margin_pct'].max():.2f}% ({year_data.loc[year_data['gross_margin_pct'].idxmax(), 'product_name']})")
    print(f"最低粗利率: {year_data['gross_margin_pct'].min():.2f}% ({year_data.loc[year_data['gross_margin_pct'].idxmin(), 'product_name']})")
    print(f"\n総売上: {year_data['total_revenue'].sum() / 100000000:.2f}億円")
    print(f"総コスト: {year_data['total_cost'].sum() / 100000000:.2f}億円")
    print(f"総粗利: {year_data['total_gross_profit'].sum() / 100000000:.2f}億円")
    
    # 全社平均の粗利率を計算
    company_margin = (year_data['total_gross_profit'].sum() / year_data['total_revenue'].sum() * 100) if year_data['total_revenue'].sum() > 0 else 0
    print(f"全社粗利率: {company_margin:.2f}%")
    
    # 異常値をチェック
    abnormal_high = year_data[year_data['gross_margin_pct'] > 50]
    abnormal_low = year_data[year_data['gross_margin_pct'] < 10]
    
    if len(abnormal_high) > 0:
        print(f"\n⚠️ 高すぎる粗利率 (>50%):")
        for _, row in abnormal_high.iterrows():
            print(f"  - {row['product_name']}: {row['gross_margin_pct']:.2f}% (売上{row['total_revenue']/100000000:.2f}億円, コスト{row['total_cost']/100000000:.2f}億円)")
    
    if len(abnormal_low) > 0:
        print(f"\n⚠️ 低すぎる粗利率 (<10%):")
        for _, row in abnormal_low.iterrows():
            print(f"  - {row['product_name']}: {row['gross_margin_pct']:.2f}% (売上{row['total_revenue']/100000000:.2f}億円, コスト{row['total_cost']/100000000:.2f}億円)")

# 月次データの異常値をチェック
print("\n" + "=" * 100)
print("月次データの異常値チェック")
print("=" * 100)

monthly_query = """
SELECT 
    product_name,
    year_month,
    revenue,
    cost,
    gross_profit,
    gross_margin
FROM gold_monthly_product_gross_margin
WHERE gross_margin > 50 OR gross_margin < 10
ORDER BY year_month, product_name
"""

monthly_df = pd.read_sql_query(monthly_query, conn)

if len(monthly_df) > 0:
    print(f"\n異常な月次データ: {len(monthly_df)}件")
    print(monthly_df.to_string(index=False))
else:
    print("\n異常な月次データはありません")

conn.close()

print("\n" + "=" * 100)
print("分析完了")
print("=" * 100)
