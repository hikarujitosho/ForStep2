import sqlite3
import pandas as pd

conn = sqlite3.connect('database/data_lake.db')

# 車種別・年次別の粗利率を算出
query = """
SELECT 
    i.product_name as vehicle_name,
    i.product_id,
    substr(g.year_month, 1, 4) as year,
    SUM(g.revenue) as total_revenue,
    SUM(g.cost) as total_cost,
    SUM(g.gross_profit) as total_gross_profit,
    CASE 
        WHEN SUM(g.revenue) > 0 THEN 
            ROUND((SUM(g.gross_profit) * 100.0 / SUM(g.revenue)), 2)
        ELSE 0
    END as gross_margin_pct
FROM gold_monthly_product_gross_margin g
JOIN silver_item_master i ON g.product_id = i.product_id
GROUP BY i.product_name, i.product_id, substr(g.year_month, 1, 4)
ORDER BY year, gross_margin_pct DESC
"""

df = pd.read_sql_query(query, conn)

print("=" * 120)
print("車種別・年次別 粗利率ランキング")
print("=" * 120)

for year in sorted(df['year'].unique()):
    year_data = df[df['year'] == year].copy()
    
    print(f"\n{'='*120}")
    print(f"【{year}年】車種別粗利率ランキング")
    print(f"{'='*120}")
    
    # ランキング表示
    for idx, row in year_data.iterrows():
        rank = year_data.index.get_loc(idx) + 1
        revenue_oku = row['total_revenue'] / 100000000
        cost_oku = row['total_cost'] / 100000000
        profit_oku = row['total_gross_profit'] / 100000000
        
        # 粗利率の評価
        if row['gross_margin_pct'] >= 40:
            status = "🟢 優良"
        elif row['gross_margin_pct'] >= 20:
            status = "🟡 標準"
        elif row['gross_margin_pct'] >= 0:
            status = "🟠 要改善"
        else:
            status = "🔴 赤字"
        
        print(f"\n{rank:2d}位: {row['vehicle_name']:12s} ({row['product_id']})")
        print(f"      粗利率: {row['gross_margin_pct']:6.2f}% {status}")
        print(f"      売上: {revenue_oku:8.2f}億円")
        print(f"      原価: {cost_oku:8.2f}億円")
        print(f"      粗利: {profit_oku:8.2f}億円")
    
    # 年次統計
    print(f"\n{'-'*120}")
    print(f"【{year}年 統計サマリー】")
    print(f"  総売上:     {year_data['total_revenue'].sum() / 100000000:,.2f}億円")
    print(f"  総原価:     {year_data['total_cost'].sum() / 100000000:,.2f}億円")
    print(f"  総粗利:     {year_data['total_gross_profit'].sum() / 100000000:,.2f}億円")
    print(f"  平均粗利率: {year_data['gross_margin_pct'].mean():.2f}%")
    print(f"  最高粗利率: {year_data['gross_margin_pct'].max():.2f}% ({year_data.loc[year_data['gross_margin_pct'].idxmax(), 'vehicle_name']})")
    print(f"  最低粗利率: {year_data['gross_margin_pct'].min():.2f}% ({year_data.loc[year_data['gross_margin_pct'].idxmin(), 'vehicle_name']})")

# 全期間での車種別集計
print(f"\n\n{'='*120}")
print("【全期間(2022-2025年) 車種別粗利率ランキング】")
print(f"{'='*120}")

all_period_query = """
SELECT 
    i.product_name as vehicle_name,
    i.product_id,
    SUM(g.revenue) as total_revenue,
    SUM(g.cost) as total_cost,
    SUM(g.gross_profit) as total_gross_profit,
    CASE 
        WHEN SUM(g.revenue) > 0 THEN 
            ROUND((SUM(g.gross_profit) * 100.0 / SUM(g.revenue)), 2)
        ELSE 0
    END as gross_margin_pct,
    COUNT(DISTINCT g.year_month) as months_count
FROM gold_monthly_product_gross_margin g
JOIN silver_item_master i ON g.product_id = i.product_id
GROUP BY i.product_name, i.product_id
ORDER BY gross_margin_pct DESC
"""

all_df = pd.read_sql_query(all_period_query, conn)

for idx, row in all_df.iterrows():
    rank = idx + 1
    revenue_oku = row['total_revenue'] / 100000000
    cost_oku = row['total_cost'] / 100000000
    profit_oku = row['total_gross_profit'] / 100000000
    
    # 粗利率の評価
    if row['gross_margin_pct'] >= 40:
        status = "🟢 優良"
    elif row['gross_margin_pct'] >= 20:
        status = "🟡 標準"
    elif row['gross_margin_pct'] >= 0:
        status = "🟠 要改善"
    else:
        status = "🔴 赤字"
    
    print(f"\n{rank:2d}位: {row['vehicle_name']:12s} ({row['product_id']})")
    print(f"      粗利率:   {row['gross_margin_pct']:6.2f}% {status}")
    print(f"      累計売上: {revenue_oku:8.2f}億円")
    print(f"      累計原価: {cost_oku:8.2f}億円")
    print(f"      累計粗利: {profit_oku:8.2f}億円")
    print(f"      販売月数: {row['months_count']:2d}ヶ月")

print(f"\n{'-'*120}")
print(f"【全期間 統計サマリー】")
print(f"  総売上:     {all_df['total_revenue'].sum() / 100000000:,.2f}億円")
print(f"  総原価:     {all_df['total_cost'].sum() / 100000000:,.2f}億円")
print(f"  総粗利:     {all_df['total_gross_profit'].sum() / 100000000:,.2f}億円")
print(f"  平均粗利率: {all_df['gross_margin_pct'].mean():.2f}%")

# CSV出力
output_file = 'data/Gold/車種別粗利率サマリー.csv'
all_df['total_revenue_億円'] = all_df['total_revenue'] / 100000000
all_df['total_cost_億円'] = all_df['total_cost'] / 100000000
all_df['total_gross_profit_億円'] = all_df['total_gross_profit'] / 100000000

export_df = all_df[['vehicle_name', 'product_id', 'gross_margin_pct', 
                     'total_revenue_億円', 'total_cost_億円', 'total_gross_profit_億円', 'months_count']]
export_df.columns = ['車種名', '車種ID', '粗利率(%)', '累計売上(億円)', '累計原価(億円)', '累計粗利(億円)', '販売月数']
export_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"\n✅ CSV出力完了: {output_file}")

conn.close()
