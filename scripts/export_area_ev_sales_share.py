import sqlite3
import pandas as pd
import os

# データベース接続
db_path = 'database/data_lake.db'
conn = sqlite3.connect(db_path)

# テーブル構造を確認
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(gold_monthly_area_ev_sales_share)")
columns = cursor.fetchall()

print("=" * 80)
print("gold_monthly_area_ev_sales_share テーブル構造")
print("=" * 80)
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# 月次エリア別EV販売率データを取得
query = """
SELECT *
FROM gold_monthly_area_ev_sales_share
ORDER BY year_month, location_id
"""

df = pd.read_sql_query(query, conn)
conn.close()

print("\n" + "=" * 80)
print("月次エリア別EV販売率KPI")
print("=" * 80)
print(f"\n総レコード数: {len(df)}件\n")

# カラム名を確認
print("【カラム情報】")
for col in df.columns:
    print(f"  - {col}")

# サンプルデータ表示
print("\n【サンプルデータ（最初の10件）】")
print(df.head(10).to_string(index=False))

# EV販売率のカラムを特定
ev_share_col = [col for col in df.columns if 'share' in col.lower()][0]
print(f"\nEV販売率カラム: {ev_share_col}")

# 基本統計
print("\n【統計サマリー】")
print(f"平均EV販売率: {df[ev_share_col].mean():.2f}%")
print(f"最大EV販売率: {df[ev_share_col].max():.2f}%")
print(f"最小EV販売率: {df[ev_share_col].min():.2f}%")

# エリア別の平均EV販売率
print("\n【エリア別平均EV販売率】")
area_avg = df.groupby('location_name')[ev_share_col].mean().sort_values(ascending=False)
for area, rate in area_avg.items():
    print(f"  {area}: {rate:.2f}%")

# 最近の月のエリア別EV販売率
print("\n【最新月(2025-12)のエリア別EV販売率】")
latest_month = df[df['year_month'] == '2025-12'].sort_values('location_name')
if len(latest_month) > 0:
    for _, row in latest_month.iterrows():
        print(f"  {row['location_name']}: {row[ev_share_col]:.2f}% (売上: {row['total_revenue']:,.0f}円, EV売上: {row['ev_revenue']:,.0f}円)")
else:
    print("  データなし")

# 2025年のエリア別平均EV販売率
print("\n【2025年のエリア別平均EV販売率】")
df_2025 = df[df['year_month'].str.startswith('2025')]
area_2025_avg = df_2025.groupby('location_name')[ev_share_col].mean().sort_values(ascending=False)
for area, rate in area_2025_avg.items():
    print(f"  {area}: {rate:.2f}%")

# エリア×月で最もEV販売率が高いケース（上位10件）
print("\n【EV販売率が高いエリア×月（上位10件）】")
display_cols = [col for col in df.columns if col != 'created_at']
top10 = df.nlargest(10, ev_share_col)[display_cols]
print(top10.to_string(index=False))

# エリア×月で最もEV販売率が低いケース（下位10件）
print("\n【EV販売率が低いエリア×月（下位10件）】")
bottom10 = df.nsmallest(10, ev_share_col)[display_cols]
print(bottom10.to_string(index=False))

# 月別・エリア別の詳細データ（2025年のみ）
print("\n【2025年 月別・エリア別EV販売率】")
pivot_2025 = df_2025.pivot_table(
    values=ev_share_col, 
    index='location_name', 
    columns='year_month', 
    aggfunc='first'
)
print(pivot_2025.round(2).to_string())

# CSV出力
output_dir = 'data/Gold'
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, '月次エリア別EV販売率.csv')

df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\n✓ CSVファイルを出力しました: {output_file}")
print("=" * 80)
