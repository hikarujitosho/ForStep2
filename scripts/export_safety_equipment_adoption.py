import sqlite3
import pandas as pd
import os

# データベース接続
db_path = 'database/data_lake.db'
conn = sqlite3.connect(db_path)

# テーブル構造を確認
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(gold_monthly_safety_equipment_adoption)")
columns = cursor.fetchall()

print("=" * 80)
print("gold_monthly_safety_equipment_adoption テーブル構造")
print("=" * 80)
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# 月次先進安全装置適用率データを取得
query = """
SELECT *
FROM gold_monthly_safety_equipment_adoption
ORDER BY year_month
"""

df = pd.read_sql_query(query, conn)
conn.close()

print("\n" + "=" * 80)
print("月次先進安全装置適用率KPI")
print("=" * 80)
print(f"\n総レコード数: {len(df)}件\n")

# カラム名を確認
print("【カラム情報】")
for col in df.columns:
    print(f"  - {col}")

# サンプルデータ表示
print("\n【サンプルデータ（最初の10件）】")
print(df.head(10).to_string(index=False))

# 先進安全装置適用率のカラムを特定
safety_col = [col for col in df.columns if 'adoption' in col.lower() or 'rate' in col.lower()][0]
print(f"\n先進安全装置適用率カラム: {safety_col}")

# 基本統計
print("\n【統計サマリー】")
print(f"平均適用率: {df[safety_col].mean():.2f}%")
print(f"最大適用率: {df[safety_col].max():.2f}%")
print(f"最小適用率: {df[safety_col].min():.2f}%")

# 年別の平均適用率
print("\n【年別平均適用率】")
df['year'] = df['year_month'].str[:4]
yearly_avg = df.groupby('year')[safety_col].mean()
for year, rate in yearly_avg.items():
    print(f"  {year}年: {rate:.2f}%")

# 月次推移（全データ）
print("\n【月次先進安全装置適用率推移（全期間）】")
for _, row in df.iterrows():
    print(f"  {row['year_month']}: {row[safety_col]:6.2f}% (総売上: {row['total_revenue']:>14,.0f}円, 搭載車売上: {row['safety_equipped_revenue']:>14,.0f}円)")

# 最近12ヶ月の推移
print("\n【最近12ヶ月の適用率推移】")
recent_12 = df.tail(12)
for _, row in recent_12.iterrows():
    print(f"  {row['year_month']}: {row[safety_col]:6.2f}% (総売上: {row['total_revenue']:>14,.0f}円, 搭載車売上: {row['safety_equipped_revenue']:>14,.0f}円)")

# 適用率が高い月（上位10件）
print("\n【適用率が高い月（上位10件）】")
display_cols = [col for col in df.columns if col != 'created_at']
top10 = df.nlargest(10, safety_col)[display_cols]
print(top10.to_string(index=False))

# 適用率が低い月（下位10件）
print("\n【適用率が低い月（下位10件）】")
bottom10 = df.nsmallest(10, safety_col)[display_cols]
print(bottom10.to_string(index=False))

# 2025年の詳細分析
print("\n【2025年 月別詳細】")
df_2025 = df[df['year_month'].str.startswith('2025')]
for _, row in df_2025.iterrows():
    safety_revenue_billion = row['safety_equipped_revenue'] / 1_000_000_000
    total_revenue_billion = row['total_revenue'] / 1_000_000_000
    print(f"  {row['year_month']}: {row[safety_col]:6.2f}% (総売上: {total_revenue_billion:6.2f}億円, 搭載車売上: {safety_revenue_billion:6.2f}億円)")

print(f"\n2025年平均適用率: {df_2025[safety_col].mean():.2f}%")

# CSV出力
output_dir = 'data/Gold'
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, '月次先進安全装置適用率.csv')

df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\n✓ CSVファイルを出力しました: {output_file}")
print("=" * 80)
