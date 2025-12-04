import sqlite3
import pandas as pd
import os

# データベース接続
db_path = 'database/data_lake.db'
conn = sqlite3.connect(db_path)

# 緊急輸送費率データを取得
query = """
SELECT 
    year_month,
    total_cost,
    emergency_cost,
    emergency_cost_share,
    created_at
FROM gold_emergency_transportation_cost_share
ORDER BY year_month
"""

df = pd.read_sql_query(query, conn)
conn.close()

print("=" * 80)
print("緊急輸送費率KPI")
print("=" * 80)
print(f"\n総レコード数: {len(df)}件\n")

# 基本統計
print("【統計サマリー】")
print(f"平均緊急輸送費率: {df['emergency_cost_share'].mean():.2f}%")
print(f"最大緊急輸送費率: {df['emergency_cost_share'].max():.2f}%")
print(f"最小緊急輸送費率: {df['emergency_cost_share'].min():.2f}%")

# 年月別の上位5件
print("\n【緊急輸送費率が高い月（上位5件）】")
top5 = df.nlargest(5, 'emergency_cost_share')[
    ['year_month', 'total_cost', 'emergency_cost', 'emergency_cost_share']
]
print(top5.to_string(index=False))

# 年月別の下位5件
print("\n【緊急輸送費率が低い月（下位5件）】")
bottom5 = df.nsmallest(5, 'emergency_cost_share')[
    ['year_month', 'total_cost', 'emergency_cost', 'emergency_cost_share']
]
print(bottom5.to_string(index=False))

# 全データ表示
print("\n【全データ】")
print(df[['year_month', 'total_cost', 'emergency_cost', 'emergency_cost_share']].to_string(index=False))

# CSV出力
output_dir = 'data/Gold'
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, '緊急輸送費率.csv')

df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\n✓ CSVファイルを出力しました: {output_file}")
print("=" * 80)
