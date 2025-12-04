import sqlite3
import pandas as pd
import os

# データベース接続
db_path = 'database/data_lake.db'
conn = sqlite3.connect(db_path)

# テーブル構造を確認
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(gold_monthly_delivery_compliance_rate)")
columns = cursor.fetchall()

print("=" * 80)
print("gold_monthly_delivery_compliance_rate テーブル構造")
print("=" * 80)
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# 月次納期遵守率データを取得
query = """
SELECT *
FROM gold_monthly_delivery_compliance_rate
ORDER BY year_month
"""

df = pd.read_sql_query(query, conn)
conn.close()

print("\n" + "=" * 80)
print("月次納期遵守率KPI")
print("=" * 80)
print(f"\n総レコード数: {len(df)}件\n")

# カラム名を確認して表示
print("【カラム情報】")
for col in df.columns:
    print(f"  - {col}")

# サンプルデータ表示
print("\n【サンプルデータ（最初の5件）】")
print(df.head().to_string(index=False))

# 納期遵守率のカラムを特定
compliance_col = [col for col in df.columns if 'compliance' in col.lower() or '遵守' in col][0]
print(f"\n納期遵守率カラム: {compliance_col}")

# 基本統計
print("\n【統計サマリー】")
print(f"平均納期遵守率: {df[compliance_col].mean():.2f}%")
print(f"最大納期遵守率: {df[compliance_col].max():.2f}%")
print(f"最小納期遵守率: {df[compliance_col].min():.2f}%")

# 年月別の上位5件
print("\n【納期遵守率が高い月（上位5件）】")
display_cols = [col for col in df.columns if col != 'created_at']
top5 = df.nlargest(5, compliance_col)[display_cols]
print(top5.to_string(index=False))

# 年月別の下位5件
print("\n【納期遵守率が低い月（下位5件）】")
bottom5 = df.nsmallest(5, compliance_col)[display_cols]
print(bottom5.to_string(index=False))

# 全データ表示
print("\n【全データ】")
print(df[display_cols].to_string(index=False))

# CSV出力
output_dir = 'data/Gold'
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, '完成品出荷リードタイム遵守率.csv')

df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\n✓ CSVファイルを出力しました: {output_file}")
print("=" * 80)
