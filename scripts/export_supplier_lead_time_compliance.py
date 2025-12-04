import sqlite3
import pandas as pd
import os

# データベース接続
db_path = 'database/data_lake.db'
conn = sqlite3.connect(db_path)

# テーブル構造を確認
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(gold_monthly_supplier_lead_time_compliance)")
columns = cursor.fetchall()

print("=" * 80)
print("gold_monthly_supplier_lead_time_compliance テーブル構造")
print("=" * 80)
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# 月次仕入先リードタイム遵守率データを取得
query = """
SELECT *
FROM gold_monthly_supplier_lead_time_compliance
ORDER BY year_month, supplier_id
"""

df = pd.read_sql_query(query, conn)
conn.close()

print("\n" + "=" * 80)
print("月次仕入先リードタイム遵守率KPI")
print("=" * 80)
print(f"\n総レコード数: {len(df)}件\n")

# カラム名を確認して表示
print("【カラム情報】")
for col in df.columns:
    print(f"  - {col}")

# サンプルデータ表示
print("\n【サンプルデータ（最初の10件）】")
print(df.head(10).to_string(index=False))

# 遵守率のカラムを特定
compliance_col = [col for col in df.columns if 'compliance' in col.lower() or '遵守' in col][0]
print(f"\n遵守率カラム: {compliance_col}")

# 基本統計
print("\n【統計サマリー】")
print(f"平均遵守率: {df[compliance_col].mean():.2f}%")
print(f"最大遵守率: {df[compliance_col].max():.2f}%")
print(f"最小遵守率: {df[compliance_col].min():.2f}%")

# 取引先別の平均遵守率
print("\n【取引先別平均遵守率（上位10社）】")
supplier_avg = df.groupby('supplier_name')[compliance_col].mean().sort_values(ascending=False).head(10)
for supplier, rate in supplier_avg.items():
    print(f"  {supplier}: {rate:.2f}%")

# 取引先別の平均遵守率（下位10社）
print("\n【取引先別平均遵守率（下位10社）】")
supplier_avg_bottom = df.groupby('supplier_name')[compliance_col].mean().sort_values(ascending=True).head(10)
for supplier, rate in supplier_avg_bottom.items():
    print(f"  {supplier}: {rate:.2f}%")

# 年月別の平均遵守率
print("\n【年月別平均遵守率（最新12ヶ月）】")
monthly_avg = df.groupby('year_month')[compliance_col].mean().tail(12)
for month, rate in monthly_avg.items():
    print(f"  {month}: {rate:.2f}%")

# 遵守率が低い取引先（下位10件）
print("\n【遵守率が低い取引先×月（下位10件）】")
display_cols = [col for col in df.columns if col != 'created_at']
bottom10 = df.nsmallest(10, compliance_col)[display_cols]
print(bottom10.to_string(index=False))

# CSV出力
output_dir = 'data/Gold'
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, '取引先別部品入荷リードタイム遵守率.csv')

df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\n✓ CSVファイルを出力しました: {output_file}")
print("=" * 80)
