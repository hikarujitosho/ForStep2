import sqlite3
import pandas as pd
import os

# データベース接続
db_path = 'database/data_lake.db'
conn = sqlite3.connect(db_path)

# テーブル構造を確認
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(gold_monthly_product_gross_margin)")
columns = cursor.fetchall()

print("=" * 80)
print("gold_monthly_product_gross_margin テーブル構造")
print("=" * 80)
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# 月次商品別粗利率データを取得
query = """
SELECT *
FROM gold_monthly_product_gross_margin
ORDER BY year_month, product_id
"""

df = pd.read_sql_query(query, conn)
conn.close()

print("\n" + "=" * 80)
print("月次商品別粗利率KPI")
print("=" * 80)
print(f"\n総レコード数: {len(df)}件\n")

# カラム名を確認
print("【カラム情報】")
for col in df.columns:
    print(f"  - {col}")

# サンプルデータ表示
print("\n【サンプルデータ（最初の10件）】")
print(df.head(10).to_string(index=False))

# 粗利率のカラムを特定
margin_col = [col for col in df.columns if 'margin' in col.lower() and 'gross_profit' not in col.lower()][0]
print(f"\n粗利率カラム: {margin_col}")

# 基本統計
print("\n【統計サマリー】")
print(f"平均粗利率: {df[margin_col].mean():.2f}%")
print(f"最大粗利率: {df[margin_col].max():.2f}%")
print(f"最小粗利率: {df[margin_col].min():.2f}%")

# 商品別の平均粗利率
print("\n【商品別平均粗利率（全期間）】")
product_avg = df.groupby('product_name')[margin_col].mean().sort_values(ascending=False)
for product, rate in product_avg.items():
    print(f"  {product}: {rate:.2f}%")

# 最近の月の商品別粗利率
print("\n【最新月(2025-12)の商品別粗利率】")
latest_month = df[df['year_month'] == '2025-12'].sort_values(margin_col, ascending=False)
if len(latest_month) > 0:
    for _, row in latest_month.iterrows():
        print(f"  {row['product_name']}: {row[margin_col]:.2f}% (売上: {row['revenue']:,.0f}円, 原価: {row['cost']:,.0f}円, 粗利: {row['gross_profit']:,.0f}円)")
else:
    print("  データなし")

# 2025年の商品別平均粗利率
print("\n【2025年の商品別平均粗利率】")
df_2025 = df[df['year_month'].str.startswith('2025')]
product_2025_avg = df_2025.groupby('product_name')[margin_col].mean().sort_values(ascending=False)
for product, rate in product_2025_avg.items():
    print(f"  {product}: {rate:.2f}%")

# 商品×月で最も粗利率が高いケース（上位10件）
print("\n【粗利率が高い商品×月（上位10件）】")
display_cols = [col for col in df.columns if col != 'created_at']
top10 = df.nlargest(10, margin_col)[display_cols]
print(top10.to_string(index=False))

# 商品×月で最も粗利率が低いケース（下位10件）
print("\n【粗利率が低い商品×月（下位10件）】")
bottom10 = df.nsmallest(10, margin_col)[display_cols]
print(bottom10.to_string(index=False))

# 商品別売上・粗利集計（2025年）
print("\n【2025年 商品別売上・粗利集計】")
summary_2025 = df_2025.groupby('product_name').agg({
    'revenue': 'sum',
    'cost': 'sum',
    'gross_profit': 'sum',
    margin_col: 'mean'
}).sort_values('revenue', ascending=False)
summary_2025.columns = ['売上合計', '原価合計', '粗利合計', '平均粗利率']
print(summary_2025.round(2).to_string())

# CSV出力
output_dir = 'data/Gold'
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, '月次商品別粗利率.csv')

df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\n✓ CSVファイルを出力しました: {output_file}")
print("=" * 80)
