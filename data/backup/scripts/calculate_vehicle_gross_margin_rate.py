import pandas as pd

# 販売額と原価額のデータを読み込む
sales_df = pd.read_csv('output/車種別販売額合計.csv')
cost_df = pd.read_csv('output/車種別部品購入額合計.csv')

# カラム名を統一
sales_df.columns = ['車種', '販売額']
cost_df.columns = ['車種', '原価額']

# 両方のデータをマージ
merged_df = pd.merge(sales_df, cost_df, on='車種', how='outer')

# 欠損値を0で埋める（販売はあるが調達がない、または逆のケース）
merged_df = merged_df.fillna(0)

# 粗利と粗利率を計算
merged_df['粗利'] = merged_df['販売額'] - merged_df['原価額']
merged_df['粗利率'] = (merged_df['粗利'] / merged_df['販売額'] * 100).round(2)

# 販売額で降順ソート
merged_df = merged_df.sort_values('販売額', ascending=False)

# 結果を表示
print("=" * 80)
print("車種別粗利率")
print("=" * 80)
print(f"{'車種':<15} {'販売額':>15} {'原価額':>15} {'粗利':>15} {'粗利率':>10}")
print("-" * 80)

for idx, row in merged_df.iterrows():
    vehicle = row['車種']
    sales = row['販売額']
    cost = row['原価額']
    profit = row['粗利']
    margin_rate = row['粗利率']
    
    print(f"{vehicle:<15} {sales:>15,.0f} {cost:>15,.0f} {profit:>15,.0f} {margin_rate:>9.2f}%")

print("=" * 80)

# 合計行を計算
total_sales = merged_df['販売額'].sum()
total_cost = merged_df['原価額'].sum()
total_profit = merged_df['粗利'].sum()
total_margin_rate = (total_profit / total_sales * 100) if total_sales > 0 else 0

print(f"{'合計':<15} {total_sales:>15,.0f} {total_cost:>15,.0f} {total_profit:>15,.0f} {total_margin_rate:>9.2f}%")
print("=" * 80)

# 分析結果を表示
print("\n【分析】")
high_margin = merged_df[merged_df['粗利率'] > 0].sort_values('粗利率', ascending=False).head(3)
print(f"\n粗利率が高い車種TOP3:")
for idx, row in high_margin.iterrows():
    print(f"  {row['車種']}: {row['粗利率']:.2f}%")

low_margin = merged_df[merged_df['販売額'] > 0].sort_values('粗利率', ascending=True).head(3)
print(f"\n粗利率が低い車種TOP3:")
for idx, row in low_margin.iterrows():
    print(f"  {row['車種']}: {row['粗利率']:.2f}%")

# CSVファイルとして出力
output_path = 'output/車種別粗利率.csv'
merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n結果を {output_path} に保存しました。")
