import pandas as pd

# 調達伝票_itemを読み込む
df = pd.read_csv('data/Bronze/P2P/調達伝票_item.csv')

# 車種別の部品購入額合計を計算
vehicle_total = df.groupby('product_id')['line_total_incl_tax'].sum().reset_index()
vehicle_total.columns = ['車種', '部品購入額合計（税込）']

# 金額を降順でソート
vehicle_total = vehicle_total.sort_values('部品購入額合計（税込）', ascending=False)

# 結果を表示
print("=" * 60)
print("車種別部品購入額合計")
print("=" * 60)
print(vehicle_total.to_string(index=False))
print("=" * 60)

# 合計金額も表示
total = vehicle_total['部品購入額合計（税込）'].sum()
print(f"\n総合計: {total:,.0f} 円")

# CSVファイルとして出力
output_path = 'output/車種別部品購入額合計.csv'
vehicle_total.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n結果を {output_path} に保存しました。")
