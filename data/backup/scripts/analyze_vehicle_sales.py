import pandas as pd
from datetime import datetime

# データ読み込み
order_items = pd.read_csv('data/Bronze/ERP/受注伝票_item.csv')
price_conditions = pd.read_csv('data/Bronze/ERP/条件マスタ.csv')

# 日付をdatetime型に変換
order_items['pricing_date'] = pd.to_datetime(order_items['pricing_date'])
price_conditions['valid_from'] = pd.to_datetime(price_conditions['valid_from'])
price_conditions['valid_to'] = pd.to_datetime(price_conditions['valid_to'])

# 結果を格納するリスト
results = []

# 各受注明細に対して適切な価格を取得
for idx, row in order_items.iterrows():
    product_id = row['product_id']
    quantity = row['quantity']
    pricing_date = row['pricing_date']
    
    # 該当する価格条件を検索
    matching_prices = price_conditions[
        (price_conditions['product_id'] == product_id) &
        (price_conditions['valid_from'] <= pricing_date) &
        (price_conditions['valid_to'] >= pricing_date)
    ]
    
    if len(matching_prices) > 0:
        # 複数マッチした場合は最初のものを使用（または適切な選択ロジック）
        price_record = matching_prices.iloc[0]
        selling_price = price_record['selling_price_ex_tax']
        
        # 販売額を計算（税込）
        tax_rate = 0.1
        line_total = selling_price * quantity * (1 + tax_rate)
        
        results.append({
            'order_id': row['order_id'],
            'line_number': row['line_number'],
            'product_id': product_id,
            'quantity': quantity,
            'pricing_date': pricing_date,
            'selling_price_ex_tax': selling_price,
            'line_total_incl_tax': line_total
        })
    else:
        print(f"警告: {product_id} の {pricing_date.date()} に該当する価格が見つかりません")

# データフレーム化
result_df = pd.DataFrame(results)

# 車種別の販売額合計を計算
vehicle_sales = result_df.groupby('product_id')['line_total_incl_tax'].sum().reset_index()
vehicle_sales.columns = ['車種', '販売額合計（税込）']

# 金額を降順でソート
vehicle_sales = vehicle_sales.sort_values('販売額合計（税込）', ascending=False)

# 結果を表示
print("=" * 60)
print("車種別販売額合計")
print("=" * 60)
print(vehicle_sales.to_string(index=False))
print("=" * 60)

# 合計金額も表示
total = vehicle_sales['販売額合計（税込）'].sum()
print(f"\n総合計: {total:,.0f} 円")

# 詳細情報
print(f"\n受注明細数: {len(order_items):,}")
print(f"価格マッチング成功: {len(result_df):,}")
print(f"価格マッチング率: {len(result_df)/len(order_items)*100:.2f}%")

# CSVファイルとして出力
output_path = 'output/車種別販売額合計.csv'
vehicle_sales.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n結果を {output_path} に保存しました。")

# 明細データも保存
detail_output_path = 'output/受注明細_販売額計算済.csv'
result_df.to_csv(detail_output_path, index=False, encoding='utf-8-sig')
print(f"明細データを {detail_output_path} に保存しました。")
