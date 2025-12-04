import pandas as pd

print("=" * 100)
print("調達伝票_headerの金額再計算")
print("=" * 100)

# データ読み込み
header_df = pd.read_csv('data/Bronze/P2P/調達伝票_header.csv', low_memory=False)
item_df = pd.read_csv('data/Bronze/P2P/調達伝票_item.csv')

print(f"\n読み込み:")
print(f"  調達伝票_header: {len(header_df):,}件")
print(f"  調達伝票_item: {len(item_df):,}件")

# 修正前の金額を記録
before_total = header_df['order_total_incl_tax'].sum()
print(f"\n修正前のheader合計金額: {before_total:,.0f}円")

# itemから各発注の金額を集計
item_aggregated = item_df.groupby('purchase_order_id').agg({
    'line_subtotal_ex_tax': 'sum',
    'line_tax_amount': 'sum',
    'line_shipping_fee_incl_tax': 'sum',
    'line_discount_incl_tax': 'sum',
    'line_total_incl_tax': 'sum'
}).reset_index()

item_aggregated.columns = [
    'purchase_order_id',
    'item_subtotal_ex_tax',
    'item_tax_amount',
    'item_shipping_fee',
    'item_discount',
    'item_total_incl_tax'
]

print(f"\nitemから集計した発注数: {len(item_aggregated):,}件")

# headerとマージ
header_df = header_df.merge(
    item_aggregated[['purchase_order_id', 'item_subtotal_ex_tax', 'item_tax_amount', 
                     'item_shipping_fee', 'item_discount', 'item_total_incl_tax']], 
    on='purchase_order_id', 
    how='left'
)

# 金額フィールドを更新
# itemから集計した値で上書き
header_df['order_subtotal_ex_tax'] = header_df['item_subtotal_ex_tax']
header_df['tax_amount'] = header_df['item_tax_amount']
header_df['shipping_fee_ex_tax'] = header_df['item_shipping_fee'] / 1.1  # 税抜きに変換
header_df['discount_amount_incl_tax'] = header_df['item_discount']
header_df['order_total_incl_tax'] = header_df['item_total_incl_tax']

# payment_amountも更新（通常は発注総額と同じ）
header_df['payment_amount'] = header_df['order_total_incl_tax']

# 一時カラムを削除
header_df = header_df.drop(columns=[
    'item_subtotal_ex_tax', 'item_tax_amount', 'item_shipping_fee', 
    'item_discount', 'item_total_incl_tax'
])

# 修正後の金額
after_total = header_df['order_total_incl_tax'].sum()
reduction = before_total - after_total

print(f"\n修正後のheader合計金額: {after_total:,.0f}円")
print(f"削減額: {reduction:,.0f}円 ({reduction/before_total*100:.2f}%)")

# 修正された発注の統計
header_df['amount_changed'] = header_df['order_total_incl_tax'].notna()
changed_count = header_df['amount_changed'].sum()
print(f"\n金額が更新された発注数: {changed_count:,}件")

# 修正前後の比較サンプル
print(f"\n修正された発注のサンプル（最初の5件）:")
problem_vehicles = ['ACD-CV1', 'RDG-YF6', 'NOE-JG4', 'PRO-ZC5', 'HDE-ZC1', 'ENP-ENP1']
sample_items = item_df[item_df['product_id'].isin(problem_vehicles)].head(5)
for idx, item in sample_items.iterrows():
    po_id = item['purchase_order_id']
    header_row = header_df[header_df['purchase_order_id'] == po_id]
    if len(header_row) > 0:
        print(f"  {po_id}: {header_row['order_total_incl_tax'].values[0]:,.0f}円 ({item['product_id']})")

# バックアップ（修正前のheaderを保存）
backup_path = 'output/調達伝票_header_修正前.csv'
original_header = pd.read_csv('data/Bronze/P2P/調達伝票_header.csv', low_memory=False)
original_header.to_csv(backup_path, index=False, encoding='utf-8-sig')
print(f"\n修正前のheaderを {backup_path} に保存しました。")

# 更新したheaderを保存
output_path = 'data/Bronze/P2P/調達伝票_header.csv'
header_df.to_csv(output_path, index=False, encoding='utf-8')
print(f"更新したheaderを {output_path} に保存しました。")

# 最終確認
print(f"\n" + "=" * 100)
print("【最終確認】")
print("=" * 100)

# 再度整合性をチェック
item_totals = item_df.groupby('purchase_order_id')['line_total_incl_tax'].sum()
header_totals = header_df.set_index('purchase_order_id')['order_total_incl_tax']

mismatches = 0
for po_id in item_totals.index:
    if po_id in header_totals.index:
        diff = abs(header_totals[po_id] - item_totals[po_id])
        if diff > 0.01:  # 1銭以上の差異
            mismatches += 1

if mismatches == 0:
    print("✓ 整合性チェック合格！")
    print(f"  headerとitemの金額が完全に一致しています。")
    print(f"  header合計: {header_df['order_total_incl_tax'].sum():,.0f}円")
    print(f"  item合計: {item_df['line_total_incl_tax'].sum():,.0f}円")
else:
    print(f"⚠ まだ{mismatches}件の金額不一致があります")

print("=" * 100)
