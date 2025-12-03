"""
調達伝票_item.csvから車種別の部品購入額合計を集計
"""
import pandas as pd

csv_path = 'data/Bronze/P2P/調達伝票_item.csv'

print("=" * 80)
print("調達伝票_item.csv 車種別部品購入額集計")
print("=" * 80)

# CSVファイルを読み込み
df = pd.read_csv(csv_path, encoding='utf-8-sig')

print(f"\n総レコード数: {len(df):,}件")

# product_idが存在するレコードのみを抽出
df_with_product = df[df['product_id'].notna()].copy()
print(f"product_idあり: {len(df_with_product):,}件")

# 車種別に集計
vehicle_summary = df_with_product.groupby('product_id').agg({
    'line_subtotal_ex_tax': 'sum',
    'purchase_order_id': 'count'
}).round(0)

vehicle_summary.columns = ['購入額合計_税抜', '購入明細件数']
vehicle_summary = vehicle_summary.sort_values('購入額合計_税抜', ascending=False)

print("\n" + "=" * 80)
print("【車種別部品購入額合計】")
print("=" * 80)
print(vehicle_summary.to_string())

# 合計
total_amount = vehicle_summary['購入額合計_税抜'].sum()
total_records = vehicle_summary['購入明細件数'].sum()

print("\n" + "=" * 80)
print(f"全車種合計:")
print(f"  購入額合計: {total_amount:,.0f}円")
print(f"  購入明細件数: {total_records:,.0f}件")
print("=" * 80)

# 新規補充データ（PO-SUPP-で始まるもの）を確認
df_supplemented = df[df['purchase_order_id'].str.startswith('PO-SUPP-', na=False)].copy()
print(f"\n補充データ（PO-SUPP-*）: {len(df_supplemented):,}件")

if len(df_supplemented) > 0:
    supp_summary = df_supplemented.groupby('product_id').agg({
        'line_subtotal_ex_tax': 'sum',
        'purchase_order_id': 'count'
    }).round(0)
    supp_summary.columns = ['補充額', '補充件数']
    supp_summary = supp_summary.sort_values('補充額', ascending=False)
    
    print("\n【補充データの内訳】")
    print(supp_summary.to_string())
    print(f"\n補充データ合計額: {supp_summary['補充額'].sum():,.0f}円")

print("\n処理完了")
