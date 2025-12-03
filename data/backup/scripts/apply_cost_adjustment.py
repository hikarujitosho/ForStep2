import pandas as pd
import numpy as np

# 調整率
adjustment_rates = {
    'ACD-CV1': 0.1805,
    'RDG-YF6': 0.2060,
    'NOE-JG4': 0.2473,
    'PRO-ZC5': 0.2274,
    'HDE-ZC1': 0.2845,
    'ENP-ENP1': 0.2685
}

print("=" * 100)
print("調達伝票_itemの修正を開始します")
print("=" * 100)

# 元のデータを読み込む
df = pd.read_csv('data/Bronze/P2P/調達伝票_item.csv')
print(f"\n読み込みレコード数: {len(df):,}件")

# 修正前のバックアップ
original_total = df['line_total_incl_tax'].sum()
print(f"修正前の総額: {original_total:,.0f}円")

# 各車種の修正状況を記録
修正サマリー = []

for vehicle, rate in adjustment_rates.items():
    # 該当車種のデータを抽出
    vehicle_mask = df['product_id'] == vehicle
    vehicle_count = vehicle_mask.sum()
    
    if vehicle_count == 0:
        print(f"\n{vehicle}: データなし")
        continue
    
    before_total = df.loc[vehicle_mask, 'line_total_incl_tax'].sum()
    
    # 単価を調整（調整率を掛ける）
    df.loc[vehicle_mask, 'unit_price_ex_tax'] = df.loc[vehicle_mask, 'unit_price_ex_tax'] * rate
    df.loc[vehicle_mask, 'reference_price_ex_tax'] = df.loc[vehicle_mask, 'reference_price_ex_tax'] * rate
    
    # 金額を再計算
    # line_subtotal_ex_tax = quantity × unit_price_ex_tax
    df.loc[vehicle_mask, 'line_subtotal_ex_tax'] = (
        df.loc[vehicle_mask, 'quantity'] * df.loc[vehicle_mask, 'unit_price_ex_tax']
    )
    
    # line_tax_amount = line_subtotal_ex_tax × line_tax_rate
    df.loc[vehicle_mask, 'line_tax_amount'] = (
        df.loc[vehicle_mask, 'line_subtotal_ex_tax'] * df.loc[vehicle_mask, 'line_tax_rate']
    )
    
    # line_subtotal_incl_tax = line_subtotal_ex_tax + line_tax_amount
    df.loc[vehicle_mask, 'line_subtotal_incl_tax'] = (
        df.loc[vehicle_mask, 'line_subtotal_ex_tax'] + df.loc[vehicle_mask, 'line_tax_amount']
    )
    
    # line_total_incl_tax = line_subtotal_incl_tax + line_shipping_fee_incl_tax - line_discount_incl_tax
    df.loc[vehicle_mask, 'line_total_incl_tax'] = (
        df.loc[vehicle_mask, 'line_subtotal_incl_tax'] +
        df.loc[vehicle_mask, 'line_shipping_fee_incl_tax'] -
        df.loc[vehicle_mask, 'line_discount_incl_tax']
    )
    
    after_total = df.loc[vehicle_mask, 'line_total_incl_tax'].sum()
    reduction = before_total - after_total
    reduction_pct = (1 - after_total / before_total) * 100
    
    修正サマリー.append({
        '車種': vehicle,
        '修正件数': vehicle_count,
        '修正前金額': before_total,
        '修正後金額': after_total,
        '削減額': reduction,
        '削減率': reduction_pct
    })
    
    print(f"\n{vehicle}:")
    print(f"  修正件数: {vehicle_count:,}件")
    print(f"  修正前: {before_total:,.0f}円")
    print(f"  修正後: {after_total:,.0f}円")
    print(f"  削減額: {reduction:,.0f}円 ({reduction_pct:.2f}%削減)")

# 修正後の総額
modified_total = df['line_total_incl_tax'].sum()
total_reduction = original_total - modified_total

print("\n" + "=" * 100)
print(f"修正後の総額: {modified_total:,.0f}円")
print(f"総削減額: {total_reduction:,.0f}円 ({(1 - modified_total/original_total)*100:.2f}%削減)")
print("=" * 100)

# 修正したデータを保存
output_path = 'data/Bronze/P2P/調達伝票_item.csv'
df.to_csv(output_path, index=False, encoding='utf-8')
print(f"\n修正したデータを {output_path} に保存しました。")

# サマリーも保存
summary_df = pd.DataFrame(修正サマリー)
summary_path = 'output/調達伝票修正サマリー.csv'
summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
print(f"修正サマリーを {summary_path} に保存しました。")

print("\n【重要】")
print("調達伝票_itemが更新されました。")
print("次のステップで粗利率を再計算して、目標が達成されたか確認してください。")
