import pandas as pd

# データ読み込み
procurement_df = pd.read_csv('data/Bronze/P2P/調達伝票_item.csv')

# 問題のある6車種
problem_vehicles = ['ACD-CV1', 'RDG-YF6', 'NOE-JG4', 'PRO-ZC5', 'HDE-ZC1', 'ENP-ENP1']

print("=" * 100)
print("問題の6車種の調達データ詳細分析")
print("=" * 100)

for vehicle in problem_vehicles:
    # 該当車種の調達データを抽出
    vehicle_data = procurement_df[procurement_df['product_id'] == vehicle].copy()
    
    if len(vehicle_data) == 0:
        print(f"\n【{vehicle}】 調達データなし")
        continue
    
    # 合計金額を計算
    total_amount = vehicle_data['line_total_incl_tax'].sum()
    total_lines = len(vehicle_data)
    
    print(f"\n【{vehicle}】")
    print(f"  調達明細数: {total_lines:,}件")
    print(f"  合計金額: {total_amount:,.0f}円")
    print(f"  平均単価: {total_amount/total_lines:,.0f}円/明細")
    
    # カテゴリ別集計
    category_summary = vehicle_data.groupby('material_category').agg({
        'line_total_incl_tax': 'sum',
        'line_number': 'count'
    }).reset_index()
    category_summary.columns = ['カテゴリ', '金額', '件数']
    category_summary = category_summary.sort_values('金額', ascending=False)
    
    print(f"\n  カテゴリ別内訳:")
    for idx, row in category_summary.head(10).iterrows():
        pct = row['金額'] / total_amount * 100
        print(f"    {row['カテゴリ']:<30} {row['金額']:>15,.0f}円 ({pct:>5.1f}%) {row['件数']:>4}件")
    
    # 高額部品トップ5
    print(f"\n  高額部品TOP5:")
    top_items = vehicle_data.nlargest(5, 'line_total_incl_tax')[
        ['material_name', 'quantity', 'unit_price_ex_tax', 'line_total_incl_tax']
    ]
    for idx, row in top_items.iterrows():
        print(f"    {row['material_name']:<40} 数量:{row['quantity']:>3} 単価:{row['unit_price_ex_tax']:>10,.0f}円 合計:{row['line_total_incl_tax']:>12,.0f}円")

print("\n" + "=" * 100)
