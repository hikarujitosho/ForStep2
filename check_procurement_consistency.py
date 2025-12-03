import pandas as pd

print("=" * 100)
print("調達伝票の整合性チェック")
print("=" * 100)

# データ読み込み
header_df = pd.read_csv('data/Bronze/P2P/調達伝票_header.csv')
item_df = pd.read_csv('data/Bronze/P2P/調達伝票_item.csv')

print(f"\n調達伝票_header: {len(header_df):,}件")
print(f"調達伝票_item: {len(item_df):,}件")

# 1. purchase_order_idの整合性チェック
header_po_ids = set(header_df['purchase_order_id'].unique())
item_po_ids = set(item_df['purchase_order_id'].unique())

print(f"\n【1. purchase_order_IDの整合性】")
print(f"  header内のユニークなpurchase_order_id: {len(header_po_ids):,}件")
print(f"  item内のユニークなpurchase_order_id: {len(item_po_ids):,}件")

# headerにあってitemにないID
missing_in_item = header_po_ids - item_po_ids
if missing_in_item:
    print(f"  ⚠ headerにあってitemにないpurchase_order_id: {len(missing_in_item)}件")
    if len(missing_in_item) <= 10:
        for po_id in sorted(list(missing_in_item))[:10]:
            print(f"    - {po_id}")
else:
    print(f"  ✓ headerのすべてのpurchase_order_idがitemに存在")

# itemにあってheaderにないID
missing_in_header = item_po_ids - header_po_ids
if missing_in_header:
    print(f"  ⚠ itemにあってheaderにないpurchase_order_id: {len(missing_in_header)}件")
    if len(missing_in_header) <= 10:
        for po_id in sorted(list(missing_in_header))[:10]:
            print(f"    - {po_id}")
else:
    print(f"  ✓ itemのすべてのpurchase_order_idがheaderに存在")

# 2. 金額の整合性チェック
print(f"\n【2. 金額の整合性】")

# itemの金額を集計
item_totals = item_df.groupby('purchase_order_id').agg({
    'line_total_incl_tax': 'sum'
}).reset_index()
item_totals.columns = ['purchase_order_id', 'item_total']

# headerと比較
merged = header_df.merge(item_totals, on='purchase_order_id', how='left')
merged['amount_match'] = (merged['order_total_incl_tax'] - merged['item_total']).abs() < 0.01

mismatch_count = (~merged['amount_match']).sum()

if mismatch_count > 0:
    print(f"  ⚠ 金額が一致しない発注: {mismatch_count:,}件")
    print(f"    一致率: {(len(merged) - mismatch_count) / len(merged) * 100:.2f}%")
    
    # サンプルを表示
    mismatches = merged[~merged['amount_match']][
        ['purchase_order_id', 'order_total_incl_tax', 'item_total']
    ].head(5)
    print("\n  不一致のサンプル:")
    for idx, row in mismatches.iterrows():
        diff = row['order_total_incl_tax'] - row['item_total']
        print(f"    {row['purchase_order_id']}: header={row['order_total_incl_tax']:,.0f}円, item={row['item_total']:,.0f}円 (差額={diff:,.0f}円)")
else:
    print(f"  ✓ すべての金額が一致")

# 3. 発注IDごとの明細数を確認
print(f"\n【3. 発注IDごとの明細数】")
item_counts = item_df.groupby('purchase_order_id').size()
print(f"  明細数の統計:")
print(f"    最小: {item_counts.min()}件")
print(f"    最大: {item_counts.max()}件")
print(f"    平均: {item_counts.mean():.2f}件")
print(f"    中央値: {item_counts.median():.0f}件")

# 明細数が多い発注を表示
top_pos = item_counts.nlargest(5)
print(f"\n  明細数が多い発注TOP5:")
for po_id, count in top_pos.items():
    print(f"    {po_id}: {count}件")

# 4. 修正された6車種の影響確認
print(f"\n【4. 修正された6車種の影響確認】")
problem_vehicles = ['ACD-CV1', 'RDG-YF6', 'NOE-JG4', 'PRO-ZC5', 'HDE-ZC1', 'ENP-ENP1']
modified_items = item_df[item_df['product_id'].isin(problem_vehicles)]

if len(modified_items) > 0:
    print(f"  修正対象車種の明細数: {len(modified_items):,}件")
    
    # 修正対象のユニークな発注ID
    modified_po_ids = modified_items['purchase_order_id'].unique()
    print(f"  影響を受ける発注数: {len(modified_po_ids):,}件")
    
    # これらの発注のheaderレコード数を確認
    affected_headers = header_df[header_df['purchase_order_id'].isin(modified_po_ids)]
    print(f"  対応するheaderレコード数: {len(affected_headers):,}件")
    
    if len(modified_po_ids) == len(affected_headers):
        print(f"  ✓ 修正された明細の発注IDはすべてheaderに存在")
    else:
        missing = len(modified_po_ids) - len(affected_headers)
        print(f"  ⚠ {missing}件の発注IDがheaderに存在しない")

# 5. 金額の再計算が必要なheaderの確認
print(f"\n【5. header金額の再計算】")
print(f"  調達伝票_itemの単価が修正されたため、headerのorder_total_incl_taxも")
print(f"  itemの合計と一致するよう再計算が必要です。")

# headerの現在の合計金額
current_header_total = header_df['order_total_incl_tax'].sum()
# itemの明細合計金額
current_item_total = item_df['line_total_incl_tax'].sum()

print(f"\n  現在のheader合計: {current_header_total:,.0f}円")
print(f"  現在のitem合計: {current_item_total:,.0f}円")
print(f"  差額: {current_header_total - current_item_total:,.0f}円")

if abs(current_header_total - current_item_total) > 1:
    print(f"\n  ⚠ header金額の再計算が必要です")
else:
    print(f"\n  ✓ header金額は正確です")

# 6. データ品質サマリー
print(f"\n" + "=" * 100)
print("【整合性チェック結果サマリー】")
print("=" * 100)

issues = []
if missing_in_item:
    issues.append(f"headerにあってitemにないpurchase_order_id: {len(missing_in_item)}件")
if missing_in_header:
    issues.append(f"itemにあってheaderにないpurchase_order_id: {len(missing_in_header)}件")
if mismatch_count > 0:
    issues.append(f"金額不一致: {mismatch_count}件")
if abs(current_header_total - current_item_total) > 1:
    issues.append(f"header合計金額とitem合計金額の差異: {abs(current_header_total - current_item_total):,.0f}円")

if issues:
    print("⚠ 検出された問題:")
    for i, issue in enumerate(issues, 1):
        print(f"  {i}. {issue}")
    print("\n推奨対応:")
    print("  - headerの金額フィールドをitemの合計から再計算する必要があります")
else:
    print("✓ すべてのチェックに合格しました！データの整合性は保たれています。")

print("=" * 100)
