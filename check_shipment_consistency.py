import pandas as pd

print("=" * 100)
print("出荷伝票の整合性チェック")
print("=" * 100)

# データ読み込み
header_df = pd.read_csv('data/Bronze/MES/出荷伝票_header.csv')
item_df = pd.read_csv('data/Bronze/MES/出荷伝票_item.csv')

print(f"\n出荷伝票_header: {len(header_df):,}件")
print(f"出荷伝票_item: {len(item_df):,}件")

# 1. shipment_idの整合性チェック
header_shipment_ids = set(header_df['shipment_id'].unique())
item_shipment_ids = set(item_df['shipment_id'].unique())

print(f"\n【1. shipment_IDの整合性】")
print(f"  header内のユニークなshipment_id: {len(header_shipment_ids):,}件")
print(f"  item内のユニークなshipment_id: {len(item_shipment_ids):,}件")

# headerにあってitemにないID
missing_in_item = header_shipment_ids - item_shipment_ids
if missing_in_item:
    print(f"  ⚠ headerにあってitemにないshipment_id: {len(missing_in_item)}件")
    if len(missing_in_item) <= 10:
        for sid in sorted(list(missing_in_item))[:10]:
            print(f"    - {sid}")
else:
    print(f"  ✓ headerのすべてのshipment_idがitemに存在")

# itemにあってheaderにないID
missing_in_header = item_shipment_ids - header_shipment_ids
if missing_in_header:
    print(f"  ⚠ itemにあってheaderにないshipment_id: {len(missing_in_header)}件")
    if len(missing_in_header) <= 10:
        for sid in sorted(list(missing_in_header))[:10]:
            print(f"    - {sid}")
else:
    print(f"  ✓ itemのすべてのshipment_idがheaderに存在")

# 2. shipment_timestampの整合性チェック
print(f"\n【2. shipment_timestampの整合性】")

# headerのshipment_timestampとitemのactual_ship_timestampを比較
merged = item_df.merge(
    header_df[['shipment_id', 'shipment_timestamp']], 
    on='shipment_id', 
    how='left'
)

# タイムスタンプの一致をチェック
merged['timestamp_match'] = merged['shipment_timestamp'] == merged['actual_ship_timestamp']
mismatch_count = (~merged['timestamp_match']).sum()

if mismatch_count > 0:
    print(f"  ⚠ タイムスタンプが一致しない明細: {mismatch_count:,}件")
    print(f"    一致率: {(len(merged) - mismatch_count) / len(merged) * 100:.2f}%")
    
    # サンプルを表示
    mismatches = merged[~merged['timestamp_match']][
        ['shipment_id', 'order_id', 'line_number', 'shipment_timestamp', 'actual_ship_timestamp']
    ].head(5)
    print("\n  不一致のサンプル:")
    for idx, row in mismatches.iterrows():
        print(f"    {row['shipment_id']} (Order: {row['order_id']}, Line: {row['line_number']})")
        print(f"      header: {row['shipment_timestamp']}")
        print(f"      item:   {row['actual_ship_timestamp']}")
else:
    print(f"  ✓ すべてのタイムスタンプが一致")

# 3. 出荷IDごとの明細数を確認
print(f"\n【3. 出荷IDごとの明細数】")
item_counts = item_df.groupby('shipment_id').size()
print(f"  明細数の統計:")
print(f"    最小: {item_counts.min()}件")
print(f"    最大: {item_counts.max()}件")
print(f"    平均: {item_counts.mean():.2f}件")
print(f"    中央値: {item_counts.median():.0f}件")

# 明細数が多い出荷を表示
top_shipments = item_counts.nlargest(5)
print(f"\n  明細数が多い出荷TOP5:")
for shipment_id, count in top_shipments.items():
    print(f"    {shipment_id}: {count}件")

# 4. order_idの分布確認
print(f"\n【4. order_idの分布】")
unique_orders_in_item = item_df['order_id'].nunique()
print(f"  item内のユニークなorder_id: {unique_orders_in_item:,}件")

# 1つの注文が複数の出荷に分割されているケースを確認
order_shipment_map = item_df.groupby('order_id')['shipment_id'].nunique()
multi_shipment_orders = order_shipment_map[order_shipment_map > 1]
print(f"  複数出荷に分割されている注文: {len(multi_shipment_orders)}件")

if len(multi_shipment_orders) > 0:
    print(f"\n  複数出荷の注文サンプル（TOP5）:")
    for order_id, count in multi_shipment_orders.nlargest(5).items():
        print(f"    {order_id}: {count}出荷に分割")
        shipments = item_df[item_df['order_id'] == order_id]['shipment_id'].unique()
        for sid in shipments[:3]:  # 最大3件表示
            print(f"      - {sid}")

# 5. データ品質サマリー
print(f"\n" + "=" * 100)
print("【整合性チェック結果サマリー】")
print("=" * 100)

issues = []
if missing_in_item:
    issues.append(f"headerにあってitemにないshipment_id: {len(missing_in_item)}件")
if missing_in_header:
    issues.append(f"itemにあってheaderにないshipment_id: {len(missing_in_header)}件")
if mismatch_count > 0:
    issues.append(f"タイムスタンプ不一致: {mismatch_count}件")

if issues:
    print("⚠ 検出された問題:")
    for i, issue in enumerate(issues, 1):
        print(f"  {i}. {issue}")
else:
    print("✓ すべてのチェックに合格しました！データの整合性は保たれています。")

print("=" * 100)
