import pandas as pd

print("=" * 100)
print("出荷伝票_headerのクリーンアップ")
print("=" * 100)

# データ読み込み
header_df = pd.read_csv('data/Bronze/MES/出荷伝票_header.csv')
item_df = pd.read_csv('data/Bronze/MES/出荷伝票_item.csv')

print(f"\n修正前:")
print(f"  出荷伝票_header: {len(header_df):,}件")
print(f"  出荷伝票_item: {len(item_df):,}件")

# itemに存在するshipment_idを取得
valid_shipment_ids = set(item_df['shipment_id'].unique())
print(f"\n  itemに存在するshipment_id: {len(valid_shipment_ids):,}件")

# headerでitemに存在するshipment_idのみを保持
header_before = len(header_df)
header_df_cleaned = header_df[header_df['shipment_id'].isin(valid_shipment_ids)].copy()
header_after = len(header_df_cleaned)
removed_count = header_before - header_after

print(f"\n削除対象:")
print(f"  itemに存在しないheaderレコード: {removed_count}件")

# 削除されるレコードのサンプルを表示
removed_records = header_df[~header_df['shipment_id'].isin(valid_shipment_ids)]
print(f"\n削除されるレコードのサンプル（最初の10件）:")
for idx, row in removed_records.head(10).iterrows():
    print(f"  {row['shipment_id']}: {row['shipment_timestamp']} (customer: {row['customer_id']})")

# 削除されるレコードを保存（バックアップ）
backup_path = 'output/削除された出荷伝票_header.csv'
removed_records.to_csv(backup_path, index=False, encoding='utf-8-sig')
print(f"\n削除されるレコードを {backup_path} に保存しました。")

# クリーンアップしたheaderを保存
output_path = 'data/Bronze/MES/出荷伝票_header.csv'
header_df_cleaned.to_csv(output_path, index=False, encoding='utf-8')

print(f"\n修正後:")
print(f"  出荷伝票_header: {len(header_df_cleaned):,}件")
print(f"  削除件数: {removed_count}件")
print(f"\nクリーンアップしたデータを {output_path} に保存しました。")

# 最終確認
print(f"\n" + "=" * 100)
print("【確認】")
print("=" * 100)
header_shipment_ids = set(header_df_cleaned['shipment_id'].unique())
item_shipment_ids = set(item_df['shipment_id'].unique())

# headerにあってitemにないID
missing_in_item = header_shipment_ids - item_shipment_ids
# itemにあってheaderにないID
missing_in_header = item_shipment_ids - header_shipment_ids

if not missing_in_item and not missing_in_header:
    print("✓ 整合性チェック合格！")
    print(f"  headerとitemのshipment_idが完全に一致しています。")
    print(f"  共通のshipment_id数: {len(header_shipment_ids):,}件")
else:
    if missing_in_item:
        print(f"⚠ headerにあってitemにないID: {len(missing_in_item)}件")
    if missing_in_header:
        print(f"⚠ itemにあってheaderにないID: {len(missing_in_header)}件")

print("=" * 100)
