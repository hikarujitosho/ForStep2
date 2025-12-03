"""
調達伝票のheaderとitemの整合性を確認するスクリプト
header.csvに存在する全てのpurchase_order_idに対して、
対応するitem.csvのレコードが存在するかを確認
"""
import pandas as pd

# ファイルパス
header_file = r'c:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_header.csv'
item_file = r'c:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_item.csv'

# データ読み込み
print("データを読み込んでいます...")
df_header = pd.read_csv(header_file)
df_item = pd.read_csv(item_file)

print(f"\nheader.csv: {len(df_header)} 件のレコード")
print(f"item.csv: {len(df_item)} 件のレコード")

# headerの全purchase_order_idを取得
header_po_ids = set(df_header['purchase_order_id'].unique())
print(f"\nheader.csvのユニークなpurchase_order_id: {len(header_po_ids)} 件")

# itemの全purchase_order_idを取得
item_po_ids = set(df_item['purchase_order_id'].unique())
print(f"item.csvのユニークなpurchase_order_id: {len(item_po_ids)} 件")

# headerに存在するがitemに存在しないpurchase_order_idを確認
missing_in_item = header_po_ids - item_po_ids

if missing_in_item:
    print(f"\n【警告】headerには存在するが、itemには存在しないpurchase_order_id: {len(missing_in_item)} 件")
    print("\n該当するpurchase_order_id:")
    for po_id in sorted(missing_in_item)[:20]:  # 最初の20件を表示
        header_info = df_header[df_header['purchase_order_id'] == po_id].iloc[0]
        print(f"  - {po_id}: {header_info['order_date']}, {header_info['supplier_name']}, {header_info['order_status']}")
    if len(missing_in_item) > 20:
        print(f"  ... 他 {len(missing_in_item) - 20} 件")
else:
    print("\n【OK】すべてのheaderのpurchase_order_idに対応するitemが存在します")

# 逆のチェック: itemに存在するがheaderに存在しないpurchase_order_id
extra_in_item = item_po_ids - header_po_ids
if extra_in_item:
    print(f"\n【警告】itemには存在するが、headerには存在しないpurchase_order_id: {len(extra_in_item)} 件")
    print("\n該当するpurchase_order_id:")
    for po_id in sorted(extra_in_item)[:20]:
        print(f"  - {po_id}")
    if len(extra_in_item) > 20:
        print(f"  ... 他 {len(extra_in_item) - 20} 件")
else:
    print("\n【OK】すべてのitemのpurchase_order_idに対応するheaderが存在します")

# 統計情報
print("\n=== 統計情報 ===")
print(f"headerとitemの両方に存在するpurchase_order_id: {len(header_po_ids & item_po_ids)} 件")

# 各purchase_order_idごとのitem数を集計
item_counts = df_item['purchase_order_id'].value_counts()
print(f"\npurchase_order_idあたりの平均item数: {item_counts.mean():.2f}")
print(f"最小item数: {item_counts.min()}")
print(f"最大item数: {item_counts.max()}")

print("\n=== 完了 ===")
