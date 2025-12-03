# -*- coding: utf-8 -*-
import csv
import os
from collections import Counter

# ファイルパス
p2p_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P"
input_file = os.path.join(p2p_path, "調達伝票_item.csv")

# CSVを読み込み
with open(input_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"=== 調達伝票_item データ検証 ===\n")
print(f"総レコード数: {len(rows)}")

# 1. material_typeの分布
material_types = Counter(row['material_type'] for row in rows)
print(f"\n【material_type分布】")
for mtype, count in material_types.items():
    print(f"  {mtype}: {count}件")

# 2. 完成車別の調達件数（上位10件）
product_counts = Counter(row['product_id'] for row in rows)
print(f"\n【完成車別調達件数（上位10件）】")
for product, count in product_counts.most_common(10):
    print(f"  {product}: {count}件")

# 3. 部品カテゴリ別の件数
category_counts = Counter(row['material_category'] for row in rows)
print(f"\n【部品カテゴリ別件数】")
for category, count in sorted(category_counts.items()):
    print(f"  {category}: {count}件")

# 4. サンプルデータ確認（最初の3件）
print(f"\n【サンプルデータ（最初の3件）】")
for i, row in enumerate(rows[:3], 1):
    print(f"\n{i}. purchase_order_id: {row['purchase_order_id']}")
    print(f"   material_id: {row['material_id']}")
    print(f"   material_name: {row['material_name']}")
    print(f"   material_type: {row['material_type']}")
    print(f"   product_id: {row['product_id']}")
    print(f"   quantity: {row['quantity']}")
    print(f"   unit_price_ex_tax: ¥{int(row['unit_price_ex_tax']):,}")
    print(f"   ship_date: {row['ship_date']}")
    print(f"   received_date: {row['received_date']}")

# 5. 金額統計
total_amount = sum(int(row['line_total_incl_tax']) for row in rows)
avg_amount = total_amount / len(rows)
print(f"\n【金額統計】")
print(f"  総調達金額: ¥{total_amount:,}")
print(f"  平均金額/件: ¥{int(avg_amount):,}")

# 6. 日付範囲
dates = sorted([row['ship_date'] for row in rows])
print(f"\n【期間】")
print(f"  最初の出荷: {dates[0]}")
print(f"  最後の出荷: {dates[-1]}")

# 7. データ整合性チェック
print(f"\n【データ整合性チェック】")
# material_idが空のレコード
empty_material_id = sum(1 for row in rows if not row['material_id'])
print(f"  material_idが空: {empty_material_id}件")

# product_idが空のレコード
empty_product_id = sum(1 for row in rows if not row['product_id'])
print(f"  product_idが空: {empty_product_id}件")

# material_typeがdirectのレコード
direct_materials = [row for row in rows if row['material_type'] == 'direct']
print(f"  直接材レコード: {len(direct_materials)}件")

# 直接材でmaterial_idとproduct_idの両方が入っているか確認
if direct_materials:
    sample_direct = direct_materials[0]
    print(f"\n  直接材サンプル:")
    print(f"    material_id: {sample_direct['material_id']}")
    print(f"    material_name: {sample_direct['material_name']}")
    print(f"    product_id: {sample_direct['product_id']}")
