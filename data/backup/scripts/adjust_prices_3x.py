# -*- coding: utf-8 -*-
import csv
import os

# ファイルパス
p2p_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P"
input_file = os.path.join(p2p_path, "調達伝票_item.csv")
output_file = os.path.join(p2p_path, "調達伝票_item.csv")

# CSVを読み込み
with open(input_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"総レコード数: {len(rows)}")

# 各金額を3倍に調整
for row in rows:
    # 単価を3倍
    row['unit_price_ex_tax'] = str(int(float(row['unit_price_ex_tax']) * 3))
    
    # 各金額項目を3倍
    row['line_subtotal_incl_tax'] = str(int(float(row['line_subtotal_incl_tax']) * 3))
    row['line_subtotal_ex_tax'] = str(int(float(row['line_subtotal_ex_tax']) * 3))
    row['line_tax_amount'] = str(int(float(row['line_tax_amount']) * 3))
    row['line_shipping_fee_incl_tax'] = str(int(float(row['line_shipping_fee_incl_tax']) * 3))
    row['line_discount_incl_tax'] = str(int(float(row['line_discount_incl_tax']) * 3))
    row['line_total_incl_tax'] = str(int(float(row['line_total_incl_tax']) * 3))
    row['reference_price_ex_tax'] = str(int(float(row['reference_price_ex_tax']) * 3))

# CSVに書き込み
fieldnames = ['purchase_order_id', 'line_number', 'material_id', 'material_name', 
              'material_category', 'material_type', 'product_id', 'unspsc_code', 
              'quantity', 'unit_price_ex_tax', 'line_subtotal_incl_tax', 
              'line_subtotal_ex_tax', 'line_tax_amount', 'line_tax_rate', 
              'line_shipping_fee_incl_tax', 'line_discount_incl_tax', 
              'line_total_incl_tax', 'reference_price_ex_tax', 'purchase_rule', 
              'ship_date', 'shipping_status', 'carrier_tracking_number', 
              'shipped_quantity', 'carrier_name', 'delivery_address', 
              'receiving_status', 'received_quantity', 'received_date', 
              'receiver_name', 'receiver_email', 'cost_center', 'project_code', 
              'department_code', 'account_user', 'user_email']

with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# 調整後の金額サマリー
total_amount = sum(int(row['line_total_incl_tax']) for row in rows)
avg_amount = total_amount / len(rows)

print(f"\n=== 調整完了 ===")
print(f"総調達金額: ¥{total_amount:,} (調整前の約3倍)")
print(f"平均金額/件: ¥{int(avg_amount):,}")
print(f"\n出力ファイル: {output_file}")
