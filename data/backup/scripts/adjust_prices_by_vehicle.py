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

# 車種別の調整倍率（現在の価格は既に3倍になっている前提）
# 高級車をさらに高く、コンパクトカーを控えめに
vehicle_multipliers = {
    # プレミアムSUV/大型車（最も高価）
    'PLT-YF3': 1.5,    # PILOT
    'PSP-YF7': 1.5,    # PASSPORT
    'ODY-RC1': 1.4,    # ODYSSEY
    'PRO-ZC5': 1.4,    # Prologue (プレミアムEV)
    
    # 中型SUV/ミニバン
    'CRV-RT5': 1.2,    # CR-V
    'FRD-GB5': 1.0,    # FREED
    'SWN-RP6': 1.0,    # STEP WGN
    'VZL-RV3': 1.0,    # VEZEL
    
    # コンパクトカー/セダン
    'ZRV-RZ3': 0.9,    # ZR-V
    'FIT-GR3': 0.8,    # FIT
    'ACD-CV1': 1.1,    # ACCORD
    
    # EV（バッテリーコストを考慮）
    'HDE-ZC1': 0.9,    # Honda e
    'NOE-JG4': 0.7,    # N-ONE e:
    'ENP-ENP1': 0.8,   # e:NP1
    
    # その他
    'RDG-YF6': 1.3     # RIDGELINE
}

# 各レコードに倍率を適用（3倍ベースから調整）
for row in rows:
    product_id = row['product_id']
    multiplier = vehicle_multipliers.get(product_id, 1.0)
    
    # 各金額項目を倍率で調整
    row['unit_price_ex_tax'] = str(int(float(row['unit_price_ex_tax']) * multiplier))
    row['line_subtotal_incl_tax'] = str(int(float(row['line_subtotal_incl_tax']) * multiplier))
    row['line_subtotal_ex_tax'] = str(int(float(row['line_subtotal_ex_tax']) * multiplier))
    row['line_tax_amount'] = str(int(float(row['line_tax_amount']) * multiplier))
    row['line_shipping_fee_incl_tax'] = str(int(float(row['line_shipping_fee_incl_tax']) * multiplier))
    row['line_discount_incl_tax'] = str(int(float(row['line_discount_incl_tax']) * multiplier))
    row['line_total_incl_tax'] = str(int(float(row['line_total_incl_tax']) * multiplier))
    row['reference_price_ex_tax'] = str(int(float(row['reference_price_ex_tax']) * multiplier))

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

print(f"\n=== 車種別調整完了 ===")
print(f"総調達金額: ¥{total_amount:,}")
print(f"平均金額/件: ¥{int(avg_amount):,}")
print(f"\n調整倍率:")
for vehicle, mult in sorted(vehicle_multipliers.items()):
    print(f"  {vehicle}: {mult}x")
print(f"\n出力ファイル: {output_file}")
