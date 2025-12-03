import csv
from datetime import datetime, timedelta
import random

# 既存の調達伝票_itemを読み込み
item_file = r'H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\backup\調達伝票_item.csv'
header_file = r'H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\backup\調達伝票_header.csv'

# headerを読み込み
with open(header_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    headers = {row['purchase_order_id']: row for row in reader}

# 既存のitemを読み込み
with open(item_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    existing_items = list(reader)
    fieldnames = reader.fieldnames

# 既存のPO IDを取得
existing_po_ids = set(item['purchase_order_id'] for item in existing_items)

# 間接材のマスターデータ
indirect_materials = [
    {'id': 'MRO-SAFETY-GOGGLES', 'name': '保護メガネ 防曇コート 10個セット', 'category': 'Safety Equipment', 'unit_price': 190, 'qty': 10, 'unspsc': '46181504'},
    {'id': 'MRO-COOLANT-20L', 'name': '冷却液 20Lポリタンク', 'category': 'Factory Consumables', 'unit_price': 1902, 'qty': 1, 'unspsc': '15121502'},
    {'id': 'MRO-DRILL-BIT-SET', 'name': 'ドリルビットセット HSS鋼 50本組', 'category': 'Maintenance Tools', 'unit_price': 15000, 'qty': 1, 'unspsc': '27112702'},
    {'id': 'MRO-GRINDING-WHEEL', 'name': '研削砥石 10枚セット', 'category': 'Maintenance Tools', 'unit_price': 12500, 'qty': 10, 'unspsc': '27112801'},
    {'id': 'MRO-HYDRAULIC-OIL', 'name': '作動油 18L缶', 'category': 'Factory Supplies', 'unit_price': 8900, 'qty': 5, 'unspsc': '15121503'},
    {'id': 'MRO-CUTTING-TOOL', 'name': '切削工具セット 20本組', 'category': 'Maintenance Tools', 'unit_price': 22500, 'qty': 1, 'unspsc': '27112702'},
    {'id': 'MRO-WELDING-ROD', 'name': '溶接棒 2.6mm 1kg×10箱', 'category': 'Factory Consumables', 'unit_price': 6750, 'qty': 10, 'unspsc': '30131501'},
    {'id': 'MRO-GLOVE-L', 'name': '作業用手袋 Lサイズ 50双', 'category': 'Safety Equipment', 'unit_price': 918, 'qty': 50, 'unspsc': '46181501'},
    {'id': 'MRO-TOOL-WRENCH', 'name': 'トルクレンチセット 12本組', 'category': 'Maintenance Tools', 'unit_price': 8900, 'qty': 1, 'unspsc': '27112101'},
    {'id': 'MRO-CUTTING-FLUID', 'name': '切削油 20L缶', 'category': 'Factory Consumables', 'unit_price': 7800, 'qty': 3, 'unspsc': '15121509'},
    {'id': 'PT-8500BK', 'name': 'ブラザー 感熱ラベルプリンター P-touch', 'category': 'Office Equipment', 'unit_price': 38500, 'qty': 1, 'unspsc': '44103203'},
    {'id': 'PT-TAPE24', 'name': 'ラベルテープ 24mm幅 10本セット', 'category': 'Office Supplies', 'unit_price': 4650, 'qty': 10, 'unspsc': '44121701'},
    {'id': 'MRO-LUBRICANT-5L', 'name': '工業用潤滑油 5L缶 6本', 'category': 'Factory Supplies', 'unit_price': 2083, 'qty': 6, 'unspsc': '15121501'},
]

# 不足しているPOのitemデータを生成
new_items = []

# PO-2025-007 (SUP-IND-001, 125000円)
if 'PO-2025-007' not in existing_po_ids:
    header = headers['PO-2025-007']
    material = {'id': 'MRO-GRINDING-WHEEL', 'name': '研削砥石 10枚セット', 'category': 'Maintenance Tools', 'unit_price': 12500, 'qty': 10, 'unspsc': '27112801'}
    
    order_date = datetime.strptime(header['order_date'], '%Y-%m-%d')
    ship_date = order_date
    received_date = datetime.strptime(header['expected_delivery_date'], '%Y-%m-%d')
    
    subtotal_ex_tax = material['unit_price'] * material['qty']
    tax = int(subtotal_ex_tax * 0.1)
    subtotal_incl_tax = subtotal_ex_tax + tax
    shipping_fee = int(header['shipping_fee_ex_tax'])
    total = subtotal_incl_tax + shipping_fee
    
    item = {
        'purchase_order_id': 'PO-2025-007',
        'line_number': '1',
        'material_id': material['id'],
        'material_name': material['name'],
        'material_category': material['category'],
        'material_type': 'indirect',
        'product_id': '',
        'unspsc_code': material['unspsc'],
        'quantity': str(material['qty']),
        'unit_price_ex_tax': str(material['unit_price']),
        'line_subtotal_incl_tax': str(int(header['order_total_incl_tax'])),
        'line_subtotal_ex_tax': str(subtotal_ex_tax),
        'line_tax_amount': str(tax),
        'line_tax_rate': '0.1',
        'line_shipping_fee_incl_tax': str(shipping_fee),
        'line_discount_incl_tax': '0',
        'line_total_incl_tax': str(int(header['order_total_incl_tax'])),
        'reference_price_ex_tax': str(material['unit_price'] + random.randint(200, 800)),
        'purchase_rule': '該当無し',
        'ship_date': ship_date.strftime('%Y-%m-%d'),
        'shipping_status': 'delivered',
        'carrier_tracking_number': f'YM{random.randint(1000000000, 9999999999)}',
        'shipped_quantity': str(material['qty']),
        'carrier_name': 'ヤマト運輸',
        'delivery_address': '川崎市高津区久本3-6-1, 213-0011, 神奈川県',
        'receiving_status': 'received',
        'received_quantity': str(material['qty']),
        'received_date': received_date.strftime('%Y-%m-%d'),
        'receiver_name': header['approver'],
        'receiver_email': 's.yamamoto@example.com',
        'cost_center': 'CC-005',
        'project_code': 'PRJ-2025-F',
        'department_code': 'DEPT-MFG',
        'account_user': header['approver'],
        'user_email': 's.yamamoto@example.com'
    }
    new_items.append(item)

# PO-2025-009 (SUP-TEC-001, 67500円)
if 'PO-2025-009' not in existing_po_ids:
    header = headers['PO-2025-009']
    material = {'id': 'MRO-CUTTING-TOOL', 'name': '切削工具セット 20本組', 'category': 'Maintenance Tools', 'unit_price': 22500, 'qty': 3, 'unspsc': '27112702'}
    
    order_date = datetime.strptime(header['order_date'], '%Y-%m-%d')
    ship_date = order_date
    received_date = datetime.strptime(header['payment_date'], '%Y-%m-%d')
    
    subtotal_ex_tax = material['unit_price'] * material['qty']
    tax = int(subtotal_ex_tax * 0.1)
    subtotal_incl_tax = subtotal_ex_tax + tax
    shipping_fee = int(header['shipping_fee_ex_tax'])
    discount = int(header['discount_amount_incl_tax'])
    total = int(header['order_total_incl_tax'])
    
    item = {
        'purchase_order_id': 'PO-2025-009',
        'line_number': '1',
        'material_id': material['id'],
        'material_name': material['name'],
        'material_category': material['category'],
        'material_type': 'indirect',
        'product_id': '',
        'unspsc_code': material['unspsc'],
        'quantity': str(material['qty']),
        'unit_price_ex_tax': str(material['unit_price']),
        'line_subtotal_incl_tax': str(total),
        'line_subtotal_ex_tax': str(subtotal_ex_tax),
        'line_tax_amount': str(tax),
        'line_tax_rate': '0.1',
        'line_shipping_fee_incl_tax': str(shipping_fee),
        'line_discount_incl_tax': str(discount),
        'line_total_incl_tax': str(total),
        'reference_price_ex_tax': str(material['unit_price'] + random.randint(300, 1000)),
        'purchase_rule': '該当無し',
        'ship_date': ship_date.strftime('%Y-%m-%d'),
        'shipping_status': 'delivered',
        'carrier_tracking_number': f'YM{random.randint(1000000000, 9999999999)}',
        'shipped_quantity': str(material['qty']),
        'carrier_name': 'ヤマト運輸',
        'delivery_address': '川崎市高津区久本3-6-1, 213-0011, 神奈川県',
        'receiving_status': 'received',
        'received_quantity': str(material['qty']),
        'received_date': received_date.strftime('%Y-%m-%d'),
        'receiver_name': header['approver'],
        'receiver_email': 'm.kobayashi@example.com',
        'cost_center': 'CC-006',
        'project_code': 'PRJ-2025-H',
        'department_code': 'DEPT-MFG',
        'account_user': header['approver'],
        'user_email': 'm.kobayashi@example.com'
    }
    new_items.append(item)

# 既存データと結合
all_items = existing_items + new_items

# 保存
with open(item_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_items)

print(f'間接材itemレコードを追加しました: {len(new_items)}件')
print(f'総レコード数: {len(all_items)}件')
for item in new_items:
    print(f"  {item['purchase_order_id']}: {item['material_name']} x{item['quantity']} = ¥{item['line_total_incl_tax']:,}")
