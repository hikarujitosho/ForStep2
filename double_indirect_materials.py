import csv
from datetime import datetime, timedelta
import random

# ファイルパス
item_file = r'H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P\調達伝票_item.csv'
header_file = r'H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P\調達伝票_header.csv'

# 既存データ読み込み
print('既存データを読み込み中...')
with open(item_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    existing_items = list(reader)
    fieldnames = reader.fieldnames

with open(header_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    existing_headers = list(reader)
    header_fieldnames = reader.fieldnames

# 既存の間接材データを抽出
indirect_items = [item for item in existing_items if item['material_type'] == 'indirect']
print(f'既存の間接材明細数: {len(indirect_items)}件')

# 既存の間接材POを抽出
indirect_po_ids = set(item['purchase_order_id'] for item in indirect_items)
indirect_headers = [h for h in existing_headers if h['purchase_order_id'] in indirect_po_ids]
print(f'既存の間接材PO数: {len(indirect_headers)}件')

# 既存の最大PO番号を取得
existing_po_numbers = [int(h['purchase_order_id'].split('-')[1]) for h in existing_headers]
next_po_number = max(existing_po_numbers) + 1

# 新しいデータを生成（既存の間接材をベースに2倍に）
new_headers = []
new_items = []

print(f'\n新規間接材データを生成中...')
print(f'目標: 既存{len(indirect_items)}件の2倍 = 約{len(indirect_items)*2}件')

# 既存の間接材データを複製して新しいデータを作成
for header in indirect_headers:
    original_po_id = header['purchase_order_id']
    
    # このPOの明細を取得
    po_items = [item for item in indirect_items if item['purchase_order_id'] == original_po_id]
    
    # 新しいPO番号
    new_po_id = f"PO-{header['purchase_order_id'].split('-')[1]}-{next_po_number:06d}"
    next_po_number += 1
    
    # ヘッダーを複製（日付を少し変更）
    new_header = header.copy()
    new_header['purchase_order_id'] = new_po_id
    
    # 発注日を2-10日後に変更
    original_date = datetime.strptime(header['order_date'], '%Y-%m-%d')
    new_date = original_date + timedelta(days=random.randint(2, 10))
    new_header['order_date'] = new_date.strftime('%Y-%m-%d')
    
    # 期待納期も調整
    if header['expected_delivery_date']:
        original_exp_date = datetime.strptime(header['expected_delivery_date'], '%Y-%m-%d')
        new_exp_date = original_exp_date + timedelta(days=random.randint(2, 10))
        new_header['expected_delivery_date'] = new_exp_date.strftime('%Y-%m-%d')
    
    # 実納期も調整
    if header['actual_delivery_date']:
        original_act_date = datetime.strptime(header['actual_delivery_date'], '%Y-%m-%d')
        new_act_date = original_act_date + timedelta(days=random.randint(2, 10))
        new_header['actual_delivery_date'] = new_act_date.strftime('%Y-%m-%d')
    
    new_headers.append(new_header)
    
    # 明細も複製（数量と価格を少し変動）
    for item in po_items:
        new_item = item.copy()
        new_item['purchase_order_id'] = new_po_id
        
        # 数量を80-120%の範囲で変動
        original_qty = int(item['quantity'])
        new_qty = max(1, int(original_qty * random.uniform(0.8, 1.2)))
        new_item['quantity'] = str(new_qty)
        
        # 単価を95-105%の範囲で変動
        original_unit_price = float(item['unit_price_ex_tax'])
        new_unit_price = int(original_unit_price * random.uniform(0.95, 1.05))
        new_item['unit_price_ex_tax'] = str(new_unit_price)
        
        # 金額を再計算
        tax_rate = float(item['line_tax_rate'])
        line_subtotal_ex_tax = new_qty * new_unit_price
        line_tax_amount = int(line_subtotal_ex_tax * tax_rate)
        line_subtotal_incl_tax = line_subtotal_ex_tax + line_tax_amount
        
        # 送料をランダムに調整
        shipping_fee = random.randint(1500, 5000)
        
        # 割引を少し調整
        discount = random.randint(0, int(line_subtotal_incl_tax * 0.03))
        
        # 最終金額
        line_total_incl_tax = line_subtotal_incl_tax + shipping_fee - discount
        
        new_item['line_subtotal_ex_tax'] = str(line_subtotal_ex_tax)
        new_item['line_tax_amount'] = str(line_tax_amount)
        new_item['line_subtotal_incl_tax'] = str(line_subtotal_incl_tax)
        new_item['line_shipping_fee_incl_tax'] = str(shipping_fee)
        new_item['line_discount_incl_tax'] = str(discount)
        new_item['line_total_incl_tax'] = str(line_total_incl_tax)
        
        # 参考価格も調整
        new_item['reference_price_ex_tax'] = str(int(float(item['reference_price_ex_tax']) * random.uniform(0.95, 1.05)))
        
        # 配送日を調整
        if item['ship_date']:
            original_ship = datetime.strptime(item['ship_date'], '%Y-%m-%d')
            new_ship = original_ship + timedelta(days=random.randint(2, 10))
            new_item['ship_date'] = new_ship.strftime('%Y-%m-%d')
        
        if item['received_date']:
            original_rcv = datetime.strptime(item['received_date'], '%Y-%m-%d')
            new_rcv = original_rcv + timedelta(days=random.randint(2, 10))
            new_item['received_date'] = new_rcv.strftime('%Y-%m-%d')
        
        # 配送追跡番号を新規生成
        carrier_prefixes = {'佐川急便': 'SG', 'ヤマト運輸': 'YM', '日本通運': 'NT'}
        carrier = item['carrier_name']
        prefix = carrier_prefixes.get(carrier, 'XX')
        new_item['carrier_tracking_number'] = f"{prefix}{random.randint(1000000000, 9999999999)}"
        
        new_items.append(new_item)

print(f'新規生成: PO {len(new_headers)}件, 明細 {len(new_items)}件')

# 既存データと新規データを結合
all_items = existing_items + new_items
all_headers = existing_headers + new_headers

# ファイルに書き込み
print('\nファイルに書き込み中...')
with open(item_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_items)

with open(header_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=header_fieldnames)
    writer.writeheader()
    writer.writerows(all_headers)

# 統計情報
total_indirect = len([item for item in all_items if item['material_type'] == 'indirect'])
total_direct = len([item for item in all_items if item['material_type'] == 'direct'])
new_indirect_amount = sum(float(item['line_total_incl_tax']) for item in new_items)

print('\n' + '='*60)
print('間接材データを2倍に増やしました')
print('='*60)
print(f'新規PO: {len(new_headers)}件')
print(f'新規明細: {len(new_items)}件')
print(f'追加金額: ¥{new_indirect_amount:,.0f}')
print()
print('【更新後の統計】')
print(f'  総明細数: {len(all_items)}件')
print(f'  間接材: {total_indirect}件 (元: {len(indirect_items)}件 → 約{total_indirect/len(indirect_items):.1f}倍)')
print(f'  直接材: {total_direct}件')
print(f'  総PO数: {len(all_headers)}件')
print('='*60)
