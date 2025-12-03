import csv
from datetime import datetime, timedelta
import random

# ファイルパス（Bronzeフォルダを直接参照）
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

# 既存の最大PO番号を取得
existing_po_numbers = [int(h['purchase_order_id'].split('-')[1]) for h in existing_headers]
next_po_number = max(existing_po_numbers) + 1

# 間接材マスターデータ（工場用MRO品）
indirect_materials = [
    # 切削工具
    {'id': 'MRO-ENDMILL-SET', 'name': 'エンドミルセット 超硬 10本組', 'category': 'Cutting Tools', 'unit_price': 35000, 'qty_range': (1, 3), 'unspsc': '27112702'},
    {'id': 'MRO-DRILL-HSS', 'name': 'HSS鋼ドリル刃 1-13mm 25本', 'category': 'Cutting Tools', 'unit_price': 18000, 'qty_range': (1, 2), 'unspsc': '27112702'},
    {'id': 'MRO-TAP-SET', 'name': 'タップダイスセット M3-M12', 'category': 'Cutting Tools', 'unit_price': 28000, 'qty_range': (1, 2), 'unspsc': '27112702'},
    {'id': 'MRO-REAMER-SET', 'name': 'リーマーセット 5-20mm', 'category': 'Cutting Tools', 'unit_price': 42000, 'qty_range': (1, 1), 'unspsc': '27112702'},
    
    # 測定工具
    {'id': 'MRO-CALIPER-DIGITAL', 'name': 'デジタルノギス 300mm', 'category': 'Measuring Tools', 'unit_price': 15000, 'qty_range': (2, 5), 'unspsc': '41111500'},
    {'id': 'MRO-MICROMETER-SET', 'name': 'マイクロメーターセット 0-100mm', 'category': 'Measuring Tools', 'unit_price': 45000, 'qty_range': (1, 2), 'unspsc': '41111500'},
    {'id': 'MRO-HEIGHT-GAUGE', 'name': 'ハイトゲージ デジタル 600mm', 'category': 'Measuring Tools', 'unit_price': 68000, 'qty_range': (1, 1), 'unspsc': '41111500'},
    {'id': 'MRO-GAUGE-BLOCK', 'name': 'ブロックゲージセット 47個組', 'category': 'Measuring Tools', 'unit_price': 95000, 'qty_range': (1, 1), 'unspsc': '41111500'},
    
    # 溶接・研削用品
    {'id': 'MRO-WELDING-WIRE', 'name': 'MIG溶接ワイヤ 1.2mm 15kg', 'category': 'Welding Supplies', 'unit_price': 18000, 'qty_range': (5, 10), 'unspsc': '30131501'},
    {'id': 'MRO-GRINDING-DISC', 'name': '研削ディスク 180mm 50枚', 'category': 'Abrasive Tools', 'unit_price': 8500, 'qty_range': (5, 15), 'unspsc': '27112801'},
    {'id': 'MRO-CUT-OFF-WHEEL', 'name': '切断砥石 305mm 25枚', 'category': 'Abrasive Tools', 'unit_price': 12000, 'qty_range': (3, 10), 'unspsc': '27112801'},
    {'id': 'MRO-SANDING-BELT', 'name': 'サンディングベルト #80 50本', 'category': 'Abrasive Tools', 'unit_price': 15000, 'qty_range': (2, 5), 'unspsc': '27112801'},
    
    # 潤滑油・作動油
    {'id': 'MRO-HYDRAULIC-OIL-32', 'name': '作動油 VG32 18L缶', 'category': 'Hydraulic Fluids', 'unit_price': 8900, 'qty_range': (4, 12), 'unspsc': '15121503'},
    {'id': 'MRO-GEAR-OIL', 'name': 'ギアオイル 90W 20L', 'category': 'Lubricants', 'unit_price': 12000, 'qty_range': (3, 8), 'unspsc': '15121501'},
    {'id': 'MRO-SPINDLE-OIL', 'name': '主軸油 ISO VG2 4L', 'category': 'Lubricants', 'unit_price': 6800, 'qty_range': (5, 10), 'unspsc': '15121501'},
    {'id': 'MRO-GREASE-MP', 'name': '万能グリース 16kg缶', 'category': 'Lubricants', 'unit_price': 15000, 'qty_range': (2, 6), 'unspsc': '15121505'},
    
    # 清掃・洗浄剤
    {'id': 'MRO-PARTS-CLEANER', 'name': '部品洗浄剤 20L', 'category': 'Cleaning Supplies', 'unit_price': 9800, 'qty_range': (3, 10), 'unspsc': '47131801'},
    {'id': 'MRO-BRAKE-CLEANER', 'name': 'ブレーキクリーナー 840ml×12本', 'category': 'Cleaning Supplies', 'unit_price': 7200, 'qty_range': (5, 15), 'unspsc': '47131801'},
    {'id': 'MRO-DEGREASER', 'name': '脱脂洗浄剤 18L', 'category': 'Cleaning Supplies', 'unit_price': 11000, 'qty_range': (2, 8), 'unspsc': '47131801'},
    {'id': 'MRO-WIPING-CLOTH', 'name': '工業用ウエス 10kg', 'category': 'Cleaning Supplies', 'unit_price': 3500, 'qty_range': (10, 30), 'unspsc': '47131803'},
    
    # 保護具
    {'id': 'MRO-SAFETY-SHOES', 'name': '安全靴 26.5cm 10足', 'category': 'Safety Equipment', 'unit_price': 35000, 'qty_range': (3, 8), 'unspsc': '46181701'},
    {'id': 'MRO-HELMET', 'name': '保護ヘルメット 20個', 'category': 'Safety Equipment', 'unit_price': 28000, 'qty_range': (2, 5), 'unspsc': '46181601'},
    {'id': 'MRO-EARPLUGS', 'name': '耳栓 使い捨て 200組', 'category': 'Safety Equipment', 'unit_price': 4500, 'qty_range': (10, 30), 'unspsc': '46182001'},
    {'id': 'MRO-FACE-SHIELD', 'name': 'フェイスシールド 15枚', 'category': 'Safety Equipment', 'unit_price': 12000, 'qty_range': (3, 10), 'unspsc': '42132100'},
    {'id': 'MRO-WELDING-GLOVES', 'name': '溶接用手袋 牛革 20双', 'category': 'Safety Equipment', 'unit_price': 18000, 'qty_range': (2, 8), 'unspsc': '46181501'},
    
    # 締結部品・消耗品
    {'id': 'MRO-BOLT-SET-M8', 'name': '六角ボルト M8 ステンレス 500本', 'category': 'Fasteners', 'unit_price': 8500, 'qty_range': (5, 15), 'unspsc': '31161500'},
    {'id': 'MRO-BOLT-SET-M10', 'name': '六角ボルト M10 ステンレス 300本', 'category': 'Fasteners', 'unit_price': 9800, 'qty_range': (3, 12), 'unspsc': '31161500'},
    {'id': 'MRO-WASHER-ASSORT', 'name': 'ワッシャー各種アソート 1000個', 'category': 'Fasteners', 'unit_price': 6500, 'qty_range': (5, 20), 'unspsc': '31161700'},
    {'id': 'MRO-SPRING-PIN', 'name': 'スプリングピン 各種 500本', 'category': 'Fasteners', 'unit_price': 7800, 'qty_range': (3, 10), 'unspsc': '31161600'},
    
    # 工場消耗品
    {'id': 'MRO-PALLET-PLASTIC', 'name': 'プラスチックパレット 1100×1100', 'category': 'Material Handling', 'unit_price': 8500, 'qty_range': (10, 30), 'unspsc': '24101500'},
    {'id': 'MRO-STRETCH-FILM', 'name': 'ストレッチフィルム 500mm×300m 6本', 'category': 'Packaging', 'unit_price': 9600, 'qty_range': (5, 15), 'unspsc': '55121500'},
    {'id': 'MRO-CABLE-TIE', 'name': '結束バンド 各種 1000本', 'category': 'Factory Supplies', 'unit_price': 4200, 'qty_range': (10, 30), 'unspsc': '26121700'},
    {'id': 'MRO-MARKING-SPRAY', 'name': 'マーキングスプレー 12色×12本', 'category': 'Factory Supplies', 'unit_price': 8400, 'qty_range': (3, 10), 'unspsc': '31201500'},
    
    # エアツール用品
    {'id': 'MRO-AIR-HOSE', 'name': 'エアホース 8.5mm×20m', 'category': 'Pneumatic Tools', 'unit_price': 12000, 'qty_range': (2, 8), 'unspsc': '40141700'},
    {'id': 'MRO-AIR-COUPLER', 'name': 'エアカプラ 各種セット 20個', 'category': 'Pneumatic Tools', 'unit_price': 8500, 'qty_range': (3, 10), 'unspsc': '40141700'},
    {'id': 'MRO-AIR-FILTER', 'name': 'エアフィルターエレメント 10個', 'category': 'Pneumatic Tools', 'unit_price': 15000, 'qty_range': (2, 6), 'unspsc': '40141700'},
]

# サプライヤー情報
suppliers = [
    {'id': 'SUP-IND-001', 'name': 'インダストリアル商事'},
    {'id': 'SUP-TEC-001', 'name': 'テックサプライ株式会社'},
    {'id': 'SUP-TOOL-001', 'name': '工具商社A'},
    {'id': 'SUP-TOOL-002', 'name': '工具商社B'},
]

# 承認者・受取人
approvers = [
    {'name': '山本進', 'email': 's.yamamoto@example.com'},
    {'name': '小林誠', 'email': 'm.kobayashi@example.com'},
    {'name': '高橋美咲', 'email': 'm.takahashi@example.com'}
]

locations = ['STM', 'HTB', 'SZK', 'KMM', 'HMM']
cost_centers = ['CC-005', 'CC-006', 'CC-007']
departments = ['DEPT-MFG', 'DEPT-QC', 'DEPT-MAINT']
carriers = ['佐川急便', 'ヤマト運輸', '日本通運']
carrier_prefixes = {'佐川急便': 'SG', 'ヤマト運輸': 'YM', '日本通運': 'NT'}

# 間接材POを生成（60件 = 2倍）
new_headers = []
new_items = []

print(f'\n間接材POを生成中（目標: 260件程度）...')

# 2022年1月から2025年12月まで満遍なく分散
start_date = datetime(2022, 1, 1)
end_date = datetime(2025, 12, 31)
total_days = (end_date - start_date).days

# 260件のPOを生成
num_pos = 260

for i in range(num_pos):
    # 日付を均等に分散
    days_offset = int(total_days * i / num_pos)
    order_date = start_date + timedelta(days=days_offset)
    
    # PO番号
    po_id = f"PO-{order_date.year}-{next_po_number:06d}"
    next_po_number += 1
    
    # サプライヤーをランダム選択
    supplier = random.choice(suppliers)
    
    # 承認者をランダム選択
    approver = random.choice(approvers)
    
    # 拠点をランダム選択
    location = random.choice(locations)
    
    # 納期
    expected_delivery = order_date + timedelta(days=random.randint(7, 14))
    actual_delivery = expected_delivery + timedelta(days=random.randint(-2, 3))
    
    # ヘッダー作成（POヘッダーには明細情報も含める）
    po_subtotal = 0  # 後で計算
    po_shipping = 0
    po_tax = 0
    po_discount = 0
    
    header = {
        'purchase_order_id': po_id,
        'order_date': order_date.strftime('%Y-%m-%d'),
        'expected_delivery_date': expected_delivery.strftime('%Y-%m-%d'),
        'supplier_id': supplier['id'],
        'supplier_name': supplier['name'],
        'account_group': 'MRO',
        'location_id': location,
        'purchase_order_number': f'PO{order_date.strftime("%Y%m")}{next_po_number-1:04d}',
        'currency': 'JPY',
        'order_subtotal_ex_tax': '0',  # 後で更新
        'shipping_fee_ex_tax': '0',
        'tax_amount': '0',
        'discount_amount_incl_tax': '0',
        'order_total_incl_tax': '0',
        'order_status': 'completed',
        'approver': approver['name'],
        'payment_method': random.choice(['銀行振込', '手形', '現金']),
        'payment_confirmation_id': f'PAY-{order_date.year}-{random.randint(100000, 999999)}',
        'payment_date': (expected_delivery + timedelta(days=30)).strftime('%Y-%m-%d'),
        'payment_amount': '0'  # 後で更新
    }
    new_headers.append(header)
    
    # 各POに1-2品目を追加
    num_items = random.randint(1, 2)
    selected_materials = random.sample(indirect_materials, num_items)
    
    po_items = []  # このPOの明細を一時保存
    
    for line_num, material in enumerate(selected_materials, 1):
        quantity = random.randint(material['qty_range'][0], material['qty_range'][1])
        unit_price = int(material['unit_price'] * random.uniform(0.95, 1.05))
        
        # 金額計算
        line_subtotal_ex_tax = quantity * unit_price
        tax_rate = 0.10
        line_tax_amount = int(line_subtotal_ex_tax * tax_rate)
        line_subtotal_incl_tax = line_subtotal_ex_tax + line_tax_amount
        
        # 送料・割引
        shipping_fee = random.randint(2000, 5000)
        discount = random.randint(0, int(line_subtotal_incl_tax * 0.02))
        
        line_total_incl_tax = line_subtotal_incl_tax + shipping_fee - discount
        
        # 配送情報
        carrier = random.choice(carriers)
        tracking_number = f"{carrier_prefixes[carrier]}{random.randint(1000000000, 9999999999)}"
        
        # 出荷・受領日
        ship_date = order_date + timedelta(days=random.randint(1, 3))
        received_date = ship_date + timedelta(days=random.randint(3, 7))
        
        # 受取人
        receiver = random.choice(approvers)
        
        # コストセンター・部門
        cost_center = random.choice(cost_centers)
        department = random.choice(departments)
        
        # 明細作成
        item = {
            'purchase_order_id': po_id,
            'line_number': str(line_num),
            'material_id': material['id'],
            'material_name': material['name'],
            'material_category': material['category'],
            'material_type': 'indirect',  # 間接材
            'product_id': '',
            'unspsc_code': material['unspsc'],
            'quantity': str(quantity),
            'unit_price_ex_tax': str(unit_price),
            'line_subtotal_incl_tax': str(line_subtotal_incl_tax),
            'line_subtotal_ex_tax': str(line_subtotal_ex_tax),
            'line_tax_amount': str(line_tax_amount),
            'line_tax_rate': str(tax_rate),
            'line_shipping_fee_incl_tax': str(shipping_fee),
            'line_discount_incl_tax': str(discount),
            'line_total_incl_tax': str(line_total_incl_tax),
            'reference_price_ex_tax': str(int(unit_price * random.uniform(1.05, 1.15))),
            'purchase_rule': '該当無し',
            'ship_date': ship_date.strftime('%Y-%m-%d'),
            'shipping_status': 'delivered',
            'carrier_tracking_number': tracking_number,
            'shipped_quantity': str(quantity),
            'carrier_name': carrier,
            'delivery_address': f'{location}工場',
            'receiving_status': 'received',
            'received_quantity': str(quantity),
            'received_date': received_date.strftime('%Y-%m-%d'),
            'receiver_name': receiver['name'],
            'receiver_email': receiver['email'],
            'cost_center': cost_center,
            'project_code': f'PRJ-{random.choice(["2022", "2023", "2024", "2025"])}-{random.choice(["A", "B", "C", "D"])}',
            'department_code': department,
            'account_user': receiver['name'],
            'user_email': receiver['email']
        }
        po_items.append(item)
        new_items.append(item)
    
    # ヘッダーの金額を明細から集計
    po_subtotal_ex = sum(int(item['line_subtotal_ex_tax']) for item in po_items)
    po_shipping_total = sum(int(item['line_shipping_fee_incl_tax']) for item in po_items)
    po_tax_total = sum(int(item['line_tax_amount']) for item in po_items)
    po_discount_total = sum(int(item['line_discount_incl_tax']) for item in po_items)
    po_total = sum(int(item['line_total_incl_tax']) for item in po_items)
    
    # ヘッダーを更新
    header['order_subtotal_ex_tax'] = str(po_subtotal_ex)
    header['shipping_fee_ex_tax'] = str(int(po_shipping_total / 1.1))  # 税抜き
    header['tax_amount'] = str(po_tax_total)
    header['discount_amount_incl_tax'] = str(po_discount_total)
    header['order_total_incl_tax'] = str(po_total)
    header['payment_amount'] = str(po_total)

# データを結合
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
print('間接材データを追加しました')
print('='*60)
print(f'新規PO: {len(new_headers)}件')
print(f'新規明細: {len(new_items)}件')
print(f'追加金額: ¥{new_indirect_amount:,.0f}')
print()
print('【更新後の統計】')
print(f'  総明細数: {len(all_items)}件')
print(f'  間接材: {total_indirect}件')
print(f'  直接材: {total_direct}件')
print(f'  総PO数: {len(all_headers)}件')
print('='*60)
