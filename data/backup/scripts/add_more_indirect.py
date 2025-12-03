import csv
from datetime import datetime, timedelta
import random

# ファイルパス
item_file = r'H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\backup\調達伝票_item.csv'
header_file = r'H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\backup\調達伝票_header.csv'

# 既存データ読み込み
with open(item_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    existing_items = list(reader)
    fieldnames = reader.fieldnames

with open(header_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    existing_headers = list(reader)

# 既存の最大PO番号を取得
existing_po_numbers = [int(h['purchase_order_id'].split('-')[1]) for h in existing_headers]
next_po_number = max(existing_po_numbers) + 1

# 間接材マスターデータ（工場用）
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
    {'id': 'SUP-OFC-001', 'name': 'オフィスマート株式会社'},
    {'id': 'SUP-TOOL-001', 'name': '工具商社A'},
    {'id': 'SUP-TOOL-002', 'name': '工具商社B'},
]

# 承認者・受取人
approvers = [
    {'name': '山本進', 'email': 's.yamamoto@example.com'},
    {'name': '小林誠', 'email': 'm.kobayashi@example.com'},
    {'name': '高橋美咲', 'email': 'm.takahashi@example.com'}
]
locations = ['A1', 'A2', 'A3', 'A4', 'A5']
cost_centers = ['CC-005', 'CC-006', 'CC-007']
departments = ['DEPT-MFG', 'DEPT-QC', 'DEPT-MAINT']

# 新しい間接材POとitemを生成（30件程度）
new_headers = []
new_items = []

start_date = datetime(2025, 1, 15)
po_count = 30

for i in range(po_count):
    po_id = f'PO-2025-{next_po_number:03d}'
    next_po_number += 1
    
    # 発注日を1-2週間間隔でランダムに設定
    order_date = start_date + timedelta(days=random.randint(0, 14))
    start_date = order_date + timedelta(days=random.randint(7, 14))
    
    # ランダムに1-3種類の間接材を選択
    num_items = random.randint(1, 3)
    selected_materials = random.sample(indirect_materials, num_items)
    
    supplier = random.choice(suppliers)
    approver_info = random.choice(approvers)
    location = random.choice(locations)
    
    # 合計金額計算
    total_ex_tax = 0
    for material in selected_materials:
        qty = random.randint(material['qty_range'][0], material['qty_range'][1])
        total_ex_tax += material['unit_price'] * qty
    
    tax = int(total_ex_tax * 0.1)
    shipping = random.choice([0, 500, 750, 1000, 1500]) if total_ex_tax < 50000 else random.randint(2000, 5000)
    discount = -int(total_ex_tax * random.choice([0, 0, 0, 0.05])) if total_ex_tax > 50000 else 0
    total_incl_tax = total_ex_tax + tax + shipping + discount
    
    expected_delivery = order_date + timedelta(days=random.randint(3, 10))
    payment_date = expected_delivery + timedelta(days=random.randint(5, 15))
    
    # header作成
    header = {
        'purchase_order_id': po_id,
        'order_date': order_date.strftime('%Y-%m-%d'),
        'expected_delivery_date': expected_delivery.strftime('%Y-%m-%d'),
        'supplier_id': supplier['id'],
        'supplier_name': supplier['name'],
        'account_group': 'Honda Motor',
        'location_id': location,
        'purchase_order_number': f'{next_po_number-1:03d}',
        'currency': 'JPY',
        'order_subtotal_ex_tax': str(total_ex_tax),
        'shipping_fee_ex_tax': str(shipping),
        'tax_amount': str(tax),
        'discount_amount_incl_tax': str(discount),
        'order_total_incl_tax': str(total_incl_tax),
        'order_status': '終了',
        'approver': approver_info['name'],
        'payment_method': 'bank_transfer' if total_ex_tax > 30000 else random.choice(['credit_card', 'invoice']),
        'payment_confirmation_id': f'INV-2025-{order_date.strftime("%m%d")}' if total_ex_tax > 30000 else f'{random.randint(1000000000, 9999999999):010d}',
        'payment_date': payment_date.strftime('%Y-%m-%d'),
        'payment_amount': str(total_incl_tax)
    }
    new_headers.append(header)
    
    # item作成
    for line_num, material in enumerate(selected_materials, 1):
        qty = random.randint(material['qty_range'][0], material['qty_range'][1])
        unit_price = material['unit_price']
        subtotal_ex_tax = unit_price * qty
        line_tax = int(subtotal_ex_tax * 0.1)
        
        # 最後の明細に送料と割引を含める
        if line_num == len(selected_materials):
            line_shipping = shipping
            line_discount = discount
            line_total = subtotal_ex_tax + line_tax + line_shipping + line_discount
        else:
            line_shipping = 0
            line_discount = 0
            line_total = subtotal_ex_tax + line_tax
        
        ship_date = order_date
        received_date = expected_delivery
        
        item = {
            'purchase_order_id': po_id,
            'line_number': str(line_num),
            'material_id': material['id'],
            'material_name': material['name'],
            'material_category': material['category'],
            'material_type': 'indirect',
            'product_id': '',
            'unspsc_code': material['unspsc'],
            'quantity': str(qty),
            'unit_price_ex_tax': str(unit_price),
            'line_subtotal_incl_tax': str(line_total),
            'line_subtotal_ex_tax': str(subtotal_ex_tax),
            'line_tax_amount': str(line_tax),
            'line_tax_rate': '0.1',
            'line_shipping_fee_incl_tax': str(line_shipping),
            'line_discount_incl_tax': str(line_discount),
            'line_total_incl_tax': str(line_total),
            'reference_price_ex_tax': str(unit_price + random.randint(100, 2000)),
            'purchase_rule': '該当無し',
            'ship_date': ship_date.strftime('%Y-%m-%d'),
            'shipping_status': 'delivered',
            'carrier_tracking_number': f'YM{random.randint(1000000000, 9999999999):010d}',
            'shipped_quantity': str(qty),
            'carrier_name': random.choice(['ヤマト運輸', '佐川急便', 'ゆうパック']),
            'delivery_address': '川崎市高津区久本3-6-1, 213-0011, 神奈川県',
            'receiving_status': 'received',
            'received_quantity': str(qty),
            'received_date': received_date.strftime('%Y-%m-%d'),
            'receiver_name': approver_info['name'],
            'receiver_email': approver_info['email'],
            'cost_center': random.choice(cost_centers),
            'project_code': f'PRJ-2025-{chr(65+random.randint(0,25))}',
            'department_code': random.choice(departments),
            'account_user': approver_info['name'],
            'user_email': approver_info['email']
        }
        new_items.append(item)

# 既存データと結合
all_items = existing_items + new_items
all_headers = existing_headers + new_headers

# 保存
with open(item_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_items)

with open(header_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=all_headers[0].keys())
    writer.writeheader()
    writer.writerows(all_headers)

# 統計情報
indirect_count = len([item for item in all_items if item['material_type'] == 'indirect'])
direct_count = len([item for item in all_items if item['material_type'] == 'direct'])
total_indirect_amount = sum(int(item['line_total_incl_tax']) for item in new_items)

print(f'間接材データを追加しました')
print(f'  新規PO: {len(new_headers)}件')
print(f'  新規明細: {len(new_items)}件')
print(f'  追加金額: ¥{total_indirect_amount:,}')
print()
print(f'【更新後の統計】')
print(f'  総明細数: {len(all_items)}件')
print(f'  間接材: {indirect_count}件')
print(f'  直接材: {direct_count}件')
print(f'  総PO数: {len(all_headers)}件')
