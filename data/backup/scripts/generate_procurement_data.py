# -*- coding: utf-8 -*-
import csv
import os
from datetime import datetime, timedelta
import random

# ファイルパス
base_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze"
p2p_path = os.path.join(base_path, "P2P")
erp_path = os.path.join(base_path, "ERP")

# 受注データを読み込み
order_headers = {}
with open(os.path.join(erp_path, "受注伝票_header.csv"), 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        order_headers[row['order_id']] = row

order_items = []
with open(os.path.join(erp_path, "受注伝票_item.csv"), 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    order_items = list(reader)

# BOMマスタを読み込み
bom_master = []
with open(os.path.join(p2p_path, "BOMマスタ.csv"), 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    bom_master = list(reader)

print(f"受注ヘッダー数: {len(order_headers)}")
print(f"受注明細数: {len(order_items)}")
print(f"BOMマスタ数: {len(bom_master)}")

# 拠点マッピング（受注location_id → BOM site_id）
location_mapping = {
    'KMM': 'STM',
    'HTB': 'HTB',
    'SZK': 'SZK',
    'HMM': 'SZK'
}

# サプライヤー情報
suppliers = [
    {"id": "SUP-001", "name": "日本製鉄"},
    {"id": "SUP-002", "name": "住友電気工業"},
    {"id": "SUP-003", "name": "デンソー"},
    {"id": "SUP-004", "name": "アイシン"},
    {"id": "SUP-005", "name": "矢崎総業"}
]

# 配送業者
carriers = ["ヤマト運輸", "佐川急便", "日本通運"]

# 担当者
receivers = [
    ("田中太郎", "t.tanaka@example.com"),
    ("佐藤花子", "h.sato@example.com"),
    ("鈴木一郎", "i.suzuki@example.com"),
    ("高橋美咲", "m.takahashi@example.com"),
    ("山本進", "s.yamamoto@example.com"),
    ("小林誠", "m.kobayashi@example.com")
]

# コストセンター/プロジェクト
cost_centers = ["CC-001", "CC-002", "CC-003", "CC-004", "CC-005", "CC-006"]
projects = ["PRJ-2022-A", "PRJ-2023-B", "PRJ-2024-C", "PRJ-2025-D"]

# 配送先住所（拠点別）
delivery_addresses = {
    'STM': "富田2354, 大里郡寄居町, 369-1293, 埼玉県",
    'HTB': "川尻町1, 四日市市, 510-8114, 三重県",
    'SZK': "平田町1907, 鈴鹿市, 513-8611, 三重県"
}

# 価格レンジ（部品種類別、最低20,000円）
def get_price_range(component_id):
    if component_id.startswith('ENG-'):
        return (300000, 700000)  # エンジン
    elif component_id.startswith('TRN-'):
        return (80000, 200000)  # トランスミッション
    elif component_id.startswith('ECU-'):
        return (30000, 80000)  # ECU
    elif component_id.startswith('BAT-'):
        return (30000, 80000)  # バッテリー
    elif component_id.startswith('MTR-'):
        return (100000, 250000)  # モーター
    elif component_id.startswith('INV-'):
        return (50000, 120000)  # インバーター
    elif component_id.startswith('MAT-'):
        return (500, 2000)  # 材料（鋼板、アルミ等）
    elif component_id.startswith('CMP-'):
        return (50, 200)  # 電子部品チップ
    elif component_id.startswith('EXH-') or component_id.startswith('MFL-'):
        return (20000, 50000)  # エキマニ/マフラー
    elif component_id.startswith('ARM-'):
        return (20000, 40000)  # アーム類
    elif component_id.startswith('INT-'):
        return (20000, 100000)  # 内装部品
    elif component_id.startswith('SUS-'):
        return (20000, 50000)  # サスペンション
    else:
        return (20000, 50000)  # デフォルト

# 部品カテゴリの判定
def get_material_category(component_id):
    if component_id.startswith('ENG-'):
        return 'Engine Parts'
    elif component_id.startswith('TRN-'):
        return 'Transmission Parts'
    elif component_id.startswith('ECU-'):
        return 'Electronic Control Units'
    elif component_id.startswith('BAT-'):
        return 'Battery Components'
    elif component_id.startswith('MTR-'):
        return 'Motor Components'
    elif component_id.startswith('INV-'):
        return 'Inverter Components'
    elif component_id.startswith('MAT-'):
        return 'Raw Materials'
    elif component_id.startswith('CMP-'):
        return 'Electronic Components'
    elif component_id.startswith('EXH-'):
        return 'Exhaust Parts'
    elif component_id.startswith('MFL-'):
        return 'Muffler Parts'
    elif component_id.startswith('ARM-'):
        return 'Suspension Arms'
    elif component_id.startswith('INT-'):
        return 'Interior Parts'
    elif component_id.startswith('SUS-'):
        return 'Suspension Systems'
    else:
        return 'Other Parts'

# 調達伝票を生成
procurement_items = []
po_counter = 1
processed_orders = 0
skipped_orders = 0

for order_item in order_items:
    order_id = order_item['order_id']
    product_id = order_item['product_id']
    quantity = int(order_item['quantity'])
    
    if order_id not in order_headers:
        skipped_orders += 1
        continue
    
    order_header = order_headers[order_id]
    order_timestamp_str = order_header['order_timestamp']
    order_date = datetime.strptime(order_timestamp_str.split()[0], '%Y-%m-%d')
    location_id = order_header['location_id']
    
    # 拠点マッピング
    site_id = location_mapping.get(location_id, 'STM')
    
    # このproduct_idとsite_idに対応するBOMを検索
    matching_boms = [bom for bom in bom_master 
                     if bom['product_id'] == product_id and bom['site_id'] == site_id]
    
    if not matching_boms:
        skipped_orders += 1
        continue
    
    processed_orders += 1
    
    # 各BOM部品に対して購買レコードを生成
    for bom in matching_boms:
        component_id = bom['component_product_id']
        component_name = bom['bom_name']
        component_qty_per = float(bom['component_quantity_per'])
        procurement_qty = int(quantity * component_qty_per)
        
        # 購買発注ID
        purchase_order_id = f"PO-{order_date.year}-{po_counter:06d}"
        po_counter += 1
        
        # サプライヤー選定
        supplier = random.choice(suppliers)
        
        # 価格設定
        price_range = get_price_range(component_id)
        unit_price_ex_tax = random.randint(price_range[0], price_range[1])
        
        # 金額計算
        line_subtotal_ex_tax = unit_price_ex_tax * procurement_qty
        line_tax_amount = int(line_subtotal_ex_tax * 0.1)
        line_subtotal_incl_tax = line_subtotal_ex_tax + line_tax_amount
        
        # 配送料・割引
        line_shipping_fee = random.randint(500, 1500)
        line_discount = random.randint(0, int(line_subtotal_incl_tax * 0.03))
        line_total_incl_tax = line_subtotal_incl_tax + line_shipping_fee - line_discount
        
        # 納期（7日後に納品予定）
        expected_delivery_date = order_date + timedelta(days=7)
        # 実際の納品日（±5日のずれ）
        received_date = expected_delivery_date + timedelta(days=random.randint(-5, 5))
        # 出荷日（納品予定日の2日前）
        ship_date = expected_delivery_date - timedelta(days=2)
        
        # 配送先
        delivery_address = delivery_addresses.get(site_id, delivery_addresses['STM'])
        
        # 担当者
        receiver = random.choice(receivers)
        
        # トラッキング番号
        carrier = random.choice(carriers)
        if carrier == "ヤマト運輸":
            tracking = f"YM{random.randint(1000000000, 9999999999)}"
        elif carrier == "佐川急便":
            tracking = f"SG{random.randint(1000000000, 9999999999)}"
        else:
            tracking = f"NT{random.randint(1000000000, 9999999999)}"
        
        # 部品カテゴリ
        material_category = get_material_category(component_id)
        
        procurement_item = {
            'purchase_order_id': purchase_order_id,
            'line_number': '1',
            'material_id': component_id,  # BOMのcomponent_product_idがここに入る
            'material_name': component_name,  # BOMのbom_nameがここに入る
            'material_category': material_category,
            'material_type': 'direct',
            'product_id': product_id,  # 完成車のID（リレーション用）
            'unspsc_code': '',
            'quantity': str(procurement_qty),
            'unit_price_ex_tax': str(unit_price_ex_tax),
            'line_subtotal_incl_tax': str(line_subtotal_incl_tax),
            'line_subtotal_ex_tax': str(line_subtotal_ex_tax),
            'line_tax_amount': str(line_tax_amount),
            'line_tax_rate': '0.1',
            'line_shipping_fee_incl_tax': str(line_shipping_fee),
            'line_discount_incl_tax': str(line_discount),
            'line_total_incl_tax': str(line_total_incl_tax),
            'reference_price_ex_tax': str(unit_price_ex_tax + random.randint(-5000, 5000)),
            'purchase_rule': '該当無し',
            'ship_date': ship_date.strftime('%Y-%m-%d'),
            'shipping_status': 'delivered',
            'carrier_tracking_number': tracking,
            'shipped_quantity': str(procurement_qty),
            'carrier_name': carrier,
            'delivery_address': delivery_address,
            'receiving_status': 'received',
            'received_quantity': str(procurement_qty),
            'received_date': received_date.strftime('%Y-%m-%d'),
            'receiver_name': receiver[0],
            'receiver_email': receiver[1],
            'cost_center': random.choice(cost_centers),
            'project_code': random.choice(projects),
            'department_code': 'DEPT-MFG',
            'account_user': receiver[0],
            'user_email': receiver[1]
        }
        
        procurement_items.append(procurement_item)

print(f"\n処理済み受注: {processed_orders}")
print(f"スキップ受注: {skipped_orders}")
print(f"生成された調達レコード数: {len(procurement_items)}")

# CSVに出力
output_file = os.path.join(p2p_path, "調達伝票_item.csv")
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
    writer.writerows(procurement_items)

print(f"\n出力ファイル: {output_file}")
print(f"総レコード数: {len(procurement_items)}")

# サマリー統計
if procurement_items:
    total_amount = sum(int(item['line_total_incl_tax']) for item in procurement_items)
    print(f"\n=== サマリー ===")
    print(f"総調達金額: ¥{total_amount:,}")
    print(f"期間: {procurement_items[0]['ship_date']} ～ {procurement_items[-1]['ship_date']}")
