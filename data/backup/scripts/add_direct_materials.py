# -*- coding: utf-8 -*-
import csv
import os
from datetime import datetime, timedelta
import random

# ファイルパス
base_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze"
p2p_path = os.path.join(base_path, "P2P")
erp_path = os.path.join(base_path, "ERP")

# 既存の間接材データを読み込み
indirect_items = []
with open(os.path.join(p2p_path, "調達伝票_item.csv"), 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    indirect_items = list(reader)

print(f"既存間接材レコード数: {len(indirect_items)}")

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

# 価格レンジ（部品種類別）
def get_price_range(component_id):
    if component_id.startswith('ENG-'):
        return (300000, 700000)  # エンジン
    elif component_id.startswith('TRN-'):
        return (80000, 200000)  # トランスミッション
    elif component_id.startswith('ECU-'):
        return (30000, 80000)  # ECU
    elif component_id.startswith('MAT-'):
        return (500, 2000)  # 材料
    elif component_id.startswith('BAT-'):
        return (30000, 80000)  # バッテリー
    elif component_id.startswith('MTR-'):
        return (100000, 250000)  # モーター
    elif component_id.startswith('INV-'):
        return (50000, 120000)  # インバーター
    elif component_id.startswith('CMP-'):
        return (50, 200)  # チップ
    elif component_id.startswith('EXH-'):
        return (10000, 30000)  # エキマニ
    elif component_id.startswith('MFL-'):
        return (15000, 50000)  # マフラー
    elif component_id.startswith('ARM-'):
        return (8000, 25000)  # アーム
    elif component_id.startswith('INT-SEAT-'):
        return (20000, 80000)  # シート
    elif component_id.startswith('INT-DASHBOARD-'):
        return (30000, 100000)  # ダッシュボード
    elif component_id.startswith('INT-STEERING-'):
        return (15000, 50000)  # ステアリング
    elif component_id.startswith('INT-CONSOLE-'):
        return (10000, 40000)  # コンソール
    elif component_id.startswith('INT-CARPET-'):
        return (5000, 20000)  # カーペット
    elif component_id.startswith('SUS-'):
        return (8000, 30000)  # サスペンション
    else:
        return (1000, 10000)  # デフォルト

# 直接材の購買データを生成
direct_items = []
po_counter = 1

for order_item in order_items:
    order_id = order_item['order_id']
    product_id = order_item['product_id']
    quantity = int(order_item['quantity'])
    
    if order_id not in order_headers:
        continue
    
    order_header = order_headers[order_id]
    order_timestamp_str = order_header['order_timestamp']
    order_date = datetime.strptime(order_timestamp_str.split()[0], '%Y-%m-%d')
    site_id = order_header['location_id']
    
    # このproduct_idとsite_idに対応するBOMを検索
    matching_boms = [bom for bom in bom_master 
                     if bom['product_id'] == product_id and bom['site_id'] == site_id]
    
    if not matching_boms:
        continue
    
    # 各BOM部品に対して購買レコードを生成
    for bom in matching_boms:
        component_id = bom['component_product_id']
        component_qty_per = float(bom['component_quantity_per'])
        procurement_qty = int(quantity * component_qty_per)
        
        # 購買発注
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
        line_shipping_fee = random.randint(300, 800)
        line_discount = random.randint(0, int(line_subtotal_incl_tax * 0.05))
        line_total_incl_tax = line_subtotal_incl_tax + line_shipping_fee - line_discount
        
        # 納期
        expected_delivery_date = order_date + timedelta(days=7)
        received_date = expected_delivery_date + timedelta(days=random.randint(0, 2))
        ship_date = expected_delivery_date - timedelta(days=2)
        
        # 配送先
        delivery_addresses = {
            'STM': "埼玉県狭山市新狭山1-10-1, 350-1305, 埼玉県",
            'HTB': "北海道苫小牧市字勇払145-1, 059-1364, 北海道",
            'SZK': "静岡県浜松市中区富塚町5200, 432-8002, 静岡県"
        }
        delivery_address = delivery_addresses.get(site_id, "埼玉県狭山市新狭山1-10-1, 350-1305, 埼玉県")
        
        # 担当者
        receivers = [
            ("田中太郎", "t.tanaka@example.com"),
            ("佐藤花子", "h.sato@example.com"),
            ("鈴木一郎", "i.suzuki@example.com"),
            ("高橋美咲", "m.takahashi@example.com")
        ]
        receiver = random.choice(receivers)
        
        # トラッキング番号
        carrier = random.choice(carriers)
        if carrier == "ヤマト運輸":
            tracking = f"YM{random.randint(1000000000, 9999999999)}"
        elif carrier == "佐川急便":
            tracking = f"SG{random.randint(1000000000, 9999999999)}"
        else:
            tracking = f"NT{random.randint(1000000000, 9999999999)}"
        
        # コストセンター
        cost_centers = ["CC-001", "CC-002", "CC-003", "CC-004", "CC-005"]
        projects = ["PRJ-2022-A", "PRJ-2023-B", "PRJ-2024-C", "PRJ-2025-D"]
        
        direct_item = {
            'purchase_order_id': purchase_order_id,
            'line_number': '1',
            'material_id': '',  # 直接材は空欄
            'material_name': '',
            'material_category': '',
            'material_type': 'direct',
            'product_id': component_id,
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
            'reference_price_ex_tax': str(unit_price_ex_tax + random.randint(-1000, 1000)),
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
        
        direct_items.append(direct_item)

print(f"\n生成された直接材購買レコード数: {len(direct_items)}")

# 間接材と直接材を結合
all_items = indirect_items + direct_items

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
    writer.writerows(all_items)

print(f"\n=== 最終結果 ===")
print(f"間接材レコード数: {len(indirect_items)}")
print(f"直接材レコード数: {len(direct_items)}")
print(f"合計レコード数: {len(all_items)}")
print(f"\n出力ファイル: {output_file}")
