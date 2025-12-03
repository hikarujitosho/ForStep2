import pandas as pd
import random
from datetime import datetime, timedelta
from collections import defaultdict

# ファイルパス
order_header_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_header.csv"
order_item_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_item.csv"
bom_master_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P\BOMマスタ.csv"
output_header_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P\調達伝票_header.csv"
output_item_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P\調達伝票_item.csv"

print("Loading data...")

# データ読み込み
order_headers = pd.read_csv(order_header_path, encoding='utf-8-sig')
order_items = pd.read_csv(order_item_path, encoding='utf-8-sig')
bom_master = pd.read_csv(bom_master_path, encoding='utf-8-sig')

# 受注データと明細を結合
orders = pd.merge(order_headers, order_items, on='order_id')
orders['order_date'] = pd.to_datetime(orders['order_timestamp']).dt.date

print(f"Total orders: {len(orders)}")
print(f"BOM records: {len(bom_master)}")

# サプライヤーマスタ（ダミー）
suppliers = [
    {'supplier_id': 'SUP-001', 'supplier_name': '日本製鉄株式会社'},
    {'supplier_id': 'SUP-002', 'supplier_name': '住友電気工業株式会社'},
    {'supplier_id': 'SUP-003', 'supplier_name': 'デンソー株式会社'},
    {'supplier_id': 'SUP-004', 'supplier_name': 'アイシン株式会社'},
    {'supplier_id': 'SUP-005', 'supplier_name': '矢崎総業株式会社'},
]

# 部品カテゴリごとの単価レンジ（円）
price_ranges = {
    'MAT-': (500, 2000),      # 材料
    'CMP-': (100, 500),        # チップ・小型部品
    'ENG-': (300000, 700000),  # エンジン
    'TRN-': (80000, 200000),   # トランスミッション
    'ECU-': (30000, 80000),    # ECU
    'EXH-': (10000, 30000),    # 排気系
    'MFL-': (15000, 40000),    # マフラー
    'ARM-': (5000, 15000),     # アーム
    'SUS-': (20000, 60000),    # サスペンション
    'BRK-': (8000, 25000),     # ブレーキ
    'WHE-': (15000, 40000),    # ホイール
    'TIR-': (10000, 30000),    # タイヤ
    'STR-': (40000, 100000),   # ステアリング
    'FUE-': (8000, 20000),     # 燃料系
    'BAT-': (30000, 80000),    # バッテリー
    'default': (1000, 10000),
}

def get_unit_price(component_id):
    """部品IDから単価を生成"""
    for prefix, (min_price, max_price) in price_ranges.items():
        if component_id.startswith(prefix):
            return random.randint(min_price, max_price)
    return random.randint(*price_ranges['default'])

# 調達伝票生成
purchase_orders_header = []
purchase_orders_item = []

po_counter = 1
line_counter = 0

print("\nGenerating purchase orders...")

for idx, order_row in orders.iterrows():
    order_id = order_row['order_id']
    product_id = order_row['product_id']
    location_id = order_row['location_id']
    quantity = order_row['quantity']
    order_date = order_row['order_date']
    
    # このproduct_idとlocation_idに対応するBOMを取得
    bom_records = bom_master[
        (bom_master['product_id'] == product_id) & 
        (bom_master['site_id'] == location_id)
    ]
    
    if len(bom_records) == 0:
        # BOMが見つからない場合はスキップ
        continue
    
    # 各BOM明細に対して調達伝票を生成
    for _, bom_row in bom_records.iterrows():
        component_id = bom_row['component_product_id']
        component_qty_per = bom_row['component_quantity_per']
        
        # 調達数量 = 受注数量 × BOM必要数
        purchase_qty = quantity * component_qty_per
        
        # サプライヤーをランダムに選択
        supplier = random.choice(suppliers)
        
        # 発注日 = 受注日
        po_date = order_date
        
        # 納品予定日 = 発注日 + 7日
        expected_delivery = po_date + timedelta(days=7)
        
        # 実際の納品日 = 予定日 ± 0-2日
        actual_delivery_delta = random.randint(-2, 2)
        received_date = expected_delivery + timedelta(days=actual_delivery_delta)
        
        # 単価生成
        unit_price = get_unit_price(component_id)
        
        # 金額計算
        line_subtotal_ex_tax = purchase_qty * unit_price
        tax_rate = 0.1
        line_tax_amount = int(line_subtotal_ex_tax * tax_rate)
        line_subtotal_incl_tax = line_subtotal_ex_tax + line_tax_amount
        
        # 送料・値引き
        line_shipping_fee = random.choice([0, 300, 500, 800])
        line_discount = 0  # 通常は値引きなし
        
        line_total_incl_tax = line_subtotal_incl_tax + line_shipping_fee - line_discount
        
        # purchase_order_id生成
        purchase_order_id = f"PO-{po_date.year}-{po_counter:06d}"
        po_counter += 1
        
        # Header作成
        header_record = {
            'purchase_order_id': purchase_order_id,
            'order_date': po_date.strftime('%Y-%m-%d'),
            'expected_delivery_date': expected_delivery.strftime('%Y-%m-%d'),
            'supplier_id': supplier['supplier_id'],
            'supplier_name': supplier['supplier_name'],
            'account_group': 'DIRECT_MATERIAL',
            'location_id': location_id,
            'purchase_order_number': f"PO-NUM-{po_counter:08d}",
            'currency': 'JPY',
            'order_subtotal_ex_tax': line_subtotal_ex_tax,
            'shipping_fee_ex_tax': line_shipping_fee,
            'tax_amount': line_tax_amount,
            'discount_amount_incl_tax': line_discount,
            'order_total_incl_tax': line_total_incl_tax,
            'order_status': 'received',
            'approver': random.choice(['山田太郎', '佐藤花子', '田中一郎', '鈴木次郎']),
            'payment_method': 'bank_transfer',
            'payment_confirmation_id': f"PAY-{po_counter:08d}",
            'payment_date': (received_date + timedelta(days=30)).strftime('%Y-%m-%d'),
            'payment_amount': line_total_incl_tax,
        }
        purchase_orders_header.append(header_record)
        
        # Item作成
        line_counter += 1
        item_record = {
            'purchase_order_id': purchase_order_id,
            'line_number': 1,  # 1明細1発注
            'material_id': '',  # 直接材なので空欄
            'material_name': '',
            'material_category': '',
            'material_type': 'direct',
            'product_id': component_id,
            'unspsc_code': '',
            'quantity': purchase_qty,
            'unit_price_ex_tax': unit_price,
            'line_subtotal_incl_tax': line_subtotal_incl_tax,
            'line_subtotal_ex_tax': line_subtotal_ex_tax,
            'line_tax_amount': line_tax_amount,
            'line_tax_rate': tax_rate,
            'line_shipping_fee_incl_tax': line_shipping_fee,
            'line_discount_incl_tax': line_discount,
            'line_total_incl_tax': line_total_incl_tax,
            'reference_price_ex_tax': unit_price,
            'purchase_rule': '該当無し',
            'ship_date': (received_date - timedelta(days=1)).strftime('%Y-%m-%d'),
            'shipping_status': 'delivered',
            'carrier_tracking_number': f"TRK-{random.randint(1000000000, 9999999999)}",
            'shipped_quantity': purchase_qty,
            'carrier_name': random.choice(['ヤマト運輸', '佐川急便', '日本通運']),
            'delivery_address': f'{location_id}工場, 日本',
            'receiving_status': 'received',
            'received_quantity': purchase_qty,
            'received_date': received_date.strftime('%Y-%m-%d'),
            'receiver_name': random.choice(['山本進', '小林誠', '高橋美咲']),
            'receiver_email': f"receiver{random.randint(1, 100)}@example.com",
            'cost_center': f'CC-{random.randint(1, 10):03d}',
            'project_code': f'PRJ-{po_date.year}-{random.choice(["A", "B", "C"])}',
            'department_code': 'DEPT-MFG',
            'account_user': random.choice(['山田太郎', '佐藤花子', '田中一郎']),
            'user_email': f"user{random.randint(1, 50)}@example.com",
        }
        purchase_orders_item.append(item_record)
    
    if (idx + 1) % 100 == 0:
        print(f"Processed {idx + 1}/{len(orders)} orders...")

print(f"\nGenerated {len(purchase_orders_header)} purchase order headers")
print(f"Generated {len(purchase_orders_item)} purchase order items")

# CSV出力
print("\nWriting to CSV...")
df_header = pd.DataFrame(purchase_orders_header)
df_item = pd.DataFrame(purchase_orders_item)

df_header.to_csv(output_header_path, index=False, encoding='utf-8-sig')
df_item.to_csv(output_item_path, index=False, encoding='utf-8-sig')

print(f"Header CSV: {output_header_path}")
print(f"Item CSV: {output_item_path}")
print("\nGeneration complete!")

# サマリー
print("\n=== Summary ===")
print(f"Date range: {df_header['order_date'].min()} to {df_header['order_date'].max()}")
print(f"Total purchase orders: {len(df_header)}")
print(f"Total items: {len(df_item)}")
print(f"Total amount (incl. tax): ¥{df_header['order_total_incl_tax'].sum():,.0f}")
print(f"Unique suppliers: {df_header['supplier_id'].nunique()}")
print(f"Unique locations: {df_header['location_id'].nunique()}")
