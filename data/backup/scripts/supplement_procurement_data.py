"""
BOMデータを確認し、調達データが不足している車種の部品調達データを生成・補充するスクリプト
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ファイルパス
BOM_PATH = r'data\Bronze\P2P\BOMマスタ.csv'
PROCUREMENT_HEADER_PATH = r'data\Bronze\P2P\調達伝票_header.csv'
PROCUREMENT_ITEM_PATH = r'data\Bronze\P2P\調達伝票_item.csv'
ORDER_HEADER_PATH = r'data\Bronze\ERP\受注伝票_header.csv'
ORDER_ITEM_PATH = r'data\Bronze\ERP\受注伝票_item.csv'

# データ読み込み
print("=" * 100)
print("BOMデータと調達データの確認")
print("=" * 100)

df_bom = pd.read_csv(BOM_PATH)
df_proc_header = pd.read_csv(PROCUREMENT_HEADER_PATH)
df_proc_item = pd.read_csv(PROCUREMENT_ITEM_PATH)
df_order_header = pd.read_csv(ORDER_HEADER_PATH)
df_order_item = pd.read_csv(ORDER_ITEM_PATH)

# 対象車種
target_products = ['ACD-CV1', 'RDG-YF6', 'ENP-ENP1', 'NOE-JG4', 'HDE-ZC1', 'PRO-ZC5']

print("\n【対象車種のBOM確認】")
for product_id in target_products:
    bom_records = df_bom[df_bom['product_id'] == product_id]
    print(f"\n{product_id}: BOMレコード数 = {len(bom_records)}")
    if len(bom_records) > 0:
        print(f"  部品数: {bom_records['component_product_id'].nunique()}")
        print(f"  サンプル部品:")
        print(bom_records[['bom_name', 'component_product_id', 'component_quantity_per']].head(3))

# 既存の調達データ確認
print("\n" + "=" * 100)
print("【既存の調達データ確認】")
for product_id in target_products:
    existing = df_proc_item[df_proc_item['product_id'] == product_id]
    print(f"{product_id}: {len(existing)}件")

# 受注データから必要な調達量を推定
print("\n" + "=" * 100)
print("【受注データから調達必要量を推定】")

orders_summary = df_order_item[df_order_item['product_id'].isin(target_products)].groupby('product_id').agg({
    'quantity': 'sum',
    'order_id': 'count'
}).reset_index()
orders_summary.columns = ['product_id', 'total_order_quantity', 'order_count']
print(orders_summary)

# 調達データ生成の準備
print("\n" + "=" * 100)
print("調達データ生成中...")
print("=" * 100)

# 既存の調達データの最大IDを取得
existing_po_ids = df_proc_header['purchase_order_id'].str.extract(r'PO-(\d+)-(\d+)').astype(int)
max_year = existing_po_ids[0].max()
max_seq = df_proc_header[df_proc_header['purchase_order_id'].str.startswith(f'PO-{max_year}')]['purchase_order_id'].str.extract(r'PO-\d+-(\d+)').astype(int).max()[0]

new_headers = []
new_items = []
po_counter = max_seq + 1

# サプライヤーと配送業者のリスト
suppliers = df_proc_header['supplier_id'].dropna().unique()[:10]
carriers = ['ヤマト運輸', '佐川急便', '日本通運']
locations = ['KMM', 'SYM', 'SZK', 'HTB', 'YKK']
cost_centers = ['CC-001', 'CC-002', 'CC-003', 'CC-004', 'CC-005', 'CC-006']
users = [
    ('田中太郎', 't.tanaka@example.com'),
    ('鈴木一郎', 'i.suzuki@example.com'),
    ('山本進', 's.yamamoto@example.com'),
    ('佐藤花子', 'h.sato@example.com'),
    ('小林誠', 'm.kobayashi@example.com'),
    ('高橋美咲', 'm.takahashi@example.com')
]

# 各車種の受注データから期間を取得
df_order_merged = df_order_item.merge(df_order_header[['order_id', 'order_timestamp']], on='order_id')

for product_id in target_products:
    print(f"\n{product_id} の調達データを生成中...")
    
    # BOMデータ取得
    bom_records = df_bom[df_bom['product_id'] == product_id]
    
    if len(bom_records) == 0:
        print(f"  ⚠️ {product_id}のBOMデータが存在しません。スキップします。")
        continue
    
    # この車種の受注データ取得
    product_orders = df_order_merged[df_order_merged['product_id'] == product_id].copy()
    
    if len(product_orders) == 0:
        print(f"  ⚠️ {product_id}の受注データが存在しません。スキップします。")
        continue
    
    # 受注データを月別にグループ化
    product_orders['order_date'] = pd.to_datetime(product_orders['order_timestamp']).dt.date
    product_orders['year_month'] = pd.to_datetime(product_orders['order_timestamp']).dt.to_period('M')
    
    monthly_orders = product_orders.groupby('year_month').agg({
        'quantity': 'sum',
        'order_date': 'min'
    }).reset_index()
    
    # 各月の受注に対して調達データを生成
    for _, month_data in monthly_orders.iterrows():
        year_month = month_data['year_month']
        order_quantity = month_data['quantity']
        base_date = pd.to_datetime(str(year_month)).date()
        
        # 各BOM部品に対して調達データ生成
        for _, bom in bom_records.iterrows():
            component_id = bom['component_product_id']
            component_name = bom['bom_name']
            component_qty_per = bom['component_quantity_per']
            
            # 必要数量 = 受注数量 × BOM数量
            required_qty = int(order_quantity * component_qty_per)
            
            if required_qty <= 0:
                continue
            
            # 調達日（受注月の少し前）
            order_date = base_date - timedelta(days=random.randint(5, 15))
            
            # ヘッダー作成
            po_id = f"PO-{order_date.year}-{po_counter:06d}"
            supplier_id = random.choice(suppliers)
            supplier_name = df_proc_header[df_proc_header['supplier_id'] == supplier_id]['supplier_name'].iloc[0] if len(df_proc_header[df_proc_header['supplier_id'] == supplier_id]) > 0 else 'サプライヤー'
            
            # 単価推定（既存データの平均を使用、なければ仮単価）
            existing_prices = df_proc_item[df_proc_item['material_id'] == component_id]['unit_price_ex_tax']
            if len(existing_prices) > 0:
                unit_price = int(existing_prices.mean() * random.uniform(0.95, 1.05))
            else:
                # BOMから推定（部品カテゴリによって異なる単価）
                if 'ENG' in component_id or 'MTR' in component_id:
                    unit_price = random.randint(800000, 1200000)
                elif 'BAT' in component_id or 'INV' in component_id:
                    unit_price = random.randint(100000, 300000)
                elif 'TRN' in component_id:
                    unit_price = random.randint(200000, 400000)
                elif 'ECU' in component_id:
                    unit_price = random.randint(80000, 150000)
                else:
                    unit_price = random.randint(30000, 100000)
            
            subtotal_ex_tax = required_qty * unit_price
            tax = int(subtotal_ex_tax * 0.1)
            total_incl_tax = subtotal_ex_tax + tax
            
            user_name, user_email = random.choice(users)
            
            header = {
                'purchase_order_id': po_id,
                'order_date': order_date,
                'expected_delivery_date': order_date + timedelta(days=random.randint(7, 14)),
                'supplier_id': supplier_id,
                'supplier_name': supplier_name,
                'account_group': f'AG-{random.randint(1, 5):03d}',
                'location_id': random.choice(locations),
                'purchase_order_number': f'PO-{po_counter:06d}',
                'currency': 'JPY',
                'order_subtotal_ex_tax': subtotal_ex_tax,
                'shipping_fee_ex_tax': random.randint(1000, 5000),
                'tax_amount': tax,
                'discount_amount_incl_tax': 0,
                'order_total_incl_tax': total_incl_tax,
                'order_status': 'received',
                'approver': user_name,
                'payment_method': 'bank_transfer',
                'payment_confirmation_id': f'PAY-{po_counter:06d}',
                'payment_date': order_date + timedelta(days=30),
                'payment_amount': total_incl_tax
            }
            
            # アイテム作成
            item = {
                'purchase_order_id': po_id,
                'line_number': 1,
                'material_id': component_id,
                'material_name': component_name,
                'material_category': bom.get('component_category', 'Parts'),
                'material_type': 'direct',
                'product_id': product_id,
                'unspsc_code': '',
                'quantity': required_qty,
                'unit_price_ex_tax': unit_price,
                'line_subtotal_incl_tax': total_incl_tax,
                'line_subtotal_ex_tax': subtotal_ex_tax,
                'line_tax_amount': tax,
                'line_tax_rate': 0.1,
                'line_shipping_fee_incl_tax': random.randint(1000, 5000),
                'line_discount_incl_tax': 0,
                'line_total_incl_tax': total_incl_tax,
                'reference_price_ex_tax': unit_price,
                'purchase_rule': '該当無し',
                'ship_date': order_date + timedelta(days=2),
                'shipping_status': 'delivered',
                'carrier_tracking_number': f'{random.choice(["YM", "SG", "NT"])}{random.randint(1000000000, 9999999999)}',
                'shipped_quantity': required_qty,
                'carrier_name': random.choice(carriers),
                'delivery_address': f'富田2354, 大里郡寄居町, 369-1293, 埼玉県',
                'receiving_status': 'received',
                'received_quantity': required_qty,
                'received_date': order_date + timedelta(days=random.randint(5, 10)),
                'receiver_name': user_name,
                'receiver_email': user_email,
                'cost_center': random.choice(cost_centers),
                'project_code': f'PRJ-{random.randint(2022, 2025)}-{random.choice(["A", "B", "C", "D"])}',
                'department_code': 'DEPT-MFG',
                'account_user': user_name,
                'user_email': user_email
            }
            
            new_headers.append(header)
            new_items.append(item)
            po_counter += 1
    
    generated_count = len([h for h in new_headers[-len(bom_records)*len(monthly_orders):]])
    print(f"  [OK] {product_id}: {generated_count}件の調達データを生成")

# 新規データをDataFrameに変換
df_new_headers = pd.DataFrame(new_headers)
df_new_items = pd.DataFrame(new_items)

print(f"\n生成した調達データ:")
print(f"  ヘッダー: {len(df_new_headers)}件")
print(f"  明細: {len(df_new_items)}件")

# 既存データと結合
df_proc_header_updated = pd.concat([df_proc_header, df_new_headers], ignore_index=True)
df_proc_item_updated = pd.concat([df_proc_item, df_new_items], ignore_index=True)

# CSVに保存
df_proc_header_updated.to_csv(PROCUREMENT_HEADER_PATH, index=False, encoding='utf-8-sig')
df_proc_item_updated.to_csv(PROCUREMENT_ITEM_PATH, index=False, encoding='utf-8-sig')

print(f"\n[OK] 調達データを更新しました")
print(f"  ヘッダー: {len(df_proc_header)} -> {len(df_proc_header_updated)}件 (+{len(df_new_headers)})")
print(f"  明細: {len(df_proc_item)} -> {len(df_proc_item_updated)}件 (+{len(df_new_items)})")

# 更新後の確認
print("\n" + "=" * 100)
print("【更新後の調達データ確認】")
for product_id in target_products:
    updated = df_proc_item_updated[df_proc_item_updated['product_id'] == product_id]
    direct = updated[updated['material_type'] == 'direct']
    total_cost = (direct['quantity'] * direct['unit_price_ex_tax']).sum()
    print(f"{product_id}: {len(direct)}件, 総調達額: ¥{total_cost:,.0f}")

print("\n処理完了")
