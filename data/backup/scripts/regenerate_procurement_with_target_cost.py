"""
適正な原価率でBOM調達データを再生成
目標: ICE 60%原価率、EV 70%原価率
"""
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import random

# 設定
TARGET_COST_RATE_ICE = 0.60  # ICE車の目標原価率
TARGET_COST_RATE_EV = 0.70   # EV車の目標原価率
TARGET_VEHICLES = ['ACD-CV1', 'RDG-YF6', 'ENP-ENP1', 'NOE-JG4', 'HDE-ZC1', 'PRO-ZC5']

db_path = 'data/bronze_data.db'
conn = sqlite3.connect(db_path)

print("=" * 80)
print("適正原価率でのBOM調達データ再生成")
print(f"目標原価率: ICE {TARGET_COST_RATE_ICE*100:.0f}%, EV {TARGET_COST_RATE_EV*100:.0f}%")
print("=" * 80)

# 1. 対象車種の情報を取得
vehicle_info_query = """
SELECT DISTINCT
    p.product_id,
    p.product_name,
    p.item_hierarchy,
    COUNT(DISTINCT b.component_product_id) as bom_part_count
FROM bronze_erp_product_master p
INNER JOIN bronze_p2p_bom_master b ON p.product_id = b.product_id
WHERE p.product_id IN ('{}')
GROUP BY p.product_id, p.product_name, p.item_hierarchy
""".format("','".join(TARGET_VEHICLES))

vehicle_info = pd.read_sql_query(vehicle_info_query, conn)
print("\n【対象車種情報】")
print(vehicle_info.to_string(index=False))

# 2. 月次受注データを取得
orders_query = """
SELECT 
    year_month,
    product_id,
    total_quantity,
    total_sales_ex_tax,
    ROUND(total_sales_ex_tax / total_quantity, 0) as unit_price
FROM silver_monthly_orders
WHERE product_id IN ('{}')
    AND total_quantity > 0
ORDER BY product_id, year_month
""".format("','".join(TARGET_VEHICLES))

monthly_orders = pd.read_sql_query(orders_query, conn)

# 3. BOMマスタを取得
bom_query = """
SELECT 
    product_id,
    component_product_id,
    component_quantity_per
FROM bronze_p2p_bom_master
WHERE product_id IN ('{}')
ORDER BY product_id, component_product_id
""".format("','".join(TARGET_VEHICLES))

bom_master = pd.read_sql_query(bom_query, conn)

print("\n【BOMマスタ件数】")
for vehicle in TARGET_VEHICLES:
    count = len(bom_master[bom_master['product_id'] == vehicle])
    print(f"  {vehicle}: {count}部品")

# 4. 既存の調達データを削除対象としてマーク
existing_procurement_query = """
SELECT purchase_order_id
FROM bronze_p2p_procurement_header
WHERE purchase_order_id LIKE 'PO-SUPP-%'
"""
existing_po_ids = pd.read_sql_query(existing_procurement_query, conn)
print(f"\n削除対象の既存補充データ: {len(existing_po_ids)}件")

conn.close()

# 5. CSVファイルから既存データを読み込み
header_csv = 'data/Bronze/P2P/調達伝票_header.csv'
item_csv = 'data/Bronze/P2P/調達伝票_item.csv'

print("\n既存CSVデータを読み込み中...")
df_header = pd.read_csv(header_csv, encoding='utf-8-sig')
df_item = pd.read_csv(item_csv, encoding='utf-8-sig')

print(f"  調達header件数: {len(df_header):,}件")
print(f"  調達item件数: {len(df_item):,}件")

# 6. 補充データ（PO-SUPP-で始まるもの）を削除
df_header_clean = df_header[~df_header['purchase_order_id'].str.startswith('PO-SUPP-', na=False)].copy()
df_item_clean = df_item[~df_item['purchase_order_id'].str.startswith('PO-SUPP-', na=False)].copy()

deleted_header = len(df_header) - len(df_header_clean)
deleted_item = len(df_item) - len(df_item_clean)

print(f"\n削除したデータ:")
print(f"  Header: {deleted_header:,}件")
print(f"  Item: {deleted_item:,}件")
print(f"\n残存データ:")
print(f"  Header: {len(df_header_clean):,}件")
print(f"  Item: {len(df_item_clean):,}件")

# 7. 新しい調達データを生成
new_headers = []
new_items = []
po_counter = 1

print("\n" + "=" * 80)
print("新規調達データ生成中...")
print("=" * 80)

for vehicle in TARGET_VEHICLES:
    vehicle_data = vehicle_info[vehicle_info['product_id'] == vehicle].iloc[0]
    hierarchy = vehicle_data['item_hierarchy']
    bom_parts = bom_master[bom_master['product_id'] == vehicle]
    orders = monthly_orders[monthly_orders['product_id'] == vehicle]
    
    # 目標原価率を設定
    target_cost_rate = TARGET_COST_RATE_ICE if hierarchy == 'ICE' else TARGET_COST_RATE_EV
    
    print(f"\n{vehicle} ({vehicle_data['product_name']}) - {hierarchy}")
    print(f"  目標原価率: {target_cost_rate*100:.0f}%")
    print(f"  BOM部品数: {len(bom_parts)}個")
    print(f"  受注月数: {len(orders)}ヶ月")
    
    total_cost_generated = 0
    total_sales = orders['total_sales_ex_tax'].sum()
    
    # 月次受注ごとにループ
    for _, order in orders.iterrows():
        year_month = order['year_month']
        order_qty = int(order['total_quantity'])
        unit_selling_price = order['unit_price']
        
        # 目標原価/台を計算
        target_cost_per_vehicle = unit_selling_price * target_cost_rate
        
        # BOM部品数で均等割り（実際には部品によって価格差があるが、簡略化）
        target_cost_per_part = target_cost_per_vehicle / len(bom_parts)
        
        # 部品ごとに若干のバラツキを持たせる（±30%）
        for _, bom in bom_parts.iterrows():
            component_id = bom['component_product_id']
            component_qty_per = bom['component_quantity_per']
            
            # 部品単価を計算（バラツキを持たせる）
            variation_factor = random.uniform(0.7, 1.3)
            unit_price = round(target_cost_per_part * variation_factor / component_qty_per, 2)
            
            # 必要数量
            required_qty = int(order_qty * component_qty_per)
            
            # 金額計算
            line_subtotal = round(unit_price * required_qty, 2)
            tax_rate = 0.10
            line_tax = round(line_subtotal * tax_rate, 2)
            line_total = line_subtotal + line_tax
            
            # 伝票日付（受注月の15日前後に調達）
            year, month = map(int, year_month.split('-'))
            procurement_date = datetime(year, month, 15)
            ship_date = procurement_date + timedelta(days=7)
            received_date = procurement_date + timedelta(days=14)
            
            # PO ID生成
            po_id = f"PO-SUPP-{vehicle}-{year_month}-{po_counter:04d}"
            po_counter += 1
            
            # Header作成（部品ごとに1つのPO）
            header = {
                'purchase_order_id': po_id,
                'order_date': procurement_date.strftime('%Y-%m-%d'),
                'purchase_order_date': procurement_date.strftime('%Y-%m-%d'),
                'supplier_id': 'SUP-001',  # ダミーサプライヤー
                'supplier_name': 'BOM補充サプライヤー',
                'order_status': 'completed',
                'order_subtotal_incl_tax': line_total,
                'order_subtotal_ex_tax': line_subtotal,
                'order_total_tax_amount': line_tax,
                'order_total_incl_tax': line_total,
                'order_shipping_fee_incl_tax': 0,
                'order_discount_incl_tax': 0,
                'payment_terms': 'NET30',
                'expected_delivery_date': ship_date.strftime('%Y-%m-%d'),
                'delivery_address': 'STM',
                'billing_address': 'STM',
                'order_approval_status': 'approved',
                'approver_id': 'SYS-AUTO',
                'approval_date': procurement_date.strftime('%Y-%m-%d'),
                'order_notes': f'BOM-based procurement for {vehicle}',
                'cost_center': 'CC-MFG',
                'project_code': '',
                'department_code': 'DEPT-MFG',
                'account_user': 'system',
                'user_email': 'system@example.com'
            }
            new_headers.append(header)
            
            # Item作成
            item = {
                'purchase_order_id': po_id,
                'line_number': 1,
                'material_id': component_id,
                'material_name': f'Component {component_id}',
                'material_category': 'automotive_parts',
                'material_type': 'direct',
                'product_id': vehicle,
                'unspsc_code': '25000000',
                'quantity': required_qty,
                'unit_price_ex_tax': unit_price,
                'line_subtotal_incl_tax': line_total,
                'line_subtotal_ex_tax': line_subtotal,
                'line_tax_amount': line_tax,
                'line_tax_rate': tax_rate,
                'line_shipping_fee_incl_tax': 0,
                'line_discount_incl_tax': 0,
                'line_total_incl_tax': line_total,
                'reference_price_ex_tax': unit_price,
                'purchase_rule': 'standard',
                'ship_date': ship_date.strftime('%Y-%m-%d'),
                'shipping_status': 'delivered',
                'carrier_tracking_number': '',
                'shipped_quantity': required_qty,
                'carrier_name': '',
                'delivery_address': 'STM',
                'receiving_status': 'received',
                'received_quantity': required_qty,
                'received_date': received_date.strftime('%Y-%m-%d'),
                'receiver_name': 'Warehouse',
                'receiver_email': 'warehouse@example.com',
                'cost_center': 'CC-MFG',
                'project_code': '',
                'department_code': 'DEPT-MFG',
                'account_user': 'system',
                'user_email': 'system@example.com'
            }
            new_items.append(item)
            
            total_cost_generated += line_subtotal
    
    actual_cost_rate = (total_cost_generated / total_sales * 100) if total_sales > 0 else 0
    print(f"  総売上: ¥{total_sales:,.0f}")
    print(f"  生成原価: ¥{total_cost_generated:,.0f}")
    print(f"  実際原価率: {actual_cost_rate:.2f}%")
    print(f"  生成PO数: {len([h for h in new_headers if vehicle in h['purchase_order_id']])}件")

# 8. 新しいデータを既存データに結合
df_new_headers = pd.DataFrame(new_headers)
df_new_items = pd.DataFrame(new_items)

df_header_final = pd.concat([df_header_clean, df_new_headers], ignore_index=True)
df_item_final = pd.concat([df_item_clean, df_new_items], ignore_index=True)

print("\n" + "=" * 80)
print("最終データ件数")
print("=" * 80)
print(f"Header: {len(df_header_final):,}件 (元: {len(df_header_clean):,}, 新規: {len(df_new_headers):,})")
print(f"Item: {len(df_item_final):,}件 (元: {len(df_item_clean):,}, 新規: {len(df_new_items):,})")

# 9. CSVファイルに保存
print("\nCSVファイルに保存中...")
df_header_final.to_csv(header_csv, index=False, encoding='utf-8-sig')
df_item_final.to_csv(item_csv, index=False, encoding='utf-8-sig')

print("\n[OK] 調達データの再生成完了")
print(f"  {header_csv}")
print(f"  {item_csv}")
print("\n次のステップ: データベースを再構築してください")
print("  python build_bronze_silver_db.py")
