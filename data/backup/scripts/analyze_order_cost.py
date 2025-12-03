# -*- coding: utf-8 -*-
import csv
import os
from collections import defaultdict

# ファイルパス
base_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze"
p2p_path = os.path.join(base_path, "P2P")
erp_path = os.path.join(base_path, "ERP")

# 調達データを読み込み
procurement_items = []
with open(os.path.join(p2p_path, "調達伝票_item.csv"), 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    procurement_items = list(reader)

# 受注データを読み込み（purchase_order_idから元のorder_idを逆引きするため）
order_headers = {}
with open(os.path.join(erp_path, "受注伝票_header.csv"), 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        order_headers[row['order_id']] = row

order_items = []
with open(os.path.join(erp_path, "受注伝票_item.csv"), 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    order_items = list(reader)

# 受注伝票から order_id と product_id の組み合わせを作成
order_product_map = {}
for item in order_items:
    key = f"{item['order_id']}_{item['product_id']}"
    order_product_map[key] = {
        'order_id': item['order_id'],
        'product_id': item['product_id'],
        'quantity': int(item['quantity'])
    }

# 調達データから product_id ごとに集計
# purchase_order_idの年月から対応する受注を推定
from datetime import datetime

# product_idと年月ごとに調達金額を集計
product_cost = defaultdict(lambda: {'total': 0, 'count': 0, 'items': []})

for proc_item in procurement_items:
    product_id = proc_item['product_id']
    amount = int(proc_item['line_total_incl_tax'])
    po_id = proc_item['purchase_order_id']
    
    product_cost[product_id]['total'] += amount
    product_cost[product_id]['count'] += 1
    product_cost[product_id]['items'].append({
        'po_id': po_id,
        'material_id': proc_item['material_id'],
        'material_name': proc_item['material_name'],
        'quantity': int(proc_item['quantity']),
        'amount': amount
    })

# 受注1件×車種1台あたりの部品コストを計算
print("=== 1オーダー×1車種あたりの部品調達コスト ===\n")

# 各車種の1台あたりの部品コスト（BOMベース）
vehicle_unit_cost = {}
for product_id in sorted(product_cost.keys()):
    data = product_cost[product_id]
    
    # この車種の受注総数を計算
    total_quantity = sum(int(item['quantity']) for item in order_items if item['product_id'] == product_id)
    
    if total_quantity > 0:
        cost_per_unit = data['total'] / total_quantity
        vehicle_unit_cost[product_id] = {
            'total_cost': data['total'],
            'total_quantity': total_quantity,
            'cost_per_unit': cost_per_unit,
            'part_count': data['count']
        }

# 車種別コストを表示（単価順）
print("【車種別1台あたりの部品調達コスト】")
print(f"{'車種':<15} {'受注総台数':>10} {'総調達金額':>20} {'1台あたりコスト':>20} {'部品明細数':>10}")
print("-" * 85)

sorted_vehicles = sorted(vehicle_unit_cost.items(), key=lambda x: x[1]['cost_per_unit'], reverse=True)
for product_id, data in sorted_vehicles:
    print(f"{product_id:<15} {data['total_quantity']:>10}台 {data['total_cost']:>18,}円 {int(data['cost_per_unit']):>18,}円 {data['part_count']:>10}件")

# サンプル受注の詳細表示
print("\n\n【サンプル受注の詳細（最初の3オーダー）】")
sample_orders = list(order_product_map.values())[:3]

for sample in sample_orders:
    order_id = sample['order_id']
    product_id = sample['product_id']
    quantity = sample['quantity']
    
    if product_id in vehicle_unit_cost:
        cost_per_unit = vehicle_unit_cost[product_id]['cost_per_unit']
        total_cost = cost_per_unit * quantity
        
        print(f"\n受注ID: {order_id}")
        print(f"  車種: {product_id}")
        print(f"  台数: {quantity}台")
        print(f"  1台あたりコスト: ¥{int(cost_per_unit):,}")
        print(f"  この受注の総部品コスト: ¥{int(total_cost):,}")
        
        # 部品内訳（上位5件）
        parts = product_cost[product_id]['items'][:5]
        print(f"  主要部品内訳（上位5件）:")
        for part in parts:
            print(f"    - {part['material_name']}: ¥{part['amount']:,}")

# 統計サマリー
print("\n\n【統計サマリー】")
all_costs = [data['cost_per_unit'] for data in vehicle_unit_cost.values()]
if all_costs:
    print(f"  平均1台あたりコスト: ¥{int(sum(all_costs)/len(all_costs)):,}")
    print(f"  最高1台あたりコスト: ¥{int(max(all_costs)):,}")
    print(f"  最低1台あたりコスト: ¥{int(min(all_costs)):,}")
