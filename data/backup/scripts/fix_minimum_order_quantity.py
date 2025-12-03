#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
受注伝票_itemの数量を最低発注数量に合わせて修正するスクリプト
"""

import csv
from datetime import datetime

# 最低発注数量
MINIMUM_ORDER_QTY = {
    'DEAL-001': 1,
    'DEAL-002': 1,
    'DEAL-003': 1,
    'DEAL-004': 1,
    'DEAL-005': 1,
    'DEAL-006': 5,
    'DEAL-007': 5,
    'DEAL-008': 5,
}

def load_headers():
    """受注伝票_headerを読み込んでcustomer_idを取得"""
    headers = {}
    base_path = r'c:\Users\at04\Desktop\肥川作業\肥川個人\dev\ForStep2\data\Bronze\ERP'
    with open(f'{base_path}\\受注伝票_header.csv', 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            headers[row['order_id']] = row['customer_id']
    return headers

def fix_item_quantities():
    """受注伝票_itemの数量を最低発注数量に合わせて修正"""
    
    headers = load_headers()
    base_path = r'c:\Users\at04\Desktop\肥川作業\肥川個人\dev\ForStep2\data\Bronze\ERP'
    input_path = f'{base_path}\\受注伝票_item.csv'
    output_path = input_path
    
    items = []
    modified_count = 0
    
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            order_id = row['order_id']
            quantity = int(row['quantity'])
            
            # headerから取引先を取得
            customer_id = headers.get(order_id, '')
            min_qty = MINIMUM_ORDER_QTY.get(customer_id, 1)
            
            # 最低発注数量未満の場合は修正
            if quantity < min_qty:
                row['quantity'] = str(min_qty)
                modified_count += 1
            
            items.append(row)
    
    # CSV出力
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        fieldnames = items[0].keys() if items else []
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(items)
    
    print(f"✓ 受注伝票_item.csv を修正しました")
    print(f"  修正件数: {modified_count} 件")
    print(f"  総明細行数: {len(items)} 件")

if __name__ == '__main__':
    print("受注伝票_itemの数量を最低発注数量に合わせて修正中...")
    fix_item_quantities()
