#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
受注伝票_itemのサンプルデータ生成スクリプト
受注伝票_headerのorder_idに紐づく明細行を生成
"""

import csv
from datetime import datetime, timedelta
import random

# 国内ディーラーと海外ディーラーの区分
DOMESTIC_DEALERS = ['DEAL-001', 'DEAL-002', 'DEAL-003', 'DEAL-004', 'DEAL-005']
OVERSEAS_DEALERS = ['DEAL-006', 'DEAL-007', 'DEAL-008']

# 拠点ごとの納期目安（days）
DELIVERY_DAYS_BY_LOCATION = {
    'A1': (7, 14),     # 埼玉製作所: 1～2週間
    'A2': (7, 14),     # ホンダオートボディー: 1～2週間
    'A3': (10, 21),    # 鈴鹿製作所: 1.5～3週間
    'A4': (14, 21),    # 熊本製作所: 2～3週間
    'A5': (7, 14),     # 浜松製作所: 1～2週間
}

def load_csv(filepath):
    """CSVファイルを読み込んで辞書のリストで返す"""
    data = []
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:  # BOMを自動削除
            reader = csv.DictReader(f)
            data = list(reader)
    except Exception as e:
        print(f"エラー: {filepath} を読み込めません - {e}")
        raise
    return data

def load_order_header(filepath):
    """受注伝票_headerを読み込む"""
    orders = {}
    with open(filepath, 'r', encoding='utf-8-sig') as f:  # BOMを自動削除
        reader = csv.DictReader(f)
        for row in reader:
            orders[row['order_id']] = {
                'order_timestamp': row['order_timestamp'],
                'location_id': row['location_id'],
                'customer_id': row['customer_id']
            }
    return orders

def get_available_products(customer_id, products):
    """顧客が取り扱い可能な品目を取得"""
    
    # customer_id から region を特定
    if customer_id in DOMESTIC_DEALERS:
        region = 'domestic'
        allowed_import_export = 'domestic'
    elif customer_id in OVERSEAS_DEALERS:
        region = 'overseas'
        allowed_import_export = 'export'
    else:
        return []
    
    # 条件に合致する品目を抽出
    available = [
        p for p in products
        if p['import_export_group'] == allowed_import_export
    ]
    
    return available

def generate_order_items(order_header, products):
    """受注伝票_itemのデータを生成"""
    
    items = []
    item_counter = 0
    
    for order_id, header_info in order_header.items():
        customer_id = header_info['customer_id']
        location_id = header_info['location_id']
        order_timestamp_str = header_info['order_timestamp']
        order_timestamp = datetime.strptime(order_timestamp_str, '%Y-%m-%d %H:%M:%S')
        
        # 顧客が取り扱い可能な品目を取得
        available_products = get_available_products(customer_id, products)
        
        if not available_products:
            continue
        
        # 国内ディーラーか海外ディーラーかで処理を分け
        if customer_id in DOMESTIC_DEALERS:
            # 国内: 1～3車種、合計1～10台
            num_items = random.randint(1, 3)
            total_quantity = random.randint(1, 10)
        else:
            # 海外: 1～4車種、合計10～50台
            num_items = random.randint(1, 4)
            total_quantity = random.randint(10, 50)
        
        # ランダムに品目を選択（重複なし）
        selected_products = random.sample(available_products, min(num_items, len(available_products)))
        
        # 各品目の数量を割り当て
        quantities = []
        remaining = total_quantity
        for i, product in enumerate(selected_products):
            if i == len(selected_products) - 1:
                # 最後の品目に残りすべてを割り当て
                qty = remaining
            else:
                # ランダムに割り当て（最後のアイテムのために最小値1を確保）
                min_qty = 1
                max_qty = remaining - (len(selected_products) - i - 1)
                # max_qtyが1より小さくならないようにチェック
                max_qty = max(1, max_qty)
                qty = random.randint(min_qty, max_qty)
            quantities.append(qty)
            remaining -= qty
        
        # 納期を計算（location_idに基づいて）
        delivery_day_range = DELIVERY_DAYS_BY_LOCATION.get(location_id, (7, 14))
        days_offset = random.randint(delivery_day_range[0], delivery_day_range[1])
        promised_delivery_date = order_timestamp + timedelta(days=days_offset)
        
        # 各品目ごとにアイテム行を生成
        for line_num, (product, qty) in enumerate(zip(selected_products, quantities), start=1):
            item_counter += 1
            items.append({
                'order_id': order_id,
                'line_number': str(line_num),
                'product_id': product['product_id'],
                'quantity': str(qty),
                'promised_delivery_date': promised_delivery_date.strftime('%Y-%m-%d %H:%M:%S'),
                'pricing_date': order_timestamp.strftime('%Y-%m-%d')
            })
    
    return items

def write_csv(items, filepath):
    """CSVファイルに書き込み"""
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:  # BOM付きで出力
        fieldnames = ['order_id', 'line_number', 'product_id', 'quantity', 'promised_delivery_date', 'pricing_date']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(items)
    
    print(f"✓ {len(items)} 件の明細データを {filepath} に保存しました")

if __name__ == '__main__':
    print("マスタデータを読み込み中...")
    
    # マスタデータを読み込む
    base_path = r'c:\Users\at04\Desktop\肥川作業\肥川個人\dev\ForStep2\data\Bronze\ERP'
    
    # 品目マスタを読み込む
    products = load_csv(f'{base_path}\\品目マスタ.csv')
    print(f"  品目マスタ: {len(products)} 件")
    if products:
        print(f"  最初のレコードのキー: {products[0].keys()}")
    
    # 受注伝票_headerを読み込む
    order_header = load_order_header(f'{base_path}\\受注伝票_header.csv')
    print(f"  受注伝票_header: {len(order_header)} 件")
    
    # 明細データを生成
    print("\n明細データを生成中...")
    items = generate_order_items(order_header, products)
    
    # 統計情報を表示
    order_counts = {}
    for item in items:
        order_id = item['order_id']
        order_counts[order_id] = order_counts.get(order_id, 0) + 1
    
    print(f"\n生成された明細数: {len(items)} 件")
    print(f"受注件数: {len(order_counts)} 件")
    print(f"1受注あたりの平均明細行数: {len(items) / len(order_counts):.2f} 行")
    
    # 数量統計
    quantities = [int(item['quantity']) for item in items]
    print(f"1行あたりの平均数量: {sum(quantities) / len(quantities):.2f} 台")
    print(f"総数量: {sum(quantities)} 台")
    
    # CSV出力
    output_path = f'{base_path}\\受注伝票_item.csv'
    write_csv(items, output_path)
