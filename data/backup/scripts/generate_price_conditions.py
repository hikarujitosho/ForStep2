#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
条件マスタのサンプルデータ生成スクリプト
2022年1月以降、各取引先・品目ごとに価格条件を生成
"""

import csv
from datetime import datetime, timedelta
import random
import hashlib

# 定数定義
DOMESTIC_DEALERS = ['DEAL-001', 'DEAL-002', 'DEAL-003', 'DEAL-004', 'DEAL-005']
OVERSEAS_DEALERS = ['DEAL-006', 'DEAL-007', 'DEAL-008']

# 取引先ごとの割引率（%)
DEALER_DISCOUNTS = {
    'DEAL-001': 5,     # Honda Cars 東京中央
    'DEAL-002': 5,     # Honda Cars 大阪
    'DEAL-003': 6,     # Honda Cars 横浜
    'DEAL-004': 5,     # Honda Cars 名古屋
    'DEAL-005': 7,     # Honda Cars 福岡
    'DEAL-006': 10,    # Honda of America
    'DEAL-007': 12,    # Honda Europe
    'DEAL-008': 11,    # Honda China
}

# 取引先ごとの最低発注数量
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

# 2025年時点の基準価格（既存ファイルから）
BASE_PRICES_2025 = {
    'F101000': 2500000,
    'F102000': 3100000,
    'F103000': 3200000,
    'F104000': 3600000,
    'F105000': 3800000,
    'F106000': 4500000,
    'F107000': 5200000,
    'F108000': 5800000,
    'F109000': 4200000,
    'F111000': 4100000,
    'F201000': 4500000,
    'F202000': 3200000,
    'F203000': 5500000,
    'F204000': 6500000,
}

# 品目マスタ読み込み
def load_products():
    products = {}
    base_path = r'c:\Users\at04\Desktop\肥川作業\肥川個人\dev\ForStep2\data\Bronze\ERP'
    with open(f'{base_path}\\品目マスタ.csv', 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            products[row['product_id']] = row
    return products

# 取引先マスタ読み込み
def load_dealers():
    dealers = {}
    base_path = r'c:\Users\at04\Desktop\肥川作業\肥川個人\dev\ForStep2\data\Bronze\ERP'
    with open(f'{base_path}\\取引先マスタ.csv', 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['partner_id'].startswith('DEAL'):
                dealers[row['partner_id']] = row['partner_name']
    return dealers

# 価格条件IDを生成
def generate_condition_id(product_id, customer_id, valid_from):
    # product_id、customer_id、valid_from の組み合わせでハッシュ生成
    key = f"{product_id}-{customer_id}-{valid_from}"
    hash_val = hashlib.md5(key.encode()).hexdigest()[:8].upper()
    return f"PC-{hash_val}"

# 2022年1月から2025年12月までの価格条件を生成
def generate_price_conditions():
    products = load_products()
    dealers = load_dealers()
    
    conditions = []
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    for product_id, product_info in products.items():
        for dealer_id in DOMESTIC_DEALERS + OVERSEAS_DEALERS:
            # 基準価格（2025年）
            base_price = BASE_PRICES_2025.get(product_id, 3000000)
            discount_pct = DEALER_DISCOUNTS[dealer_id]
            
            # 2022年1月の価格を逆算（年3%上昇を想定）
            price_2022 = base_price / ((1.03) ** 3)  # 3年分逆算
            
            # 2年ごとの価格改定（2022/1, 2024/1）
            price_versions = [
                (datetime(2022, 1, 1), price_2022),
                (datetime(2024, 1, 1), price_2022 * 1.06),  # 2年で6%上昇
            ]
            
            # 各価格版について
            for version_idx, (version_date, list_price) in enumerate(price_versions):
                year = version_date.year
                
                # その年のキャンペーン期間を決定（年1回、ただし50%の確率）
                campaign_month = None
                if random.random() < 0.5:  # 50%の確率でキャンペーンあり
                    campaign_month = random.randint(1, 12)
                
                # 版の期間を決定
                if version_idx == 0:
                    # 2022版は2022/1～2023/12
                    version_start = datetime(2022, 1, 1)
                    version_end = datetime(2023, 12, 31)
                else:
                    # 2024版は2024/1～2025/12
                    version_start = datetime(2024, 1, 1)
                    version_end = datetime(2025, 12, 31)
                
                # 通常期間と キャンペーン期間を作成
                # 1. 通常期間（年初～キャンペーン前）
                if campaign_month:
                    normal_start = version_start
                    normal_end = datetime(year, campaign_month, 1) - timedelta(days=1)
                    
                    # valid_from > valid_to のチェック
                    if normal_start > normal_end:
                        normal_end = normal_start
                    
                    valid_from_str = normal_start.strftime('%Y-%m-%d')
                    valid_to_str = normal_end.strftime('%Y-%m-%d')
                    
                    discount_pct_effective = discount_pct
                    price_type = 'standard'
                    remarks = f'標準価格（割引率: {discount_pct}%）'
                    
                    selling_price = list_price * (1 - discount_pct_effective / 100)
                    discount_rate = discount_pct_effective / 100
                    
                    condition_id = generate_condition_id(product_id, dealer_id, valid_from_str)
                    
                    conditions.append({
                        'price_condition_id': condition_id,
                        'product_id': product_id,
                        'product_name': product_info['product_name'],
                        'customer_id': dealer_id,
                        'customer_name': dealers.get(dealer_id, ''),
                        'list_price_ex_tax': f'{int(list_price)}',
                        'selling_price_ex_tax': f'{int(selling_price)}',
                        'discount_rate': f'{discount_rate:.2f}',
                        'price_type': price_type,
                        'minimum_order_quantity': str(MINIMUM_ORDER_QTY[dealer_id]),
                        'currency': 'JPY',
                        'valid_from': valid_from_str,
                        'valid_to': valid_to_str,
                        'remarks': remarks
                    })
                    
                    # 2. キャンペーン期間
                    campaign_start = datetime(year, campaign_month, 1)
                    if campaign_month == 12:
                        campaign_end = datetime(year, 12, 31)
                    else:
                        campaign_end = datetime(year, campaign_month + 1, 1) - timedelta(days=1)
                    
                    valid_from_str = campaign_start.strftime('%Y-%m-%d')
                    valid_to_str = campaign_end.strftime('%Y-%m-%d')
                    
                    # キャンペーン期間：割引を2倍にする
                    discount_pct_effective = discount_pct * 2
                    price_type = 'promotional'
                    remarks = f'キャンペーン期間中の特別価格（通常割引: {discount_pct}% → {discount_pct_effective}%）'
                    
                    selling_price = list_price * (1 - discount_pct_effective / 100)
                    discount_rate = discount_pct_effective / 100
                    
                    condition_id = generate_condition_id(product_id, dealer_id, valid_from_str)
                    
                    conditions.append({
                        'price_condition_id': condition_id,
                        'product_id': product_id,
                        'product_name': product_info['product_name'],
                        'customer_id': dealer_id,
                        'customer_name': dealers.get(dealer_id, ''),
                        'list_price_ex_tax': f'{int(list_price)}',
                        'selling_price_ex_tax': f'{int(selling_price)}',
                        'discount_rate': f'{discount_rate:.2f}',
                        'price_type': price_type,
                        'minimum_order_quantity': str(MINIMUM_ORDER_QTY[dealer_id]),
                        'currency': 'JPY',
                        'valid_from': valid_from_str,
                        'valid_to': valid_to_str,
                        'remarks': remarks
                    })
                    
                    # 3. キャンペーン後の期間（年末まで）
                    if campaign_month < 12:
                        after_campaign_start = datetime(year, campaign_month + 1, 1)
                        after_campaign_end = version_end
                        
                        # キャンペーン後の期間が有効な場合のみ追加
                        if after_campaign_start <= after_campaign_end:
                            valid_from_str = after_campaign_start.strftime('%Y-%m-%d')
                            valid_to_str = after_campaign_end.strftime('%Y-%m-%d')
                            
                            discount_pct_effective = discount_pct
                            price_type = 'standard'
                            remarks = f'標準価格（割引率: {discount_pct}%）'
                            
                            selling_price = list_price * (1 - discount_pct_effective / 100)
                            discount_rate = discount_pct_effective / 100
                            
                            condition_id = generate_condition_id(product_id, dealer_id, valid_from_str)
                            
                            conditions.append({
                                'price_condition_id': condition_id,
                                'product_id': product_id,
                                'product_name': product_info['product_name'],
                                'customer_id': dealer_id,
                                'customer_name': dealers.get(dealer_id, ''),
                                'list_price_ex_tax': f'{int(list_price)}',
                                'selling_price_ex_tax': f'{int(selling_price)}',
                                'discount_rate': f'{discount_rate:.2f}',
                                'price_type': price_type,
                                'minimum_order_quantity': str(MINIMUM_ORDER_QTY[dealer_id]),
                                'currency': 'JPY',
                                'valid_from': valid_from_str,
                                'valid_to': valid_to_str,
                                'remarks': remarks
                            })
                else:
                    # キャンペーンなし：版の開始から終了まで通常価格
                    valid_from_str = version_start.strftime('%Y-%m-%d')
                    valid_to_str = version_end.strftime('%Y-%m-%d')
                    
                    discount_pct_effective = discount_pct
                    price_type = 'standard'
                    remarks = f'標準価格（割引率: {discount_pct}%）'
                    
                    selling_price = list_price * (1 - discount_pct_effective / 100)
                    discount_rate = discount_pct_effective / 100
                    
                    condition_id = generate_condition_id(product_id, dealer_id, valid_from_str)
                    
                    conditions.append({
                        'price_condition_id': condition_id,
                        'product_id': product_id,
                        'product_name': product_info['product_name'],
                        'customer_id': dealer_id,
                        'customer_name': dealers.get(dealer_id, ''),
                        'list_price_ex_tax': f'{int(list_price)}',
                        'selling_price_ex_tax': f'{int(selling_price)}',
                        'discount_rate': f'{discount_rate:.2f}',
                        'price_type': price_type,
                        'minimum_order_quantity': str(MINIMUM_ORDER_QTY[dealer_id]),
                        'currency': 'JPY',
                        'valid_from': valid_from_str,
                        'valid_to': valid_to_str,
                        'remarks': remarks
                    })
    
    return conditions

# CSV出力
def write_csv(conditions):
    base_path = r'c:\Users\at04\Desktop\肥川作業\肥川個人\dev\ForStep2\data\Bronze\ERP'
    output_path = f'{base_path}\\条件マスタ.csv'
    
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        fieldnames = [
            'price_condition_id', 'product_id', 'product_name', 'customer_id', 'customer_name',
            'list_price_ex_tax', 'selling_price_ex_tax', 'discount_rate', 'price_type',
            'minimum_order_quantity', 'currency', 'valid_from', 'valid_to', 'remarks'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(conditions)
    
    print(f"✓ {len(conditions)} 件の価格条件を {output_path} に保存しました")

if __name__ == '__main__':
    print("価格条件マスタを生成中...")
    conditions = generate_price_conditions()
    
    # 統計情報
    print(f"\n生成された価格条件数: {len(conditions)} 件")
    
    # ディーラーごとの条件数
    dealer_counts = {}
    for cond in conditions:
        dealer = cond['customer_id']
        dealer_counts[dealer] = dealer_counts.get(dealer, 0) + 1
    
    print("\nディーラーごとの条件数:")
    for dealer in sorted(dealer_counts.keys()):
        print(f"  {dealer}: {dealer_counts[dealer]} 件")
    
    # CSV出力
    write_csv(conditions)
