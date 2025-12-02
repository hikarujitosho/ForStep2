#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
受注伝票_headerのサンプルデータ生成スクリプト
2022年1月～2025年12月の期間で生成
"""

import csv
from datetime import datetime, timedelta
import random

# 取引先と拠点のマッピング
DEALER_LOCATION_MAP = {
    'DEAL-001': 'A1',  # Honda Cars 東京中央
    'DEAL-002': 'A2',  # Honda Cars 大阪
    'DEAL-003': 'A3',  # Honda Cars 横浜
    'DEAL-004': 'A4',  # Honda Cars 名古屋
    'DEAL-005': 'A5',  # Honda Cars 福岡
    'DEAL-006': 'A1',  # Honda of America (A1から開始)
    'DEAL-007': 'A2',  # Honda Europe
    'DEAL-008': 'A3',  # Honda China
}

# Honda Cars（DEAL-001～005）: 週1～2回
HONDA_CARS_DEALERS = ['DEAL-001', 'DEAL-002', 'DEAL-003', 'DEAL-004', 'DEAL-005']
# その他（DEAL-006～008）: 月1～2回
OTHER_DEALERS = ['DEAL-006', 'DEAL-007', 'DEAL-008']

def generate_order_id(counter):
    """注文IDを生成"""
    return f'ORD-{counter:06d}'

def generate_order_data():
    """2022年1月～2025年12月のサンプルデータを生成"""
    
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2025, 12, 2)
    
    orders = []
    order_counter = 1
    current_date = start_date
    
    # 各ディーラーごとの次の発注予定日を追跡
    next_order_dates = {}
    
    # Honda Cars: 週1～2回の頻度で開始
    for dealer in HONDA_CARS_DEALERS:
        days_offset = random.randint(1, 7)  # 最初の発注は1～7日後
        next_order_dates[dealer] = start_date + timedelta(days=days_offset)
    
    # その他: 月1～2回の頻度で開始
    for dealer in OTHER_DEALERS:
        days_offset = random.randint(10, 30)  # 最初の発注は10～30日後
        next_order_dates[dealer] = start_date + timedelta(days=days_offset)
    
    # 日付ベースのループで発注を生成
    while current_date <= end_date:
        # Honda Cars ディーラーをチェック
        for dealer in HONDA_CARS_DEALERS:
            if current_date >= next_order_dates[dealer]:
                # 営業時間内のランダムな時刻（8:00～17:00）
                hour = random.randint(8, 17)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                timestamp = current_date.replace(hour=hour, minute=minute, second=second)
                
                location_id = DEALER_LOCATION_MAP[dealer]
                order_id = generate_order_id(order_counter)
                
                orders.append({
                    'order_id': order_id,
                    'order_timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'location_id': location_id,
                    'customer_id': dealer
                })
                
                order_counter += 1
                
                # 次の発注日を設定（5～12日後：週1回弱の頻度に調整）
                days_to_next = random.randint(5, 12)
                next_order_dates[dealer] = current_date + timedelta(days=days_to_next)
        
        # その他ディーラーをチェック（月に一度程度なので日数が少ないときのみ）
        if current_date.day in [1, 15, random.randint(5, 25)]:
            for dealer in OTHER_DEALERS:
                # 月1～2回なので、確率で判定
                if current_date >= next_order_dates[dealer]:
                    # 営業時間内のランダムな時刻（8:00～17:00）
                    hour = random.randint(8, 17)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    timestamp = current_date.replace(hour=hour, minute=minute, second=second)
                    
                    location_id = DEALER_LOCATION_MAP[dealer]
                    order_id = generate_order_id(order_counter)
                    
                    orders.append({
                        'order_id': order_id,
                        'order_timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        'location_id': location_id,
                        'customer_id': dealer
                    })
                    
                    order_counter += 1
                    
                    # 次の発注日を設定（28～42日後：月1回弱の頻度に調整）
                    days_to_next = random.randint(28, 42)
                    next_order_dates[dealer] = current_date + timedelta(days=days_to_next)
        
        current_date += timedelta(days=1)
    
    return orders

def write_csv(orders, filepath):
    """CSVファイルに書き込み"""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['order_id', 'order_timestamp', 'location_id', 'customer_id']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(orders)
    
    print(f"✓ {len(orders)} 件の注文データを {filepath} に保存しました")
    print(f"  期間: {orders[0]['order_timestamp']} ~ {orders[-1]['order_timestamp']}")

if __name__ == '__main__':
    # データ生成
    print("サンプルデータ生成中...")
    orders = generate_order_data()
    
    # ディーラーごとの件数を表示
    dealer_counts = {}
    for order in orders:
        dealer = order['customer_id']
        dealer_counts[dealer] = dealer_counts.get(dealer, 0) + 1
    
    print(f"\n生成されたデータ件数: {len(orders)} 件")
    print("\nディーラーごとの発注件数:")
    for dealer in sorted(dealer_counts.keys()):
        print(f"  {dealer}: {dealer_counts[dealer]} 件")
    
    # CSV出力
    output_path = r'c:\Users\at04\Desktop\肥川作業\肥川個人\dev\ForStep2\data\Bronze\ERP\受注伝票_header.csv'
    write_csv(orders, output_path)
