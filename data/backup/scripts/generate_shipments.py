# -*- coding: utf-8 -*-
import csv
import random
from datetime import datetime, timedelta

# Paths
base_path = r"H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze"
order_header_path = f"{base_path}\\ERP\\受注伝票_header.csv"
order_item_path = f"{base_path}\\ERP\\受注伝票_item.csv"
product_master_path = f"{base_path}\\ERP\\品目マスタ.csv"
shipment_header_path = f"{base_path}\\MES\\出荷伝票_header.csv"
shipment_item_path = f"{base_path}\\MES\\出荷伝票_item.csv"

print("Loading data...")

# Load product master
product_map = {}
with open(product_master_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        product_map[row['product_id']] = row['product_name']

# Load order headers
order_headers = []
with open(order_header_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    order_headers = list(reader)

# Load order items
order_items = []
with open(order_item_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    order_items = list(reader)

# Create order items lookup
order_items_dict = {}
for item in order_items:
    order_id = item['order_id']
    if order_id not in order_items_dict:
        order_items_dict[order_id] = []
    order_items_dict[order_id].append(item)

# Define options
carriers = ["ホンダロジスティクス", "ホンダトランスポート", "日本通運", "ヤマトロジスティクス", "西濃運輸"]
carrier_codes = {
    "ホンダロジスティクス": "HNL",
    "ホンダトランスポート": "HNT",
    "日本通運": "NPL",
    "ヤマトロジスティクス": "YML",
    "西濃運輸": "SEI"
}
transport_modes = ["road", "sea", "air"]

shipment_headers = []
shipment_items = []
# Track shipment counter per date
shipment_counters = {}

print(f"Processing {len(order_headers)} orders...")
processed_count = 0

for header in order_headers:
    order_id = header['order_id']
    order_timestamp = datetime.strptime(header['order_timestamp'], '%Y-%m-%d %H:%M:%S')
    
    if order_id not in order_items_dict:
        continue
    
    related_items = order_items_dict[order_id]
    
    # Group by line_number and product_id
    item_groups = {}
    for item in related_items:
        key = (item['line_number'], item['product_id'])
        if key not in item_groups:
            item_groups[key] = item
    
    # Randomly split shipment (1-3 splits)
    split_count = random.randint(1, 3)
    
    for i in range(split_count):
        # Random carrier and transport mode for this shipment
        carrier = random.choice(carriers)
        transport_mode = random.choice(transport_modes)
        carrier_code = carrier_codes[carrier]
        
        # Calculate shipment date - use the earliest promised date among all items
        earliest_promised = min([datetime.strptime(item['promised_delivery_date'], '%Y-%m-%d %H:%M:%S') 
                                 for item in item_groups.values()])
        
        days_between = (earliest_promised - order_timestamp).days
        if days_between <= 0:
            days_between = 7
        
        ship_day_offset = random.randint(0, max(days_between - 1, 0))
        ship_date = order_timestamp + timedelta(days=ship_day_offset)
        
        actual_ship_timestamp = ship_date + timedelta(
            hours=random.randint(8, 16),
            minutes=random.randint(0, 59)
        )
        expected_ship_date = ship_date.date()
        planned_ship_date = ship_date.date()
        
        # Generate shipment_id: CarrierCode-TransportMode-Date-Sequence
        date_key = planned_ship_date.strftime('%Y%m%d')
        counter_key = f"{carrier_code}-{transport_mode}-{date_key}"
        if counter_key not in shipment_counters:
            shipment_counters[counter_key] = 0
        shipment_counters[counter_key] += 1
        sequence = shipment_counters[counter_key]
        shipment_id = f"{carrier_code}-{transport_mode}-{date_key}-{sequence:03d}"
        
        # Calculate arrival date
        delivery_days = random.randint(1, 6)
        actual_arrival_timestamp = actual_ship_timestamp + timedelta(
            days=delivery_days,
            hours=random.randint(8, 17),
            minutes=random.randint(0, 59)
        )
        
        # Add shipment header
        shipment_headers.append({
            'shipment_id': shipment_id,
            'shipment_timestamp': actual_ship_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'location_id': header['location_id'],
            'customer_id': header['customer_id']
        })
        
        # Add items to this shipment
        for key, item in item_groups.items():
            total_qty = int(item['quantity'])
            promised_date = datetime.strptime(item['promised_delivery_date'], '%Y-%m-%d %H:%M:%S')
            
            # Determine quantity for this shipment
            if split_count == 1:
                qty = total_qty
            elif i == split_count - 1:
                # Last shipment gets remaining quantity
                qty = total_qty // split_count + (total_qty % split_count)
            else:
                qty = total_qty // split_count
            
            # Skip if quantity is 0
            if qty == 0:
                continue
            
            # Determine delivery status based on this item's promised date
            is_delayed = actual_arrival_timestamp > promised_date
            status = "delayed" if is_delayed else "delivered"
            
            # Add shipment item
            product_name = product_map.get(item['product_id'], '')
            shipment_items.append({
                'shipment_id': shipment_id,
                'order_id': order_id,
                'line_number': item['line_number'],
                'product_id': item['product_id'],
                'product_name': product_name,
                'quantity': qty,
                'carrier_name': carrier,
                'transportation_mode': transport_mode,
                'planned_ship_date': planned_ship_date.strftime('%Y-%m-%d'),
                'actual_ship_timestamp': actual_ship_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'expected_ship_date': expected_ship_date.strftime('%Y-%m-%d'),
                'actual_arrival_timestamp': actual_arrival_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'delivery_status': status
            })
    
    processed_count += 1
    if processed_count % 100 == 0:
        print(f"Processed {processed_count} orders...")

print(f"Generated {len(shipment_headers)} shipment headers")
print(f"Generated {len(shipment_items)} shipment items")

print("Exporting data...")

# Export shipment headers
with open(shipment_header_path, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['shipment_id', 'shipment_timestamp', 'location_id', 'customer_id'])
    writer.writeheader()
    writer.writerows(shipment_headers)

# Export shipment items
with open(shipment_item_path, 'w', encoding='utf-8', newline='') as f:
    fieldnames = ['shipment_id', 'order_id', 'line_number', 'product_id', 'product_name', 
                  'quantity', 'carrier_name', 'transportation_mode', 'planned_ship_date',
                  'actual_ship_timestamp', 'expected_ship_date', 'actual_arrival_timestamp', 'delivery_status']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(shipment_items)

print("Export completed successfully!")
