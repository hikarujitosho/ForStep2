# -*- coding: utf-8 -*-
import csv
from datetime import datetime, timedelta
from collections import defaultdict
import random
from dateutil.relativedelta import relativedelta

# Paths
base_path = r"H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze"
order_header_path = f"{base_path}\\ERP\\受注伝票_header.csv"
order_item_path = f"{base_path}\\ERP\\受注伝票_item.csv"
product_master_path = f"{base_path}\\ERP\\品目マスタ.csv"
output_path = f"{base_path}\\WMS\\月次在庫履歴.csv"

print("Loading data...")

# Load order headers
order_headers = {}
with open(order_header_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        order_headers[row['order_id']] = row

# Load order items
order_items = []
with open(order_item_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        order_items.append(row)

# Load product master for product names
product_names = {}
with open(product_master_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        product_names[row['product_id']] = row['product_name']

print("Analyzing orders...")

# Calculate monthly demand by product and location
# Structure: {(product_id, location_id, year_month): total_quantity}
monthly_demand = defaultdict(int)

for item in order_items:
    order_id = item['order_id']
    order_header = order_headers.get(order_id)
    
    if not order_header:
        continue
    
    order_date = datetime.strptime(order_header['order_timestamp'], '%Y-%m-%d %H:%M:%S')
    year_month = order_date.strftime('%Y-%m')
    
    product_id = item['product_id']
    location_id = order_header['location_id']
    quantity = int(item['quantity'])
    
    key = (product_id, location_id, year_month)
    monthly_demand[key] += quantity

print(f"Found {len(monthly_demand)} unique product-location-month combinations")

# Generate monthly inventory history
print("Generating monthly inventory data...")

# Get all unique product-location combinations
product_location_pairs = set()
for (product_id, location_id, year_month) in monthly_demand.keys():
    product_location_pairs.add((product_id, location_id))

print(f"Processing {len(product_location_pairs)} product-location pairs")

# Generate monthly inventory records from 2022-01 to 2025-11
start_date = datetime(2022, 1, 1)
end_date = datetime(2025, 11, 30)

inventory_records = []

# Initialize inventory for each product-location pair
# Structure: {(product_id, location_id): current_inventory}
current_inventory = {}

# Process each month
current_month = start_date
while current_month <= end_date:
    year_month = current_month.strftime('%Y-%m')
    
    # Get last day of month
    if current_month.month == 12:
        last_day = current_month.replace(day=31)
    else:
        next_month = current_month.replace(day=28) + timedelta(days=4)
        last_day = next_month - timedelta(days=next_month.day)
    
    snapshot_date = last_day.strftime('%Y-%m-%d')
    
    for product_id, location_id in product_location_pairs:
        key = (product_id, location_id, year_month)
        demand = monthly_demand.get(key, 0)
        
        # Get current inventory level
        inv_key = (product_id, location_id)
        
        if inv_key not in current_inventory:
            # Initialize inventory for first month
            # Set initial inventory based on first month's demand with ~30 days turnover
            if demand > 0:
                # Target inventory turnover days: 30 days (1 month supply)
                target_days = 30
                # Calculate month-end inventory: demand * (target_days / 30) = demand
                initial_inventory = demand
                current_inventory[inv_key] = initial_inventory
            else:
                # If no demand in first month, set a minimal inventory
                current_inventory[inv_key] = random.randint(10, 20)
        
        # Calculate inventory after fulfilling orders
        inventory_after_orders = current_inventory[inv_key] - demand
        
        # Replenish to maintain ~30 days inventory turnover
        if demand > 0:
            # Target inventory turnover days: 30 days
            target_days = 30
            target_inventory = demand  # 1 month supply
            
            # Replenish if needed
            if inventory_after_orders < target_inventory:
                replenishment = target_inventory - inventory_after_orders
                month_end_inventory = target_inventory
            else:
                # No replenishment needed
                month_end_inventory = inventory_after_orders
        else:
            # No demand this month, gradually reduce inventory
            month_end_inventory = max(inventory_after_orders - random.randint(0, 2), 0)
        
        # Ensure non-negative inventory
        if month_end_inventory < 0:
            month_end_inventory = 0
        
        # Update current inventory for next month
        current_inventory[inv_key] = month_end_inventory
        
        # Create inventory record
        product_name = product_names.get(product_id, product_id)
        
        record = {
            'product_id': product_id,
            'product_name': product_name,
            'location_id': location_id,
            'year_month': year_month,
            'inventory_quantity': month_end_inventory,
            'inventory_status': 'available',
            'snapshot_date': snapshot_date
        }
        
        inventory_records.append(record)
    
    # Move to next month
    current_month = current_month + relativedelta(months=1)

print(f"Generated {len(inventory_records)} inventory records")

# Write to CSV
print("Writing to CSV...")
with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
    fieldnames = ['product_id', 'product_name', 'location_id', 'year_month', 
                  'inventory_quantity', 'inventory_status', 'snapshot_date']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(inventory_records)

print(f"Successfully generated {len(inventory_records)} inventory records")
print(f"Output file: {output_path}")
