import pandas as pd

# ファイルパス
order_header_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_header.csv"
order_item_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_item.csv"
current_inventory_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\WMS\現在在庫.csv"

print("Loading data...")
order_headers = pd.read_csv(order_header_path, encoding='utf-8-sig')
order_items = pd.read_csv(order_item_path, encoding='utf-8-sig')
current_inventory = pd.read_csv(current_inventory_path, encoding='utf-8-sig')

print("\n=== December 2025 Orders ===")
order_headers['order_timestamp'] = pd.to_datetime(order_headers['order_timestamp'])
dec_2025_orders = order_headers[
    (order_headers['order_timestamp'].dt.year == 2025) & 
    (order_headers['order_timestamp'].dt.month == 12)
]

print(f"Total December orders: {len(dec_2025_orders)}")
print(f"Order IDs: {', '.join(dec_2025_orders['order_id'].tolist())}")

# 受注明細と結合
dec_orders_with_items = pd.merge(
    dec_2025_orders[['order_id', 'location_id']],
    order_items[['order_id', 'product_id', 'quantity']],
    on='order_id'
)

print("\n=== December Order Details ===")
print(dec_orders_with_items.to_string(index=False))

# 車種×拠点ごとの受注量を集計
dec_demand = dec_orders_with_items.groupby(['product_id', 'location_id'])['quantity'].sum().reset_index()
dec_demand.columns = ['product_id', 'location_id', 'order_quantity']

print("\n=== December Demand by Product-Location ===")
print(dec_demand.to_string(index=False))

# 現在在庫と照合
inventory_check = pd.merge(
    dec_demand,
    current_inventory[['product_id', 'location_id', 'inventory_quantity']],
    on=['product_id', 'location_id'],
    how='left'
)

inventory_check['sufficient'] = inventory_check['inventory_quantity'] >= inventory_check['order_quantity']
inventory_check['shortage'] = inventory_check.apply(
    lambda row: max(0, row['order_quantity'] - row['inventory_quantity']) if pd.notna(row['inventory_quantity']) else row['order_quantity'],
    axis=1
)

print("\n=== Inventory Sufficiency Check ===")
print(inventory_check.to_string(index=False))

print("\n=== Summary ===")
print(f"Total product-location combinations with December orders: {len(inventory_check)}")
print(f"Sufficient inventory: {inventory_check['sufficient'].sum()}")
print(f"Insufficient inventory: {(~inventory_check['sufficient']).sum()}")
print(f"Total shortage: {inventory_check['shortage'].sum()}")

print("\n=== Current Inventory Statistics ===")
print(f"Total inventory records: {len(current_inventory)}")
print(f"Total inventory quantity: {current_inventory['inventory_quantity'].sum()}")
print(f"Average inventory per record: {current_inventory['inventory_quantity'].mean():.1f}")
print(f"Min inventory: {current_inventory['inventory_quantity'].min()}")
print(f"Max inventory: {current_inventory['inventory_quantity'].max()}")
