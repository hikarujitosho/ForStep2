import pandas as pd
import csv
import random
from datetime import datetime

# ファイルパス
order_header_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_header.csv"
order_item_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_item.csv"
monthly_inventory_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\WMS\月次在庫履歴.csv"
product_master_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\品目マスタ.csv"
output_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\WMS\現在在庫.csv"

print("Loading data...")

# データ読み込み
order_headers = pd.read_csv(order_header_path, encoding='utf-8-sig')
order_items = pd.read_csv(order_item_path, encoding='utf-8-sig')
monthly_inventory = pd.read_csv(monthly_inventory_path, encoding='utf-8-sig')
product_master = pd.read_csv(product_master_path, encoding='utf-8-sig')

# 品目マスタから product_id -> product_name のマッピング
product_names = dict(zip(product_master['product_id'], product_master['product_name']))

print("Analyzing December 2025 orders...")

# 12月の受注データを抽出
order_headers['order_timestamp'] = pd.to_datetime(order_headers['order_timestamp'])
dec_2025_orders = order_headers[
    (order_headers['order_timestamp'].dt.year == 2025) & 
    (order_headers['order_timestamp'].dt.month == 12)
]

# 受注明細と結合
dec_orders_with_items = pd.merge(
    dec_2025_orders[['order_id', 'location_id']],
    order_items[['order_id', 'product_id', 'quantity']],
    on='order_id'
)

# 12月の車種×拠点ごとの受注量を集計
dec_demand = dec_orders_with_items.groupby(['product_id', 'location_id'])['quantity'].sum().to_dict()

print(f"Found {len(dec_demand)} product-location combinations with December orders")

print("Analyzing November 2025 inventory...")

# 11月末の在庫データを取得
nov_inventory = monthly_inventory[monthly_inventory['year_month'] == '2025-11'].copy()
nov_inventory_dict = {}
for _, row in nov_inventory.iterrows():
    key = (row['product_id'], row['location_id'])
    nov_inventory_dict[key] = row['inventory_quantity']

print(f"Found {len(nov_inventory_dict)} product-location combinations in November inventory")

print("Generating current inventory...")

# 現在在庫レコードを生成
current_inventory = []
snapshot_date = "2025-12-15"
snapshot_timestamp = "2025-12-15 08:00:00"

# すべての車種×拠点の組み合わせを取得（11月在庫から）
all_combinations = set(nov_inventory_dict.keys())

# 12月受注がある組み合わせを追加
for key in dec_demand.keys():
    all_combinations.add(key)

print(f"Processing {len(all_combinations)} product-location combinations")

for product_id, location_id in sorted(all_combinations):
    product_name = product_names.get(product_id, product_id)
    
    # 12月の受注量を確認
    dec_quantity = dec_demand.get((product_id, location_id), 0)
    
    if dec_quantity > 0:
        # 12月受注がある場合：受注量に対して十分な在庫（受注量と同じ）
        inventory_quantity = dec_quantity
    else:
        # 12月受注がない場合：11月末在庫に近いランダムな数量
        nov_quantity = nov_inventory_dict.get((product_id, location_id), 0)
        if nov_quantity > 0:
            # 11月在庫の±20%の範囲でランダム
            min_qty = max(0, int(nov_quantity * 0.8))
            max_qty = int(nov_quantity * 1.2)
            inventory_quantity = random.randint(min_qty, max_qty)
        else:
            # 11月在庫がない場合はスキップ
            continue
    
    record = {
        'product_id': product_id,
        'product_name': product_name,
        'location_id': location_id,
        'inventory_quantity': inventory_quantity,
        'inventory_status': 'available',
        'last_updated_timestamp': snapshot_timestamp
    }
    
    current_inventory.append(record)

print(f"Generated {len(current_inventory)} current inventory records")

# CSVに書き込み
print("Writing to CSV...")
df_output = pd.DataFrame(current_inventory)
df_output = df_output.sort_values(['location_id', 'product_id'])
df_output.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"Successfully generated {len(current_inventory)} current inventory records")
print(f"Output file: {output_path}")

# サマリー表示
print("\n=== Summary ===")
print(f"Records with December orders: {sum(1 for k in current_inventory if dec_demand.get((k['product_id'], k['location_id']), 0) > 0)}")
print(f"Records without December orders: {sum(1 for k in current_inventory if dec_demand.get((k['product_id'], k['location_id']), 0) == 0)}")
print(f"Total records: {len(current_inventory)}")
