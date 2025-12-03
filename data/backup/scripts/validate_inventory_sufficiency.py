# -*- coding: utf-8 -*-
import csv
from datetime import datetime
from collections import defaultdict

# Paths
base_path = r"H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze"
order_header_path = f"{base_path}\\ERP\\受注伝票_header.csv"
order_item_path = f"{base_path}\\ERP\\受注伝票_item.csv"
inventory_path = f"{base_path}\\WMS\\月次在庫履歴.csv"
report_path = r"H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\在庫充足性検証レポート.md"

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

# Load inventory history
inventory_records = []
with open(inventory_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        inventory_records.append(row)

print("Analyzing data...")

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

# Build inventory index
# Structure: {(product_id, location_id, year_month): inventory_quantity}
inventory_index = {}

for record in inventory_records:
    key = (record['product_id'], record['location_id'], record['year_month'])
    inventory_index[key] = int(record['inventory_quantity'])

print("Validating inventory sufficiency...")

# Validation results
validation_results = {
    'total_checks': 0,
    'sufficient': 0,
    'insufficient': [],
    'no_inventory_data': [],
    'zero_demand': 0
}

# Check each product-location-month combination
for (product_id, location_id, year_month), demand in monthly_demand.items():
    validation_results['total_checks'] += 1
    
    if demand == 0:
        validation_results['zero_demand'] += 1
        continue
    
    # Get inventory for this combination
    inventory = inventory_index.get((product_id, location_id, year_month))
    
    if inventory is None:
        # No inventory data for this combination
        validation_results['no_inventory_data'].append({
            'product_id': product_id,
            'location_id': location_id,
            'year_month': year_month,
            'demand': demand
        })
    elif inventory < demand:
        # Insufficient inventory
        validation_results['insufficient'].append({
            'product_id': product_id,
            'location_id': location_id,
            'year_month': year_month,
            'demand': demand,
            'inventory': inventory,
            'shortage': demand - inventory
        })
    else:
        # Sufficient inventory
        validation_results['sufficient'] += 1

# Calculate inventory turnover statistics
inventory_turnover_stats = []

for record in inventory_records:
    product_id = record['product_id']
    location_id = record['location_id']
    year_month = record['year_month']
    inventory = int(record['inventory_quantity'])
    
    key = (product_id, location_id, year_month)
    demand = monthly_demand.get(key, 0)
    
    if demand > 0:
        # Calculate inventory turnover days: (inventory / demand) * 30
        turnover_days = (inventory / demand) * 30
        inventory_turnover_stats.append({
            'product_id': product_id,
            'location_id': location_id,
            'year_month': year_month,
            'inventory': inventory,
            'demand': demand,
            'turnover_days': turnover_days
        })

# Calculate average turnover days
if inventory_turnover_stats:
    avg_turnover = sum(s['turnover_days'] for s in inventory_turnover_stats) / len(inventory_turnover_stats)
    min_turnover = min(s['turnover_days'] for s in inventory_turnover_stats)
    max_turnover = max(s['turnover_days'] for s in inventory_turnover_stats)
else:
    avg_turnover = 0
    min_turnover = 0
    max_turnover = 0

print("Generating report...")

# Generate report
with open(report_path, 'w', encoding='utf-8') as f:
    f.write("# 在庫充足性検証レポート\n\n")
    f.write(f"**生成日時**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
    f.write("---\n\n")
    
    # Summary
    f.write("## 1. 検証結果サマリー\n\n")
    f.write(f"- **検証対象**: {validation_results['total_checks']:,}件（拠点×車種×月の組み合わせ）\n")
    f.write(f"- **在庫充足**: {validation_results['sufficient']:,}件\n")
    f.write(f"- **在庫不足**: {len(validation_results['insufficient'])}件\n")
    f.write(f"- **在庫データなし**: {len(validation_results['no_inventory_data'])}件\n")
    f.write(f"- **受注なし（需要ゼロ）**: {validation_results['zero_demand']:,}件\n\n")
    
    sufficiency_rate = (validation_results['sufficient'] / validation_results['total_checks'] * 100) if validation_results['total_checks'] > 0 else 0
    f.write(f"### 在庫充足率: {sufficiency_rate:.2f}%\n\n")
    
    if len(validation_results['insufficient']) == 0 and len(validation_results['no_inventory_data']) == 0:
        f.write("### ✅ すべての受注に対して在庫が充足しています\n\n")
    else:
        f.write("### ⚠️ 一部で在庫不足または在庫データ欠損があります\n\n")
    
    f.write("---\n\n")
    
    # Inventory shortage details
    if validation_results['insufficient']:
        f.write("## 2. 在庫不足の詳細\n\n")
        f.write(f"在庫不足が発生している組み合わせ: {len(validation_results['insufficient'])}件\n\n")
        f.write("| 拠点 | 車種 | 年月 | 受注量 | 在庫量 | 不足量 |\n")
        f.write("|------|------|------|--------|--------|--------|\n")
        
        for shortage in validation_results['insufficient'][:20]:
            f.write(f"| {shortage['location_id']} | {shortage['product_id']} | {shortage['year_month']} | "
                   f"{shortage['demand']} | {shortage['inventory']} | {shortage['shortage']} |\n")
        
        if len(validation_results['insufficient']) > 20:
            f.write(f"\n*他 {len(validation_results['insufficient']) - 20}件の在庫不足があります*\n")
        
        f.write("\n---\n\n")
    
    # Missing inventory data
    if validation_results['no_inventory_data']:
        f.write("## 3. 在庫データ欠損の詳細\n\n")
        f.write(f"在庫データが存在しない組み合わせ: {len(validation_results['no_inventory_data'])}件\n\n")
        f.write("| 拠点 | 車種 | 年月 | 受注量 |\n")
        f.write("|------|------|------|--------|\n")
        
        for missing in validation_results['no_inventory_data'][:20]:
            f.write(f"| {missing['location_id']} | {missing['product_id']} | {missing['year_month']} | {missing['demand']} |\n")
        
        if len(validation_results['no_inventory_data']) > 20:
            f.write(f"\n*他 {len(validation_results['no_inventory_data']) - 20}件の在庫データ欠損があります*\n")
        
        f.write("\n---\n\n")
    
    # Inventory turnover statistics
    f.write("## 4. 在庫回転日数の分析\n\n")
    f.write(f"- **平均在庫回転日数**: {avg_turnover:.1f}日\n")
    f.write(f"- **最小在庫回転日数**: {min_turnover:.1f}日\n")
    f.write(f"- **最大在庫回転日数**: {max_turnover:.1f}日\n")
    f.write(f"- **目標範囲**: 35～50日\n\n")
    
    # Sample of turnover statistics
    f.write("### 在庫回転日数サンプル（10件）\n\n")
    f.write("| 拠点 | 車種 | 年月 | 在庫量 | 受注量 | 回転日数 |\n")
    f.write("|------|------|------|--------|--------|----------|\n")
    
    for stat in inventory_turnover_stats[:10]:
        f.write(f"| {stat['location_id']} | {stat['product_id']} | {stat['year_month']} | "
               f"{stat['inventory']} | {stat['demand']} | {stat['turnover_days']:.1f}日 |\n")
    
    f.write("\n---\n\n")
    
    # Detailed validation by product and location
    f.write("## 5. 拠点×車種別の検証結果\n\n")
    
    # Group by product and location
    product_location_summary = defaultdict(lambda: {'sufficient': 0, 'insufficient': 0, 'total_months': 0})
    
    for (product_id, location_id, year_month), demand in monthly_demand.items():
        if demand == 0:
            continue
        
        key = (product_id, location_id)
        product_location_summary[key]['total_months'] += 1
        
        inventory = inventory_index.get((product_id, location_id, year_month))
        
        if inventory is not None and inventory >= demand:
            product_location_summary[key]['sufficient'] += 1
        else:
            product_location_summary[key]['insufficient'] += 1
    
    f.write("| 拠点 | 車種 | 総月数 | 充足月数 | 不足月数 | 充足率 |\n")
    f.write("|------|------|--------|----------|----------|--------|\n")
    
    for (product_id, location_id), summary in sorted(product_location_summary.items()):
        sufficiency_rate = (summary['sufficient'] / summary['total_months'] * 100) if summary['total_months'] > 0 else 0
        status = "✅" if summary['insufficient'] == 0 else "⚠️"
        f.write(f"| {location_id} | {product_id} | {summary['total_months']} | "
               f"{summary['sufficient']} | {summary['insufficient']} | {sufficiency_rate:.1f}% {status} |\n")
    
    f.write("\n---\n\n")
    f.write("*このレポートは自動生成されました*\n")

print(f"Report generated: {report_path}")
print("Validation complete!")
