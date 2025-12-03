# -*- coding: utf-8 -*-
import csv
from datetime import datetime
from collections import defaultdict

# Paths
base_path = r"H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze"
order_header_path = f"{base_path}\\ERP\\受注伝票_header.csv"
order_item_path = f"{base_path}\\ERP\\受注伝票_item.csv"
shipment_header_path = f"{base_path}\\MES\\出荷伝票_header.csv"
shipment_item_path = f"{base_path}\\MES\\出荷伝票_item.csv"
report_path = r"H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\受注出荷整合性レポート.md"

print("Loading data...")

# Load order headers
order_headers = {}
with open(order_header_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        order_headers[row['order_id']] = row

# Load order items
order_items = []
order_items_by_order = defaultdict(list)
order_qty_summary = {}
with open(order_item_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        order_items.append(row)
        order_items_by_order[row['order_id']].append(row)
        key = (row['order_id'], row['line_number'], row['product_id'])
        if key not in order_qty_summary:
            order_qty_summary[key] = 0
        order_qty_summary[key] += int(row['quantity'])

# Load shipment headers
shipment_headers = {}
with open(shipment_header_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        shipment_headers[row['shipment_id']] = row

# Load shipment items
shipment_items = []
shipment_items_by_order = defaultdict(list)
shipment_qty_summary = {}
with open(shipment_item_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        shipment_items.append(row)
        shipment_items_by_order[row['order_id']].append(row)
        key = (row['order_id'], row['line_number'], row['product_id'])
        if key not in shipment_qty_summary:
            shipment_qty_summary[key] = 0
        shipment_qty_summary[key] += int(row['quantity'])

print("Analyzing data...")

# Validation checks
validation_results = {
    'total_orders': len(order_headers),
    'total_order_items': len(order_items),
    'total_shipments': len(shipment_headers),
    'total_shipment_items': len(shipment_items),
    'orders_with_shipments': 0,
    'orders_without_shipments': [],
    'quantity_matches': 0,
    'quantity_mismatches': [],
    'date_validations': [],
    'shipment_split_examples': []
}

# Check 1: All orders have shipments
orders_with_shipments = set(shipment_items_by_order.keys())
for order_id in order_headers.keys():
    if order_id in orders_with_shipments:
        validation_results['orders_with_shipments'] += 1
    else:
        validation_results['orders_without_shipments'].append(order_id)

# Check 2: Quantity consistency
for key, ordered_qty in order_qty_summary.items():
    order_id, line_number, product_id = key
    total_shipped = shipment_qty_summary.get(key, 0)
    
    if total_shipped == ordered_qty:
        validation_results['quantity_matches'] += 1
    else:
        validation_results['quantity_mismatches'].append({
            'order_id': order_id,
            'line_number': line_number,
            'product_id': product_id,
            'ordered_qty': ordered_qty,
            'shipped_qty': total_shipped
        })

# Check 3: Date validations (all shipments)
for order_id, order_header in order_headers.items():
    order_time = datetime.strptime(order_header['order_timestamp'], '%Y-%m-%d %H:%M:%S')
    shipments = shipment_items_by_order.get(order_id, [])
    
    # Check all shipments for this order
    checked_shipments = set()
    for shipment in shipments:
        shipment_id = shipment['shipment_id']
        if shipment_id in checked_shipments:
            continue
        checked_shipments.add(shipment_id)
        
        shipment_header = shipment_headers.get(shipment_id)
        if shipment_header:
            ship_time = datetime.strptime(shipment_header['shipment_timestamp'], '%Y-%m-%d %H:%M:%S')
            validation_results['date_validations'].append({
                'order_id': order_id,
                'order_timestamp': order_header['order_timestamp'],
                'shipment_id': shipment_id,
                'shipment_timestamp': shipment_header['shipment_timestamp'],
                'valid': ship_time >= order_time
            })

# Check 4: Shipment split examples
split_example_count = 0
for order_id in list(order_items_by_order.keys())[:20]:
    shipments = shipment_items_by_order.get(order_id, [])
    unique_shipments = len(set([s['shipment_id'] for s in shipments]))
    
    if unique_shipments > 1:
        order_item = order_items_by_order[order_id][0]
        validation_results['shipment_split_examples'].append({
            'order_id': order_id,
            'order_items_count': len(order_items_by_order[order_id]),
            'shipments_count': unique_shipments,
            'shipment_ids': list(set([s['shipment_id'] for s in shipments]))[:3]
        })
        split_example_count += 1
        if split_example_count >= 5:
            break

# Check 5: Product quantity validation per order
product_qty_validation = {}
product_qty_mismatches = []

for order_id in order_headers.keys():
    # Count ordered quantities by product
    order_product_qty = {}
    for item in order_items_by_order.get(order_id, []):
        product_id = item['product_id']
        if product_id not in order_product_qty:
            order_product_qty[product_id] = 0
        order_product_qty[product_id] += int(item['quantity'])
    
    # Count shipped quantities by product
    shipment_product_qty = {}
    for item in shipment_items_by_order.get(order_id, []):
        product_id = item['product_id']
        if product_id not in shipment_product_qty:
            shipment_product_qty[product_id] = 0
        shipment_product_qty[product_id] += int(item['quantity'])
    
    # Compare
    for product_id, ordered_qty in order_product_qty.items():
        shipped_qty = shipment_product_qty.get(product_id, 0)
        key = (order_id, product_id)
        product_qty_validation[key] = {
            'order_id': order_id,
            'product_id': product_id,
            'ordered_qty': ordered_qty,
            'shipped_qty': shipped_qty,
            'match': ordered_qty == shipped_qty
        }
        
        if ordered_qty != shipped_qty:
            product_qty_mismatches.append({
                'order_id': order_id,
                'product_id': product_id,
                'ordered_qty': ordered_qty,
                'shipped_qty': shipped_qty
            })

validation_results['product_qty_validation'] = product_qty_validation
validation_results['product_qty_mismatches'] = product_qty_mismatches

print("Generating report...")

# Generate report
with open(report_path, 'w', encoding='utf-8') as f:
    f.write("# 受注伝票と出荷伝票の整合性検証レポート\n\n")
    f.write(f"**生成日時**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
    f.write("---\n\n")
    
    # Summary
    f.write("## 1. データ概要\n\n")
    f.write("### 受注データ\n")
    f.write(f"- **受注伝票数**: {validation_results['total_orders']:,}件\n")
    f.write(f"- **受注明細数**: {validation_results['total_order_items']:,}件\n")
    f.write(f"- **期間**: 2022年1月2日 〜 2025年12月2日\n\n")
    
    f.write("### 出荷データ\n")
    f.write(f"- **出荷伝票数**: {validation_results['total_shipments']:,}件\n")
    f.write(f"- **出荷明細数**: {validation_results['total_shipment_items']:,}件\n\n")
    
    f.write("---\n\n")
    
    # Validation results
    f.write("## 2. 整合性検証結果\n\n")
    
    # Check 1
    f.write("### 2.1 受注に対する出荷の存在確認\n\n")
    coverage_rate = (validation_results['orders_with_shipments'] / validation_results['total_orders'] * 100)
    f.write(f"- **出荷データが存在する受注**: {validation_results['orders_with_shipments']:,}件 / {validation_results['total_orders']:,}件\n")
    f.write(f"- **カバレッジ率**: {coverage_rate:.2f}%\n")
    
    if validation_results['orders_without_shipments']:
        f.write(f"- **⚠️ 出荷データが存在しない受注**: {len(validation_results['orders_without_shipments'])}件\n")
        if len(validation_results['orders_without_shipments']) <= 10:
            f.write(f"  - 該当受注: {', '.join(validation_results['orders_without_shipments'])}\n")
    else:
        f.write("- **✅ 結果**: すべての受注に対して出荷データが存在します\n")
    
    f.write("\n")
    
    # Check 2
    f.write("### 2.2 受注数量と出荷数量の整合性\n\n")
    total_lines = validation_results['quantity_matches'] + len(validation_results['quantity_mismatches'])
    match_rate = (validation_results['quantity_matches'] / total_lines * 100) if total_lines > 0 else 0
    f.write(f"- **検証対象明細行数**: {total_lines:,}件\n")
    f.write(f"- **数量が一致**: {validation_results['quantity_matches']:,}件\n")
    f.write(f"- **数量が不一致**: {len(validation_results['quantity_mismatches'])}件\n")
    f.write(f"- **一致率**: {match_rate:.2f}%\n\n")
    
    if validation_results['quantity_mismatches']:
        f.write("**⚠️ 数量不一致の詳細（最大10件表示）**:\n\n")
        f.write("| 受注ID | 明細番号 | 品目ID | 受注数量 | 出荷数量 | 差異 |\n")
        f.write("|--------|----------|--------|----------|----------|------|\n")
        for mismatch in validation_results['quantity_mismatches'][:10]:
            diff = mismatch['shipped_qty'] - mismatch['ordered_qty']
            f.write(f"| {mismatch['order_id']} | {mismatch['line_number']} | {mismatch['product_id']} | {mismatch['ordered_qty']} | {mismatch['shipped_qty']} | {diff:+d} |\n")
    else:
        f.write("**✅ 結果**: すべての明細行で受注数量と出荷数量が一致しています\n")
    
    f.write("\n")
    
    # Check 3: Product quantity validation
    f.write("### 2.3 車種別受注数量と出荷数量の整合性\n\n")
    f.write("各受注ID内で、車種（product_id）ごとの受注数量と出荷数量が一致するかを検証します。\n\n")
    
    total_product_checks = len(validation_results['product_qty_validation'])
    product_match_count = sum(1 for v in validation_results['product_qty_validation'].values() if v['match'])
    product_mismatch_count = len(validation_results['product_qty_mismatches'])
    product_match_rate = (product_match_count / total_product_checks * 100) if total_product_checks > 0 else 0
    
    f.write(f"- **検証対象**: {total_product_checks:,}件（受注ID × 車種の組み合わせ）\n")
    f.write(f"- **数量が一致**: {product_match_count:,}件\n")
    f.write(f"- **数量が不一致**: {product_mismatch_count}件\n")
    f.write(f"- **一致率**: {product_match_rate:.2f}%\n\n")
    
    if product_mismatch_count == 0:
        f.write("**✅ 結果**: すべての受注で車種別の数量が一致しています\n\n")
        
        # Show all validations
        f.write("#### 検証結果（全件）\n\n")
        f.write("| 受注ID | 車種 | 受注数量 | 出荷数量 | 状態 |\n")
        f.write("|--------|------|----------|----------|------|\n")
        for key, validation in validation_results['product_qty_validation'].items():
            f.write(f"| {validation['order_id']} | {validation['product_id']} | {validation['ordered_qty']} | {validation['shipped_qty']} | ✅ |\n")
        f.write("\n")
    else:
        f.write("**⚠️ 結果**: 一部の受注で車種別の数量に不一致があります\n\n")
        f.write("#### 不一致の詳細\n\n")
        f.write("| 受注ID | 車種 | 受注数量 | 出荷数量 | 差分 |\n")
        f.write("|--------|------|----------|----------|------|\n")
        for mismatch in validation_results['product_qty_mismatches']:
            diff = mismatch['shipped_qty'] - mismatch['ordered_qty']
            f.write(f"| {mismatch['order_id']} | {mismatch['product_id']} | {mismatch['ordered_qty']} | {mismatch['shipped_qty']} | {diff:+d} |\n")
        f.write("\n")
    
    # Check 4: Date validation
    f.write("### 2.4 日時の整合性確認（全件）\n\n")
    f.write("出荷日時が受注日時より後であることを確認します。\n\n")
    
    valid_count = 0
    invalid_count = 0
    for validation in validation_results['date_validations']:
        if validation['valid']:
            valid_count += 1
        else:
            invalid_count += 1
    
    total_validations = len(validation_results['date_validations'])
    date_validation_rate = (valid_count / total_validations * 100) if total_validations > 0 else 0
    
    f.write(f"- **検証対象出荷数**: {total_validations:,}件\n")
    f.write(f"- **妥当（出荷日時 ≥ 受注日時）**: {valid_count:,}件\n")
    f.write(f"- **不正（出荷日時 < 受注日時）**: {invalid_count}件\n")
    f.write(f"- **妥当性確認率**: {date_validation_rate:.2f}%\n\n")
    
    if invalid_count > 0:
        f.write("**⚠️ 日時が不正な出荷（最大10件表示）**:\n\n")
        f.write("| 受注ID | 受注日時 | 出荷ID | 出荷日時 |\n")
        f.write("|--------|----------|--------|----------|\n")
        invalid_validations = [v for v in validation_results['date_validations'] if not v['valid']]
        for validation in invalid_validations[:10]:
            f.write(f"| {validation['order_id']} | {validation['order_timestamp']} | {validation['shipment_id']} | {validation['shipment_timestamp']} |\n")
        f.write("\n")
    else:
        f.write("**✅ 結果**: すべての出荷で日時の整合性が確認されました\n\n")
    
    f.write("---\n\n")
    
    # Conclusion
    f.write("## 3. 検証結果サマリー\n\n")
    
    all_checks_passed = (
        len(validation_results['orders_without_shipments']) == 0 and
        len(validation_results['quantity_mismatches']) == 0 and
        len(validation_results['product_qty_mismatches']) == 0 and
        valid_count == len(validation_results['date_validations'])
    )
    
    if all_checks_passed:
        f.write("### ✅ すべての検証項目に合格\n\n")
        f.write("受注伝票と出荷伝票のデータは整合性が取れており、以下の点が確認されました:\n\n")
        f.write("1. **完全カバレッジ**: すべての受注に対して出荷データが存在\n")
        f.write("2. **数量一致**: 受注数量と出荷数量が完全一致\n")
        f.write("3. **車種別数量一致**: 各受注内で車種ごとの数量が完全一致\n")
        f.write("4. **時系列整合性**: 出荷日時は受注日時より後\n\n")
    else:
        f.write("### ⚠️ 一部の検証項目で問題を検出\n\n")
        f.write("以下の項目について確認が必要です:\n\n")
        if validation_results['orders_without_shipments']:
            f.write(f"- 出荷データが存在しない受注: {len(validation_results['orders_without_shipments'])}件\n")
        if validation_results['quantity_mismatches']:
            f.write(f"- 数量が不一致の明細: {len(validation_results['quantity_mismatches'])}件\n")
        if validation_results['product_qty_mismatches']:
            f.write(f"- 車種別数量が不一致の受注: {len(validation_results['product_qty_mismatches'])}件\n")
        if valid_count < len(validation_results['date_validations']):
            f.write(f"- 日時の整合性が取れていない出荷: {len(validation_results['date_validations']) - valid_count}件\n")
    
    f.write("\n---\n\n")
    f.write("*このレポートは自動生成されました*\n")

print(f"Report generated: {report_path}")
print("Validation complete!")
