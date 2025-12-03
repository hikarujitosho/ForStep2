import csv

item_file = r'H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\backup\調達伝票_item.csv'

with open(item_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    items = list(reader)

# 間接材のみフィルター
indirect_items = [item for item in items if item['material_type'] == 'indirect']

print(f'総レコード数: {len(items)}件')
print(f'間接材レコード数: {len(indirect_items)}件')
print(f'直接材レコード数: {len(items) - len(indirect_items)}件')
print()

# 間接材のPO IDをリストアップ
indirect_po_ids = sorted(set(item['purchase_order_id'] for item in indirect_items))
print(f'間接材のPO数: {len(indirect_po_ids)}件')
print('間接材のPO一覧:')
for po_id in indirect_po_ids:
    po_items = [item for item in indirect_items if item['purchase_order_id'] == po_id]
    total = sum(int(item['line_total_incl_tax']) for item in po_items)
    print(f'  {po_id}: {len(po_items)}明細, 合計¥{total:,}')
