import csv

item_file = r'H:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\backup\調達伝票_item.csv'

with open(item_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    items = list(reader)

# 直接材と間接材に分類
direct_items = [item for item in items if item['material_type'] == 'direct']
indirect_items = [item for item in items if item['material_type'] == 'indirect']

# 金額集計
direct_total = sum(int(item['line_total_incl_tax']) for item in direct_items)
indirect_total = sum(int(item['line_total_incl_tax']) for item in indirect_items)
grand_total = direct_total + indirect_total

# 比率計算
indirect_ratio = (indirect_total / direct_total * 100) if direct_total > 0 else 0
direct_ratio = (direct_total / grand_total * 100) if grand_total > 0 else 0
indirect_ratio_to_total = (indirect_total / grand_total * 100) if grand_total > 0 else 0

print('【調達金額サマリー】')
print()
print(f'直接材（Direct Materials）:')
print(f'  明細数: {len(direct_items):,}件')
print(f'  合計金額: ¥{direct_total:,}')
print(f'  全体比率: {direct_ratio:.1f}%')
print()
print(f'間接材（Indirect Materials）:')
print(f'  明細数: {len(indirect_items):,}件')
print(f'  合計金額: ¥{indirect_total:,}')
print(f'  全体比率: {indirect_ratio_to_total:.1f}%')
print(f'  直接材に対する比率: {indirect_ratio:.2f}%')
print()
print(f'総合計:')
print(f'  総明細数: {len(items):,}件')
print(f'  総調達金額: ¥{grand_total:,}')
print()

# カテゴリ別集計（間接材）
print('【間接材カテゴリ別内訳】')
category_totals = {}
for item in indirect_items:
    category = item['material_category']
    amount = int(item['line_total_incl_tax'])
    if category not in category_totals:
        category_totals[category] = {'count': 0, 'amount': 0}
    category_totals[category]['count'] += 1
    category_totals[category]['amount'] += amount

# 金額順にソート
sorted_categories = sorted(category_totals.items(), key=lambda x: x[1]['amount'], reverse=True)

for category, data in sorted_categories:
    ratio = (data['amount'] / indirect_total * 100) if indirect_total > 0 else 0
    print(f'  {category}: ¥{data["amount"]:,} ({data["count"]}件, {ratio:.1f}%)')
