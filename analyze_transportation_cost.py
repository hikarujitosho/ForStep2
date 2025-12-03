import csv
from datetime import datetime
import calendar

# ファイルパス
order_item_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_item.csv'
procurement_item_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P\調達伝票_item.csv'
transportation_cost_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\TMS\輸送コスト.csv'
payroll_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\HR\給与テーブル.csv'
price_condition_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\条件マスタ.csv'
order_header_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_header.csv'

# 受注ヘッダーを読み込み（顧客情報取得用）
with open(order_header_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    order_headers = {row['order_id']: row for row in reader}

# 条件マスタを読み込み（販売価格取得用）
with open(price_condition_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    price_conditions = list(reader)

# 価格条件のインデックスを作成（product_id + customer_id + 日付範囲）
def get_price(product_id, customer_id, pricing_date):
    """指定された商品・顧客・日付に対する販売価格を取得"""
    pricing_dt = datetime.strptime(pricing_date, '%Y-%m-%d')
    
    for condition in price_conditions:
        if (condition['product_id'] == product_id and 
            condition['customer_id'] == customer_id):
            valid_from = datetime.strptime(condition['valid_from'], '%Y-%m-%d')
            valid_to = datetime.strptime(condition['valid_to'], '%Y-%m-%d')
            
            if valid_from <= pricing_dt <= valid_to:
                return float(condition['selling_price_ex_tax'])
    
    return None

# 受注データを読み込み（売上）
with open(order_item_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    order_items = list(reader)

# 調達データを読み込み（原価）
with open(procurement_item_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    procurement_items = list(reader)

# 輸送コストデータを読み込み
with open(transportation_cost_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    transportation_costs = list(reader)

# 給与データを読み込み（人件費＝販管費の一部）
with open(payroll_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    payroll_data = list(reader)

print('=' * 80)
print('輸送コスト分析レポート')
print('=' * 80)
print()

# 1. 売上の計算
total_revenue = 0
tax_rate = 0.10  # 消費税率10%

for item in order_items:
    product_id = item['product_id']
    quantity = int(item['quantity'])
    pricing_date = item['pricing_date']
    order_id = item['order_id']
    
    # 顧客IDを取得
    if order_id in order_headers:
        customer_id = order_headers[order_id]['customer_id']
        
        # 販売価格を取得
        price_ex_tax = get_price(product_id, customer_id, pricing_date)
        
        if price_ex_tax:
            price_incl_tax = price_ex_tax * (1 + tax_rate)
            revenue = quantity * price_incl_tax
            total_revenue += revenue

print(f'【売上高】')
print(f'  総売上高（税込）: ¥{total_revenue:,.0f}')
print()

# 2. 原価の計算（調達費用）
total_procurement_direct = 0
total_procurement_indirect = 0
for item in procurement_items:
    line_total = float(item['line_total_incl_tax'])
    material_type = item['material_type']
    
    if material_type == 'direct':
        total_procurement_direct += line_total
    elif material_type == 'indirect':
        total_procurement_indirect += line_total

total_procurement = total_procurement_direct + total_procurement_indirect

print(f'【調達原価】')
print(f'  直接材調達費: ¥{total_procurement_direct:,.0f}')
print(f'  間接材調達費: ¥{total_procurement_indirect:,.0f}')
print(f'  総調達費: ¥{total_procurement:,.0f}')
print()

# 3. 人件費の計算（販管費の主要項目）
total_payroll = 0
manufacturing_payroll = 0
non_manufacturing_payroll = 0

for record in payroll_data:
    gross_salary = float(record['base_salary']) + float(record['overtime_pay']) + float(record['allowances'])
    total_payroll += gross_salary
    
    if record['department'] == 'manufacturing':
        manufacturing_payroll += gross_salary
    else:
        non_manufacturing_payroll += gross_salary

print(f'【人件費】')
print(f'  製造部門人件費: ¥{manufacturing_payroll:,.0f}')
print(f'  非製造部門人件費: ¥{non_manufacturing_payroll:,.0f}')
print(f'  総人件費: ¥{total_payroll:,.0f}')
print()

# 4. 輸送コストの計算
total_transportation_cost = 0
freight_cost = 0
expedite_cost = 0

for cost in transportation_costs:
    amount = float(cost['cost_amount'])
    cost_type = cost['cost_type']
    
    total_transportation_cost += amount
    
    if cost_type == 'freight':
        freight_cost += amount
    elif cost_type == 'expedite':
        expedite_cost += amount

print(f'【輸送コスト】')
print(f'  通常運賃（freight）: ¥{freight_cost:,.0f}')
print(f'  緊急輸送費（expedite）: ¥{expedite_cost:,.0f}')
print(f'  総輸送コスト: ¥{total_transportation_cost:,.0f}')
print()

# 5. 売上原価の推定（直接材 + 製造人件費 + 輸送コスト）
estimated_cogs = total_procurement_direct + manufacturing_payroll + total_transportation_cost

# 6. 販管費の推定（間接材 + 非製造人件費）
estimated_sga = total_procurement_indirect + non_manufacturing_payroll

print('=' * 80)
print('【財務指標分析】')
print('=' * 80)
print()

print(f'売上高:           ¥{total_revenue:>15,.0f}  (100.0%)')
print(f'売上原価（推定）: ¥{estimated_cogs:>15,.0f}  ({estimated_cogs/total_revenue*100:>5.1f}%)')
print(f'  - 直接材費:     ¥{total_procurement_direct:>15,.0f}  ({total_procurement_direct/total_revenue*100:>5.1f}%)')
print(f'  - 製造人件費:   ¥{manufacturing_payroll:>15,.0f}  ({manufacturing_payroll/total_revenue*100:>5.1f}%)')
print(f'  - 輸送コスト:   ¥{total_transportation_cost:>15,.0f}  ({total_transportation_cost/total_revenue*100:>5.1f}%)')
print(f'販管費（推定）:   ¥{estimated_sga:>15,.0f}  ({estimated_sga/total_revenue*100:>5.1f}%)')
print(f'  - 間接材費:     ¥{total_procurement_indirect:>15,.0f}  ({total_procurement_indirect/total_revenue*100:>5.1f}%)')
print(f'  - 非製造人件費: ¥{non_manufacturing_payroll:>15,.0f}  ({non_manufacturing_payroll/total_revenue*100:>5.1f}%)')
print()

gross_profit = total_revenue - estimated_cogs
operating_profit = gross_profit - estimated_sga

print(f'粗利益（推定）:   ¥{gross_profit:>15,.0f}  ({gross_profit/total_revenue*100:>5.1f}%)')
print(f'営業利益（推定）: ¥{operating_profit:>15,.0f}  ({operating_profit/total_revenue*100:>5.1f}%)')
print()

print('=' * 80)
print('【輸送コスト分析】')
print('=' * 80)
print()

print(f'輸送コストの売上高比率:     {total_transportation_cost/total_revenue*100:.2f}%')
print(f'輸送コストの売上原価比率:   {total_transportation_cost/estimated_cogs*100:.2f}%')
print(f'輸送コストの調達費比率:     {total_transportation_cost/total_procurement*100:.2f}%')
print()

print(f'緊急輸送費の輸送コスト比率: {expedite_cost/total_transportation_cost*100:.2f}%')
print(f'緊急輸送費の売上高比率:     {expedite_cost/total_revenue*100:.2f}%')
print()

print('=' * 80)
print('【コスト構造分析】')
print('=' * 80)
print()

total_cost = estimated_cogs + estimated_sga

print(f'総コスト: ¥{total_cost:,.0f}')
print()
print(f'コスト構成比:')
print(f'  直接材費:       {total_procurement_direct/total_cost*100:>5.1f}%')
print(f'  間接材費:       {total_procurement_indirect/total_cost*100:>5.1f}%')
print(f'  製造人件費:     {manufacturing_payroll/total_cost*100:>5.1f}%')
print(f'  非製造人件費:   {non_manufacturing_payroll/total_cost*100:>5.1f}%')
print(f'  輸送コスト:     {total_transportation_cost/total_cost*100:>5.1f}%')
print()

print('=' * 80)
print('【年度別輸送コスト推移】')
print('=' * 80)
print()

# 年度別集計
yearly_data = {}
for cost in transportation_costs:
    billing_date = cost['billing_date']
    year = billing_date[:4]
    amount = float(cost['cost_amount'])
    
    if year not in yearly_data:
        yearly_data[year] = {'freight': 0, 'expedite': 0, 'total': 0}
    
    yearly_data[year]['total'] += amount
    if cost['cost_type'] == 'freight':
        yearly_data[year]['freight'] += amount
    elif cost['cost_type'] == 'expedite':
        yearly_data[year]['expedite'] += amount

for year in sorted(yearly_data.keys()):
    data = yearly_data[year]
    print(f"{year}年:")
    print(f"  総輸送コスト: ¥{data['total']:>12,.0f}")
    print(f"  - 通常運賃:   ¥{data['freight']:>12,.0f}  ({data['freight']/data['total']*100:.1f}%)")
    print(f"  - 緊急輸送費: ¥{data['expedite']:>12,.0f}  ({data['expedite']/data['total']*100:.1f}%)")
    print()

print('=' * 80)
