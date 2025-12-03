import csv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import random

# ファイルパス
order_header_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_header.csv'
order_item_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\受注伝票_item.csv'
output_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\HR\給与テーブル.csv'

# 受注データを読み込み
with open(order_header_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    orders = list(reader)

with open(order_item_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    order_items = list(reader)

# 拠点と年月ごとの受注数量を集計
location_monthly_orders = {}
for order in orders:
    order_date = datetime.strptime(order['order_timestamp'], '%Y-%m-%d %H:%M:%S')
    year_month = order_date.strftime('%Y-%m')
    location = order['location_id']
    order_id = order['order_id']
    
    key = (location, year_month)
    if key not in location_monthly_orders:
        location_monthly_orders[key] = {'count': 0, 'total_qty': 0}
    
    location_monthly_orders[key]['count'] += 1
    
    # その注文の総数量を集計
    order_qty = sum(int(item['quantity']) for item in order_items if item['order_id'] == order_id)
    location_monthly_orders[key]['total_qty'] += order_qty

# 拠点マッピング
locations = ['KMM', 'HTB', 'STM', 'SZK', 'HMM']

# 部門と役職の定義
departments = {
    'manufacturing': {'positions': ['operator', 'supervisor', 'engineer'], 'cost_center': 'CC-005'},
    'sales': {'positions': ['sales_rep', 'manager'], 'cost_center': 'CC-002'},
    'R&D': {'positions': ['researcher', 'senior_researcher'], 'cost_center': 'CC-004'},
    'administration': {'positions': ['staff', 'manager'], 'cost_center': 'CC-001'}
}

# 基本給の範囲（部門別）
base_salary_ranges = {
    'manufacturing': (280000, 350000),
    'sales': (300000, 400000),
    'R&D': (350000, 450000),
    'administration': (320000, 380000)
}

# 残業代の基本範囲（製造部門以外）
overtime_base_range = (30000, 80000)

# 日本人の名前リスト
japanese_names = [
    '田中太郎', '鈴木花子', '佐藤健一', '高橋美咲', '渡辺誠',
    '伊藤陽子', '山本大輔', '中村麻美', '小林優太', '加藤絵里',
    '吉田拓也', '山田真理子', '佐々木翔', '井上明日香', '木村雄一',
    '松本由美', '林健太郎', '清水千尋', '森本祐介', '池田愛美'
]

# 社員マスタを生成
employees = []
emp_id = 1

for location in locations:
    for dept_name in departments.keys():
        # 各拠点・各部門に1名配置
        dept_info = departments[dept_name]
        position = random.choice(dept_info['positions'])
        base_salary = random.randint(base_salary_ranges[dept_name][0], base_salary_ranges[dept_name][1])
        
        employee = {
            'employee_id': f'EMP-{location}-{dept_name[:3].upper()}-{emp_id:03d}',
            'employee_name': japanese_names[emp_id - 1],
            'department': dept_name,
            'position': position,
            'location_id': location,
            'base_salary': base_salary,
            'cost_center': dept_info['cost_center']
        }
        employees.append(employee)
        emp_id += 1

print(f'社員数: {len(employees)}名')
print(f'拠点数: {len(locations)}')
print(f'部門数: {len(departments)}')

# 給与データを生成
payroll_records = []
start_date = datetime(2022, 1, 1)
end_date = datetime(2025, 12, 31)
current_date = start_date

payroll_id_counter = 1

while current_date <= end_date:
    year_month = current_date.strftime('%Y-%m')
    payment_date = datetime(current_date.year, current_date.month, 25)
    
    for emp in employees:
        # 基本給
        base_salary = emp['base_salary']
        
        # 残業代計算
        if emp['department'] == 'manufacturing':
            # 製造部門：受注量に応じて変動
            key = (emp['location_id'], year_month)
            if key in location_monthly_orders:
                order_count = location_monthly_orders[key]['count']
                total_qty = location_monthly_orders[key]['total_qty']
                
                # 受注件数ベースで残業代を計算（1件あたり3,000-5,000円）
                overtime_per_order = random.randint(3000, 5000)
                overtime_pay = order_count * overtime_per_order
                
                # 数量ベースの追加（数量が多い場合はさらに増加）
                if total_qty > 50:
                    overtime_pay += int((total_qty - 50) * random.randint(300, 600))
                
                # 上限・下限を設定
                overtime_pay = max(20000, min(overtime_pay, 150000))
            else:
                # 受注がない月は最低限の残業代
                overtime_pay = random.randint(20000, 40000)
        else:
            # 他部門：固定範囲内でランダム
            overtime_pay = random.randint(overtime_base_range[0], overtime_base_range[1])
        
        # 諸手当
        commute_allowance = random.randint(10000, 20000)
        housing_allowance = random.randint(20000, 30000) if random.random() > 0.3 else 0
        family_allowance = random.randint(10000, 20000) if random.random() > 0.4 else 0
        allowances = commute_allowance + housing_allowance + family_allowance
        
        # 控除（課税対象額の約22-27%）
        taxable_amount = base_salary + overtime_pay
        health_insurance = int(taxable_amount * 0.05)  # 健康保険5%
        pension_insurance = int(taxable_amount * 0.09)  # 厚生年金9%
        employment_insurance = int(taxable_amount * 0.006)  # 雇用保険0.6%
        income_tax = int(taxable_amount * random.uniform(0.04, 0.08))  # 所得税4-8%
        resident_tax = int(taxable_amount * random.uniform(0.05, 0.07))  # 住民税5-7%
        
        deductions = health_insurance + pension_insurance + employment_insurance + income_tax + resident_tax
        
        # 手取り額
        net_salary = base_salary + overtime_pay + allowances - deductions
        
        # レコード作成
        record = {
            'payroll_id': f'PAY-{year_month}-{payroll_id_counter:05d}',
            'employee_id': emp['employee_id'],
            'employee_name': emp['employee_name'],
            'department': emp['department'],
            'position': emp['position'],
            'payment_period': year_month,
            'base_salary': base_salary,
            'overtime_pay': overtime_pay,
            'allowances': allowances,
            'deductions': deductions,
            'net_salary': net_salary,
            'payment_date': payment_date.strftime('%Y-%m-%d'),
            'currency': 'JPY',
            'employment_type': 'full_time',
            'cost_center': emp['cost_center']
        }
        
        payroll_records.append(record)
        payroll_id_counter += 1
    
    # 次月へ
    current_date += relativedelta(months=1)

# CSV出力
fieldnames = [
    'payroll_id', 'employee_id', 'employee_name', 'department', 'position',
    'payment_period', 'base_salary', 'overtime_pay', 'allowances', 'deductions',
    'net_salary', 'payment_date', 'currency', 'employment_type', 'cost_center'
]

with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(payroll_records)

# 統計情報
total_payroll = sum(record['net_salary'] for record in payroll_records)
avg_net_salary = total_payroll / len(payroll_records) if payroll_records else 0

print(f'\n給与データ生成完了')
print(f'  総レコード数: {len(payroll_records):,}件')
print(f'  対象期間: 2022-01 ~ 2025-12 ({len(set(r["payment_period"] for r in payroll_records))}ヶ月)')
print(f'  総支給額: ¥{total_payroll:,}')
print(f'  平均手取り額: ¥{int(avg_net_salary):,}')
print(f'  社員数: {len(employees)}名')

# 部門別統計
print(f'\n【部門別統計】')
for dept in departments.keys():
    dept_records = [r for r in payroll_records if r['department'] == dept]
    dept_total = sum(r['net_salary'] for r in dept_records)
    dept_avg = dept_total / len(dept_records) if dept_records else 0
    print(f'  {dept}: 平均¥{int(dept_avg):,}/月 (総支給¥{dept_total:,})')

# サンプル表示
print(f'\n【サンプルデータ（2022年1月）】')
sample_records = [r for r in payroll_records if r['payment_period'] == '2022-01'][:5]
for record in sample_records:
    print(f'  {record["employee_id"]} ({record["department"]}): 基本給¥{record["base_salary"]:,} + 残業¥{record["overtime_pay"]:,} = 手取り¥{record["net_salary"]:,}')
