import csv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import random
import calendar

# ファイルパス
shipment_header_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\MES\出荷伝票_header.csv'
shipment_item_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\MES\出荷伝票_item.csv'
output_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\TMS\輸送コスト.csv'

# 出荷伝票データを読み込み
with open(shipment_header_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    shipment_headers = list(reader)

with open(shipment_item_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    shipment_items = list(reader)

# 輸送モード別の基本料金範囲（JPY）
transport_cost_ranges = {
    'road': (50000, 150000),
    'sea': (100000, 300000),
    'air': (200000, 500000)
}

# 1台あたりの追加コスト範囲
cost_per_unit = (20000, 30000)

# 緊急輸送費率（通常運賃の30～50%）
expedite_rate_range = (0.30, 0.50)

def get_last_day_of_month(date):
    """指定された日付の月の最終日を返す"""
    last_day = calendar.monthrange(date.year, date.month)[1]
    return datetime(date.year, date.month, last_day)

def calculate_freight_cost(transportation_mode, quantity):
    """運賃を計算"""
    base_cost = random.randint(transport_cost_ranges[transportation_mode][0], 
                                transport_cost_ranges[transportation_mode][1])
    quantity_cost = int(quantity) * random.randint(cost_per_unit[0], cost_per_unit[1])
    return base_cost + quantity_cost

def calculate_expedite_cost(freight_cost):
    """緊急輸送費を計算（通常運賃の30～50%）"""
    expedite_rate = random.uniform(expedite_rate_range[0], expedite_rate_range[1])
    return int(freight_cost * expedite_rate)

# 輸送コストレコードを生成
transportation_costs = []
cost_counter_by_year = {}

print(f'出荷伝票ヘッダー数: {len(shipment_headers)}')
print(f'出荷伝票明細数: {len(shipment_items)}')
print()

# 各出荷伝票について処理
for shipment_header in shipment_headers:
    shipment_id = shipment_header['shipment_id']
    location_id = shipment_header['location_id']
    shipment_timestamp = datetime.strptime(shipment_header['shipment_timestamp'], '%Y-%m-%d %H:%M:%S')
    
    # 請求日は出荷日の翌月末日
    billing_date = get_last_day_of_month(shipment_timestamp + relativedelta(months=1))
    billing_year = billing_date.year
    
    # 年度別にカウンターを初期化
    if billing_year not in cost_counter_by_year:
        cost_counter_by_year[billing_year] = 1
    
    # この出荷に紐づく明細を取得
    related_items = [item for item in shipment_items if item['shipment_id'] == shipment_id]
    
    if not related_items:
        continue
    
    # 最初の明細から輸送モードを取得（同一出荷IDは同じ輸送モード）
    transportation_mode = related_items[0]['transportation_mode']
    delivery_status = related_items[0]['delivery_status']
    
    # 総出荷数量を計算
    total_quantity = sum(int(item['quantity']) for item in related_items)
    
    # 運賃（freight）を計算
    freight_cost = calculate_freight_cost(transportation_mode, total_quantity)
    
    # cost_idを生成
    cost_id = f'COST-{billing_year}-{cost_counter_by_year[billing_year]:03d}'
    cost_counter_by_year[billing_year] += 1
    
    # freightレコードを追加
    freight_record = {
        'cost_id': cost_id,
        'shipment_id': shipment_id,
        'location_id': location_id,
        'cost_type': 'freight',
        'cost_amount': freight_cost,
        'currency': 'JPY',
        'billing_date': billing_date.strftime('%Y-%m-%d')
    }
    transportation_costs.append(freight_record)
    
    # delivery_status が delayed の場合、緊急輸送費（expedite）を追加
    if delivery_status == 'delayed':
        expedite_cost = calculate_expedite_cost(freight_cost)
        
        expedite_cost_id = f'COST-{billing_year}-{cost_counter_by_year[billing_year]:03d}'
        cost_counter_by_year[billing_year] += 1
        
        expedite_record = {
            'cost_id': expedite_cost_id,
            'shipment_id': shipment_id,
            'location_id': location_id,
            'cost_type': 'expedite',
            'cost_amount': expedite_cost,
            'currency': 'JPY',
            'billing_date': billing_date.strftime('%Y-%m-%d')
        }
        transportation_costs.append(expedite_record)

# CSVに書き込み
with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
    fieldnames = ['cost_id', 'shipment_id', 'location_id', 'cost_type', 'cost_amount', 'currency', 'billing_date']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(transportation_costs)

# 統計情報を出力
print(f'輸送コストデータ生成完了')
print(f'  総レコード数: {len(transportation_costs):,}件')
print()

# cost_type別の集計
freight_records = [r for r in transportation_costs if r['cost_type'] == 'freight']
expedite_records = [r for r in transportation_costs if r['cost_type'] == 'expedite']

total_freight = sum(r['cost_amount'] for r in freight_records)
total_expedite = sum(r['cost_amount'] for r in expedite_records)
total_cost = total_freight + total_expedite

print(f'【コストタイプ別集計】')
print(f'  freight（運賃）: {len(freight_records):,}件, 合計¥{total_freight:,}')
print(f'  expedite（緊急輸送費）: {len(expedite_records):,}件, 合計¥{total_expedite:,}')
print(f'  総輸送コスト: ¥{total_cost:,}')
print(f'  緊急輸送費率: {(total_expedite / total_cost * 100):.2f}%')
print()

# 年度別集計
print(f'【年度別集計】')
for year in sorted(cost_counter_by_year.keys()):
    year_records = [r for r in transportation_costs if r['billing_date'].startswith(str(year))]
    year_total = sum(r['cost_amount'] for r in year_records)
    print(f'  {year}年: {len(year_records):,}件, 合計¥{year_total:,}')
print()

# 輸送モード別集計（サンプル）
print(f'【サンプルデータ（最初の10件）】')
for i, record in enumerate(transportation_costs[:10]):
    print(f"  {record['cost_id']}: {record['shipment_id']} - {record['cost_type']} ¥{record['cost_amount']:,} (請求日: {record['billing_date']})")
