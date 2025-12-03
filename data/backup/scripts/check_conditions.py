import csv

# 条件マスタを読み込んで検証
with open('../../Bronze/ERP/条件マスタ.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# 日付の大小が逆転しているレコードをチェック
invalid_dates = []
for row in rows:
    valid_from = row['valid_from']
    valid_to = row['valid_to']
    if valid_from > valid_to:
        invalid_dates.append(row)

print(f"総レコード数: {len(rows)}")
print(f"日付が逆転しているレコード数: {len(invalid_dates)}")

if invalid_dates:
    print("\n【日付が逆転しているレコード（先頭5件）】")
    for i, row in enumerate(invalid_dates[:5]):
        print(f"  {i+1}. {row['product_id']} - {row['customer_id']}: {row['valid_from']} ~ {row['valid_to']}")

# キャンペーン期間の数を確認
campaigns = [row for row in rows if row['price_type'] == 'promotional']
print(f"\nキャンペーン期間のレコード数: {len(campaigns)} 件")
print(f"キャンペーン率: {len(campaigns) / len(rows) * 100:.1f}%")

# ディーラーごとのキャンペーン数
from collections import defaultdict
campaigns_by_dealer = defaultdict(int)
for row in campaigns:
    campaigns_by_dealer[row['customer_id']] += 1

print(f"\n【ディーラーごとのキャンペーン期間数】")
for dealer in sorted(campaigns_by_dealer.keys()):
    print(f"  {dealer}: {campaigns_by_dealer[dealer]} 件")
