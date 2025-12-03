import csv

input_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\BOMマスタ.csv'
output_file = input_file

# データ読み込み
with open(input_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# bom_idから拠点情報を削除
for row in rows:
    bom_id = row['bom_id']
    # BOM-FIT-GR3-STM-001 -> BOM-FIT-GR3-001 (4番目の要素を削除)
    parts = bom_id.split('-')
    if len(parts) == 5:
        # 拠点部分（4番目）を削除
        new_bom_id = f"{parts[0]}-{parts[1]}-{parts[2]}-{parts[4]}"
        row['bom_id'] = new_bom_id

# 保存
with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f'BOMマスタのbom_idを更新しました: {len(rows)}件')
print(f'修正例:')
for i in range(min(5, len(rows))):
    print(f'  {rows[i]["bom_id"]} - {rows[i]["bom_name"]}')
