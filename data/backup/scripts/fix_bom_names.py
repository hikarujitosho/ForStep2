import csv

# 車種名マッピング
vehicle_mapping = {
    'FIT': 'FIT',
    'FREED': 'FRD',
    'VEZEL': 'VZL',
    'STEP WGN': 'SWN',
    'CR-V': 'CRV',
    'ODYSSEY': 'ODY',
    'PILOT': 'PLT',
    'RIDGELINE': 'RDG',
    'PASSPORT': 'PSP',
    'ZR-V': 'ZRV',
    'ACCORD': 'ACD',
    'Honda e': 'HDE',
    'N-ONE e:': 'NOE',
    'e:NP1': 'ENP',
    'Prologue': 'PRO'
}

input_file = r'h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\ERP\BOMマスタ.csv'
output_file = input_file

# データ読み込み
with open(input_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# bom_name修正
for row in rows:
    name = row['bom_name']
    # 「組立工程」を削除
    name = name.replace(' 組立工程', '')
    
    # 車種名を3文字コードに変換
    for full_name, code in vehicle_mapping.items():
        if name.startswith(full_name):
            name = name.replace(full_name, code, 1)
            break
    
    row['bom_name'] = name

# 保存
with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f'BOMマスタを更新しました: {len(rows)}件')
print(f'修正例:')
for i in range(min(5, len(rows))):
    bom_name = rows[i]['bom_name']
    print(f'  {bom_name}')
