# -*- coding: utf-8 -*-
import csv
import os
from collections import Counter

# 入力ファイルパス
base_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P"
input_file = os.path.join(base_path, "BOMマスタ.csv")

# CSVを読み込み
with open(input_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# 車種ごとの部品数をカウント
vehicle_parts = Counter()
for row in rows:
    product_id = row['product_id']
    vehicle_parts[product_id] += 1

# 結果を表示
print("=== 車種別部品数 ===\n")
print(f"{'車種コード':<15} {'部品数':>6}")
print("-" * 25)

# 車種コード順にソート
for vehicle, count in sorted(vehicle_parts.items()):
    print(f"{vehicle:<15} {count:>6}")

print("-" * 25)
print(f"{'合計':<15} {sum(vehicle_parts.values()):>6}")
print(f"\n総車種数: {len(vehicle_parts)}")
