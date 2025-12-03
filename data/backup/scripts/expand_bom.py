import pandas as pd
import csv

# 既存BOMマスタを読み込み
bom_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P\BOMマスタ.csv"
df_existing = pd.read_csv(bom_path, encoding='utf-8-sig')

print(f"既存BOMレコード数: {len(df_existing)}")

# 新規追加部品リスト
new_bom_records = []

# 車種カテゴリ定義
compact_vehicles = ['FIT-GR3', 'ZRV-RZ3']
minivan_vehicles = ['FRD-GB5', 'SWN-RP6', 'ODY-RC1']
suv_vehicles = ['VZL-RV3', 'CRV-RT5', 'PSP-YF7', 'PLT-YF3']
truck_vehicles = ['RDG-YF6']
sedan_vehicles = ['ACD-CV1']
compact_ev_vehicles = ['HDE-ZC1', 'NOE-JG4']
suv_ev_vehicles = ['ENP-ENP1']
premium_ev_vehicles = ['PRO-ZC5']

# 内装部品カテゴリ別定義
interior_parts = {
    'compact': [
        ('INT-SEAT-COMPACT-CLOTH', 'コンパクトシート（ファブリック）', 4, 'EACHES'),
        ('INT-DASHBOARD-COMPACT', 'コンパクトダッシュボード', 1, 'EACHES'),
        ('INT-STEERING-STD', '標準ステアリングホイール', 1, 'EACHES'),
        ('INT-CONSOLE-COMPACT', 'コンパクトセンターコンソール', 1, 'EACHES'),
        ('INT-CARPET-STD', '標準フロアカーペット', 1, 'SET'),
    ],
    'minivan': [
        ('INT-SEAT-MINIVAN-CLOTH', 'ミニバンシート（ファブリック）', 7, 'EACHES'),
        ('INT-DASHBOARD-MINIVAN', 'ミニバンダッシュボード', 1, 'EACHES'),
        ('INT-STEERING-STD', '標準ステアリングホイール', 1, 'EACHES'),
        ('INT-CONSOLE-MINIVAN', 'ミニバンセンターコンソール', 1, 'EACHES'),
        ('INT-CARPET-STD', '標準フロアカーペット', 1, 'SET'),
    ],
    'suv': [
        ('INT-SEAT-SUV-LEATHER', 'SUVシート（レザー）', 5, 'EACHES'),
        ('INT-DASHBOARD-SUV', 'SUVダッシュボード', 1, 'EACHES'),
        ('INT-STEERING-LEATHER', 'レザーステアリングホイール', 1, 'EACHES'),
        ('INT-CONSOLE-SUV', 'SUVセンターコンソール', 1, 'EACHES'),
        ('INT-CARPET-PREMIUM', 'プレミアムフロアカーペット', 1, 'SET'),
    ],
    'premium': [
        ('INT-SEAT-PREMIUM-LEATHER', 'プレミアムシート（レザー）', 5, 'EACHES'),
        ('INT-DASHBOARD-PREMIUM', 'プレミアムダッシュボード', 1, 'EACHES'),
        ('INT-STEERING-PREMIUM', 'プレミアムステアリングホイール', 1, 'EACHES'),
        ('INT-CONSOLE-PREMIUM', 'プレミアムセンターコンソール', 1, 'EACHES'),
        ('INT-CARPET-PREMIUM', 'プレミアムフロアカーペット', 1, 'SET'),
    ],
}

# サスペンション部品カテゴリ別定義
suspension_parts = {
    'compact': [
        ('SUS-STRUT-FRONT-COMPACT', 'コンパクト用フロントストラット', 2, 'EACHES'),
        ('SUS-SHOCK-REAR-COMPACT', 'コンパクト用リアショックアブソーバー', 2, 'EACHES'),
        ('SUS-SPRING-FRONT-COMPACT', 'コンパクト用フロントスプリング', 2, 'EACHES'),
        ('SUS-SPRING-REAR-COMPACT', 'コンパクト用リアスプリング', 2, 'EACHES'),
    ],
    'minivan': [
        ('SUS-STRUT-FRONT-MINIVAN', 'ミニバン用フロントストラット', 2, 'EACHES'),
        ('SUS-SHOCK-REAR-MINIVAN', 'ミニバン用リアショックアブソーバー', 2, 'EACHES'),
        ('SUS-SPRING-FRONT-MINIVAN', 'ミニバン用フロントスプリング', 2, 'EACHES'),
        ('SUS-SPRING-REAR-MINIVAN', 'ミニバン用リアスプリング', 2, 'EACHES'),
    ],
    'suv': [
        ('SUS-STRUT-FRONT-SUV', 'SUV用フロントストラット', 2, 'EACHES'),
        ('SUS-SHOCK-REAR-SUV', 'SUV用リアショックアブソーバー', 2, 'EACHES'),
        ('SUS-SPRING-FRONT-SUV', 'SUV用フロントスプリング', 2, 'EACHES'),
        ('SUS-SPRING-REAR-SUV', 'SUV用リアスプリング', 2, 'EACHES'),
    ],
    'premium': [
        ('SUS-AIRSUSP-FRONT-PREMIUM', 'プレミアム用フロントエアサスペンション', 2, 'EACHES'),
        ('SUS-AIRSUSP-REAR-PREMIUM', 'プレミアム用リアエアサスペンション', 2, 'EACHES'),
        ('SUS-DAMPER-ADAPTIVE', 'アダプティブダンパー', 4, 'EACHES'),
    ],
}

# BOM ID カウンター（既存の最大値から続ける）
max_bom_num = 0
for bom_id in df_existing['bom_id']:
    parts = bom_id.split('-')
    if len(parts) >= 4:
        try:
            num = int(parts[-1])
            max_bom_num = max(max_bom_num, num)
        except:
            pass

current_bom_num = max_bom_num + 1

# 各車種に部品を追加
def add_parts_to_vehicle(product_id, vehicle_name, site_id, interior_type, suspension_type):
    global current_bom_num
    records = []
    
    # 内装部品追加
    for part_id, part_name, qty, uom in interior_parts[interior_type]:
        bom_id = f"BOM-{product_id}-{site_id}-{current_bom_num:03d}"
        bom_name = f"{vehicle_name} 組立工程 - {part_name}"
        records.append({
            'bom_id': bom_id,
            'bom_name': bom_name,
            'product_id': product_id,
            'site_id': site_id,
            'production_process_id': f"PROC-{product_id}-ASM-{site_id}",
            'component_product_id': part_id,
            'component_quantity_per': qty,
            'component_quantity_uom': uom,
            'eff_start_date': '2025-01-01',
            'eff_end_date': '9999-12-31',
        })
        current_bom_num += 1
    
    # サスペンション部品追加
    for part_id, part_name, qty, uom in suspension_parts[suspension_type]:
        bom_id = f"BOM-{product_id}-{site_id}-{current_bom_num:03d}"
        bom_name = f"{vehicle_name} 組立工程 - {part_name}"
        records.append({
            'bom_id': bom_id,
            'bom_name': bom_name,
            'product_id': product_id,
            'site_id': site_id,
            'production_process_id': f"PROC-{product_id}-ASM-{site_id}",
            'component_product_id': part_id,
            'component_quantity_per': qty,
            'component_quantity_uom': uom,
            'eff_start_date': '2025-01-01',
            'eff_end_date': '9999-12-31',
        })
        current_bom_num += 1
    
    return records

# コンパクト車
for product_id in compact_vehicles:
    vehicle_name = df_existing[df_existing['product_id'] == product_id]['bom_name'].iloc[0].split(' 組立')[0]
    site_id = df_existing[df_existing['product_id'] == product_id]['site_id'].iloc[0]
    new_bom_records.extend(add_parts_to_vehicle(product_id, vehicle_name, site_id, 'compact', 'compact'))

# ミニバン
for product_id in minivan_vehicles:
    vehicle_name = df_existing[df_existing['product_id'] == product_id]['bom_name'].iloc[0].split(' 組立')[0]
    site_id = df_existing[df_existing['product_id'] == product_id]['site_id'].iloc[0]
    new_bom_records.extend(add_parts_to_vehicle(product_id, vehicle_name, site_id, 'minivan', 'minivan'))

# SUV
for product_id in suv_vehicles:
    vehicle_name = df_existing[df_existing['product_id'] == product_id]['bom_name'].iloc[0].split(' 組立')[0]
    site_id = df_existing[df_existing['product_id'] == product_id]['site_id'].iloc[0]
    new_bom_records.extend(add_parts_to_vehicle(product_id, vehicle_name, site_id, 'suv', 'suv'))

# トラック
for product_id in truck_vehicles:
    vehicle_name = df_existing[df_existing['product_id'] == product_id]['bom_name'].iloc[0].split(' 組立')[0]
    site_id = df_existing[df_existing['product_id'] == product_id]['site_id'].iloc[0]
    new_bom_records.extend(add_parts_to_vehicle(product_id, vehicle_name, site_id, 'suv', 'suv'))

# セダン
for product_id in sedan_vehicles:
    vehicle_name = df_existing[df_existing['product_id'] == product_id]['bom_name'].iloc[0].split(' 組立')[0]
    site_id = df_existing[df_existing['product_id'] == product_id]['site_id'].iloc[0]
    new_bom_records.extend(add_parts_to_vehicle(product_id, vehicle_name, site_id, 'premium', 'premium'))

# コンパクトEV
for product_id in compact_ev_vehicles:
    vehicle_name = df_existing[df_existing['product_id'] == product_id]['bom_name'].iloc[0].split(' 組立')[0]
    site_id = df_existing[df_existing['product_id'] == product_id]['site_id'].iloc[0]
    new_bom_records.extend(add_parts_to_vehicle(product_id, vehicle_name, site_id, 'compact', 'compact'))

# SUV EV
for product_id in suv_ev_vehicles:
    vehicle_name = df_existing[df_existing['product_id'] == product_id]['bom_name'].iloc[0].split(' 組立')[0]
    site_id = df_existing[df_existing['product_id'] == product_id]['site_id'].iloc[0]
    new_bom_records.extend(add_parts_to_vehicle(product_id, vehicle_name, site_id, 'suv', 'suv'))

# プレミアムEV
for product_id in premium_ev_vehicles:
    vehicle_name = df_existing[df_existing['product_id'] == product_id]['bom_name'].iloc[0].split(' 組立')[0]
    site_id = df_existing[df_existing['product_id'] == product_id]['site_id'].iloc[0]
    new_bom_records.extend(add_parts_to_vehicle(product_id, vehicle_name, site_id, 'premium', 'premium'))

print(f"新規追加BOMレコード数: {len(new_bom_records)}")

# 既存データと結合
df_new = pd.DataFrame(new_bom_records)
df_combined = pd.concat([df_existing, df_new], ignore_index=True)

# CSV出力
output_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P\BOMマスタ.csv"
df_combined.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"更新後総BOMレコード数: {len(df_combined)}")
print(f"出力ファイル: {output_path}")

# サマリー
print("\n=== 追加部品サマリー ===")
print(f"内装部品種類: {len(set([p[0] for parts in interior_parts.values() for p in parts]))}")
print(f"サスペンション部品種類: {len(set([p[0] for parts in suspension_parts.values() for p in parts]))}")
print(f"総ユニーク部品数: {df_combined['component_product_id'].nunique()}")
