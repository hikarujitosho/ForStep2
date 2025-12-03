"""
調達伝票_itemをもとに調達伝票_headerを完全新規生成するスクリプト
"""
import pandas as pd
import numpy as np
from datetime import timedelta
import random

# ファイルパス
item_file = r'c:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_item.csv'
old_header_file = r'c:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_header.csv'
partner_file = r'c:\Users\PC\dev\ForStep2\data\Bronze\P2P\取引先マスタ.csv'
output_file = r'c:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_header.csv'

print("=" * 80)
print("調達伝票_header 新規生成スクリプト")
print("=" * 80)

# データ読み込み
print("\n[1/6] データ読み込み中...")
df_item = pd.read_csv(item_file)
df_old_header = pd.read_csv(old_header_file)
df_partner = pd.read_csv(partner_file)

print(f"  - item: {len(df_item):,} 件")
print(f"  - 旧header: {len(df_old_header):,} 件")
print(f"  - 取引先マスタ: {len(df_partner):,} 件")

# 取引先マスタからサプライヤーを抽出（partner_categoryで区別）
tier1_suppliers = df_partner[df_partner['partner_category'] == 'tier1_supplier'][['partner_id', 'partner_name']].to_dict('records')
mro_suppliers = df_partner[df_partner['partner_category'] == 'mro_supplier'][['partner_id', 'partner_name']].to_dict('records')

print(f"\n  - Tier1サプライヤー: {len(tier1_suppliers)} 社")
print(f"  - MROサプライヤー: {len(mro_suppliers)} 社")

# 既存headerから学習するデータ
print("\n[2/6] 既存headerからパターン学習中...")
approvers = df_old_header['approver'].unique().tolist()
payment_methods = df_old_header['payment_method'].unique().tolist()
account_groups = df_old_header['account_group'].unique().tolist()
locations = df_old_header['location_id'].unique().tolist()

print(f"  - 承認者: {len(approvers)} 名")
print(f"  - 支払方法: {len(payment_methods)} 種類")
print(f"  - アカウントグループ: {len(account_groups)} 種類")
print(f"  - 拠点: {len(locations)} 種類")

# purchase_order_idごとにグループ化
print("\n[3/6] purchase_order_idごとにグループ化中...")
grouped = df_item.groupby('purchase_order_id')
unique_po_ids = df_item['purchase_order_id'].unique()
print(f"  - ユニークなpurchase_order_id: {len(unique_po_ids):,} 件")

# 新しいheaderレコードを生成
print("\n[4/6] 新しいheaderレコード生成中...")
new_headers = []
po_counter = 1

for po_id in unique_po_ids:
    if po_counter % 1000 == 0:
        print(f"  - 処理中: {po_counter:,} / {len(unique_po_ids):,}")
    
    po_items = grouped.get_group(po_id)
    
    # 最初のitemからmaterial_typeを取得してサプライヤーを決定
    first_item = po_items.iloc[0]
    material_type = first_item['material_type']
    
    # material_typeに応じてサプライヤーを割り当て
    if material_type == 'direct':
        # direct → tier1_supplier
        supplier = random.choice(tier1_suppliers)
    else:  # indirect
        # indirect → mro_supplier
        supplier = random.choice(mro_suppliers)
    
    supplier_id = supplier['partner_id']
    supplier_name = supplier['partner_name']
    
    # received_dateの最小値を取得（基準日）
    received_dates = pd.to_datetime(po_items['received_date'])
    base_received_date = received_dates.min()
    
    # order_date: received_dateの3~7日前(ランダム)
    order_days_before = random.randint(3, 7)
    order_date = base_received_date - timedelta(days=order_days_before)
    
    # expected_delivery_date: received_dateの2~3日前(ランダム)
    expected_days_before = random.randint(2, 3)
    expected_delivery_date = base_received_date - timedelta(days=expected_days_before)
    
    # payment_date: order_dateの30日後
    payment_date = order_date + timedelta(days=30)
    
    # 金額集計
    order_subtotal_ex_tax = po_items['line_subtotal_ex_tax'].sum()
    shipping_fee_ex_tax = po_items['line_shipping_fee_incl_tax'].sum()  # 簡易的に合計
    tax_amount = po_items['line_tax_amount'].sum()
    discount_amount_incl_tax = po_items['line_discount_incl_tax'].sum()
    order_total_incl_tax = po_items['line_total_incl_tax'].sum()
    
    # その他の項目をランダム選択または固定値
    location_id = random.choice(locations)
    approver = random.choice(approvers)
    payment_method = random.choice(payment_methods)
    account_group = random.choice(account_groups)
    
    # purchase_order_numberを生成（連番）
    purchase_order_number = f"PO-NUM-{po_counter:08d}"
    
    # payment_confirmation_idを生成
    payment_confirmation_id = f"PAY-{po_counter:08d}"
    
    # headerレコード作成
    header_record = {
        'purchase_order_id': po_id,
        'order_date': order_date.strftime('%Y-%m-%d'),
        'expected_delivery_date': expected_delivery_date.strftime('%Y-%m-%d'),
        'supplier_id': supplier_id,
        'supplier_name': supplier_name,
        'account_group': account_group,
        'location_id': location_id,
        'purchase_order_number': purchase_order_number,
        'currency': 'JPY',
        'order_subtotal_ex_tax': int(order_subtotal_ex_tax),
        'shipping_fee_ex_tax': int(shipping_fee_ex_tax),
        'tax_amount': int(tax_amount),
        'discount_amount_incl_tax': int(discount_amount_incl_tax),
        'order_total_incl_tax': int(order_total_incl_tax),
        'order_status': 'received',
        'approver': approver,
        'payment_method': payment_method,
        'payment_confirmation_id': payment_confirmation_id,
        'payment_date': payment_date.strftime('%Y-%m-%d'),
        'payment_amount': int(order_total_incl_tax)
    }
    
    new_headers.append(header_record)
    po_counter += 1

# DataFrameに変換
print("\n[5/6] DataFrame変換中...")
df_new_header = pd.DataFrame(new_headers)

# ソート（purchase_order_idでソート）
df_new_header = df_new_header.sort_values('purchase_order_id').reset_index(drop=True)

print(f"  - 生成されたheaderレコード: {len(df_new_header):,} 件")

# CSV出力
print("\n[6/6] CSV出力中...")
df_new_header.to_csv(output_file, index=False, encoding='utf-8')
print(f"  - 出力先: {output_file}")

# 統計情報
print("\n" + "=" * 80)
print("生成完了！統計情報:")
print("=" * 80)
print(f"総レコード数: {len(df_new_header):,}")
print(f"\n日付範囲:")
print(f"  - order_date: {df_new_header['order_date'].min()} ~ {df_new_header['order_date'].max()}")
print(f"  - expected_delivery_date: {df_new_header['expected_delivery_date'].min()} ~ {df_new_header['expected_delivery_date'].max()}")
print(f"  - payment_date: {df_new_header['payment_date'].min()} ~ {df_new_header['payment_date'].max()}")

print(f"\nサプライヤー分布:")
supplier_counts = df_new_header['supplier_name'].value_counts()
for supplier, count in supplier_counts.head(10).items():
    print(f"  - {supplier}: {count:,} 件")

print(f"\n金額統計:")
print(f"  - 平均発注額: ¥{df_new_header['order_total_incl_tax'].mean():,.0f}")
print(f"  - 最小発注額: ¥{df_new_header['order_total_incl_tax'].min():,.0f}")
print(f"  - 最大発注額: ¥{df_new_header['order_total_incl_tax'].max():,.0f}")
print(f"  - 総発注額: ¥{df_new_header['order_total_incl_tax'].sum():,.0f}")

print("\n" + "=" * 80)
print("処理完了！")
print("=" * 80)
