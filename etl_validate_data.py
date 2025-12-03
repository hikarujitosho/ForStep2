"""
データ検証スクリプト：Bronze層のデータ品質を確認
"""
import pandas as pd
from pathlib import Path

# パス設定
BRONZE_ERP = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze\ERP")

print("=" * 80)
print("Bronze層データ検証")
print("=" * 80)

# 1. 品目マスタの確認
print("\n1. 品目マスタ - EV車の確認")
product_master = pd.read_csv(BRONZE_ERP / "品目マスタ.csv")
print(f"全製品数: {len(product_master)}")
print(f"\nitem_hierarchy別:")
print(product_master['item_hierarchy'].value_counts())

ev_products = product_master[product_master['item_hierarchy'] == 'EV']
print(f"\nEV車一覧:")
print(ev_products[['product_id', 'product_name', 'item_hierarchy']])

# 2. 受注伝票の確認
print("\n\n2. 受注伝票の確認")
order_header = pd.read_csv(BRONZE_ERP / "受注伝票_header.csv")
order_item = pd.read_csv(BRONZE_ERP / "受注伝票_item.csv")

order_header['order_timestamp'] = pd.to_datetime(order_header['order_timestamp'])
order_header['year_month'] = order_header['order_timestamp'].dt.to_period('M')

print(f"受注伝票ヘッダー件数: {len(order_header)}")
print(f"受注伝票明細件数: {len(order_item)}")

# 月別受注件数
monthly_orders = order_header.groupby('year_month').size()
print(f"\n月別受注伝票数（最初の10件）:")
print(monthly_orders.head(10))

# 3. 受注明細と品目マスタの結合確認
print("\n\n3. 受注明細と品目マスタの結合確認")
order_product = order_item.merge(
    product_master[['product_id', 'product_name', 'item_hierarchy']], 
    on='product_id', 
    how='left'
)

missing_products = order_product[order_product['product_name'].isna()]
print(f"品目マスタに存在しない製品の受注: {len(missing_products)}件")
if len(missing_products) > 0:
    print("存在しない製品ID:")
    print(missing_products['product_id'].unique())

# EV車の受注数
ev_orders = order_product[order_product['item_hierarchy'] == 'EV']
print(f"\nEV車の受注明細数: {len(ev_orders)}")
print(f"EV車の種類: {ev_orders['product_id'].nunique()}")

# 4. 条件マスタの確認
print("\n\n4. 条件マスタの確認")
price_conditions = pd.read_csv(BRONZE_ERP / "条件マスタ.csv")
price_conditions['valid_from'] = pd.to_datetime(price_conditions['valid_from'])
price_conditions['valid_to'] = pd.to_datetime(price_conditions['valid_to'])

print(f"条件マスタレコード数: {len(price_conditions)}")
print(f"製品数: {price_conditions['product_id'].nunique()}")
print(f"顧客数: {price_conditions['customer_id'].nunique()}")

# EV車の価格条件
ev_price = price_conditions[price_conditions['product_id'].isin(ev_products['product_id'])]
print(f"\nEV車の価格条件レコード数: {len(ev_price)}")

# 5. 受注と価格条件の結合テスト（サンプル）
print("\n\n5. 受注と価格条件の結合テスト（最初の10件）")
order_full = order_item.merge(order_header[['order_id', 'order_timestamp', 'customer_id']], on='order_id')
order_full['pricing_date'] = pd.to_datetime(order_full['pricing_date'])

# 最初の10件でテスト
sample = order_full.head(10).copy()

# 価格条件とのマッチング（valid_from <= pricing_date < valid_to）
matched_prices = []
for idx, row in sample.iterrows():
    matching = price_conditions[
        (price_conditions['product_id'] == row['product_id']) &
        (price_conditions['customer_id'] == row['customer_id']) &
        (price_conditions['valid_from'] <= row['pricing_date']) &
        (price_conditions['valid_to'] >= row['pricing_date'])
    ]
    if len(matching) > 0:
        matched_prices.append({
            'order_id': row['order_id'],
            'line_number': row['line_number'],
            'selling_price_ex_tax': matching.iloc[0]['selling_price_ex_tax'],
            'matched': True
        })
    else:
        matched_prices.append({
            'order_id': row['order_id'],
            'line_number': row['line_number'],
            'selling_price_ex_tax': None,
            'matched': False
        })

matched_df = pd.DataFrame(matched_prices)
print(f"サンプル受注: {len(sample)}")
print(f"価格マッチング成功: {matched_df['matched'].sum()}")
print(f"価格マッチング失敗: {(~matched_df['matched']).sum()}")

if (~matched_df['matched']).sum() > 0:
    print("\n価格マッチング失敗の例:")
    failed_idx = matched_df[~matched_df['matched']].index
    for idx in failed_idx:
        row = sample.iloc[idx]
        print(f"  order_id={row['order_id']}, product_id={row['product_id']}, "
              f"customer_id={row['customer_id']}, pricing_date={row['pricing_date']}")
        # この組み合わせで存在する条件を確認
        matching = price_conditions[
            (price_conditions['product_id'] == row['product_id']) &
            (price_conditions['customer_id'] == row['customer_id'])
        ]
        if len(matching) > 0:
            print(f"    -> 該当する価格条件の有効期間:")
            for _, m in matching.iterrows():
                print(f"       {m['valid_from']} ~ {m['valid_to']}")

print("\n" + "=" * 80)
print("検証完了")
print("=" * 80)
