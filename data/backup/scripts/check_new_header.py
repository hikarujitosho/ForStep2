import pandas as pd

df = pd.read_csv(r'c:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_header.csv')
print(f'headerレコード数: {len(df):,}')
print(f'ユニークなpurchase_order_id: {df["purchase_order_id"].nunique():,}')
print(f'\n日付範囲:')
print(f'  order_date: {df["order_date"].min()} ~ {df["order_date"].max()}')
print(f'\nサプライヤー分布:')
print(df['supplier_name'].value_counts().head(10))
