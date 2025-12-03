import pandas as pd

# BOMマスタ読み込み
bom_path = r"h:\.shortcut-targets-by-id\1EW-r396_YsvYt5XwQAE9H9pbljJyc7JH\共有\Step2\ForStep2\data\Bronze\P2P\BOMマスタ.csv"
df = pd.read_csv(bom_path, encoding='utf-8-sig')

print("=== 車種別BOM部品構成 ===\n")

# 車種ごとの部品数を集計
summary = df.groupby('product_id').agg({
    'component_product_id': 'count',
    'bom_name': 'first'
}).rename(columns={'component_product_id': '部品数'})

summary = summary.sort_values('部品数', ascending=False)
print(summary.to_string())

print(f"\n\n=== 全体サマリー ===")
print(f"総BOMレコード数: {len(df)}")
print(f"車種数: {df['product_id'].nunique()}")
print(f"ユニーク部品数: {df['component_product_id'].nunique()}")

# 詳細: 各車種の部品リスト
print("\n\n=== 車種別部品詳細 ===")
for product_id in df['product_id'].unique():
    product_bom = df[df['product_id'] == product_id]
    print(f"\n【{product_id}】 部品数: {len(product_bom)}")
    for idx, row in product_bom.iterrows():
        print(f"  - {row['component_product_id']}: {row['bom_name']}")
