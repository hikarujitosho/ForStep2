import pandas as pd

df = pd.read_csv(r'data\Bronze\P2P\調達伝票_item.csv')

print("=" * 100)
print("ACD-CV1とRDG-YF6の調達データ確認")
print("=" * 100)

# ACD-CV1の調達データ
print("\n=== ACD-CV1 (ACCORD) の調達データ ===")
acd = df[df['product_id'] == 'ACD-CV1']
print(f"レコード数: {len(acd)}")

if len(acd) > 0:
    print("\n直接材のみ:")
    acd_direct = acd[acd['material_type'] == 'direct']
    print(f"直接材レコード数: {len(acd_direct)}")
    if len(acd_direct) > 0:
        print(acd_direct[['purchase_order_id', 'material_id', 'material_name', 'material_type', 
                          'product_id', 'quantity', 'unit_price_ex_tax']].head(10))
        print(f"\n合計調達金額: ¥{(acd_direct['quantity'] * acd_direct['unit_price_ex_tax']).sum():,.0f}")
else:
    print("⚠️ ACD-CV1の調達データは存在しません")

# RDG-YF6の調達データ
print("\n" + "=" * 100)
print("=== RDG-YF6 (RIDGELINE) の調達データ ===")
rdg = df[df['product_id'] == 'RDG-YF6']
print(f"レコード数: {len(rdg)}")

if len(rdg) > 0:
    print("\n直接材のみ:")
    rdg_direct = rdg[rdg['material_type'] == 'direct']
    print(f"直接材レコード数: {len(rdg_direct)}")
    if len(rdg_direct) > 0:
        print(rdg_direct[['purchase_order_id', 'material_id', 'material_name', 'material_type', 
                          'product_id', 'quantity', 'unit_price_ex_tax']].head(10))
        print(f"\n合計調達金額: ¥{(rdg_direct['quantity'] * rdg_direct['unit_price_ex_tax']).sum():,.0f}")
else:
    print("⚠️ RDG-YF6の調達データは存在しません")

# 全商品IDの確認
print("\n" + "=" * 100)
print("=== 調達データに存在する全商品ID（直接材のみ） ===")
direct_materials = df[df['material_type'] == 'direct']
unique_products = sorted(direct_materials['product_id'].dropna().unique())
print(f"商品数: {len(unique_products)}")
print(unique_products)

# 受注データとの比較
print("\n" + "=" * 100)
print("=== 受注データとの比較 ===")
df_order = pd.read_csv(r'data\Bronze\ERP\受注伝票_item.csv')
order_products = sorted(df_order['product_id'].unique())
print(f"受注データの商品数: {len(order_products)}")
print("受注データの商品ID:", order_products)

print("\n受注はあるが調達データがない商品:")
missing_products = set(order_products) - set(unique_products)
print(missing_products)
