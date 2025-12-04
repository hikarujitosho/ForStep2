import pandas as pd

# BOMマスタファイルの読み込み
erp_bom = pd.read_csv('data/Bronze/ERP/BOMマスタ.csv')
p2p_bom = pd.read_csv('data/Bronze/P2P/BOMマスタ.csv')

print(f"ERPのBOMマスタ: {len(erp_bom)}件")
print(f"P2PのBOMマスタ: {len(p2p_bom)}件")
print(f"差分: {len(p2p_bom) - len(erp_bom)}件\n")

# ERPにあるbom_idのセット
erp_bom_ids = set(erp_bom['bom_id'])

# P2Pにのみ存在するレコードを抽出
extra_in_p2p = p2p_bom[~p2p_bom['bom_id'].isin(erp_bom_ids)].copy()

print(f"P2Pにのみ存在するレコード: {len(extra_in_p2p)}件\n")
print("=" * 100)

if len(extra_in_p2p) > 0:
    # 車種別に集計
    extra_in_p2p['vehicle'] = extra_in_p2p['bom_id'].str.extract(r'^BOM-([A-Z]+)-')
    vehicle_counts = extra_in_p2p['vehicle'].value_counts().sort_index()
    
    print("\n【車種別の余分なレコード数】")
    print(vehicle_counts)
    print()
    
    # 余分なレコードの詳細を表示
    print("\n【P2Pに余分に存在するレコード詳細】")
    print("=" * 100)
    
    # 車種ごとにソートして表示
    extra_sorted = extra_in_p2p.sort_values(['vehicle', 'bom_id'])
    
    for _, row in extra_sorted.iterrows():
        print(f"BOM ID: {row['bom_id']}")
        print(f"  名称: {row['bom_name']}")
        print(f"  製品ID: {row['product_id']}")
        print(f"  部品ID: {row['component_product_id']}")
        print(f"  数量: {row['component_quantity_per']} {row['component_quantity_uom']}")
        print()
    
    # CSVとして保存
    output_file = 'output/P2P_extra_bom_records.csv'
    extra_sorted.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n余分なレコードを {output_file} に保存しました。")
