import pandas as pd
from pathlib import Path

print("="*100)
print("Bronze層データ分割後の整合性検証 - 詳細レポート")
print("="*100)

base_path = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze")

# 検証対象データ
datasets = [
    {
        'name': 'ERP受注データ（2024年以前）',
        'header_file': base_path / 'ERP' / '受注伝票_header_2024以前.csv',
        'item_file': base_path / 'ERP' / '受注伝票_item_2024以前.csv',
        'id_col': 'order_id'
    },
    {
        'name': 'ERP受注データ（2025年以降）',
        'header_file': base_path / 'ERP' / '受注伝票_header_2025以降.csv',
        'item_file': base_path / 'ERP' / '受注伝票_item_2025以降.csv',
        'id_col': 'order_id'
    },
    {
        'name': 'P2P調達データ（2024年以前）',
        'header_file': base_path / 'P2P' / '調達伝票_header_2024以前.csv',
        'item_file': base_path / 'P2P' / '調達伝票_item_2024以前.csv',
        'id_col': 'purchase_order_id'
    },
    {
        'name': 'P2P調達データ（2025年以降）',
        'header_file': base_path / 'P2P' / '調達伝票_header_2025以降.csv',
        'item_file': base_path / 'P2P' / '調達伝票_item_2025以降.csv',
        'id_col': 'purchase_order_id'
    },
    {
        'name': 'MES出荷データ（2024年以前）',
        'header_file': base_path / 'MES' / '出荷伝票_header_2024以前.csv',
        'item_file': base_path / 'MES' / '出荷伝票_item_2024以前.csv',
        'id_col': 'shipment_id'
    },
    {
        'name': 'MES出荷データ（2025年以降）',
        'header_file': base_path / 'MES' / '出荷伝票_header_2025以降.csv',
        'item_file': base_path / 'MES' / '出荷伝票_item_2025以降.csv',
        'id_col': 'shipment_id'
    }
]

all_results = []

for dataset in datasets:
    print(f"\n{'='*100}")
    print(f"検証: {dataset['name']}")
    print(f"{'='*100}")
    
    # ファイル読み込み
    df_header = pd.read_csv(dataset['header_file'])
    df_item = pd.read_csv(dataset['item_file'], low_memory=False)
    
    id_col = dataset['id_col']
    
    # IDセット取得
    header_ids = set(df_header[id_col].unique())
    item_ids = set(df_item[id_col].unique())
    
    print(f"\n📊 基本情報:")
    print(f"   ヘッダーファイル: {dataset['header_file'].name}")
    print(f"   明細ファイル: {dataset['item_file'].name}")
    print(f"   IDカラム: {id_col}")
    
    print(f"\n📈 レコード数:")
    print(f"   ヘッダーレコード数: {len(df_header):,}")
    print(f"   明細レコード数: {len(df_item):,}")
    
    print(f"\n🔑 ユニークID数:")
    print(f"   ヘッダーのユニークID数: {len(header_ids):,}")
    print(f"   明細のユニークID数: {len(item_ids):,}")
    
    # 整合性チェック
    print(f"\n🔍 整合性チェック詳細:")
    
    # 1. 孤立した明細（明細にあるがヘッダーにないID）
    orphan_items = item_ids - header_ids
    if len(orphan_items) == 0:
        print(f"   ✅ 孤立した明細: 0件")
        orphan_status = "OK"
    else:
        print(f"   ❌ 孤立した明細: {len(orphan_items):,}件")
        print(f"      （明細にあるがヘッダーにないID）")
        if len(orphan_items) <= 10:
            print(f"      例: {sorted(list(orphan_items))}")
        else:
            print(f"      例（最初の10件）: {sorted(list(orphan_items))[:10]}")
        orphan_status = "NG"
    
    # 2. 明細がないヘッダー（ヘッダーにあるが明細にないID）
    missing_items = header_ids - item_ids
    if len(missing_items) == 0:
        print(f"   ✅ 明細なしヘッダー: 0件")
        missing_status = "OK"
    else:
        print(f"   ❌ 明細なしヘッダー: {len(missing_items):,}件")
        print(f"      （ヘッダーにあるが明細にないID）")
        if len(missing_items) <= 10:
            print(f"      例: {sorted(list(missing_items))}")
        else:
            print(f"      例（最初の10件）: {sorted(list(missing_items))[:10]}")
        missing_status = "NG"
    
    # 3. 完全一致チェック
    is_perfect_match = (header_ids == item_ids)
    
    if is_perfect_match:
        print(f"\n   🎉 【完全一致】ヘッダーと明細のIDが完全に一致しています")
        print(f"      - 全てのヘッダーIDに対応する明細が存在")
        print(f"      - 全ての明細IDに対応するヘッダーが存在")
        print(f"      - ID数: {len(header_ids):,}件")
        overall_status = "✅ 完全一致"
    else:
        print(f"\n   ⚠️ 【不一致】ヘッダーと明細のIDに差異があります")
        overall_status = "❌ 不一致"
    
    # 4. 明細数の分布
    print(f"\n📋 明細数の分布:")
    items_per_id = df_item.groupby(id_col).size()
    print(f"   最小明細数: {items_per_id.min()} 件")
    print(f"   最大明細数: {items_per_id.max()} 件")
    print(f"   平均明細数: {items_per_id.mean():.2f} 件")
    print(f"   中央値: {items_per_id.median():.0f} 件")
    
    # 5. サンプルID確認（最初の3件）
    print(f"\n🔬 サンプル確認（最初の3件のID）:")
    sample_ids = sorted(list(header_ids))[:3]
    for sample_id in sample_ids:
        in_header = sample_id in header_ids
        in_items = sample_id in item_ids
        item_count = len(df_item[df_item[id_col] == sample_id])
        print(f"   ID: {sample_id}")
        print(f"      ヘッダー: {'✓' if in_header else '✗'}")
        print(f"      明細: {'✓' if in_items else '✗'} ({item_count}件)")
    
    # 結果を保存
    all_results.append({
        'データセット': dataset['name'],
        'ヘッダー数': len(df_header),
        '明細数': len(df_item),
        'ヘッダーID数': len(header_ids),
        '明細ID数': len(item_ids),
        '孤立明細': len(orphan_items),
        '明細なしヘッダー': len(missing_items),
        '整合性': overall_status
    })

# 全体サマリー
print(f"\n{'='*100}")
print("全体サマリー")
print(f"{'='*100}")

df_summary = pd.DataFrame(all_results)
print(f"\n{df_summary.to_string(index=False)}")

# 最終判定
print(f"\n{'='*100}")
print("最終判定")
print(f"{'='*100}")

all_perfect = all(result['整合性'] == '✅ 完全一致' for result in all_results)

if all_perfect:
    print(f"\n🎉 【全て完全一致】")
    print(f"   全6データセット（ERP受注2件、P2P調達2件、MES出荷2件）において、")
    print(f"   ヘッダーと明細のIDが完全に一致しています。")
    print(f"\n✅ 根拠:")
    print(f"   1. 孤立した明細: 全データセットで0件")
    print(f"      → 全ての明細に対応するヘッダーが存在")
    print(f"   2. 明細なしヘッダー: 全データセットで0件")
    print(f"      → 全てのヘッダーに対応する明細が存在")
    print(f"   3. ID集合の完全一致: header_ids == item_ids")
    print(f"      → 両者が完全に同一のID集合を持つ")
    print(f"\n📝 分割方法:")
    print(f"   - ERP受注: order_timestampで分割（ヘッダー基準で明細も同一期間に）")
    print(f"   - P2P調達: order_dateで分割（ヘッダー基準、purchase_order_idでマッチング）")
    print(f"   - MES出荷: shipment_timestampで分割（ヘッダー基準、shipment_idでマッチング）")
else:
    print(f"\n⚠️ 【一部不一致あり】")
    print(f"   以下のデータセットで問題が検出されました:")
    for result in all_results:
        if result['整合性'] != '✅ 完全一致':
            print(f"   - {result['データセット']}: {result['整合性']}")

print(f"\n{'='*100}")
print("検証完了")
print(f"{'='*100}")
