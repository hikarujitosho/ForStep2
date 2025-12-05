import pandas as pd
from pathlib import Path

print("="*100)
print("TMS輸送コストデータの整合性検証")
print("="*100)

base_path = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze\TMS")

# 分割後のファイル読み込み
df_before = pd.read_csv(base_path / '輸送コスト_2024以前.csv')
df_after = pd.read_csv(base_path / '輸送コスト_2025以降.csv')

print(f"\n📊 分割結果:")
print(f"   2024年以前: {len(df_before):,} レコード")
print(f"   2025年以降: {len(df_after):,} レコード")
print(f"   合計: {len(df_before) + len(df_after):,} レコード")

# 重複チェック
print(f"\n🔍 重複チェック:")
cost_ids_before = set(df_before['cost_id'])
cost_ids_after = set(df_after['cost_id'])
overlap = cost_ids_before & cost_ids_after

if len(overlap) == 0:
    print(f"   ✅ 重複なし - 全てのcost_idがユニーク")
else:
    print(f"   ❌ 重複あり: {len(overlap)}件")
    print(f"      例: {list(overlap)[:5]}")

# shipment_ID分析
print(f"\n📦 shipment_ID分析:")
shipment_before = df_before['shipment_id'].nunique()
shipment_after = df_after['shipment_id'].nunique()
print(f"   2024年以前のユニーク輸送ID: {shipment_before:,}")
print(f"   2025年以降のユニーク輸送ID: {shipment_after:,}")

# shipment_IDが両方に存在するケース
shipment_ids_before = set(df_before['shipment_id'])
shipment_ids_after = set(df_after['shipment_id'])
cross_year_shipments = shipment_ids_before & shipment_ids_after

if len(cross_year_shipments) > 0:
    print(f"\n   ⚠️ 年をまたぐ輸送: {len(cross_year_shipments)}件")
    print(f"      （同じshipment_idが両期間に存在）")
    print(f"      例: {list(cross_year_shipments)[:5]}")
    
    # 詳細分析
    cross_year_data = []
    for sid in list(cross_year_shipments)[:3]:
        before_costs = df_before[df_before['shipment_id'] == sid]
        after_costs = df_after[df_after['shipment_id'] == sid]
        cross_year_data.append({
            'shipment_id': sid,
            '2024以前件数': len(before_costs),
            '2025以降件数': len(after_costs),
            '2024以前billing_date': before_costs['billing_date'].tolist(),
            '2025以降billing_date': after_costs['billing_date'].tolist()
        })
    
    print(f"\n   詳細（最初の3件）:")
    for item in cross_year_data:
        print(f"      shipment_id: {item['shipment_id']}")
        print(f"         2024以前: {item['2024以前件数']}件 - {item['2024以前billing_date']}")
        print(f"         2025以降: {item['2025以降件数']}件 - {item['2025以降billing_date']}")
else:
    print(f"   ✅ 年をまたぐ輸送なし - 全てのshipment_idは単一期間")

# コストタイプ分析
print(f"\n💰 コストタイプ分析:")
print(f"   2024年以前:")
cost_types_before = df_before['cost_type'].value_counts()
for ct, count in cost_types_before.items():
    print(f"      {ct}: {count:,}件")

print(f"   2025年以降:")
cost_types_after = df_after['cost_type'].value_counts()
for ct, count in cost_types_after.items():
    print(f"      {ct}: {count:,}件")

print(f"\n{'='*100}")
print("✅ TMS輸送データの整合性検証完了")
print(f"{'='*100}")
