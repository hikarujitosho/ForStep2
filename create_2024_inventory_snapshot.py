import pandas as pd
from pathlib import Path

print("="*100)
print("2024年12月31日時点の在庫スナップショット作成")
print("="*100)

base_path = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze\WMS")

# 月次在庫履歴から2024-12のデータを取得
print("\n📂 月次在庫履歴読み込み中...")
df_history = pd.read_csv(base_path / '月次在庫履歴.csv')

# 2024-12のデータを抽出
df_2024_12 = df_history[df_history['year_month'] == '2024-12'].copy()

print(f"   2024-12のレコード数: {len(df_2024_12):,}")
print(f"   製品数: {df_2024_12['product_id'].nunique()}")
print(f"   拠点数: {df_2024_12['location_id'].nunique()}")

# 現在在庫の形式に変換
# カラム: product_id, product_name, location_id, inventory_quantity, inventory_status, last_updated_timestamp
df_snapshot = df_2024_12[['product_id', 'product_name', 'location_id', 
                           'inventory_quantity', 'inventory_status']].copy()

# last_updated_timestampを2024-12-31に設定
df_snapshot['last_updated_timestamp'] = '2024-12-31 23:59:59'

# ソート（製品ID、拠点IDで）
df_snapshot = df_snapshot.sort_values(['location_id', 'product_id']).reset_index(drop=True)

print(f"\n✅ スナップショット作成完了:")
print(f"   レコード数: {len(df_snapshot):,}")
print(f"   タイムスタンプ: 2024-12-31 23:59:59")

# ファイル保存
output_file = base_path / '現在在庫_2024年12月31日.csv'
df_snapshot.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"\n💾 ファイル保存:")
print(f"   ✅ {output_file.name}")

# 内容のサンプル表示
print(f"\n📋 内容サンプル（最初の10件）:")
print(df_snapshot.head(10).to_string(index=False))

# 統計情報
print(f"\n📊 統計情報:")
print(f"   総在庫数: {df_snapshot['inventory_quantity'].sum():,} 台")
print(f"   平均在庫: {df_snapshot['inventory_quantity'].mean():.1f} 台")
print(f"   最大在庫: {df_snapshot['inventory_quantity'].max()} 台")
print(f"   最小在庫: {df_snapshot['inventory_quantity'].min()} 台")

# 拠点別在庫
print(f"\n📍 拠点別在庫数:")
location_inv = df_snapshot.groupby('location_id')['inventory_quantity'].sum().sort_values(ascending=False)
for loc, qty in location_inv.items():
    print(f"   {loc}: {qty:,} 台")

# 製品別在庫（トップ5）
print(f"\n🚗 製品別在庫数（トップ5）:")
product_inv = df_snapshot.groupby('product_name')['inventory_quantity'].sum().sort_values(ascending=False).head(5)
for prod, qty in product_inv.items():
    print(f"   {prod}: {qty:,} 台")

# 現在の在庫と比較
print(f"\n{'='*100}")
print("現在在庫との比較")
print(f"{'='*100}")

df_current = pd.read_csv(base_path / '現在在庫.csv')
print(f"\n現在在庫（2025年12月15日）:")
print(f"   レコード数: {len(df_current):,}")
print(f"   総在庫数: {df_current['inventory_quantity'].sum():,} 台")

print(f"\n2024年12月31日スナップショット:")
print(f"   レコード数: {len(df_snapshot):,}")
print(f"   総在庫数: {df_snapshot['inventory_quantity'].sum():,} 台")

diff = df_current['inventory_quantity'].sum() - df_snapshot['inventory_quantity'].sum()
print(f"\n📈 在庫変化: {diff:+,} 台")

print(f"\n{'='*100}")
print("✅ 2024年12月31日時点の在庫スナップショット作成完了")
print(f"{'='*100}")
