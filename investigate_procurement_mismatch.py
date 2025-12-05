import pandas as pd
from pathlib import Path

# 元ファイルの日付カラムを確認
print("="*100)
print("調達データの日付構造調査")
print("="*100)

# ヘッダーファイル
header_file = r"C:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_header.csv"
item_file = r"C:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_item.csv"

print("\n📂 ヘッダーファイル調査...")
df_header = pd.read_csv(header_file)
df_header['order_date'] = pd.to_datetime(df_header['order_date'], errors='coerce')
df_header['year'] = df_header['order_date'].dt.year

print(f"   総レコード数: {len(df_header):,}")
print(f"   ユニークID数: {df_header['purchase_order_id'].nunique():,}")
print(f"\n   年別ヘッダー数:")
for year in sorted(df_header['year'].dropna().unique()):
    count = len(df_header[df_header['year'] == year])
    print(f"     {int(year)}年: {count:,} レコード")

print("\n📂 明細ファイル調査...")
df_item = pd.read_csv(item_file, low_memory=False)
df_item['ship_date'] = pd.to_datetime(df_item['ship_date'], errors='coerce')
df_item['year'] = df_item['ship_date'].dt.year

print(f"   総レコード数: {len(df_item):,}")
print(f"   ユニークID数: {df_item['purchase_order_id'].nunique():,}")
print(f"   NaN日付: {df_item['ship_date'].isna().sum():,} レコード")
print(f"\n   年別明細数:")
for year in sorted(df_item['year'].dropna().unique()):
    count = len(df_item[df_item['year'] == year])
    print(f"     {int(year)}年: {count:,} レコード")
nan_count = df_item['year'].isna().sum()
if nan_count > 0:
    print(f"     不明（NaN）: {nan_count:,} レコード")

# 問題の調査：ヘッダーと明細で年が異なるケース
print("\n" + "="*100)
print("🔍 問題の根本原因調査")
print("="*100)

# ヘッダーと明細を結合して年を比較
merged = df_item.merge(
    df_header[['purchase_order_id', 'order_date']],
    on='purchase_order_id',
    how='left'
)
merged['header_year'] = pd.to_datetime(merged['order_date']).dt.year
merged['item_year'] = merged['year']

# 年が異なるケースを抽出
mismatched = merged[merged['header_year'] != merged['item_year']]

print(f"\n⚠️ ヘッダーと明細で年が異なるレコード数: {len(mismatched):,}")

if len(mismatched) > 0:
    print(f"\n年の組み合わせ:")
    year_combos = mismatched.groupby(['header_year', 'item_year']).size().reset_index(name='count')
    year_combos = year_combos.sort_values('count', ascending=False)
    print(year_combos.to_string(index=False))
    
    print(f"\n具体例（最初の5件）:")
    sample = mismatched[['purchase_order_id', 'order_date', 'ship_date', 'header_year', 'item_year']].head(5)
    print(sample.to_string(index=False))

print("\n" + "="*100)
print("結論")
print("="*100)
print("""
問題の原因:
  ヘッダー: order_date（発注日）で分割
  明細:     ship_date（出荷日）で分割
  
→ 発注日と出荷日が異なる年にまたがるケースがある
  （例: 2024年12月に発注 → 2025年1月に出荷）
  
解決策:
  1. ヘッダーのorder_dateを基準に明細も分割する
  2. または、purchase_order_idの先頭年（PO-2024-xxxなど）を使用
""")
