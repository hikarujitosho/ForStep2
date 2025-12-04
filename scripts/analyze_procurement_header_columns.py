import pandas as pd

print("=" * 100)
print("調達伝票_headerのカラム分析")
print("=" * 100)

# 定義書に記載されているカラム（順不同）
defined_columns = {
    'purchase_order_id',
    'order_date',
    'expected_delivery_date',
    'supplier_id',
    'supplier_name',
    'account_group',
    'location_id',
    'purchase_order_number',
    'currency',
    'order_subtotal_ex_tax',
    'shipping_fee_ex_tax',
    'tax_amount',
    'discount_amount_incl_tax',
    'order_total_incl_tax',
    'order_status',
    'approver',
    'payment_method',
    'payment_confirmation_id',
    'payment_date',
    'payment_amount'
}

# 実際のCSVファイルを読み込み
df = pd.read_csv('data/Bronze/P2P/調達伝票_header.csv', low_memory=False, nrows=100)

actual_columns = set(df.columns)

print(f"\n【1. カラム数の比較】")
print(f"  定義書に記載されているカラム数: {len(defined_columns)}")
print(f"  実際のCSVファイルのカラム数: {len(actual_columns)}")

# 定義書にないカラム
extra_columns = actual_columns - defined_columns
if extra_columns:
    print(f"\n【2. 定義書に記載されていないカラム】({len(extra_columns)}個)")
    for col in sorted(extra_columns):
        print(f"  - {col}")
else:
    print(f"\n【2. 定義書に記載されていないカラム】")
    print(f"  なし")

# 定義書にあって実際にはないカラム
missing_columns = defined_columns - actual_columns
if missing_columns:
    print(f"\n【3. 定義書にあるが実際のファイルにないカラム】({len(missing_columns)}個)")
    for col in sorted(missing_columns):
        print(f"  - {col}")
else:
    print(f"\n【3. 定義書にあるが実際のファイルにないカラム】")
    print(f"  なし")

# 全データを読み込んで空のカラムをチェック
print(f"\n全データを読み込んで空カラムをチェック中...")
df_full = pd.read_csv('data/Bronze/P2P/調達伝票_header.csv', low_memory=False)
print(f"総レコード数: {len(df_full):,}件")

print(f"\n【4. 値が入っていないカラム（全行がNULLまたは空）】")
empty_columns = []
for col in df_full.columns:
    null_count = df_full[col].isna().sum()
    empty_count = (df_full[col] == '').sum() if df_full[col].dtype == 'object' else 0
    total_empty = null_count + empty_count
    
    if total_empty == len(df_full):
        empty_columns.append(col)
        print(f"  - {col}: 全{len(df_full):,}件が空")

if not empty_columns:
    print(f"  なし（すべてのカラムに何らかの値が入っています）")

# ほとんど空のカラム（95%以上が空）
print(f"\n【5. ほとんど値が入っていないカラム（95%以上が空）】")
mostly_empty_columns = []
for col in df_full.columns:
    null_count = df_full[col].isna().sum()
    empty_count = (df_full[col] == '').sum() if df_full[col].dtype == 'object' else 0
    total_empty = null_count + empty_count
    empty_ratio = total_empty / len(df_full) * 100
    
    if 95 <= empty_ratio < 100:
        mostly_empty_columns.append((col, empty_ratio))
        print(f"  - {col}: {empty_ratio:.2f}%が空 (有効データ: {len(df_full) - total_empty:,}件)")

if not mostly_empty_columns:
    print(f"  なし")

# 定義書にないかつ空のカラム
extra_and_empty = set(empty_columns) & extra_columns
if extra_and_empty:
    print(f"\n【6. 定義書に記載がなく、かつ値も入っていないカラム】({len(extra_and_empty)}個)")
    for col in sorted(extra_and_empty):
        print(f"  - {col}")
    print(f"\n  → これらのカラムは削除候補です。")
else:
    print(f"\n【6. 定義書に記載がなく、かつ値も入っていないカラム】")
    print(f"  なし")

# サマリー
print(f"\n" + "=" * 100)
print("【サマリー】")
print("=" * 100)
print(f"定義書に記載されていないカラム: {len(extra_columns)}個")
print(f"値が全く入っていないカラム: {len(empty_columns)}個")
print(f"定義書にも記載がなく値も入っていないカラム: {len(extra_and_empty)}個")

if extra_and_empty:
    print(f"\n推奨アクション:")
    print(f"  以下のカラムは定義書に記載がなく、データも空なので削除を推奨:")
    for col in sorted(extra_and_empty):
        print(f"    - {col}")

print("=" * 100)
