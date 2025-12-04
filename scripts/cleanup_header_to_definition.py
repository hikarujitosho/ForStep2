import pandas as pd

print("=" * 100)
print("調達伝票_headerから定義書に記載のないカラムを削除")
print("=" * 100)

# 定義書に記載されているカラムのみを保持
defined_columns = [
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
]

# データ読み込み
df = pd.read_csv('data/Bronze/P2P/調達伝票_header.csv', low_memory=False)

print(f"\n読み込み: {len(df):,}件")
print(f"削除前のカラム数: {len(df.columns)}")

# 削除されるカラムを特定
current_columns = list(df.columns)
columns_to_remove = [col for col in current_columns if col not in defined_columns]

print(f"\n削除対象カラム ({len(columns_to_remove)}個):")
for col in columns_to_remove:
    # 各カラムのデータ状況を表示
    null_count = df[col].isna().sum()
    non_null_count = len(df) - null_count
    print(f"  - {col}: 有効データ {non_null_count:,}件 ({non_null_count/len(df)*100:.2f}%)")

# 定義書に記載されているカラムのみを保持
df_clean = df[defined_columns].copy()

print(f"\n削除後のカラム数: {len(df_clean.columns)}")
print(f"削除したカラム数: {len(columns_to_remove)}")

# バックアップ（削除前のデータ）
backup_path = 'output/調達伝票_header_全カラム版.csv'
df.to_csv(backup_path, index=False, encoding='utf-8-sig')
print(f"\n削除前のデータを {backup_path} に保存しました。")

# 保存
output_path = 'data/Bronze/P2P/調達伝票_header.csv'
df_clean.to_csv(output_path, index=False, encoding='utf-8')
print(f"クリーンアップしたファイルを {output_path} に保存しました。")

# 残っているカラムを表示
print(f"\n残っているカラム一覧 (定義書に記載の{len(df_clean.columns)}カラム):")
for i, col in enumerate(df_clean.columns, 1):
    print(f"  {i:2d}. {col}")

print("\n" + "=" * 100)
print("✓ 調達伝票_headerが定義書に合わせてクリーンアップされました")
print("=" * 100)
