import pandas as pd

print("=" * 100)
print("調達伝票_headerから不要なカラムを削除")
print("=" * 100)

# データ読み込み
df = pd.read_csv('data/Bronze/P2P/調達伝票_header.csv', low_memory=False)

print(f"\n読み込み: {len(df):,}件")
print(f"削除前のカラム数: {len(df.columns)}")

# 削除対象のカラム
columns_to_remove = ['project_code', 'amount_changed']

# 存在するカラムのみを削除
existing_columns_to_remove = [col for col in columns_to_remove if col in df.columns]

print(f"\n削除対象カラム:")
for col in existing_columns_to_remove:
    null_count = df[col].isna().sum()
    print(f"  - {col}: {null_count:,}件がNULL ({null_count/len(df)*100:.2f}%)")

if not existing_columns_to_remove:
    print("  削除対象カラムが見つかりませんでした。")
else:
    # カラムを削除
    df = df.drop(columns=existing_columns_to_remove)
    
    print(f"\n削除後のカラム数: {len(df.columns)}")
    print(f"削除したカラム数: {len(existing_columns_to_remove)}")
    
    # 保存
    output_path = 'data/Bronze/P2P/調達伝票_header.csv'
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"\n更新したファイルを {output_path} に保存しました。")
    
    # 残っているカラムを表示
    print(f"\n残っているカラム一覧:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")

print("=" * 100)
