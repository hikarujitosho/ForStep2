import pandas as pd
from pathlib import Path

def split_header_item_consistently(header_file, item_file, header_date_col, 
                                   id_col, cutoff_year=2025):
    """
    ヘッダーと明細を整合性を保って分割
    ヘッダーの日付を基準に、IDでマッチングして明細も同じグループに分割
    """
    print(f"\n{'='*100}")
    print(f"整合性を保った分割処理")
    print(f"{'='*100}")
    
    # ファイル読み込み
    print(f"\n📂 ファイル読み込み中...")
    df_header = pd.read_csv(header_file)
    df_item = pd.read_csv(item_file, low_memory=False)
    
    print(f"   ヘッダー: {len(df_header):,} レコード")
    print(f"   明細: {len(df_item):,} レコード")
    
    # ヘッダーの日付で年を判定
    df_header[header_date_col] = pd.to_datetime(df_header[header_date_col])
    df_header['split_year'] = df_header[header_date_col].dt.year
    
    # 年でヘッダーを分割
    df_header_before = df_header[df_header['split_year'] < cutoff_year].copy()
    df_header_after = df_header[df_header['split_year'] >= cutoff_year].copy()
    
    # split_year列を削除
    df_header_before = df_header_before.drop('split_year', axis=1)
    df_header_after = df_header_after.drop('split_year', axis=1)
    
    # ヘッダーのIDリストを取得
    ids_before = set(df_header_before[id_col].unique())
    ids_after = set(df_header_after[id_col].unique())
    
    print(f"\n✂️ 分割基準:")
    print(f"   ヘッダーの日付カラム: {header_date_col}")
    print(f"   {cutoff_year}年以前のヘッダーID数: {len(ids_before):,}")
    print(f"   {cutoff_year}年以降のヘッダーID数: {len(ids_after):,}")
    
    # 明細をヘッダーのIDで分割
    df_item_before = df_item[df_item[id_col].isin(ids_before)].copy()
    df_item_after = df_item[df_item[id_col].isin(ids_after)].copy()
    
    print(f"\n📊 分割結果:")
    print(f"   {cutoff_year}年以前:")
    print(f"     ヘッダー: {len(df_header_before):,} レコード")
    print(f"     明細: {len(df_item_before):,} レコード")
    print(f"   {cutoff_year}年以降:")
    print(f"     ヘッダー: {len(df_header_after):,} レコード")
    print(f"     明細: {len(df_item_after):,} レコード")
    
    # ファイル保存
    header_path = Path(header_file)
    item_path = Path(item_file)
    
    header_before_file = header_path.parent / f"{header_path.stem}_2024以前.csv"
    header_after_file = header_path.parent / f"{header_path.stem}_2025以降.csv"
    item_before_file = item_path.parent / f"{item_path.stem}_2024以前.csv"
    item_after_file = item_path.parent / f"{item_path.stem}_2025以降.csv"
    
    print(f"\n💾 ファイル保存中...")
    df_header_before.to_csv(header_before_file, index=False, encoding='utf-8-sig')
    print(f"   ✅ {header_before_file.name}")
    
    df_header_after.to_csv(header_after_file, index=False, encoding='utf-8-sig')
    print(f"   ✅ {header_after_file.name}")
    
    df_item_before.to_csv(item_before_file, index=False, encoding='utf-8-sig')
    print(f"   ✅ {item_before_file.name}")
    
    df_item_after.to_csv(item_after_file, index=False, encoding='utf-8-sig')
    print(f"   ✅ {item_after_file.name}")
    
    # 整合性検証
    print(f"\n🔍 整合性検証:")
    header_before_ids = set(df_header_before[id_col].unique())
    item_before_ids = set(df_item_before[id_col].unique())
    header_after_ids = set(df_header_after[id_col].unique())
    item_after_ids = set(df_item_after[id_col].unique())
    
    before_ok = header_before_ids == item_before_ids
    after_ok = header_after_ids == item_after_ids
    
    print(f"   2024年以前: {'✅ OK' if before_ok else '❌ NG'}")
    print(f"   2025年以降: {'✅ OK' if after_ok else '❌ NG'}")
    
    return before_ok and after_ok

def main():
    print("\n" + "="*100)
    print("調達データを整合性を保って再分割")
    print("="*100)
    
    base_path = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze\P2P")
    
    success = split_header_item_consistently(
        header_file=base_path / '調達伝票_header.csv',
        item_file=base_path / '調達伝票_item.csv',
        header_date_col='order_date',
        id_col='purchase_order_id',
        cutoff_year=2025
    )
    
    print(f"\n{'='*100}")
    if success:
        print("🎉 整合性を保った分割が完了しました！")
    else:
        print("⚠️ 整合性の問題が残っています")
    print(f"{'='*100}")
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
