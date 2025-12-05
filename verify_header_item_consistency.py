import pandas as pd
from pathlib import Path

def verify_consistency(header_file, item_file, header_id_col, item_id_col, data_type):
    """
    ヘッダーと明細ファイルの整合性を検証
    """
    print(f"\n{'='*100}")
    print(f"整合性検証: {data_type}")
    print(f"{'='*100}")
    
    # ファイル読み込み
    print(f"\n📂 ファイル読み込み中...")
    df_header = pd.read_csv(header_file)
    df_item = pd.read_csv(item_file)
    
    print(f"   ヘッダー: {len(df_header):,} レコード")
    print(f"   明細: {len(df_item):,} レコード")
    
    # ID取得
    header_ids = set(df_header[header_id_col].unique())
    item_ids = set(df_item[item_id_col].unique())
    
    print(f"\n🔍 ID数:")
    print(f"   ヘッダーのユニークID数: {len(header_ids):,}")
    print(f"   明細のユニークID数: {len(item_ids):,}")
    
    # 整合性チェック
    orphan_items = item_ids - header_ids  # 明細にあるがヘッダーにないID
    missing_items = header_ids - item_ids  # ヘッダーにあるが明細にないID
    
    print(f"\n✅ 整合性チェック結果:")
    
    if len(orphan_items) == 0 and len(missing_items) == 0:
        print(f"   🎉 完全一致！ヘッダーと明細のIDが完全に一致しています")
        return True
    
    if len(orphan_items) > 0:
        print(f"   ⚠️ 孤立した明細: {len(orphan_items):,}件")
        print(f"      （明細にあるがヘッダーにないID）")
        if len(orphan_items) <= 5:
            print(f"      例: {list(orphan_items)}")
    
    if len(missing_items) > 0:
        print(f"   ⚠️ 明細がないヘッダー: {len(missing_items):,}件")
        print(f"      （ヘッダーにあるが明細にないID）")
        if len(missing_items) <= 5:
            print(f"      例: {list(missing_items)}")
    
    return len(orphan_items) == 0 and len(missing_items) == 0

def main():
    base_path = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze")
    
    print("\n" + "="*100)
    print("Bronze層データ分割後の整合性検証")
    print("="*100)
    
    # 検証対象
    test_cases = [
        {
            'data_type': '受注データ（2024年以前）',
            'header_file': base_path / 'ERP' / '受注伝票_header_2024以前.csv',
            'item_file': base_path / 'ERP' / '受注伝票_item_2024以前.csv',
            'header_id_col': 'order_id',
            'item_id_col': 'order_id'
        },
        {
            'data_type': '受注データ（2025年以降）',
            'header_file': base_path / 'ERP' / '受注伝票_header_2025以降.csv',
            'item_file': base_path / 'ERP' / '受注伝票_item_2025以降.csv',
            'header_id_col': 'order_id',
            'item_id_col': 'order_id'
        },
        {
            'data_type': '調達データ（2024年以前）',
            'header_file': base_path / 'P2P' / '調達伝票_header_2024以前.csv',
            'item_file': base_path / 'P2P' / '調達伝票_item_2024以前.csv',
            'header_id_col': 'purchase_order_id',
            'item_id_col': 'purchase_order_id'
        },
        {
            'data_type': '調達データ（2025年以降）',
            'header_file': base_path / 'P2P' / '調達伝票_header_2025以降.csv',
            'item_file': base_path / 'P2P' / '調達伝票_item_2025以降.csv',
            'header_id_col': 'purchase_order_id',
            'item_id_col': 'purchase_order_id'
        }
    ]
    
    results = []
    for test_case in test_cases:
        result = verify_consistency(
            test_case['header_file'],
            test_case['item_file'],
            test_case['header_id_col'],
            test_case['item_id_col'],
            test_case['data_type']
        )
        results.append({
            'data_type': test_case['data_type'],
            'result': '✅ OK' if result else '❌ NG'
        })
    
    # サマリー
    print(f"\n{'='*100}")
    print("検証結果サマリー")
    print(f"{'='*100}")
    
    for r in results:
        print(f"{r['result']} {r['data_type']}")
    
    all_ok = all(r['result'] == '✅ OK' for r in results)
    
    print(f"\n{'='*100}")
    if all_ok:
        print("🎉 全ての検証に合格しました！")
        print("   ヘッダーと明細の整合性に問題はありません。")
    else:
        print("⚠️ 一部の検証で問題が見つかりました。")
        print("   上記の詳細を確認してください。")
    print(f"{'='*100}")
    
    return all_ok

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
