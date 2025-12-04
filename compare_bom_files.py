import pandas as pd
import hashlib

print("=" * 100)
print("BOMマスタの一致確認")
print("=" * 100)

# 2つのBOMマスタを読み込み
erp_bom = pd.read_csv('data/Bronze/ERP/BOMマスタ.csv')
p2p_bom = pd.read_csv('data/Bronze/P2P/BOMマスタ.csv')

print(f"\nERPのBOMマスタ: {len(erp_bom):,}件")
print(f"P2PのBOMマスタ: {len(p2p_bom):,}件")

# カラムの比較
erp_columns = set(erp_bom.columns)
p2p_columns = set(p2p_bom.columns)

print(f"\n【1. カラムの比較】")
print(f"ERPのカラム数: {len(erp_columns)}")
print(f"P2Pのカラム数: {len(p2p_columns)}")

if erp_columns == p2p_columns:
    print("✓ カラムは一致しています")
else:
    erp_only = erp_columns - p2p_columns
    p2p_only = p2p_columns - erp_columns
    
    if erp_only:
        print(f"\nERPにのみ存在するカラム ({len(erp_only)}個):")
        for col in sorted(erp_only):
            print(f"  - {col}")
    
    if p2p_only:
        print(f"\nP2Pにのみ存在するカラム ({len(p2p_only)}個):")
        for col in sorted(p2p_only):
            print(f"  - {col}")

# レコード数の比較
print(f"\n【2. レコード数の比較】")
if len(erp_bom) == len(p2p_bom):
    print(f"✓ レコード数は一致: {len(erp_bom):,}件")
else:
    print(f"⚠ レコード数が不一致:")
    print(f"  ERP: {len(erp_bom):,}件")
    print(f"  P2P: {len(p2p_bom):,}件")
    print(f"  差分: {abs(len(erp_bom) - len(p2p_bom)):,}件")

# データ内容の比較（完全一致チェック）
print(f"\n【3. データ内容の比較】")

# 共通のカラムでソートして比較
common_columns = sorted(list(erp_columns & p2p_columns))

if common_columns:
    # 共通カラムのみで比較
    erp_subset = erp_bom[common_columns].copy()
    p2p_subset = p2p_bom[common_columns].copy()
    
    # ソート（bom_idで）
    if 'bom_id' in common_columns:
        erp_subset = erp_subset.sort_values('bom_id').reset_index(drop=True)
        p2p_subset = p2p_subset.sort_values('bom_id').reset_index(drop=True)
    
    # 完全一致チェック
    if erp_subset.equals(p2p_subset):
        print("✓ データ内容は完全に一致しています")
        are_identical = True
    else:
        print("⚠ データ内容が一致しません")
        are_identical = False
        
        # 差分の詳細を確認
        print(f"\n差分の詳細:")
        
        # bom_idベースで比較
        if 'bom_id' in common_columns:
            erp_ids = set(erp_bom['bom_id'])
            p2p_ids = set(p2p_bom['bom_id'])
            
            erp_only_ids = erp_ids - p2p_ids
            p2p_only_ids = p2p_ids - erp_ids
            common_ids = erp_ids & p2p_ids
            
            print(f"  ERPにのみ存在するbom_id: {len(erp_only_ids)}件")
            print(f"  P2Pにのみ存在するbom_id: {len(p2p_only_ids)}件")
            print(f"  共通のbom_id: {len(common_ids)}件")
            
            if erp_only_ids and len(erp_only_ids) <= 5:
                print(f"\n  ERPにのみ存在するbom_idのサンプル:")
                for bom_id in sorted(list(erp_only_ids))[:5]:
                    print(f"    - {bom_id}")
            
            if p2p_only_ids and len(p2p_only_ids) <= 5:
                print(f"\n  P2Pにのみ存在するbom_idのサンプル:")
                for bom_id in sorted(list(p2p_only_ids))[:5]:
                    print(f"    - {bom_id}")
            
            # 共通IDでデータの違いをチェック
            if common_ids:
                diff_count = 0
                for bom_id in list(common_ids)[:5]:  # 最初の5件をチェック
                    erp_row = erp_bom[erp_bom['bom_id'] == bom_id].iloc[0]
                    p2p_row = p2p_bom[p2p_bom['bom_id'] == bom_id].iloc[0]
                    
                    for col in common_columns:
                        if str(erp_row[col]) != str(p2p_row[col]):
                            if diff_count == 0:
                                print(f"\n  データ値の違い（サンプル）:")
                            print(f"    {bom_id} - {col}:")
                            print(f"      ERP: {erp_row[col]}")
                            print(f"      P2P: {p2p_row[col]}")
                            diff_count += 1
                            if diff_count >= 10:
                                break
                    if diff_count >= 10:
                        break
else:
    print("共通カラムがありません")
    are_identical = False

# ファイルハッシュの比較
print(f"\n【4. ファイルハッシュの比較】")
def get_file_hash(filepath):
    with open(filepath, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

erp_hash = get_file_hash('data/Bronze/ERP/BOMマスタ.csv')
p2p_hash = get_file_hash('data/Bronze/P2P/BOMマスタ.csv')

print(f"ERP MD5: {erp_hash}")
print(f"P2P MD5: {p2p_hash}")

if erp_hash == p2p_hash:
    print("✓ ファイルは完全に同一です")
else:
    print("⚠ ファイルが異なります")

# サマリー
print(f"\n" + "=" * 100)
print("【サマリー】")
print("=" * 100)

if erp_hash == p2p_hash:
    print("✓ 2つのBOMマスタは完全に一致しています。")
    print("  対応不要です。")
else:
    print("⚠ 2つのBOMマスタは一致していません。")
    print("\n推奨対応:")
    print("  ERPのBOMマスタをP2Pフォルダにコピーして同期します。")
    print(f"\n  コマンド:")
    print(f"    Copy-Item 'data/Bronze/ERP/BOMマスタ.csv' 'data/Bronze/P2P/BOMマスタ.csv' -Force")

print("=" * 100)
