import pandas as pd
import hashlib

def get_file_hash(filepath):
    """ファイルのMD5ハッシュを計算"""
    md5_hash = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

# BOMマスタファイルの読み込み
erp_bom = pd.read_csv('data/Bronze/ERP/BOMマスタ.csv')
p2p_bom = pd.read_csv('data/Bronze/P2P/BOMマスタ.csv')

print("=" * 80)
print("BOMマスタ同期確認")
print("=" * 80)

print(f"\nERPのBOMマスタ: {len(erp_bom)}件")
print(f"P2PのBOMマスタ: {len(p2p_bom)}件")

# レコード数の確認
if len(erp_bom) == len(p2p_bom):
    print("✓ レコード数が一致しています")
else:
    print(f"✗ レコード数が不一致です (差分: {abs(len(erp_bom) - len(p2p_bom))}件)")

# ファイルハッシュの確認
erp_hash = get_file_hash('data/Bronze/ERP/BOMマスタ.csv')
p2p_hash = get_file_hash('data/Bronze/P2P/BOMマスタ.csv')

print(f"\nファイルハッシュ:")
print(f"  ERP MD5: {erp_hash}")
print(f"  P2P MD5: {p2p_hash}")

if erp_hash == p2p_hash:
    print("✓ ファイルが完全に一致しています")
else:
    print("✗ ファイルハッシュが異なります")

# データ内容の確認
erp_bom_ids = set(erp_bom['bom_id'])
p2p_bom_ids = set(p2p_bom['bom_id'])

only_in_erp = erp_bom_ids - p2p_bom_ids
only_in_p2p = p2p_bom_ids - erp_bom_ids

print(f"\nデータ内容:")
if len(only_in_erp) == 0 and len(only_in_p2p) == 0:
    print("✓ BOM IDが完全に一致しています")
else:
    print(f"  ERPにのみ存在: {len(only_in_erp)}件")
    print(f"  P2Pにのみ存在: {len(only_in_p2p)}件")

print("\n" + "=" * 80)
if erp_hash == p2p_hash:
    print("【結果】BOMマスタの同期が完了しました ✓")
else:
    print("【結果】同期に問題がある可能性があります")
print("=" * 80)
