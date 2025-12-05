import os
from pathlib import Path

print("="*100)
print("Bronze層ファイル名 vs テーブル定義書 整合性検証")
print("="*100)

bronze_path = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze")

# 実際のファイル名を収集（pre24/post25などのサフィックスを除く）
actual_files = {}

for csv_file in bronze_path.rglob("*.csv"):
    file_name = csv_file.stem  # 拡張子を除く
    
    # pre24, post25, dec24などのサフィックスを除去
    base_name = file_name
    for suffix in ["_pre24", "_post25", "_dec24"]:
        if base_name.endswith(suffix):
            base_name = base_name[:-len(suffix)]
            break
    
    # どのシステムフォルダに属するか判定
    parts = csv_file.parts
    if "original" in parts:
        folder_idx = parts.index("original")
        if folder_idx + 1 < len(parts):
            system = parts[folder_idx + 1]
        else:
            system = "unknown"
    elif "pre24" in parts:
        folder_idx = parts.index("pre24")
        if folder_idx + 1 < len(parts):
            system = parts[folder_idx + 1]
        else:
            system = "unknown"
    elif "post25" in parts:
        folder_idx = parts.index("post25")
        if folder_idx + 1 < len(parts):
            system = parts[folder_idx + 1]
        else:
            system = "unknown"
    else:
        system = "unknown"
    
    if system not in actual_files:
        actual_files[system] = set()
    actual_files[system].add(base_name)

# テーブル定義書に記載されているテーブル名（英語版）
expected_tables = {
    "ERP": {
        "sales_order_header",
        "sales_order_item",
        "product_master",
        "pricing_conditions",
        "bom_master",
        "partner_master",
        "location_master"
    },
    "P2P": {
        "procurement_header",
        "procurement_item",
        "partner_master",
        "bom_master"
    },
    "TMS": {
        "transportation_cost",
        "partner_master",
        "location_master"
    },
    "WMS": {
        "current_inventory",
        "monthly_inventory",
        "location_master"
    },
    "MES": {
        "shipment_header",
        "shipment_item",
        "partner_master",
        "location_master"
    },
    "HR": {
        "payroll"
    }
}

# 日本語ファイル名から英語テーブル名へのマッピング
japanese_to_english = {
    "受注伝票_header": "sales_order_header",
    "受注伝票_item": "sales_order_item",
    "品目マスタ": "product_master",
    "条件マスタ": "pricing_conditions",
    "BOMマスタ": "bom_master",
    "取引先マスタ": "partner_master",
    "拠点マスタ": "location_master",
    "調達伝票_header": "procurement_header",
    "調達伝票_item": "procurement_item",
    "輸送コスト": "transportation_cost",
    "現在在庫": "current_inventory",
    "月次在庫履歴": "monthly_inventory",
    "出荷伝票_header": "shipment_header",
    "出荷伝票_item": "shipment_item",
    "給与テーブル": "payroll"
}

print("\n【検証結果】\n")

all_match = True
issues = []

for system in sorted(expected_tables.keys()):
    print(f"{'='*100}")
    print(f"システム: {system}")
    print(f"{'='*100}")
    
    expected = expected_tables[system]
    actual = actual_files.get(system, set())
    
    # 日本語ファイル名を英語に変換
    actual_english = set()
    for file_name in actual:
        english_name = japanese_to_english.get(file_name, file_name)
        actual_english.add(english_name)
    
    print(f"\n📋 期待されるテーブル名（定義書）: {len(expected)}件")
    for table in sorted(expected):
        print(f"   - {table}")
    
    print(f"\n📁 実際のファイル名（英語変換後）: {len(actual_english)}件")
    for table in sorted(actual_english):
        print(f"   - {table}")
    
    if actual != actual_english:
        print(f"\n📁 実際のファイル名（変換前）: {len(actual)}件")
        for file in sorted(actual):
            english = japanese_to_english.get(file, file)
            if file != english:
                print(f"   - {file} → {english}")
            else:
                print(f"   - {file}")
    
    # 一致チェック
    missing = expected - actual_english
    extra = actual_english - expected
    
    if missing or extra:
        all_match = False
        print(f"\n⚠️ 不一致あり")
        
        if missing:
            print(f"\n   ❌ 定義書にあるがファイルが存在しない:")
            for table in sorted(missing):
                print(f"      - {table}")
                issues.append(f"{system}: ファイル不足 - {table}")
        
        if extra:
            print(f"\n   ❌ ファイルはあるが定義書にない:")
            for table in sorted(extra):
                print(f"      - {table}")
                issues.append(f"{system}: 定義書不足 - {table}")
    else:
        print(f"\n✅ 完全一致！")
    
    print()

print(f"{'='*100}")
print("総合判定")
print(f"{'='*100}\n")

if all_match:
    print("🎉 全システムで完全一致！")
    print("   すべてのファイル名とテーブル定義書が整合しています。\n")
else:
    print(f"⚠️ 不一致が検出されました（{len(issues)}件）\n")
    for issue in issues:
        print(f"   - {issue}")
    
    print("\n📝 対応が必要な項目:")
    print("   1. originalフォルダの日本語ファイル名を英語にリネーム")
    print("   2. または、定義書を実際のファイル構造に合わせて修正")

print(f"\n{'='*100}")
print("検証完了")
print(f"{'='*100}")
