"""
Bronze層のCSVファイルからデータベースを再構築するスクリプト
"""
import sqlite3
import pandas as pd
import os
from pathlib import Path

# パス設定
BRONZE_DIR = Path(r'C:\Users\PC\dev\ForStep2\data\Bronze')
DB_PATH = Path(r'C:\Users\PC\dev\ForStep2\data\bronze_data.db')

# 既存のデータベースを削除
if DB_PATH.exists():
    print(f"既存のデータベースを削除: {DB_PATH}")
    DB_PATH.unlink()

# データベース接続
conn = sqlite3.connect(DB_PATH)
print(f"新しいデータベースを作成: {DB_PATH}")

# テーブルマッピング: (CSVファイルパス, テーブル名)
table_mappings = [
    # ERP
    ('ERP/受注伝票_header.csv', 'erp_order_header'),
    ('ERP/受注伝票_item.csv', 'erp_order_item'),
    ('ERP/品目マスタ.csv', 'erp_product_master'),
    ('ERP/条件マスタ.csv', 'erp_price_condition_master'),
    ('ERP/BOMマスタ.csv', 'erp_bom_master'),
    ('ERP/取引先マスタ.csv', 'erp_partner_master'),
    ('ERP/拠点マスタ.csv', 'erp_location_master'),
    
    # MES
    ('MES/出荷伝票_header.csv', 'mes_shipment_header'),
    ('MES/出荷伝票_item.csv', 'mes_shipment_item'),
    ('MES/取引先マスタ.csv', 'mes_partner_master'),
    ('MES/拠点マスタ.csv', 'mes_location_master'),
    
    # P2P
    ('P2P/調達伝票_header.csv', 'p2p_procurement_header'),
    ('P2P/調達伝票_item.csv', 'p2p_procurement_item'),
    ('P2P/BOMマスタ.csv', 'p2p_bom_master'),
    ('P2P/取引先マスタ.csv', 'p2p_partner_master'),
    
    # TMS
    ('TMS/輸送コスト.csv', 'tms_transportation_cost'),
    ('TMS/取引先マスタ.csv', 'tms_partner_master'),
    ('TMS/拠点マスタ.csv', 'tms_location_master'),
    
    # WMS
    ('WMS/現在在庫.csv', 'wms_current_inventory'),
    ('WMS/月次在庫履歴.csv', 'wms_monthly_inventory_history'),
    ('WMS/拠点マスタ.csv', 'wms_location_master'),
    
    # HR
    ('HR/給与.csv', 'hr_payroll'),
]

print("\n" + "=" * 100)
print("CSVファイルをデータベースにロード")
print("=" * 100)

loaded_count = 0
error_count = 0

for csv_file, table_name in table_mappings:
    csv_path = BRONZE_DIR / csv_file
    
    if not csv_path.exists():
        print(f"⚠️  スキップ: {csv_file} (ファイルが存在しません)")
        error_count += 1
        continue
    
    try:
        # CSVファイルを読み込み
        df = pd.read_csv(csv_path)
        
        # データベースに書き込み
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        print(f"✓  {table_name}: {len(df)}件のレコードをロード ({csv_file})")
        loaded_count += 1
        
    except Exception as e:
        print(f"✗  エラー: {csv_file} -> {table_name}")
        print(f"   {str(e)}")
        error_count += 1

# 統計情報を表示
print("\n" + "=" * 100)
print("データベース構築完了")
print("=" * 100)
print(f"成功: {loaded_count} テーブル")
print(f"エラー: {error_count} テーブル")

# テーブル一覧を確認
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = cursor.fetchall()

print(f"\n作成されたテーブル数: {len(tables)}")
print("\nテーブル一覧:")
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
    count = cursor.fetchone()[0]
    print(f"  - {table[0]}: {count:,}件")

conn.close()
print(f"\nデータベースファイル: {DB_PATH}")
print("処理完了")
