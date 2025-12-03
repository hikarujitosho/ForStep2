"""
Bronze層のCSVファイルからSilver層データを作成し、月次粒度でサマライズするスクリプト
日付が一致しない複数のソースシステムのデータを統合処理します。
"""
import sqlite3
import pandas as pd
import os
from pathlib import Path
from datetime import datetime

# パス設定
BRONZE_DIR = Path(r'C:\Users\PC\dev\ForStep2\data\Bronze')
DB_PATH = Path(r'C:\Users\PC\dev\ForStep2\data\bronze_data.db')

print("=" * 100)
print("Bronze層からSilver層への変換（月次サマライズ）")
print("=" * 100)

# 既存のデータベースを削除
if DB_PATH.exists():
    print(f"\n既存のデータベースを削除: {DB_PATH}")
    DB_PATH.unlink()

# データベース接続
conn = sqlite3.connect(DB_PATH)
print(f"新しいデータベースを作成: {DB_PATH}\n")

# ========================================
# ステップ1: Bronze層データのロード
# ========================================
print("=" * 100)
print("ステップ1: Bronze層CSVファイルのロード")
print("=" * 100)

# テーブルマッピング
bronze_tables = [
    # ERP
    ('ERP/受注伝票_header.csv', 'bronze_erp_order_header'),
    ('ERP/受注伝票_item.csv', 'bronze_erp_order_item'),
    ('ERP/品目マスタ.csv', 'bronze_erp_product_master'),
    ('ERP/条件マスタ.csv', 'bronze_erp_price_condition_master'),
    ('ERP/BOMマスタ.csv', 'bronze_erp_bom_master'),
    ('ERP/取引先マスタ.csv', 'bronze_erp_partner_master'),
    ('ERP/拠点マスタ.csv', 'bronze_erp_location_master'),
    
    # MES
    ('MES/出荷伝票_header.csv', 'bronze_mes_shipment_header'),
    ('MES/出荷伝票_item.csv', 'bronze_mes_shipment_item'),
    ('MES/取引先マスタ.csv', 'bronze_mes_partner_master'),
    ('MES/拠点マスタ.csv', 'bronze_mes_location_master'),
    
    # P2P
    ('P2P/調達伝票_header.csv', 'bronze_p2p_procurement_header'),
    ('P2P/調達伝票_item.csv', 'bronze_p2p_procurement_item'),
    ('P2P/BOMマスタ.csv', 'bronze_p2p_bom_master'),
    ('P2P/取引先マスタ.csv', 'bronze_p2p_partner_master'),
    
    # TMS
    ('TMS/輸送コスト.csv', 'bronze_tms_transportation_cost'),
    ('TMS/取引先マスタ.csv', 'bronze_tms_partner_master'),
    ('TMS/拠点マスタ.csv', 'bronze_tms_location_master'),
    
    # WMS
    ('WMS/現在在庫.csv', 'bronze_wms_current_inventory'),
    ('WMS/月次在庫履歴.csv', 'bronze_wms_monthly_inventory_history'),
    ('WMS/拠点マスタ.csv', 'bronze_wms_location_master'),
    
    # HR
    ('HR/給与テーブル.csv', 'bronze_hr_payroll'),
]

for csv_file, table_name in bronze_tables:
    csv_path = BRONZE_DIR / csv_file
    
    if not csv_path.exists():
        print(f"[SKIP] {csv_file}")
        continue
    
    try:
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"[OK] {table_name}: {len(df):,}件")
    except Exception as e:
        print(f"[ERROR] {csv_file} - {str(e)}")

# ========================================
# ステップ2: Silver層への変換（月次サマライズ）
# ========================================
print("\n" + "=" * 100)
print("ステップ2: Silver層への変換（月次粒度でサマライズ）")
print("=" * 100)

# 2-1. 月次受注データ（売上）
print("\n[Silver] 月次受注データ作成...")
query_monthly_orders = """
CREATE TABLE silver_monthly_orders AS
SELECT 
    strftime('%Y-%m', boh.order_timestamp) as year_month,
    boi.product_id,
    bpm.product_name,
    bpm.item_hierarchy,
    bpm.detail_category,
    bpm.import_export_group,
    COUNT(DISTINCT boh.order_id) as order_count,
    SUM(boi.quantity) as total_quantity,
    SUM(boi.quantity * bpc.selling_price_ex_tax) as total_sales_ex_tax,
    AVG(bpc.selling_price_ex_tax) as avg_selling_price
FROM bronze_erp_order_item boi
INNER JOIN bronze_erp_order_header boh ON boi.order_id = boh.order_id
INNER JOIN bronze_erp_product_master bpm ON boi.product_id = bpm.product_id
INNER JOIN bronze_erp_price_condition_master bpc ON 
    boi.product_id = bpc.product_id 
    AND boh.customer_id = bpc.customer_id
    AND DATE(boh.order_timestamp) >= bpc.valid_from 
    AND DATE(boh.order_timestamp) < bpc.valid_to
GROUP BY strftime('%Y-%m', boh.order_timestamp), boi.product_id, bpm.product_name, 
         bpm.item_hierarchy, bpm.detail_category, bpm.import_export_group
"""
conn.execute(query_monthly_orders)
result = conn.execute("SELECT COUNT(*) FROM silver_monthly_orders").fetchone()
print(f"  作成: {result[0]:,}件の月次受注レコード")

# 2-2. 月次調達データ（売上原価）
print("\n[Silver] 月次調達データ作成...")
query_monthly_procurement = """
CREATE TABLE silver_monthly_procurement AS
SELECT 
    strftime('%Y-%m', bph.order_date) as year_month,
    bpi.product_id,
    bpi.material_type,
    bpi.material_category,
    COUNT(DISTINCT bph.purchase_order_id) as procurement_count,
    SUM(bpi.quantity) as total_quantity,
    SUM(bpi.quantity * bpi.unit_price_ex_tax) as total_cost_ex_tax,
    AVG(bpi.unit_price_ex_tax) as avg_unit_price
FROM bronze_p2p_procurement_item bpi
INNER JOIN bronze_p2p_procurement_header bph ON bpi.purchase_order_id = bph.purchase_order_id
WHERE bpi.product_id IS NOT NULL AND bpi.product_id != ''
GROUP BY strftime('%Y-%m', bph.order_date), bpi.product_id, bpi.material_type, bpi.material_category
"""
conn.execute(query_monthly_procurement)
result = conn.execute("SELECT COUNT(*) FROM silver_monthly_procurement").fetchone()
print(f"  作成: {result[0]:,}件の月次調達レコード")

# 2-3. 月次出荷データ（リードタイム）
print("\n[Silver] 月次出荷データ作成...")
query_monthly_shipment = """
CREATE TABLE silver_monthly_shipment AS
SELECT 
    strftime('%Y-%m', bsh.shipment_timestamp) as year_month,
    bsi.product_id,
    bsi.order_id,
    COUNT(DISTINCT bsh.shipment_id) as shipment_count,
    SUM(bsi.quantity) as total_quantity,
    SUM(CASE WHEN bsi.delivery_status = 'delivered' THEN 1 ELSE 0 END) as delivered_count,
    SUM(CASE WHEN bsi.delivery_status = 'delayed' THEN 1 ELSE 0 END) as delayed_count,
    AVG(CASE 
        WHEN bsi.actual_arrival_timestamp IS NOT NULL 
        THEN julianday(bsi.actual_arrival_timestamp) - julianday(bsi.actual_ship_timestamp)
        ELSE NULL 
    END) as avg_delivery_days
FROM bronze_mes_shipment_item bsi
INNER JOIN bronze_mes_shipment_header bsh ON bsi.shipment_id = bsh.shipment_id
GROUP BY strftime('%Y-%m', bsh.shipment_timestamp), bsi.product_id, bsi.order_id
"""
conn.execute(query_monthly_shipment)
result = conn.execute("SELECT COUNT(*) FROM silver_monthly_shipment").fetchone()
print(f"  作成: {result[0]:,}件の月次出荷レコード")

# 2-4. 月次輸送コストデータ
print("\n[Silver] 月次輸送コストデータ作成...")
query_monthly_transport = """
CREATE TABLE silver_monthly_transport AS
SELECT 
    strftime('%Y-%m', btc.billing_date) as year_month,
    btc.shipment_id,
    btc.cost_type,
    btc.location_id,
    COUNT(*) as cost_record_count,
    SUM(btc.cost_amount) as total_cost_amount,
    AVG(btc.cost_amount) as avg_cost_amount
FROM bronze_tms_transportation_cost btc
GROUP BY strftime('%Y-%m', btc.billing_date), btc.shipment_id, btc.cost_type, btc.location_id
"""
conn.execute(query_monthly_transport)
result = conn.execute("SELECT COUNT(*) FROM silver_monthly_transport").fetchone()
print(f"  作成: {result[0]:,}件の月次輸送コストレコード")

# 2-5. 月次給与データ（人件費）
print("\n[Silver] 月次給与データ作成...")
query_monthly_payroll = """
CREATE TABLE silver_monthly_payroll AS
SELECT 
    bhp.payment_period as year_month,
    bhp.department,
    bhp.cost_center,
    bhp.employment_type,
    COUNT(DISTINCT bhp.employee_id) as employee_count,
    SUM(bhp.base_salary) as total_base_salary,
    SUM(bhp.overtime_pay) as total_overtime_pay,
    SUM(bhp.allowances) as total_allowances,
    SUM(bhp.deductions) as total_deductions,
    SUM(bhp.net_salary) as total_net_salary,
    AVG(bhp.base_salary) as avg_base_salary
FROM bronze_hr_payroll bhp
GROUP BY bhp.payment_period, bhp.department, bhp.cost_center, bhp.employment_type
"""
conn.execute(query_monthly_payroll)
result = conn.execute("SELECT COUNT(*) FROM silver_monthly_payroll").fetchone()
print(f"  作成: {result[0]:,}件の月次給与レコード")

# 2-6. マスタデータはそのまま継承（最新版を使用）
print("\n[Silver] マスタデータの継承...")
master_tables = [
    ('bronze_erp_product_master', 'silver_product_master'),
    ('bronze_erp_partner_master', 'silver_partner_master'),
    ('bronze_erp_location_master', 'silver_location_master'),
    ('bronze_erp_bom_master', 'silver_bom_master'),
]

for bronze_table, silver_table in master_tables:
    query = f"CREATE TABLE {silver_table} AS SELECT * FROM {bronze_table}"
    conn.execute(query)
    result = conn.execute(f"SELECT COUNT(*) FROM {silver_table}").fetchone()
    print(f"  {silver_table}: {result[0]:,}件")

# ========================================
# ステップ3: 後方互換性のためのビュー作成
# ========================================
print("\n" + "=" * 100)
print("ステップ3: 後方互換性のためのビュー作成")
print("=" * 100)

# 既存のクエリが動作するように、従来のテーブル名でビューを作成
compatibility_views = [
    ('erp_order_header', 'bronze_erp_order_header'),
    ('erp_order_item', 'bronze_erp_order_item'),
    ('erp_product_master', 'silver_product_master'),
    ('erp_price_condition_master', 'bronze_erp_price_condition_master'),
    ('erp_bom_master', 'silver_bom_master'),
    ('erp_partner_master', 'silver_partner_master'),
    ('erp_location_master', 'silver_location_master'),
    ('mes_shipment_header', 'bronze_mes_shipment_header'),
    ('mes_shipment_item', 'bronze_mes_shipment_item'),
    ('mes_partner_master', 'bronze_mes_partner_master'),
    ('mes_location_master', 'bronze_mes_location_master'),
    ('p2p_procurement_header', 'bronze_p2p_procurement_header'),
    ('p2p_procurement_item', 'bronze_p2p_procurement_item'),
    ('p2p_bom_master', 'bronze_p2p_bom_master'),
    ('p2p_partner_master', 'bronze_p2p_partner_master'),
    ('tms_transportation_cost', 'bronze_tms_transportation_cost'),
    ('tms_partner_master', 'bronze_tms_partner_master'),
    ('tms_location_master', 'bronze_tms_location_master'),
    ('wms_current_inventory', 'bronze_wms_current_inventory'),
    ('wms_monthly_inventory_history', 'bronze_wms_monthly_inventory_history'),
    ('wms_location_master', 'bronze_wms_location_master'),
    ('hr_payroll', 'bronze_hr_payroll'),
]

for view_name, source_table in compatibility_views:
    try:
        query = f"CREATE VIEW {view_name} AS SELECT * FROM {source_table}"
        conn.execute(query)
        print(f"[OK] ビュー作成: {view_name} -> {source_table}")
    except Exception as e:
        print(f"[SKIP] {view_name} - {str(e)}")

# ========================================
# ステップ4: 統計情報の表示
# ========================================
print("\n" + "=" * 100)
print("データベース構築完了")
print("=" * 100)

cursor = conn.cursor()
cursor.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY type, name")
objects = cursor.fetchall()

print("\n【Bronze層テーブル】")
bronze_count = 0
for name, obj_type in objects:
    if name.startswith('bronze_'):
        cursor.execute(f"SELECT COUNT(*) FROM {name}")
        count = cursor.fetchone()[0]
        print(f"  {name}: {count:,}件")
        bronze_count += 1

print(f"\nBronze層テーブル合計: {bronze_count}個")

print("\n【Silver層テーブル（月次サマライズ）】")
silver_count = 0
for name, obj_type in objects:
    if name.startswith('silver_'):
        cursor.execute(f"SELECT COUNT(*) FROM {name}")
        count = cursor.fetchone()[0]
        print(f"  {name}: {count:,}件")
        silver_count += 1

print(f"\nSilver層テーブル合計: {silver_count}個")

print("\n【互換性ビュー】")
view_count = 0
for name, obj_type in objects:
    if obj_type == 'view':
        print(f"  {name}")
        view_count += 1

print(f"\n互換性ビュー合計: {view_count}個")

# データ期間の確認
print("\n" + "=" * 100)
print("データ期間の確認")
print("=" * 100)

print("\n【月次受注データ】")
cursor.execute("SELECT MIN(year_month), MAX(year_month), COUNT(*) FROM silver_monthly_orders")
min_ym, max_ym, count = cursor.fetchone()
print(f"  期間: {min_ym} ～ {max_ym}")
print(f"  レコード数: {count:,}件")

print("\n【月次調達データ】")
cursor.execute("SELECT MIN(year_month), MAX(year_month), COUNT(*) FROM silver_monthly_procurement")
min_ym, max_ym, count = cursor.fetchone()
print(f"  期間: {min_ym} ～ {max_ym}")
print(f"  レコード数: {count:,}件")

print("\n【月次出荷データ】")
cursor.execute("SELECT MIN(year_month), MAX(year_month), COUNT(*) FROM silver_monthly_shipment")
min_ym, max_ym, count = cursor.fetchone()
print(f"  期間: {min_ym} ～ {max_ym}")
print(f"  レコード数: {count:,}件")

conn.close()

print("\n" + "=" * 100)
print(f"データベースファイル: {DB_PATH}")
print("処理完了")
print("=" * 100)
