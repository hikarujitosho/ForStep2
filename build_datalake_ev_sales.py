"""
データレイク構築スクリプト - 月次EV販売率
メダリオンアーキテクチャ（Bronze→Silver→Gold）でSQLiteデータベースを構築
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# パス設定
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "Bronze" / "ERP"
DB_PATH = BASE_DIR / "datalake.db"

# データベース接続
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print("=" * 80)
print("データレイク構築開始 - 月次EV販売率")
print("=" * 80)

# ============================================================================
# Bronze層の構築
# ============================================================================
print("\n[1/3] Bronze層の構築")
print("-" * 80)

# 必要なCSVファイルのリスト
csv_files = {
    'bronze_order_header': '受注伝票_header.csv',
    'bronze_order_item': '受注伝票_item.csv',
    'bronze_item_master': '品目マスタ.csv',
    'bronze_price_condition': '条件マスタ.csv',
    'bronze_partner_master': '取引先マスタ.csv'
}

# Bronze層テーブルの作成とデータロード
for table_name, csv_file in csv_files.items():
    csv_path = BRONZE_DIR / csv_file
    print(f"Loading {csv_file} -> {table_name}...", end=" ")
    
    # CSVを読み込み
    df = pd.read_csv(csv_path)
    
    # SQLiteにロード（既存テーブルは削除して再作成）
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    print(f"✓ {len(df)} レコード")

print(f"\nBronze層構築完了: {len(csv_files)} テーブル")

# ============================================================================
# Silver層の構築
# ============================================================================
print("\n[2/3] Silver層の構築")
print("-" * 80)

# Silver層: 受注データの正規化
print("Creating silver_orders...", end=" ")
cursor.execute("""
CREATE TABLE IF NOT EXISTS silver_orders AS
SELECT 
    h.order_id,
    h.order_timestamp,
    DATE(h.order_timestamp) as order_date,
    STRFTIME('%Y-%m', h.order_timestamp) as year_month,
    h.location_id,
    h.customer_id,
    i.line_number,
    i.product_id,
    CAST(i.quantity AS REAL) as quantity,
    i.promised_delivery_date,
    i.pricing_date
FROM bronze_order_header h
INNER JOIN bronze_order_item i ON h.order_id = i.order_id
""")
conn.commit()
count = cursor.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0]
print(f"✓ {count} レコード")

# Silver層: 品目マスタの正規化（EV判定を追加）
print("Creating silver_items...", end=" ")
cursor.execute("""
CREATE TABLE IF NOT EXISTS silver_items AS
SELECT 
    product_id,
    product_name,
    CAST(base_unit_quantity AS REAL) as base_unit_quantity,
    brand_name,
    item_group,
    item_hierarchy,
    detail_category,
    tax_classification,
    transport_group,
    import_export_group,
    country_of_origin,
    CASE 
        WHEN item_hierarchy = 'EV' THEN 1
        ELSE 0
    END as is_ev
FROM bronze_item_master
""")
conn.commit()
count = cursor.execute("SELECT COUNT(*) FROM silver_items").fetchone()[0]
print(f"✓ {count} レコード")

# Silver層: 価格条件の正規化
print("Creating silver_price_conditions...", end=" ")
cursor.execute("""
CREATE TABLE IF NOT EXISTS silver_price_conditions AS
SELECT 
    price_condition_id,
    product_id,
    product_name,
    customer_id,
    customer_name,
    CAST(list_price_ex_tax AS REAL) as list_price_ex_tax,
    CAST(selling_price_ex_tax AS REAL) as selling_price_ex_tax,
    CAST(discount_rate AS REAL) as discount_rate,
    price_type,
    CAST(minimum_order_quantity AS REAL) as minimum_order_quantity,
    currency,
    valid_from,
    valid_to,
    remarks
FROM bronze_price_condition
""")
conn.commit()
count = cursor.execute("SELECT COUNT(*) FROM silver_price_conditions").fetchone()[0]
print(f"✓ {count} レコード")

# Silver層: 売上明細（受注×価格）
print("Creating silver_sales_detail...", end=" ")
cursor.execute("""
CREATE TABLE IF NOT EXISTS silver_sales_detail AS
SELECT 
    o.order_id,
    o.line_number,
    o.year_month,
    o.order_date,
    o.location_id,
    o.customer_id,
    o.product_id,
    i.product_name,
    o.quantity,
    i.is_ev,
    COALESCE(pc.selling_price_ex_tax, pc.list_price_ex_tax) as unit_price,
    o.quantity * COALESCE(pc.selling_price_ex_tax, pc.list_price_ex_tax) as sales_amount
FROM silver_orders o
INNER JOIN silver_items i ON o.product_id = i.product_id
LEFT JOIN silver_price_conditions pc ON 
    o.product_id = pc.product_id 
    AND o.customer_id = pc.customer_id
    AND o.pricing_date >= pc.valid_from 
    AND o.pricing_date < COALESCE(pc.valid_to, '9999-12-31')
""")
conn.commit()
count = cursor.execute("SELECT COUNT(*) FROM silver_sales_detail").fetchone()[0]
print(f"✓ {count} レコード")

print(f"\nSilver層構築完了: 4 テーブル")

# ============================================================================
# Gold層の構築 - 月次EV販売率
# ============================================================================
print("\n[3/3] Gold層の構築 - 月次EV販売率")
print("-" * 80)

print("Creating gold_monthly_ev_sales_rate...", end=" ")
cursor.execute("""
CREATE TABLE IF NOT EXISTS gold_monthly_ev_sales_rate AS
SELECT 
    year_month,
    SUM(sales_amount) as total_revenue,
    SUM(CASE WHEN is_ev = 1 THEN sales_amount ELSE 0 END) as ev_revenue,
    ROUND(
        SUM(CASE WHEN is_ev = 1 THEN sales_amount ELSE 0 END) * 100.0 / 
        NULLIF(SUM(sales_amount), 0),
        6
    ) as ev_sales_share
FROM silver_sales_detail
GROUP BY year_month
ORDER BY year_month
""")
conn.commit()
count = cursor.execute("SELECT COUNT(*) FROM gold_monthly_ev_sales_rate").fetchone()[0]
print(f"✓ {count} レコード")

# 結果の確認
print("\n月次EV販売率の算出結果（最初の10件）:")
print("-" * 80)
result_df = pd.read_sql_query("""
    SELECT 
        year_month as 年月,
        total_revenue as 全売上金額,
        ev_revenue as EV売上金額,
        ev_sales_share as EV販売率
    FROM gold_monthly_ev_sales_rate
    ORDER BY year_month
    LIMIT 10
""", conn)
print(result_df.to_string(index=False))

# CSVへエクスポート
output_path = DATA_DIR / "Gold" / "月次EV販売率_金額ベース.csv"
result_df_full = pd.read_sql_query("""
    SELECT 
        year_month as 年月,
        total_revenue as 全売上金額,
        ev_revenue as EV売上金額,
        ev_sales_share as EV販売率
    FROM gold_monthly_ev_sales_rate
    ORDER BY year_month
""", conn)
result_df_full.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n結果を出力: {output_path}")

# 統計情報
print("\n統計情報:")
print("-" * 80)
stats = cursor.execute("""
    SELECT 
        COUNT(*) as 月数,
        ROUND(AVG(ev_sales_share), 2) as 平均EV販売率,
        ROUND(MIN(ev_sales_share), 2) as 最小EV販売率,
        ROUND(MAX(ev_sales_share), 2) as 最大EV販売率
    FROM gold_monthly_ev_sales_rate
""").fetchone()
print(f"集計月数: {stats[0]} ヶ月")
print(f"平均EV販売率: {stats[1]}%")
print(f"最小EV販売率: {stats[2]}%")
print(f"最大EV販売率: {stats[3]}%")

# 接続を閉じる
conn.close()

print("\n" + "=" * 80)
print("データレイク構築完了")
print("=" * 80)
print(f"データベース: {DB_PATH}")
print(f"出力CSV: {output_path}")
print("\n構築された層:")
print("  Bronze層: 5テーブル (CSVデータそのまま)")
print("  Silver層: 4テーブル (正規化・結合済み)")
print("  Gold層: 1テーブル (月次EV販売率KPI)")
