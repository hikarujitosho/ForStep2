"""
月次商品別粗利率KPI算出のためのデータレイク構築スクリプト
メダリオンアーキテクチャ（Bronze → Silver → Gold）に従ってSQLiteデータベースを構築
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# 定数定義
DB_PATH = "datalake.db"
BRONZE_ERP_PATH = Path("data/Bronze/ERP")
BRONZE_P2P_PATH = Path("data/Bronze/P2P")
GOLD_PATH = Path("data/Gold")


def log(message):
    """ログ出力"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def create_bronze_layer(conn):
    """Bronze層: CSVファイルをそのままSQLiteに格納"""
    log("=== Bronze層の構築開始 ===")
    
    # Bronze層に格納するCSVファイルのマッピング
    bronze_tables = {
        # ERPシステム
        "bronze_erp_受注伝票_header": BRONZE_ERP_PATH / "受注伝票_header.csv",
        "bronze_erp_受注伝票_item": BRONZE_ERP_PATH / "受注伝票_item.csv",
        "bronze_erp_条件マスタ": BRONZE_ERP_PATH / "条件マスタ.csv",
        "bronze_erp_品目マスタ": BRONZE_ERP_PATH / "品目マスタ.csv",
        # P2Pシステム
        "bronze_p2p_調達伝票_header": BRONZE_P2P_PATH / "調達伝票_header.csv",
        "bronze_p2p_調達伝票_item": BRONZE_P2P_PATH / "調達伝票_item.csv",
    }
    
    for table_name, csv_path in bronze_tables.items():
        if not csv_path.exists():
            log(f"警告: {csv_path} が見つかりません。スキップします。")
            continue
        
        log(f"読み込み中: {csv_path}")
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        
        # Bronze層はCSVをそのまま格納
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        log(f"✓ {table_name} を作成 ({len(df)} レコード)")
    
    log("=== Bronze層の構築完了 ===\n")


def create_silver_layer(conn):
    """Silver層: データクレンジングと正規化"""
    log("=== Silver層の構築開始 ===")
    
    cursor = conn.cursor()
    
    # Silver層テーブルの作成と処理
    
    # 1. 受注伝票_header
    log("処理中: silver_受注伝票_header")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_受注伝票_header AS
        SELECT 
            TRIM(order_id) AS order_id,
            order_timestamp,
            TRIM(COALESCE(location_id, '')) AS location_id,
            TRIM(COALESCE(customer_id, '')) AS customer_id
        FROM bronze_erp_受注伝票_header
        WHERE order_id IS NOT NULL
    """)
    log(f"✓ silver_受注伝票_header を作成")
    
    # 2. 受注伝票_item
    log("処理中: silver_受注伝票_item")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_受注伝票_item AS
        SELECT 
            TRIM(order_id) AS order_id,
            TRIM(line_number) AS line_number,
            TRIM(product_id) AS product_id,
            CAST(COALESCE(quantity, 0) AS REAL) AS quantity,
            promised_delivery_date,
            pricing_date
        FROM bronze_erp_受注伝票_item
        WHERE order_id IS NOT NULL AND product_id IS NOT NULL
    """)
    log(f"✓ silver_受注伝票_item を作成")
    
    # 3. 条件マスタ
    log("処理中: silver_条件マスタ")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_条件マスタ AS
        SELECT 
            TRIM(price_condition_id) AS price_condition_id,
            TRIM(product_id) AS product_id,
            TRIM(COALESCE(product_name, '')) AS product_name,
            TRIM(COALESCE(customer_id, '')) AS customer_id,
            TRIM(COALESCE(customer_name, '')) AS customer_name,
            CAST(COALESCE(list_price_ex_tax, 0) AS REAL) AS list_price_ex_tax,
            CAST(COALESCE(selling_price_ex_tax, 0) AS REAL) AS selling_price_ex_tax,
            CAST(COALESCE(discount_rate, 0) AS REAL) AS discount_rate,
            TRIM(COALESCE(price_type, '')) AS price_type,
            CAST(COALESCE(minimum_order_quantity, 0) AS REAL) AS minimum_order_quantity,
            TRIM(COALESCE(currency, 'JPY')) AS currency,
            valid_from,
            valid_to,
            TRIM(COALESCE(remarks, '')) AS remarks
        FROM bronze_erp_条件マスタ
        WHERE product_id IS NOT NULL
    """)
    log(f"✓ silver_条件マスタ を作成")
    
    # 4. 品目マスタ
    log("処理中: silver_品目マスタ")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_品目マスタ AS
        SELECT 
            TRIM(product_id) AS product_id,
            TRIM(COALESCE(product_name, '')) AS product_name,
            CAST(COALESCE(base_unit_quantity, 0) AS REAL) AS base_unit_quantity,
            TRIM(COALESCE(brand_name, '')) AS brand_name,
            TRIM(COALESCE(item_group, '')) AS item_group,
            TRIM(COALESCE(item_hierarchy, '')) AS item_hierarchy,
            TRIM(COALESCE(detail_category, '')) AS detail_category,
            TRIM(COALESCE(tax_classification, '')) AS tax_classification,
            TRIM(COALESCE(transport_group, '')) AS transport_group,
            TRIM(COALESCE(import_export_group, '')) AS import_export_group,
            TRIM(COALESCE(country_of_origin, '')) AS country_of_origin
        FROM bronze_erp_品目マスタ
        WHERE product_id IS NOT NULL
    """)
    log(f"✓ silver_品目マスタ を作成")
    
    # 5. 調達伝票_header
    log("処理中: silver_調達伝票_header")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_調達伝票_header AS
        SELECT 
            TRIM(purchase_order_id) AS purchase_order_id,
            order_date,
            expected_delivery_date,
            TRIM(COALESCE(supplier_id, '')) AS supplier_id,
            TRIM(COALESCE(supplier_name, '')) AS supplier_name,
            TRIM(COALESCE(account_group, '')) AS account_group,
            TRIM(COALESCE(location_id, '')) AS location_id,
            TRIM(COALESCE(purchase_order_number, '')) AS purchase_order_number,
            TRIM(COALESCE(currency, 'JPY')) AS currency,
            CAST(COALESCE(order_subtotal_ex_tax, 0) AS REAL) AS order_subtotal_ex_tax,
            CAST(COALESCE(shipping_fee_ex_tax, 0) AS REAL) AS shipping_fee_ex_tax,
            CAST(COALESCE(tax_amount, 0) AS REAL) AS tax_amount,
            CAST(COALESCE(discount_amount_incl_tax, 0) AS REAL) AS discount_amount_incl_tax,
            CAST(COALESCE(order_total_incl_tax, 0) AS REAL) AS order_total_incl_tax,
            TRIM(COALESCE(order_status, '')) AS order_status,
            TRIM(COALESCE(approver, '')) AS approver,
            TRIM(COALESCE(payment_method, '')) AS payment_method,
            TRIM(COALESCE(payment_confirmation_id, '')) AS payment_confirmation_id,
            payment_date,
            CAST(COALESCE(payment_amount, 0) AS REAL) AS payment_amount
        FROM bronze_p2p_調達伝票_header
        WHERE purchase_order_id IS NOT NULL
    """)
    log(f"✓ silver_調達伝票_header を作成")
    
    # 6. 調達伝票_item
    log("処理中: silver_調達伝票_item")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_調達伝票_item AS
        SELECT 
            TRIM(purchase_order_id) AS purchase_order_id,
            TRIM(line_number) AS line_number,
            TRIM(COALESCE(material_id, '')) AS material_id,
            TRIM(COALESCE(material_name, '')) AS material_name,
            TRIM(COALESCE(material_category, '')) AS material_category,
            TRIM(COALESCE(material_type, '')) AS material_type,
            TRIM(COALESCE(product_id, '')) AS product_id,
            TRIM(COALESCE(unspsc_code, '')) AS unspsc_code,
            CAST(COALESCE(quantity, 0) AS REAL) AS quantity,
            CAST(COALESCE(unit_price_ex_tax, 0) AS REAL) AS unit_price_ex_tax,
            CAST(COALESCE(line_subtotal_incl_tax, 0) AS REAL) AS line_subtotal_incl_tax,
            CAST(COALESCE(line_subtotal_ex_tax, 0) AS REAL) AS line_subtotal_ex_tax,
            CAST(COALESCE(line_tax_amount, 0) AS REAL) AS line_tax_amount,
            CAST(COALESCE(line_tax_rate, 0) AS REAL) AS line_tax_rate,
            CAST(COALESCE(line_shipping_fee_incl_tax, 0) AS REAL) AS line_shipping_fee_incl_tax,
            CAST(COALESCE(line_discount_incl_tax, 0) AS REAL) AS line_discount_incl_tax,
            CAST(COALESCE(line_total_incl_tax, 0) AS REAL) AS line_total_incl_tax,
            CAST(COALESCE(reference_price_ex_tax, 0) AS REAL) AS reference_price_ex_tax,
            TRIM(COALESCE(purchase_rule, '')) AS purchase_rule,
            ship_date,
            TRIM(COALESCE(shipping_status, '')) AS shipping_status,
            TRIM(COALESCE(carrier_tracking_number, '')) AS carrier_tracking_number,
            CAST(COALESCE(shipped_quantity, 0) AS REAL) AS shipped_quantity,
            TRIM(COALESCE(carrier_name, '')) AS carrier_name,
            TRIM(COALESCE(delivery_address, '')) AS delivery_address,
            TRIM(COALESCE(receiving_status, '')) AS receiving_status,
            CAST(COALESCE(received_quantity, 0) AS REAL) AS received_quantity,
            received_date,
            TRIM(COALESCE(receiver_name, '')) AS receiver_name,
            TRIM(COALESCE(receiver_email, '')) AS receiver_email,
            TRIM(COALESCE(cost_center, '')) AS cost_center,
            TRIM(COALESCE(project_code, '')) AS project_code,
            TRIM(COALESCE(department_code, '')) AS department_code,
            TRIM(COALESCE(account_user, '')) AS account_user,
            TRIM(COALESCE(user_email, '')) AS user_email
        FROM bronze_p2p_調達伝票_item
        WHERE purchase_order_id IS NOT NULL
    """)
    log(f"✓ silver_調達伝票_item を作成")
    
    conn.commit()
    log("=== Silver層の構築完了 ===\n")


def create_gold_layer(conn):
    """Gold層: KPI計算 - 月次商品別粗利率"""
    log("=== Gold層の構築開始 ===")
    
    cursor = conn.cursor()
    
    log("処理中: 月次商品別粗利率の計算")
    
    # 月次商品別粗利率の計算SQL
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_月次商品別粗利率 AS
        WITH 
        -- 1. 売上計算（車種別）: 受注伝票 × 条件マスタの販売価格
        sales_data AS (
            SELECT 
                h.order_id,
                strftime('%Y-%m', h.order_timestamp) AS year_month,
                i.product_id,
                c.product_name,
                i.quantity,
                c.selling_price_ex_tax,
                (i.quantity * c.selling_price_ex_tax) AS revenue
            FROM silver_受注伝票_header h
            INNER JOIN silver_受注伝票_item i ON h.order_id = i.order_id
            INNER JOIN silver_条件マスタ c 
                ON i.product_id = c.product_id 
                AND h.customer_id = c.customer_id
                AND i.pricing_date >= c.valid_from 
                AND i.pricing_date < COALESCE(c.valid_to, '9999-12-31')
            WHERE i.quantity > 0
        ),
        
        -- 2. 月次商品別売上集計（車種別）
        monthly_sales AS (
            SELECT 
                year_month,
                product_id,
                MAX(product_name) AS product_name,
                SUM(revenue) AS total_revenue
            FROM sales_data
            GROUP BY year_month, product_id
        ),
        
        -- 3. 売上原価計算（車種別）: 調達伝票の直接材のみ、product_idで車種別に集計
        cogs_data AS (
            SELECT 
                strftime('%Y-%m', h.order_date) AS year_month,
                i.product_id,
                SUM(i.line_subtotal_ex_tax) AS total_cogs
            FROM silver_調達伝票_header h
            INNER JOIN silver_調達伝票_item i ON h.purchase_order_id = i.purchase_order_id
            WHERE i.material_type = 'direct'
                AND i.product_id IS NOT NULL
                AND i.product_id != ''
            GROUP BY year_month, i.product_id
        )
        
        -- 4. 粗利率計算
        SELECT 
            s.product_id,
            s.product_name,
            s.year_month,
            s.total_revenue AS revenue,
            COALESCE(c.total_cogs, 0) AS cogs,
            (s.total_revenue - COALESCE(c.total_cogs, 0)) AS gross_profit,
            CASE 
                WHEN s.total_revenue > 0 THEN 
                    ROUND(((s.total_revenue - COALESCE(c.total_cogs, 0)) / s.total_revenue) * 100, 2)
                ELSE 0
            END AS gross_margin_rate
        FROM monthly_sales s
        LEFT JOIN cogs_data c 
            ON s.year_month = c.year_month 
            AND s.product_id = c.product_id
        ORDER BY s.year_month DESC, s.product_id
    """)
    
    # 作成されたレコード数を確認
    cursor.execute("SELECT COUNT(*) FROM gold_月次商品別粗利率")
    count = cursor.fetchone()[0]
    log(f"✓ gold_月次商品別粗利率 を作成 ({count} レコード)")
    
    conn.commit()
    log("=== Gold層の構築完了 ===\n")


def export_gold_to_csv(conn):
    """Gold層のデータをCSVにエクスポート"""
    log("=== Gold層データのCSVエクスポート開始 ===")
    
    # 出力ディレクトリの作成
    GOLD_PATH.mkdir(parents=True, exist_ok=True)
    
    # 月次商品別粗利率をCSVに出力
    output_path = GOLD_PATH / "月次商品別粗利率.csv"
    df = pd.read_sql_query("SELECT * FROM gold_月次商品別粗利率", conn)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    log(f"✓ {output_path} に出力 ({len(df)} レコード)")
    
    log("=== CSVエクスポート完了 ===\n")


def main():
    """メイン処理"""
    try:
        log("データレイク構築を開始します")
        log(f"データベース: {DB_PATH}")
        
        # 既存のデータベースファイルを削除（クリーンスタート）
        db_file = Path(DB_PATH)
        if db_file.exists():
            db_file.unlink()
            log("既存のデータベースファイルを削除しました")
        
        # SQLite接続
        conn = sqlite3.connect(DB_PATH)
        log("SQLiteデータベースに接続しました\n")
        
        # 各層の構築
        create_bronze_layer(conn)
        create_silver_layer(conn)
        create_gold_layer(conn)
        export_gold_to_csv(conn)
        
        # 接続を閉じる
        conn.close()
        log("データベース接続を閉じました")
        
        log("\n✅ データレイク構築が正常に完了しました！")
        return 0
        
    except Exception as e:
        log(f"\n❌ エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
