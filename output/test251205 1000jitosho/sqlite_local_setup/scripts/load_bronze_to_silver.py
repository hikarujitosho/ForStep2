"""
Bronze(CSV) → Silver(SQLite) データロードスクリプト
非エンジニア向け: シンプルで分かりやすい実装

実行方法:
    python load_bronze_to_silver.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# =============================================================================
# 設定: ここを環境に合わせて変更してください
# =============================================================================
BRONZE_DATA_PATH = Path(r"C:\Users\PC\dev\ForStep2\data\Bronze\pre24")
DATABASE_PATH = Path(__file__).parent.parent / "database" / "analytics.db"

# =============================================================================
# ユーティリティ関数
# =============================================================================

def print_progress(message):
    """進捗メッセージを表示"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def load_csv_safe(file_path):
    """CSVファイルを安全に読み込み"""
    try:
        if not file_path.exists():
            print(f"⚠️  ファイルが見つかりません: {file_path}")
            return None
        df = pd.read_csv(file_path, encoding='utf-8')
        print_progress(f"✓ 読込完了: {file_path.name} ({len(df)}行)")
        return df
    except Exception as e:
        print(f"❌ エラー: {file_path.name} - {str(e)}")
        return None

# =============================================================================
# Silverテーブル作成
# =============================================================================

def create_silver_tables(conn):
    """Silverレイヤーのテーブル定義を作成"""
    print_progress("Silverテーブルを作成中...")
    
    cursor = conn.cursor()
    
    # 既存テーブルを削除（クリーンスタート）
    tables_to_drop = [
        'silver_dim_product', 'silver_dim_location', 'silver_dim_partner',
        'silver_dim_material', 'silver_dim_date',
        'silver_fact_inventory_daily', 'silver_fact_procurement',
        'silver_fact_sales_order', 'silver_fact_shipment',
        'silver_fact_transportation_cost'
    ]
    
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    # 1. dim_product (製品マスタ)
    cursor.execute("""
    CREATE TABLE silver_dim_product (
        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE NOT NULL,
        product_name TEXT,
        brand_name TEXT,
        item_group TEXT,
        item_hierarchy TEXT,
        detail_category TEXT,
        country_of_origin TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 2. dim_location (拠点マスタ)
    cursor.execute("""
    CREATE TABLE silver_dim_location (
        location_key INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id TEXT UNIQUE NOT NULL,
        location_name TEXT,
        location_type TEXT,
        city TEXT,
        state_province TEXT,
        country TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 3. dim_partner (取引先マスタ)
    cursor.execute("""
    CREATE TABLE silver_dim_partner (
        partner_key INTEGER PRIMARY KEY AUTOINCREMENT,
        partner_id TEXT UNIQUE NOT NULL,
        partner_name TEXT,
        partner_type TEXT,
        partner_category TEXT,
        account_group TEXT,
        region TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 4. dim_material (材料マスタ)
    cursor.execute("""
    CREATE TABLE silver_dim_material (
        material_key INTEGER PRIMARY KEY AUTOINCREMENT,
        material_id TEXT UNIQUE NOT NULL,
        material_name TEXT,
        material_category TEXT,
        material_type TEXT,
        unspsc_code TEXT,
        related_product_id TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 5. dim_date (日付ディメンション)
    cursor.execute("""
    CREATE TABLE silver_dim_date (
        date_key INTEGER PRIMARY KEY,
        full_date TEXT UNIQUE NOT NULL,
        year INTEGER,
        quarter INTEGER,
        month INTEGER,
        month_name TEXT,
        day_of_month INTEGER,
        year_month TEXT
    )
    """)
    
    # 6. fact_inventory_daily (日次在庫)
    cursor.execute("""
    CREATE TABLE silver_fact_inventory_daily (
        inventory_key INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL,
        date_key INTEGER,
        product_key INTEGER,
        location_key INTEGER,
        inventory_quantity REAL,
        inventory_value REAL,
        inventory_status TEXT,
        unit_cost REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 7. fact_procurement (調達)
    cursor.execute("""
    CREATE TABLE silver_fact_procurement (
        procurement_key INTEGER PRIMARY KEY AUTOINCREMENT,
        purchase_order_id TEXT NOT NULL,
        line_number INTEGER NOT NULL,
        order_date TEXT,
        date_key INTEGER,
        expected_delivery_date TEXT,
        actual_received_date TEXT,
        supplier_key INTEGER,
        location_key INTEGER,
        material_key INTEGER,
        product_key INTEGER,
        quantity REAL,
        unit_price_ex_tax REAL,
        reference_price_ex_tax REAL,
        price_variance REAL,
        line_total_ex_tax REAL,
        line_total_incl_tax REAL,
        material_category TEXT,
        material_type TEXT,
        account_group TEXT,
        is_mro INTEGER,
        lead_time_days INTEGER,
        lead_time_variance_days INTEGER,
        is_on_time INTEGER,
        payment_date TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(purchase_order_id, line_number)
    )
    """)
    
    # 8. fact_sales_order (受注)
    cursor.execute("""
    CREATE TABLE silver_fact_sales_order (
        sales_order_key INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL,
        line_number INTEGER NOT NULL,
        order_date TEXT,
        date_key INTEGER,
        customer_key INTEGER,
        location_key INTEGER,
        product_key INTEGER,
        quantity REAL,
        selling_price_ex_tax REAL,
        line_total_ex_tax REAL,
        line_total_incl_tax REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(order_id, line_number)
    )
    """)
    
    # 9. fact_shipment (出荷)
    cursor.execute("""
    CREATE TABLE silver_fact_shipment (
        shipment_key INTEGER PRIMARY KEY AUTOINCREMENT,
        shipment_id TEXT NOT NULL,
        order_id TEXT,
        line_number INTEGER,
        shipment_date TEXT,
        date_key INTEGER,
        customer_key INTEGER,
        location_key INTEGER,
        product_key INTEGER,
        quantity REAL,
        carrier_name TEXT,
        transportation_mode TEXT,
        delivery_status TEXT,
        is_delayed INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(shipment_id, order_id, line_number)
    )
    """)
    
    # 10. fact_transportation_cost (輸送コスト)
    cursor.execute("""
    CREATE TABLE silver_fact_transportation_cost (
        transport_cost_key INTEGER PRIMARY KEY AUTOINCREMENT,
        cost_id TEXT UNIQUE NOT NULL,
        shipment_id TEXT,
        billing_date TEXT,
        date_key INTEGER,
        location_key INTEGER,
        cost_type TEXT,
        cost_amount REAL,
        currency TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    conn.commit()
    print_progress("✓ Silverテーブル作成完了")

# =============================================================================
# ディメンションテーブルのロード
# =============================================================================

def load_dim_product(conn, bronze_path):
    """製品マスタをロード"""
    print_progress("製品マスタをロード中...")
    df = load_csv_safe(bronze_path / "ERP" / "product_master.csv")
    if df is None:
        return
    
    df_clean = df[['product_id', 'product_name', 'brand_name', 'item_group', 
                   'item_hierarchy', 'detail_category', 'country_of_origin']].copy()
    df_clean.to_sql('silver_dim_product', conn, if_exists='append', index=False)
    print_progress(f"✓ 製品マスタ登録完了: {len(df_clean)}件")

def load_dim_location(conn, bronze_path):
    """拠点マスタをロード（複数システムから統合）"""
    print_progress("拠点マスタをロード中...")
    
    dfs = []
    for system in ['ERP', 'WMS', 'MES', 'TMS']:
        file_path = bronze_path / system / "location_master.csv"
        df = load_csv_safe(file_path)
        if df is not None:
            dfs.append(df)
    
    if not dfs:
        print("⚠️  拠点マスタが見つかりません")
        return
    
    df_all = pd.concat(dfs, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=['location_id'])
    
    df_clean = df_all[['location_id', 'location_name', 'location_type', 
                       'city', 'state_province', 'country']].copy()
    df_clean.to_sql('silver_dim_location', conn, if_exists='append', index=False)
    print_progress(f"✓ 拠点マスタ登録完了: {len(df_clean)}件")

def load_dim_partner(conn, bronze_path):
    """取引先マスタをロード（複数システムから統合）"""
    print_progress("取引先マスタをロード中...")
    
    dfs = []
    for system in ['P2P', 'ERP', 'MES', 'TMS']:
        file_path = bronze_path / system / "partner_master.csv"
        df = load_csv_safe(file_path)
        if df is not None:
            dfs.append(df)
    
    if not dfs:
        print("⚠️  取引先マスタが見つかりません")
        return
    
    df_all = pd.concat(dfs, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=['partner_id'])
    
    df_clean = df_all[['partner_id', 'partner_name', 'partner_type', 
                       'partner_category', 'account_group', 'region']].copy()
    df_clean.to_sql('silver_dim_partner', conn, if_exists='append', index=False)
    print_progress(f"✓ 取引先マスタ登録完了: {len(df_clean)}件")

def load_dim_material(conn, bronze_path):
    """材料マスタをロード"""
    print_progress("材料マスタをロード中...")
    df = load_csv_safe(bronze_path / "P2P" / "procurement_item_pre24.csv")
    if df is None:
        return
    
    df_material = df[['material_id', 'material_name', 'material_category', 
                     'material_type', 'unspsc_code', 'product_id']].copy()
    df_material = df_material.drop_duplicates(subset=['material_id'])
    df_material.rename(columns={'product_id': 'related_product_id'}, inplace=True)
    df_material.to_sql('silver_dim_material', conn, if_exists='append', index=False)
    print_progress(f"✓ 材料マスタ登録完了: {len(df_material)}件")

def load_dim_date(conn):
    """日付ディメンションを生成（2022-2025）"""
    print_progress("日付ディメンションを生成中...")
    
    dates = pd.date_range(start='2022-01-01', end='2025-12-31', freq='D')
    df_date = pd.DataFrame({
        'date_key': dates.strftime('%Y%m%d').astype(int),
        'full_date': dates.strftime('%Y-%m-%d'),
        'year': dates.year,
        'quarter': dates.quarter,
        'month': dates.month,
        'month_name': dates.strftime('%B'),
        'day_of_month': dates.day,
        'year_month': dates.strftime('%Y-%m')
    })
    
    df_date.to_sql('silver_dim_date', conn, if_exists='append', index=False)
    print_progress(f"✓ 日付ディメンション登録完了: {len(df_date)}件")

# =============================================================================
# ファクトテーブルのロード
# =============================================================================

def load_fact_inventory(conn, bronze_path):
    """在庫ファクトをロード"""
    print_progress("在庫ファクトをロード中...")
    df = load_csv_safe(bronze_path / "WMS" / "monthly_inventory_pre24.csv")
    if df is None:
        return
    
    # 製品・拠点キーを取得
    cursor = conn.cursor()
    products = pd.read_sql("SELECT product_key, product_id FROM silver_dim_product", conn)
    locations = pd.read_sql("SELECT location_key, location_id FROM silver_dim_location", conn)
    dates = pd.read_sql("SELECT date_key, full_date FROM silver_dim_date", conn)
    
    df = df.merge(products, on='product_id', how='left')
    df = df.merge(locations, on='location_id', how='left')
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.strftime('%Y-%m-%d')
    df = df.merge(dates, left_on='snapshot_date', right_on='full_date', how='left')
    
    # 簡易的な単価設定（実際は価格マスタから取得）
    df['unit_cost'] = 1000000  # 仮の単価
    df['inventory_value'] = df['inventory_quantity'] * df['unit_cost']
    
    df_fact = df[['snapshot_date', 'date_key', 'product_key', 'location_key',
                  'inventory_quantity', 'inventory_value', 'inventory_status', 'unit_cost']].copy()
    df_fact.to_sql('silver_fact_inventory_daily', conn, if_exists='append', index=False)
    print_progress(f"✓ 在庫ファクト登録完了: {len(df_fact)}件")

def load_fact_procurement(conn, bronze_path):
    """調達ファクトをロード"""
    print_progress("調達ファクトをロード中...")
    df_header = load_csv_safe(bronze_path / "P2P" / "procurement_header_pre24.csv")
    df_item = load_csv_safe(bronze_path / "P2P" / "procurement_item_pre24.csv")
    
    if df_header is None or df_item is None:
        return
    
    # ヘッダーと明細を結合
    df = df_item.merge(df_header, on='purchase_order_id', how='left')
    
    # キー結合
    suppliers = pd.read_sql("SELECT partner_key, partner_id FROM silver_dim_partner", conn)
    locations = pd.read_sql("SELECT location_key, location_id FROM silver_dim_location", conn)
    materials = pd.read_sql("SELECT material_key, material_id FROM silver_dim_material", conn)
    products = pd.read_sql("SELECT product_key, product_id FROM silver_dim_product", conn)
    dates = pd.read_sql("SELECT date_key, full_date FROM silver_dim_date", conn)
    
    df = df.merge(suppliers, left_on='supplier_id', right_on='partner_id', how='left')
    df = df.merge(locations, on='location_id', how='left')
    df = df.merge(materials, on='material_id', how='left')
    df = df.merge(products, on='product_id', how='left')
    df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime('%Y-%m-%d')
    df = df.merge(dates, left_on='order_date', right_on='full_date', how='left')
    
    # 計算フィールド
    df['price_variance'] = df['reference_price_ex_tax'] - df['unit_price_ex_tax']
    df['is_mro'] = (df['account_group'] == 'MRO').astype(int)
    df['lead_time_days'] = (pd.to_datetime(df['received_date']) - pd.to_datetime(df['order_date'])).dt.days
    df['lead_time_variance_days'] = (pd.to_datetime(df['received_date']) - pd.to_datetime(df['expected_delivery_date'])).dt.days
    df['is_on_time'] = (df['lead_time_variance_days'] <= 0).astype(int)
    
    df_fact = df[['purchase_order_id', 'line_number', 'order_date', 'date_key',
                  'expected_delivery_date', 'received_date', 'partner_key', 'location_key',
                  'material_key', 'product_key', 'quantity', 'unit_price_ex_tax',
                  'reference_price_ex_tax', 'price_variance', 'line_subtotal_ex_tax',
                  'line_total_incl_tax', 'material_category', 'material_type',
                  'account_group', 'is_mro', 'lead_time_days', 'lead_time_variance_days',
                  'is_on_time', 'payment_date']].copy()
    
    df_fact.rename(columns={
        'received_date': 'actual_received_date',
        'line_subtotal_ex_tax': 'line_total_ex_tax',
        'partner_key': 'supplier_key'
    }, inplace=True)
    
    # 重複を除外
    df_fact = df_fact.drop_duplicates(subset=['purchase_order_id', 'line_number'])
    
    df_fact.to_sql('silver_fact_procurement', conn, if_exists='append', index=False)
    print_progress(f"✓ 調達ファクト登録完了: {len(df_fact)}件")

def load_fact_sales_order(conn, bronze_path):
    """受注ファクトをロード"""
    print_progress("受注ファクトをロード中...")
    df_header = load_csv_safe(bronze_path / "ERP" / "sales_order_header_pre24.csv")
    df_item = load_csv_safe(bronze_path / "ERP" / "sales_order_item_pre24.csv")
    
    if df_header is None or df_item is None:
        return
    
    df = df_item.merge(df_header, on='order_id', how='left')
    
    # キー結合
    customers = pd.read_sql("SELECT partner_key, partner_id FROM silver_dim_partner", conn)
    locations = pd.read_sql("SELECT location_key, location_id FROM silver_dim_location", conn)
    products = pd.read_sql("SELECT product_key, product_id FROM silver_dim_product", conn)
    dates = pd.read_sql("SELECT date_key, full_date FROM silver_dim_date", conn)
    
    df = df.merge(customers, left_on='customer_id', right_on='partner_id', how='left')
    df = df.merge(locations, on='location_id', how='left')
    df = df.merge(products, on='product_id', how='left')
    df['order_date'] = pd.to_datetime(df['order_timestamp']).dt.strftime('%Y-%m-%d')
    df = df.merge(dates, left_on='order_date', right_on='full_date', how='left')
    
    # 簡易価格計算
    df['selling_price_ex_tax'] = 2000000  # 仮の単価
    df['line_total_ex_tax'] = df['quantity'] * df['selling_price_ex_tax']
    df['line_total_incl_tax'] = df['line_total_ex_tax'] * 1.1
    
    df_fact = df[['order_id', 'line_number', 'order_date', 'date_key',
                  'partner_key', 'location_key', 'product_key', 'quantity',
                  'selling_price_ex_tax', 'line_total_ex_tax', 'line_total_incl_tax']].copy()
    
    df_fact.rename(columns={'partner_key': 'customer_key'}, inplace=True)
    df_fact.to_sql('silver_fact_sales_order', conn, if_exists='append', index=False)
    print_progress(f"✓ 受注ファクト登録完了: {len(df_fact)}件")

def load_fact_shipment(conn, bronze_path):
    """出荷ファクトをロード"""
    print_progress("出荷ファクトをロード中...")
    df_header = load_csv_safe(bronze_path / "MES" / "shipment_header_pre24.csv")
    df_item = load_csv_safe(bronze_path / "MES" / "shipment_item_pre24.csv")
    
    if df_header is None or df_item is None:
        return
    
    df = df_item.merge(df_header, on='shipment_id', how='left')
    
    # キー結合
    customers = pd.read_sql("SELECT partner_key, partner_id FROM silver_dim_partner", conn)
    locations = pd.read_sql("SELECT location_key, location_id FROM silver_dim_location", conn)
    products = pd.read_sql("SELECT product_key, product_id FROM silver_dim_product", conn)
    dates = pd.read_sql("SELECT date_key, full_date FROM silver_dim_date", conn)
    
    df = df.merge(customers, left_on='customer_id', right_on='partner_id', how='left')
    df = df.merge(locations, on='location_id', how='left')
    df = df.merge(products, on='product_id', how='left')
    df['shipment_date'] = pd.to_datetime(df['shipment_timestamp']).dt.strftime('%Y-%m-%d')
    df = df.merge(dates, left_on='shipment_date', right_on='full_date', how='left')
    
    df['is_delayed'] = (df['delivery_status'] == 'delayed').astype(int)
    
    df_fact = df[['shipment_id', 'order_id', 'line_number', 'shipment_date', 'date_key',
                  'partner_key', 'location_key', 'product_key', 'quantity',
                  'carrier_name', 'transportation_mode', 'delivery_status', 'is_delayed']].copy()
    
    df_fact.rename(columns={'partner_key': 'customer_key'}, inplace=True)
    df_fact.to_sql('silver_fact_shipment', conn, if_exists='append', index=False)
    print_progress(f"✓ 出荷ファクト登録完了: {len(df_fact)}件")

def load_fact_transportation_cost(conn, bronze_path):
    """輸送コストファクトをロード"""
    print_progress("輸送コストファクトをロード中...")
    df = load_csv_safe(bronze_path / "TMS" / "transportation_cost_pre24.csv")
    if df is None:
        return
    
    # キー結合
    locations = pd.read_sql("SELECT location_key, location_id FROM silver_dim_location", conn)
    dates = pd.read_sql("SELECT date_key, full_date FROM silver_dim_date", conn)
    
    df = df.merge(locations, on='location_id', how='left')
    df['billing_date'] = pd.to_datetime(df['billing_date']).dt.strftime('%Y-%m-%d')
    df = df.merge(dates, left_on='billing_date', right_on='full_date', how='left')
    
    df_fact = df[['cost_id', 'shipment_id', 'billing_date', 'date_key',
                  'location_key', 'cost_type', 'cost_amount', 'currency']].copy()
    
    df_fact.to_sql('silver_fact_transportation_cost', conn, if_exists='append', index=False)
    print_progress(f"✓ 輸送コストファクト登録完了: {len(df_fact)}件")

# =============================================================================
# メイン処理
# =============================================================================

def main():
    """メイン処理"""
    print("\n" + "="*70)
    print("  Bronze → Silver データロードスクリプト")
    print("="*70 + "\n")
    
    # データベース接続
    print_progress(f"データベースに接続: {DATABASE_PATH}")
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DATABASE_PATH))
    
    try:
        # Silverテーブル作成
        create_silver_tables(conn)
        
        # ディメンションテーブルのロード
        print("\n--- ディメンションテーブルのロード ---")
        load_dim_product(conn, BRONZE_DATA_PATH)
        load_dim_location(conn, BRONZE_DATA_PATH)
        load_dim_partner(conn, BRONZE_DATA_PATH)
        load_dim_material(conn, BRONZE_DATA_PATH)
        load_dim_date(conn)
        
        # ファクトテーブルのロード
        print("\n--- ファクトテーブルのロード ---")
        load_fact_inventory(conn, BRONZE_DATA_PATH)
        load_fact_procurement(conn, BRONZE_DATA_PATH)
        load_fact_sales_order(conn, BRONZE_DATA_PATH)
        load_fact_shipment(conn, BRONZE_DATA_PATH)
        load_fact_transportation_cost(conn, BRONZE_DATA_PATH)
        
        # 完了
        print("\n" + "="*70)
        print("✓ Silverレイヤーのデータロードが完了しました！")
        print(f"  データベース: {DATABASE_PATH}")
        print(f"  更新日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
