"""
増分データ更新スクリプト
post25の更新データセットを既存のデータベースに追加
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

# パス設定
BASE_PATH = Path(__file__).parent.parent
DATABASE_PATH = BASE_PATH / "database" / "analytics.db"
BRONZE_PRE24_PATH = Path("C:/Users/PC/dev/ForStep2/data/Bronze/pre24")
BRONZE_POST25_PATH = Path("C:/Users/PC/dev/ForStep2/data/Bronze/post25")

def print_section(title):
    """セクションタイトル表示"""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")

def load_master_data(source_path, table_name, subfolder, filename):
    """マスターデータの読み込みとマージ（重複排除）"""
    file_path = source_path / subfolder / filename
    
    if not file_path.exists():
        print(f"  ⚠ ファイルが存在しません: {filename}")
        return pd.DataFrame()
    
    df = pd.read_csv(file_path)
    print(f"  ✓ {filename}: {len(df)} 件")
    return df

def merge_master_data(pre_df, post_df, key_columns):
    """マスターデータのマージ（重複排除）"""
    if pre_df.empty:
        return post_df
    if post_df.empty:
        return pre_df
    
    # 結合して重複排除
    combined = pd.concat([pre_df, post_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=key_columns, keep='last')
    
    return combined

def update_dimension_products(conn):
    """製品マスター更新"""
    print("📦 製品マスター (dim_product) 更新中...")
    
    # pre24とpost25のデータ読み込み
    pre_df = load_master_data(BRONZE_PRE24_PATH, 'dim_product', 'ERP', 'product_master.csv')
    post_df = load_master_data(BRONZE_POST25_PATH, 'dim_product', 'ERP', 'product_master.csv')
    
    # マージ
    df = merge_master_data(pre_df, post_df, ['product_id'])
    
    if df.empty:
        print("  ⚠ データがありません")
        return 0
    
    # データ整形（post25の列構造に対応）
    # post25: item_hierarchy(大分類), detail_category(小分類)を使用
    # 価格・単位情報は調達データから取得するため、ダミー値を設定
    df_dim = pd.DataFrame({
        'product_id': df['product_id'],
        'product_name': df['product_name'],
        'product_category': df['item_hierarchy'] if 'item_hierarchy' in df.columns else df.get('item_group', 'Unknown'),
        'unit_price': 0.0,  # 価格情報は調達データから取得
        'unit_of_measure': df.get('base_unit_quantity', 'EA')
    })
    
    df_dim['product_key'] = range(1, len(df_dim) + 1)
    
    # 既存データを削除して再挿入
    conn.execute("DELETE FROM silver_dim_product")
    df_dim.to_sql('silver_dim_product', conn, if_exists='append', index=False)
    
    print(f"  ✓ 更新完了: {len(df_dim)} 件")
    return len(df_dim)

def update_dimension_locations(conn):
    """拠点マスター更新"""
    print("🏭 拠点マスター (dim_location) 更新中...")
    
    # 複数ソースから読み込み
    sources = [
        ('ERP', 'location_master.csv'),
        ('MES', 'location_master.csv'),
        ('TMS', 'location_master.csv'),
        ('WMS', 'location_master.csv')
    ]
    
    dfs = []
    for source in sources:
        pre_df = load_master_data(BRONZE_PRE24_PATH, 'dim_location', source[0], source[1])
        post_df = load_master_data(BRONZE_POST25_PATH, 'dim_location', source[0], source[1])
        merged = merge_master_data(pre_df, post_df, ['location_id'])
        if not merged.empty:
            dfs.append(merged)
    
    if not dfs:
        print("  ⚠ データがありません")
        return 0
    
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=['location_id'], keep='last')
    
    df_dim = df[[
        'location_id', 'location_name', 'location_type', 
        'address', 'country'
    ]].copy()
    df_dim['location_key'] = range(1, len(df_dim) + 1)
    
    conn.execute("DELETE FROM dim_location")
    df_dim.to_sql('dim_location', conn, if_exists='append', index=False)
    
    print(f"  ✓ 更新完了: {len(df_dim)} 件")
    return len(df_dim)

def update_dimension_partners(conn):
    """パートナーマスター更新"""
    print("🤝 パートナーマスター (dim_partner) 更新中...")
    
    sources = [
        ('ERP', 'partner_master.csv'),
        ('MES', 'partner_master.csv'),
        ('P2P', 'partner_master.csv'),
        ('TMS', 'partner_master.csv')
    ]
    
    dfs = []
    for source in sources:
        pre_df = load_master_data(BRONZE_PRE24_PATH, 'dim_partner', source[0], source[1])
        post_df = load_master_data(BRONZE_POST25_PATH, 'dim_partner', source[0], source[1])
        merged = merge_master_data(pre_df, post_df, ['partner_id'])
        if not merged.empty:
            dfs.append(merged)
    
    if not dfs:
        print("  ⚠ データがありません")
        return 0
    
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=['partner_id'], keep='last')
    
    df_dim = df[[
        'partner_id', 'partner_name', 'partner_type', 
        'address', 'country'
    ]].copy()
    df_dim['partner_key'] = range(1, len(df_dim) + 1)
    
    conn.execute("DELETE FROM dim_partner")
    df_dim.to_sql('dim_partner', conn, if_exists='append', index=False)
    
    print(f"  ✓ 更新完了: {len(df_dim)} 件")
    return len(df_dim)

def update_dimension_materials(conn):
    """材料マスター更新（調達データから抽出）"""
    print("🔧 材料マスター (dim_material) 更新中...")
    
    dfs = []
    
    # pre24: BOMマスター
    pre_bom = BRONZE_PRE24_PATH / 'P2P' / 'bom_master.csv'
    if pre_bom.exists():
        df = pd.read_csv(pre_bom)
        print(f"  ✓ pre24 BOMマスター: {len(df)} 件")
        dfs.append(df)
    
    # post25: 調達明細から材料マスターを抽出
    post_proc_item = BRONZE_POST25_PATH / 'P2P' / 'procurement_item_post25.csv'
    if post_proc_item.exists():
        df = pd.read_csv(post_proc_item)
        print(f"  ✓ post25 調達明細から材料情報抽出: {len(df)} 件")
        
        # 材料マスター構造に変換
        df_mat = df[['material_id', 'material_name', 'material_category']].copy()
        df_mat['unit_price'] = df['unit_price_ex_tax']
        df_mat['unit_of_measure'] = 'EA'
        df_mat = df_mat.drop_duplicates(subset=['material_id'], keep='last')
        dfs.append(df_mat)
    
    if not dfs:
        print("  ⚠ データがありません")
        return 0
    
    df = pd.concat(dfs, ignore_index=True)
    
    # 重複排除（post25を優先）
    df = df.drop_duplicates(subset=['material_id'], keep='last')
    
    # 必要な列のみ選択
    required_cols = ['material_id', 'material_name', 'material_category', 'unit_price', 'unit_of_measure']
    df_dim = df[required_cols].copy()
    df_dim['material_key'] = range(1, len(df_dim) + 1)
    
    conn.execute("DELETE FROM dim_material")
    df_dim.to_sql('dim_material', conn, if_exists='append', index=False)
    
    print(f"  ✓ 更新完了: {len(df_dim)} 件")
    return len(df_dim)

def generate_date_dimension():
    """日付ディメンション生成（2020-2026）"""
    print("📅 日付ディメンション (dim_date) 生成中...")
    
    dates = pd.date_range(start='2020-01-01', end='2026-12-31', freq='D')
    
    df_date = pd.DataFrame({
        'date_key': range(1, len(dates) + 1),
        'date': dates.strftime('%Y-%m-%d'),
        'year': dates.year,
        'month': dates.month,
        'day': dates.day,
        'quarter': dates.quarter,
        'year_month': dates.strftime('%Y-%m'),
        'day_of_week': dates.dayofweek + 1,
        'week_of_year': dates.isocalendar().week
    })
    
    print(f"  ✓ 生成完了: {len(df_date)} 件")
    return df_date

def update_fact_inventory(conn):
    """在庫ファクト更新"""
    print("📊 在庫ファクト (fact_inventory) 更新中...")
    
    # pre24: 月次在庫 + 期末在庫
    # post25: 月次在庫のみ
    
    # pre24データ
    pre_monthly = BRONZE_PRE24_PATH / 'WMS' / 'monthly_inventory_pre24.csv'
    pre_current = BRONZE_PRE24_PATH / 'WMS' / 'current_inventory_dec24.csv'
    
    dfs = []
    
    if pre_monthly.exists():
        df = pd.read_csv(pre_monthly)
        print(f"  ✓ pre24月次在庫: {len(df)} 件")
        dfs.append(df)
    
    if pre_current.exists():
        df = pd.read_csv(pre_current)
        print(f"  ✓ pre24期末在庫: {len(df)} 件")
        dfs.append(df)
    
    # post25データ
    post_monthly = BRONZE_POST25_PATH / 'WMS' / 'monthly_inventory_post25.csv'
    if post_monthly.exists():
        df = pd.read_csv(post_monthly)
        print(f"  ✓ post25月次在庫: {len(df)} 件")
        dfs.append(df)
    
    if not dfs:
        print("  ⚠ データがありません")
        return 0
    
    df_inv = pd.concat(dfs, ignore_index=True)
    
    # ディメンションキーマッピング
    df_products = pd.read_sql("SELECT product_key, product_id FROM dim_product", conn)
    df_locations = pd.read_sql("SELECT location_key, location_id FROM dim_location", conn)
    df_dates = pd.read_sql("SELECT date_key, date FROM dim_date", conn)
    
    df_inv = df_inv.merge(df_products, on='product_id', how='left')
    df_inv = df_inv.merge(df_locations, on='location_id', how='left')
    df_inv = df_inv.merge(df_dates, left_on='snapshot_date', right_on='date', how='left')
    
    # ファクトテーブル作成
    df_fact = df_inv[[
        'product_key', 'location_key', 'date_key',
        'quantity_on_hand', 'quantity_reserved', 'quantity_available',
        'inventory_value', 'snapshot_date'
    ]].copy()
    
    # 欠損値除去
    df_fact = df_fact.dropna(subset=['product_key', 'location_key', 'date_key'])
    
    # 重複排除（同じ製品・拠点・日付の最新データのみ）
    df_fact = df_fact.drop_duplicates(subset=['product_key', 'location_key', 'date_key'], keep='last')
    
    # 既存データを削除して再挿入
    conn.execute("DELETE FROM fact_inventory")
    df_fact.to_sql('fact_inventory', conn, if_exists='append', index=False)
    
    print(f"  ✓ 更新完了: {len(df_fact)} 件")
    return len(df_fact)

def update_fact_procurement(conn):
    """調達ファクト更新"""
    print("📊 調達ファクト (fact_procurement) 更新中...")
    
    # pre24とpost25のヘッダー・明細データ
    pre_header = BRONZE_PRE24_PATH / 'P2P' / 'procurement_header_pre24.csv'
    pre_item = BRONZE_PRE24_PATH / 'P2P' / 'procurement_item_pre24.csv'
    post_header = BRONZE_POST25_PATH / 'P2P' / 'procurement_header_post25.csv'
    post_item = BRONZE_POST25_PATH / 'P2P' / 'procurement_item_post25.csv'
    
    headers = []
    items = []
    
    if pre_header.exists() and pre_item.exists():
        h = pd.read_csv(pre_header)
        i = pd.read_csv(pre_item)
        print(f"  ✓ pre24調達: ヘッダー{len(h)}件、明細{len(i)}件")
        headers.append(h)
        items.append(i)
    
    if post_header.exists() and post_item.exists():
        h = pd.read_csv(post_header)
        i = pd.read_csv(post_item)
        print(f"  ✓ post25調達: ヘッダー{len(h)}件、明細{len(i)}件")
        headers.append(h)
        items.append(i)
    
    if not headers or not items:
        print("  ⚠ データがありません")
        return 0
    
    df_header = pd.concat(headers, ignore_index=True)
    df_item = pd.concat(items, ignore_index=True)
    
    # ヘッダーと明細を結合
    df = df_item.merge(
        df_header[['purchase_order_id', 'supplier_id', 'order_date', 'delivery_date', 'status']],
        on='purchase_order_id',
        how='left'
    )
    
    # ディメンションキーマッピング
    df_materials = pd.read_sql("SELECT material_key, material_id FROM dim_material", conn)
    df_partners = pd.read_sql("SELECT partner_key, partner_id FROM dim_partner", conn)
    df_dates = pd.read_sql("SELECT date_key, date FROM dim_date", conn)
    
    df = df.merge(df_materials, on='material_id', how='left')
    df = df.merge(df_partners, left_on='supplier_id', right_on='partner_id', how='left')
    df['supplier_key'] = df['partner_key']
    df = df.merge(df_dates, left_on='order_date', right_on='date', how='left', suffixes=('', '_order'))
    df['order_date_key'] = df['date_key']
    df = df.merge(df_dates, left_on='delivery_date', right_on='date', how='left', suffixes=('', '_delivery'))
    df['delivery_date_key'] = df['date_key']
    
    # ファクトテーブル作成
    df_fact = df[[
        'purchase_order_id', 'line_number', 'material_key', 'supplier_key',
        'order_date_key', 'delivery_date_key',
        'quantity', 'unit_price', 'total_amount', 'status'
    ]].copy()
    
    # 欠損値除去
    df_fact = df_fact.dropna(subset=['material_key', 'supplier_key', 'order_date_key'])
    
    # 重複排除
    df_fact = df_fact.drop_duplicates(subset=['purchase_order_id', 'line_number'], keep='last')
    
    # 既存データを削除して再挿入
    conn.execute("DELETE FROM fact_procurement")
    df_fact.to_sql('fact_procurement', conn, if_exists='append', index=False)
    
    print(f"  ✓ 更新完了: {len(df_fact)} 件")
    return len(df_fact)

def update_fact_sales(conn):
    """販売ファクト更新"""
    print("📊 販売ファクト (fact_sales_order) 更新中...")
    
    pre_header = BRONZE_PRE24_PATH / 'ERP' / 'sales_order_header_pre24.csv'
    pre_item = BRONZE_PRE24_PATH / 'ERP' / 'sales_order_item_pre24.csv'
    post_header = BRONZE_POST25_PATH / 'ERP' / 'sales_order_header_post25.csv'
    post_item = BRONZE_POST25_PATH / 'ERP' / 'sales_order_item_post25.csv'
    
    headers = []
    items = []
    
    if pre_header.exists() and pre_item.exists():
        h = pd.read_csv(pre_header)
        i = pd.read_csv(pre_item)
        print(f"  ✓ pre24販売: ヘッダー{len(h)}件、明細{len(i)}件")
        headers.append(h)
        items.append(i)
    
    if post_header.exists() and post_item.exists():
        h = pd.read_csv(post_header)
        i = pd.read_csv(post_item)
        print(f"  ✓ post25販売: ヘッダー{len(h)}件、明細{len(i)}件")
        headers.append(h)
        items.append(i)
    
    if not headers or not items:
        print("  ⚠ データがありません")
        return 0
    
    df_header = pd.concat(headers, ignore_index=True)
    df_item = pd.concat(items, ignore_index=True)
    
    df = df_item.merge(
        df_header[['sales_order_id', 'customer_id', 'order_date', 'delivery_date', 'status']],
        on='sales_order_id',
        how='left'
    )
    
    # ディメンションキーマッピング
    df_products = pd.read_sql("SELECT product_key, product_id FROM dim_product", conn)
    df_partners = pd.read_sql("SELECT partner_key, partner_id FROM dim_partner", conn)
    df_dates = pd.read_sql("SELECT date_key, date FROM dim_date", conn)
    
    df = df.merge(df_products, on='product_id', how='left')
    df = df.merge(df_partners, left_on='customer_id', right_on='partner_id', how='left')
    df['customer_key'] = df['partner_key']
    df = df.merge(df_dates, left_on='order_date', right_on='date', how='left', suffixes=('', '_order'))
    df['order_date_key'] = df['date_key']
    df = df.merge(df_dates, left_on='delivery_date', right_on='date', how='left', suffixes=('', '_delivery'))
    df['delivery_date_key'] = df['date_key']
    
    # ファクトテーブル作成
    df_fact = df[[
        'sales_order_id', 'line_number', 'product_key', 'customer_key',
        'order_date_key', 'delivery_date_key',
        'quantity', 'unit_price', 'total_amount', 'status'
    ]].copy()
    
    df_fact = df_fact.dropna(subset=['product_key', 'customer_key', 'order_date_key'])
    df_fact = df_fact.drop_duplicates(subset=['sales_order_id', 'line_number'], keep='last')
    
    conn.execute("DELETE FROM fact_sales_order")
    df_fact.to_sql('fact_sales_order', conn, if_exists='append', index=False)
    
    print(f"  ✓ 更新完了: {len(df_fact)} 件")
    return len(df_fact)

def update_fact_shipment(conn):
    """出荷ファクト更新"""
    print("📊 出荷ファクト (fact_shipment) 更新中...")
    
    pre_header = BRONZE_PRE24_PATH / 'MES' / 'shipment_header_pre24.csv'
    pre_item = BRONZE_PRE24_PATH / 'MES' / 'shipment_item_pre24.csv'
    post_header = BRONZE_POST25_PATH / 'MES' / 'shipment_header_post25.csv'
    post_item = BRONZE_POST25_PATH / 'MES' / 'shipment_item_post25.csv'
    
    headers = []
    items = []
    
    if pre_header.exists() and pre_item.exists():
        h = pd.read_csv(pre_header)
        i = pd.read_csv(pre_item)
        print(f"  ✓ pre24出荷: ヘッダー{len(h)}件、明細{len(i)}件")
        headers.append(h)
        items.append(i)
    
    if post_header.exists() and post_item.exists():
        h = pd.read_csv(post_header)
        i = pd.read_csv(post_item)
        print(f"  ✓ post25出荷: ヘッダー{len(h)}件、明細{len(i)}件")
        headers.append(h)
        items.append(i)
    
    if not headers or not items:
        print("  ⚠ データがありません")
        return 0
    
    df_header = pd.concat(headers, ignore_index=True)
    df_item = pd.concat(items, ignore_index=True)
    
    df = df_item.merge(
        df_header[['shipment_id', 'origin_location_id', 'destination_partner_id', 'shipment_date', 'arrival_date', 'status']],
        on='shipment_id',
        how='left'
    )
    
    # ディメンションキーマッピング
    df_products = pd.read_sql("SELECT product_key, product_id FROM dim_product", conn)
    df_locations = pd.read_sql("SELECT location_key, location_id FROM dim_location", conn)
    df_partners = pd.read_sql("SELECT partner_key, partner_id FROM dim_partner", conn)
    df_dates = pd.read_sql("SELECT date_key, date FROM dim_date", conn)
    
    df = df.merge(df_products, on='product_id', how='left')
    df = df.merge(df_locations, left_on='origin_location_id', right_on='location_id', how='left')
    df['origin_location_key'] = df['location_key']
    df = df.merge(df_partners, left_on='destination_partner_id', right_on='partner_id', how='left')
    df['destination_partner_key'] = df['partner_key']
    df = df.merge(df_dates, left_on='shipment_date', right_on='date', how='left', suffixes=('', '_shipment'))
    df['shipment_date_key'] = df['date_key']
    df = df.merge(df_dates, left_on='arrival_date', right_on='date', how='left', suffixes=('', '_arrival'))
    df['arrival_date_key'] = df['date_key']
    
    # ファクトテーブル作成
    df_fact = df[[
        'shipment_id', 'line_number', 'product_key', 'origin_location_key',
        'destination_partner_key', 'shipment_date_key', 'arrival_date_key',
        'quantity', 'status'
    ]].copy()
    
    df_fact = df_fact.dropna(subset=['product_key', 'origin_location_key', 'shipment_date_key'])
    df_fact = df_fact.drop_duplicates(subset=['shipment_id', 'line_number'], keep='last')
    
    conn.execute("DELETE FROM fact_shipment")
    df_fact.to_sql('fact_shipment', conn, if_exists='append', index=False)
    
    print(f"  ✓ 更新完了: {len(df_fact)} 件")
    return len(df_fact)

def update_fact_transportation(conn):
    """輸送コストファクト更新"""
    print("📊 輸送コストファクト (fact_transportation_cost) 更新中...")
    
    pre_cost = BRONZE_PRE24_PATH / 'TMS' / 'transportation_cost_pre24.csv'
    post_cost = BRONZE_POST25_PATH / 'TMS' / 'transportation_cost_post25.csv'
    
    dfs = []
    
    if pre_cost.exists():
        df = pd.read_csv(pre_cost)
        print(f"  ✓ pre24輸送コスト: {len(df)} 件")
        dfs.append(df)
    
    if post_cost.exists():
        df = pd.read_csv(post_cost)
        print(f"  ✓ post25輸送コスト: {len(df)} 件")
        dfs.append(df)
    
    if not dfs:
        print("  ⚠ データがありません")
        return 0
    
    df_cost = pd.concat(dfs, ignore_index=True)
    
    # ディメンションキーマッピング
    df_locations = pd.read_sql("SELECT location_key, location_id FROM dim_location", conn)
    df_partners = pd.read_sql("SELECT partner_key, partner_id FROM dim_partner", conn)
    df_dates = pd.read_sql("SELECT date_key, date FROM dim_date", conn)
    
    df_cost = df_cost.merge(df_locations, left_on='origin_location_id', right_on='location_id', how='left')
    df_cost['origin_location_key'] = df_cost['location_key']
    df_cost = df_cost.merge(df_locations, left_on='destination_location_id', right_on='location_id', how='left', suffixes=('', '_dest'))
    df_cost['destination_location_key'] = df_cost['location_key_dest']
    df_cost = df_cost.merge(df_partners, left_on='carrier_id', right_on='partner_id', how='left')
    df_cost['carrier_key'] = df_cost['partner_key']
    df_cost = df_cost.merge(df_dates, left_on='cost_date', right_on='date', how='left')
    
    # ファクトテーブル作成
    df_fact = df_cost[[
        'shipment_id', 'origin_location_key', 'destination_location_key',
        'carrier_key', 'date_key', 'transportation_cost', 'distance_km', 'cost_date'
    ]].copy()
    
    df_fact = df_fact.dropna(subset=['origin_location_key', 'destination_location_key', 'date_key'])
    df_fact = df_fact.drop_duplicates(subset=['shipment_id'], keep='last')
    
    conn.execute("DELETE FROM fact_transportation_cost")
    df_fact.to_sql('fact_transportation_cost', conn, if_exists='append', index=False)
    
    print(f"  ✓ 更新完了: {len(df_fact)} 件")
    return len(df_fact)

def main():
    """メイン処理"""
    print_section("増分データ更新開始")
    
    print(f"データベース: {DATABASE_PATH}")
    print(f"pre24パス: {BRONZE_PRE24_PATH}")
    print(f"post25パス: {BRONZE_POST25_PATH}")
    
    # データベース接続
    conn = sqlite3.connect(str(DATABASE_PATH))
    
    try:
        print_section("ディメンションテーブル更新")
        
        # 製品マスター
        prod_count = update_dimension_products(conn)
        
        # 拠点マスター
        loc_count = update_dimension_locations(conn)
        
        # パートナーマスター
        partner_count = update_dimension_partners(conn)
        
        # 材料マスター
        mat_count = update_dimension_materials(conn)
        
        # 日付ディメンション（再生成）
        df_date = generate_date_dimension()
        conn.execute("DELETE FROM dim_date")
        df_date.to_sql('dim_date', conn, if_exists='append', index=False)
        
        print_section("ファクトテーブル更新")
        
        # 在庫ファクト
        inv_count = update_fact_inventory(conn)
        
        # 調達ファクト
        proc_count = update_fact_procurement(conn)
        
        # 販売ファクト
        sales_count = update_fact_sales(conn)
        
        # 出荷ファクト
        ship_count = update_fact_shipment(conn)
        
        # 輸送コストファクト
        trans_count = update_fact_transportation(conn)
        
        # コミット
        conn.commit()
        
        print_section("更新完了サマリー")
        print("📊 ディメンションテーブル:")
        print(f"  - 製品: {prod_count} 件")
        print(f"  - 拠点: {loc_count} 件")
        print(f"  - パートナー: {partner_count} 件")
        print(f"  - 材料: {mat_count} 件")
        print(f"  - 日付: {len(df_date)} 件")
        print(f"\n📈 ファクトテーブル:")
        print(f"  - 在庫: {inv_count} 件")
        print(f"  - 調達: {proc_count} 件")
        print(f"  - 販売: {sales_count} 件")
        print(f"  - 出荷: {ship_count} 件")
        print(f"  - 輸送コスト: {trans_count} 件")
        
        total_records = inv_count + proc_count + sales_count + ship_count + trans_count
        print(f"\n✓ 総レコード数: {total_records:,} 件")
        
    except Exception as e:
        print(f"\n❌ エラー: {str(e)}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()
    
    print_section("次のステップ: KPI再計算")
    print("次のコマンドを実行してください:")
    print("  python calculate_gold_kpis.py")

if __name__ == "__main__":
    main()
