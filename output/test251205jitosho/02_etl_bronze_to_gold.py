"""
==========================================
ETLスクリプト: Bronze → Silver → Gold
KPI: 間接材調達コスト削減率
==========================================

このスクリプトは以下の処理を実行します:
1. BronzeデータをCSVから読み込み
2. Silverテーブルにデータをクレンジング・投入
3. GoldテーブルにKPI集計データを投入

使用方法:
    python 02_etl_bronze_to_gold.py
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime
from pathlib import Path

# ============================================
# 設定
# ============================================

# パス設定
BASE_DIR = Path(r"C:\Users\PC\dev\ForStep2")
BRONZE_DIR = BASE_DIR / "data" / "Bronze" / "P2P"
DATABASE_PATH = BASE_DIR / "data" / "kpi_database.db"

# CSVファイルパス
CSV_FILES = {
    'header': BRONZE_DIR / "調達伝票_header.csv",
    'item': BRONZE_DIR / "調達伝票_item.csv",
    'supplier': BRONZE_DIR / "取引先マスタ.csv"
}

# ログ設定
LOG_FILE = BASE_DIR / "logs" / f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# ============================================
# ユーティリティ関数
# ============================================

def log_message(message, print_console=True):
    """ログメッセージを記録"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    
    if print_console:
        print(log_entry)
    
    # ログディレクトリを作成
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # ログファイルに書き込み
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')

def check_csv_files():
    """CSVファイルの存在を確認"""
    log_message("=" * 60)
    log_message("CSVファイルの存在確認")
    log_message("=" * 60)
    
    all_exist = True
    for name, path in CSV_FILES.items():
        if path.exists():
            log_message(f"✓ {name}: {path}")
        else:
            log_message(f"✗ {name}: {path} が見つかりません")
            all_exist = False
    
    if not all_exist:
        raise FileNotFoundError("必要なCSVファイルが見つかりません")
    
    log_message("すべてのCSVファイルが存在します\n")

def load_csv_data():
    """CSVファイルを読み込み"""
    log_message("=" * 60)
    log_message("CSVファイルの読み込み")
    log_message("=" * 60)
    
    data = {}
    
    try:
        # 調達伝票_header
        log_message(f"読み込み中: {CSV_FILES['header'].name}")
        data['header'] = pd.read_csv(CSV_FILES['header'], encoding='utf-8')
        log_message(f"  レコード数: {len(data['header']):,}")
        
        # 調達伝票_item
        log_message(f"読み込み中: {CSV_FILES['item'].name}")
        data['item'] = pd.read_csv(CSV_FILES['item'], encoding='utf-8')
        log_message(f"  レコード数: {len(data['item']):,}")
        
        # 取引先マスタ
        log_message(f"読み込み中: {CSV_FILES['supplier'].name}")
        data['supplier'] = pd.read_csv(CSV_FILES['supplier'], encoding='utf-8')
        log_message(f"  レコード数: {len(data['supplier']):,}")
        
        log_message("CSVファイルの読み込み完了\n")
        return data
        
    except Exception as e:
        log_message(f"エラー: CSVファイルの読み込みに失敗しました - {str(e)}")
        raise

# ============================================
# Silver層データ投入
# ============================================

def load_silver_supplier_dim(conn, df_supplier):
    """Silver層: サプライヤーディメンション投入"""
    log_message("=" * 60)
    log_message("Silver層: サプライヤーディメンション投入")
    log_message("=" * 60)
    
    # サプライヤーのみ抽出
    df_supplier_filtered = df_supplier[df_supplier['partner_type'] == 'supplier'].copy()
    
    df_silver = df_supplier_filtered[[
        'partner_id', 'partner_name', 'partner_type', 'partner_category',
        'account_group', 'country', 'region', 'payment_terms', 'is_active'
    ]].copy()
    
    df_silver['supplier_key'] = df_silver['partner_id']
    
    # データベースに投入
    cursor = conn.cursor()
    cursor.execute("DELETE FROM silver_supplier_dim")
    
    inserted = 0
    for _, row in df_silver.iterrows():
        try:
            cursor.execute("""
                INSERT INTO silver_supplier_dim (
                    supplier_key, partner_id, partner_name, partner_type, 
                    partner_category, account_group, country, region, 
                    payment_terms, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['supplier_key'], row['partner_id'], row['partner_name'], 
                row['partner_type'], row['partner_category'], row['account_group'],
                row['country'], row['region'], row['payment_terms'], row['is_active']
            ))
            inserted += 1
        except Exception as e:
            log_message(f"  警告: レコード挿入失敗 (partner_id={row['partner_id']}): {str(e)}", False)
    
    conn.commit()
    log_message(f"投入完了: {inserted:,} レコード\n")

def load_silver_material_dim(conn, df_item):
    """Silver層: 資材ディメンション投入"""
    log_message("=" * 60)
    log_message("Silver層: 資材ディメンション投入")
    log_message("=" * 60)
    
    # 間接材のみ抽出
    df_indirect = df_item[df_item['material_type'] == 'indirect'].copy()
    
    # 一意の資材を抽出
    df_silver = df_indirect[[
        'material_id', 'material_name', 'material_category', 
        'material_type', 'unspsc_code'
    ]].drop_duplicates(subset=['material_id']).copy()
    
    df_silver['material_key'] = df_silver['material_id']
    
    # データベースに投入
    cursor = conn.cursor()
    cursor.execute("DELETE FROM silver_material_dim")
    
    inserted = 0
    for _, row in df_silver.iterrows():
        try:
            cursor.execute("""
                INSERT INTO silver_material_dim (
                    material_key, material_id, material_name, 
                    material_category, material_type, unspsc_code
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['material_key'], row['material_id'], row['material_name'],
                row['material_category'], row['material_type'], 
                str(row['unspsc_code']) if pd.notna(row['unspsc_code']) else None
            ))
            inserted += 1
        except Exception as e:
            log_message(f"  警告: レコード挿入失敗 (material_id={row['material_id']}): {str(e)}", False)
    
    conn.commit()
    log_message(f"投入完了: {inserted:,} レコード\n")

def load_silver_procurement_fact(conn, df_header, df_item):
    """Silver層: 調達ファクトテーブル投入"""
    log_message("=" * 60)
    log_message("Silver層: 調達ファクトテーブル投入")
    log_message("=" * 60)
    
    # 間接材のみ抽出
    df_item_filtered = df_item[
        (df_item['material_type'] == 'indirect') &
        (df_item['unit_price_ex_tax'] > 0) &
        (df_item['quantity'] > 0)
    ].copy()
    
    log_message(f"間接材明細レコード数: {len(df_item_filtered):,}")
    
    # ヘッダーと結合
    df_merged = pd.merge(
        df_item_filtered,
        df_header[df_header['order_status'] != 'cancelled'],
        on='purchase_order_id',
        how='inner'
    )
    
    log_message(f"結合後レコード数: {len(df_merged):,}")
    
    # Silverテーブル用のカラム作成
    df_merged['procurement_fact_id'] = (
        df_merged['purchase_order_id'] + '-' + 
        df_merged['line_number'].astype(str).str.zfill(5)
    )
    df_merged['order_year_month'] = pd.to_datetime(df_merged['order_date']).dt.strftime('%Y-%m')
    df_merged['supplier_key'] = df_merged['supplier_id']
    df_merged['material_key'] = df_merged['material_id']
    
    # データベースに投入
    cursor = conn.cursor()
    cursor.execute("DELETE FROM silver_procurement_fact")
    
    inserted = 0
    for _, row in df_merged.iterrows():
        try:
            cursor.execute("""
                INSERT INTO silver_procurement_fact (
                    procurement_fact_id, purchase_order_id, line_number,
                    order_date, order_year_month, supplier_key, material_key,
                    location_id, material_id, material_name, material_category,
                    unspsc_code, cost_center, department_code,
                    quantity, unit_price_ex_tax, line_subtotal_ex_tax,
                    line_total_incl_tax, received_date, received_quantity,
                    order_status, currency
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['procurement_fact_id'], row['purchase_order_id'], int(row['line_number']),
                row['order_date'], row['order_year_month'], row['supplier_key'], row['material_key'],
                row['location_id'], row['material_id'], row['material_name'], row['material_category'],
                str(row['unspsc_code']) if pd.notna(row['unspsc_code']) else None,
                row['cost_center'] if pd.notna(row['cost_center']) else None,
                row['department_code'] if pd.notna(row['department_code']) else None,
                float(row['quantity']), float(row['unit_price_ex_tax']), float(row['line_subtotal_ex_tax']),
                float(row['line_total_incl_tax']) if pd.notna(row['line_total_incl_tax']) else None,
                row['received_date'] if pd.notna(row['received_date']) else None,
                float(row['received_quantity']) if pd.notna(row['received_quantity']) else None,
                row['order_status'], row['currency'] if pd.notna(row['currency']) else 'JPY'
            ))
            inserted += 1
        except Exception as e:
            log_message(f"  警告: レコード挿入失敗 (procurement_fact_id={row['procurement_fact_id']}): {str(e)}", False)
    
    conn.commit()
    log_message(f"投入完了: {inserted:,} レコード\n")

# ============================================
# Gold層データ投入
# ============================================

def load_gold_monthly_cost(conn):
    """Gold層: 月次コスト集計テーブル投入"""
    log_message("=" * 60)
    log_message("Gold層: 月次コスト集計テーブル投入")
    log_message("=" * 60)
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM gold_indirect_material_cost_monthly")
    
    # 集計SQLの実行
    cursor.execute("""
        INSERT INTO gold_indirect_material_cost_monthly (
            cost_summary_id, year_month, supplier_key, supplier_name,
            material_category, location_id, cost_center,
            total_order_amount, total_quantity, order_count,
            avg_unit_price, unique_material_count
        )
        SELECT 
            pf.order_year_month || '-' ||
            COALESCE(pf.supplier_key, 'UNKNOWN') || '-' ||
            COALESCE(pf.material_category, 'UNKNOWN') || '-' ||
            COALESCE(pf.location_id, 'UNKNOWN') || '-' ||
            COALESCE(pf.cost_center, 'UNKNOWN') AS cost_summary_id,
            pf.order_year_month AS year_month,
            pf.supplier_key,
            sd.partner_name AS supplier_name,
            pf.material_category,
            pf.location_id,
            pf.cost_center,
            SUM(pf.line_subtotal_ex_tax) AS total_order_amount,
            SUM(pf.quantity) AS total_quantity,
            COUNT(DISTINCT pf.purchase_order_id) AS order_count,
            SUM(pf.line_subtotal_ex_tax) / NULLIF(SUM(pf.quantity), 0) AS avg_unit_price,
            COUNT(DISTINCT pf.material_id) AS unique_material_count
        FROM 
            silver_procurement_fact AS pf
        LEFT JOIN 
            silver_supplier_dim AS sd
            ON pf.supplier_key = sd.supplier_key
        GROUP BY 
            pf.order_year_month,
            pf.supplier_key,
            sd.partner_name,
            pf.material_category,
            pf.location_id,
            pf.cost_center
    """)
    
    inserted = cursor.rowcount
    conn.commit()
    log_message(f"投入完了: {inserted:,} レコード\n")

def load_gold_cost_reduction_rate(conn):
    """Gold層: コスト削減率テーブル投入"""
    log_message("=" * 60)
    log_message("Gold層: コスト削減率テーブル投入")
    log_message("=" * 60)
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM gold_indirect_material_cost_reduction_rate")
    
    # 1. 全社レベル
    log_message("分析軸: 全社レベル (overall)")
    cursor.execute("""
        INSERT INTO gold_indirect_material_cost_reduction_rate (
            kpi_id, year_month, analysis_axis, axis_value, axis_key,
            current_amount, previous_year_amount, amount_difference,
            cost_reduction_rate, current_avg_unit_price, previous_year_avg_unit_price,
            unit_price_reduction_rate
        )
        WITH current_month AS (
            SELECT 
                year_month,
                SUM(total_order_amount) AS total_amount,
                SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
            FROM gold_indirect_material_cost_monthly
            GROUP BY year_month
        ),
        previous_year AS (
            SELECT 
                printf('%04d-%02d', 
                    CAST(substr(year_month, 1, 4) AS INTEGER) + 1,
                    CAST(substr(year_month, 6, 2) AS INTEGER)
                ) AS current_ym,
                SUM(total_order_amount) AS total_amount,
                SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
            FROM gold_indirect_material_cost_monthly
            GROUP BY current_ym
        )
        SELECT 
            'OVERALL-' || cm.year_month AS kpi_id,
            cm.year_month,
            'overall' AS analysis_axis,
            '全社' AS axis_value,
            'OVERALL' AS axis_key,
            cm.total_amount AS current_amount,
            py.total_amount AS previous_year_amount,
            py.total_amount - cm.total_amount AS amount_difference,
            CASE 
                WHEN py.total_amount > 0 THEN 
                    ((py.total_amount - cm.total_amount) / py.total_amount) * 100
                ELSE NULL 
            END AS cost_reduction_rate,
            cm.avg_price AS current_avg_unit_price,
            py.avg_price AS previous_year_avg_unit_price,
            CASE 
                WHEN py.avg_price > 0 THEN 
                    ((py.avg_price - cm.avg_price) / py.avg_price) * 100
                ELSE NULL 
            END AS unit_price_reduction_rate
        FROM 
            current_month cm
        LEFT JOIN 
            previous_year py
            ON cm.year_month = py.current_ym
    """)
    log_message(f"  投入: {cursor.rowcount} レコード")
    
    # 2. サプライヤー別
    log_message("分析軸: サプライヤー別 (supplier)")
    cursor.execute("""
        INSERT INTO gold_indirect_material_cost_reduction_rate (
            kpi_id, year_month, analysis_axis, axis_value, axis_key,
            current_amount, previous_year_amount, amount_difference,
            cost_reduction_rate, current_avg_unit_price, previous_year_avg_unit_price,
            unit_price_reduction_rate
        )
        WITH current_month AS (
            SELECT 
                year_month,
                supplier_key,
                supplier_name,
                SUM(total_order_amount) AS total_amount,
                SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
            FROM gold_indirect_material_cost_monthly
            GROUP BY year_month, supplier_key, supplier_name
        ),
        previous_year AS (
            SELECT 
                printf('%04d-%02d', 
                    CAST(substr(year_month, 1, 4) AS INTEGER) + 1,
                    CAST(substr(year_month, 6, 2) AS INTEGER)
                ) AS current_ym,
                supplier_key,
                SUM(total_order_amount) AS total_amount,
                SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
            FROM gold_indirect_material_cost_monthly
            GROUP BY current_ym, supplier_key
        )
        SELECT 
            'SUPPLIER-' || cm.year_month || '-' || cm.supplier_key AS kpi_id,
            cm.year_month,
            'supplier' AS analysis_axis,
            cm.supplier_name AS axis_value,
            cm.supplier_key AS axis_key,
            cm.total_amount AS current_amount,
            py.total_amount AS previous_year_amount,
            py.total_amount - cm.total_amount AS amount_difference,
            CASE 
                WHEN py.total_amount > 0 THEN 
                    ((py.total_amount - cm.total_amount) / py.total_amount) * 100
                ELSE NULL 
            END AS cost_reduction_rate,
            cm.avg_price AS current_avg_unit_price,
            py.avg_price AS previous_year_avg_unit_price,
            CASE 
                WHEN py.avg_price > 0 THEN 
                    ((py.avg_price - cm.avg_price) / py.avg_price) * 100
                ELSE NULL 
            END AS unit_price_reduction_rate
        FROM 
            current_month cm
        LEFT JOIN 
            previous_year py
            ON cm.year_month = py.current_ym
            AND cm.supplier_key = py.supplier_key
        WHERE cm.supplier_key IS NOT NULL
    """)
    log_message(f"  投入: {cursor.rowcount} レコード")
    
    # 3. カテゴリ別
    log_message("分析軸: 資材カテゴリ別 (category)")
    cursor.execute("""
        INSERT INTO gold_indirect_material_cost_reduction_rate (
            kpi_id, year_month, analysis_axis, axis_value, axis_key,
            current_amount, previous_year_amount, amount_difference,
            cost_reduction_rate, current_avg_unit_price, previous_year_avg_unit_price,
            unit_price_reduction_rate
        )
        WITH current_month AS (
            SELECT 
                year_month,
                material_category,
                SUM(total_order_amount) AS total_amount,
                SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
            FROM gold_indirect_material_cost_monthly
            GROUP BY year_month, material_category
        ),
        previous_year AS (
            SELECT 
                printf('%04d-%02d', 
                    CAST(substr(year_month, 1, 4) AS INTEGER) + 1,
                    CAST(substr(year_month, 6, 2) AS INTEGER)
                ) AS current_ym,
                material_category,
                SUM(total_order_amount) AS total_amount,
                SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
            FROM gold_indirect_material_cost_monthly
            GROUP BY current_ym, material_category
        )
        SELECT 
            'CATEGORY-' || cm.year_month || '-' || COALESCE(cm.material_category, 'UNKNOWN') AS kpi_id,
            cm.year_month,
            'category' AS analysis_axis,
            cm.material_category AS axis_value,
            cm.material_category AS axis_key,
            cm.total_amount AS current_amount,
            py.total_amount AS previous_year_amount,
            py.total_amount - cm.total_amount AS amount_difference,
            CASE 
                WHEN py.total_amount > 0 THEN 
                    ((py.total_amount - cm.total_amount) / py.total_amount) * 100
                ELSE NULL 
            END AS cost_reduction_rate,
            cm.avg_price AS current_avg_unit_price,
            py.avg_price AS previous_year_avg_unit_price,
            CASE 
                WHEN py.avg_price > 0 THEN 
                    ((py.avg_price - cm.avg_price) / py.avg_price) * 100
                ELSE NULL 
            END AS unit_price_reduction_rate
        FROM 
            current_month cm
        LEFT JOIN 
            previous_year py
            ON cm.year_month = py.current_ym
            AND cm.material_category = py.material_category
    """)
    log_message(f"  投入: {cursor.rowcount} レコード")
    
    conn.commit()
    log_message("Gold層データ投入完了\n")

# ============================================
# データ品質チェック
# ============================================

def run_quality_checks(conn):
    """データ品質チェック実行"""
    log_message("=" * 60)
    log_message("データ品質チェック")
    log_message("=" * 60)
    
    cursor = conn.cursor()
    
    # 異常な単価チェック
    cursor.execute("SELECT COUNT(*) FROM v_quality_check_abnormal_price")
    abnormal_price_count = cursor.fetchone()[0]
    log_message(f"異常な単価: {abnormal_price_count} 件")
    
    # 異常な変動チェック
    cursor.execute("SELECT COUNT(*) FROM v_quality_check_abnormal_change")
    abnormal_change_count = cursor.fetchone()[0]
    log_message(f"異常な変動(>50%): {abnormal_change_count} 件")
    
    # データ完全性チェック
    cursor.execute("SELECT * FROM v_data_completeness_check LIMIT 5")
    rows = cursor.fetchall()
    log_message("\nデータ完全性 (直近5ヶ月):")
    for row in rows:
        log_message(f"  {row[0]}: 完全性率 {row[3]}% (前年データ欠損: {row[2]}件)")
    
    log_message("")

# ============================================
# メイン処理
# ============================================

def main():
    """メイン処理"""
    start_time = datetime.now()
    
    log_message("=" * 60)
    log_message("ETL処理開始")
    log_message(f"開始時刻: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log_message("=" * 60)
    log_message("")
    
    try:
        # 1. CSVファイルの存在確認
        check_csv_files()
        
        # 2. CSVデータの読み込み
        data = load_csv_data()
        
        # 3. データベース接続
        log_message("=" * 60)
        log_message("データベース接続")
        log_message("=" * 60)
        DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DATABASE_PATH))
        log_message(f"接続先: {DATABASE_PATH}\n")
        
        # 4. Silver層データ投入
        load_silver_supplier_dim(conn, data['supplier'])
        load_silver_material_dim(conn, data['item'])
        load_silver_procurement_fact(conn, data['header'], data['item'])
        
        # 5. Gold層データ投入
        load_gold_monthly_cost(conn)
        load_gold_cost_reduction_rate(conn)
        
        # 6. データ品質チェック
        run_quality_checks(conn)
        
        # 7. 接続クローズ
        conn.close()
        
        # 完了メッセージ
        end_time = datetime.now()
        elapsed = end_time - start_time
        
        log_message("=" * 60)
        log_message("ETL処理完了")
        log_message(f"終了時刻: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        log_message(f"処理時間: {elapsed.total_seconds():.2f}秒")
        log_message("=" * 60)
        log_message(f"\nログファイル: {LOG_FILE}")
        log_message(f"データベース: {DATABASE_PATH}")
        
    except Exception as e:
        log_message("=" * 60)
        log_message(f"エラーが発生しました: {str(e)}")
        log_message("=" * 60)
        raise

if __name__ == "__main__":
    main()
