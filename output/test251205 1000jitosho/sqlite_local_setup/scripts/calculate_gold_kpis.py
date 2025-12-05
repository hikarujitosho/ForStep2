"""
Silver → Gold KPI計算スクリプト
5つのKPIを自動計算してGoldレイヤーに格納

実行方法:
    python calculate_gold_kpis.py
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import sys

# =============================================================================
# 設定
# =============================================================================
DATABASE_PATH = Path(__file__).parent.parent / "database" / "analytics.db"

# =============================================================================
# ユーティリティ関数
# =============================================================================

def print_progress(message):
    """進捗メッセージを表示"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def execute_sql(conn, sql, description):
    """SQLを実行してカウントを返す"""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        # INSERTの場合は挿入行数を返す
        if sql.strip().upper().startswith('INSERT'):
            count = cursor.rowcount
        else:
            count = cursor.fetchone()[0] if cursor.description else 0
        print_progress(f"✓ {description}: {count}件")
        return count
    except Exception as e:
        print(f"❌ エラー ({description}): {str(e)}")
        import traceback
        traceback.print_exc()
        return 0

# =============================================================================
# Goldテーブル作成
# =============================================================================

def create_gold_tables(conn):
    """Goldレイヤーのテーブルを作成"""
    print_progress("Goldテーブルを作成中...")
    
    cursor = conn.cursor()
    
    # 既存テーブルを削除
    tables_to_drop = [
        'gold_kpi_inventory_turnover',
        'gold_kpi_procurement_lead_time',
        'gold_kpi_logistics_cost_ratio',
        'gold_kpi_indirect_material_cost_reduction',
        'gold_kpi_cash_conversion_cycle'
    ]
    
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    # KPI1: 在庫回転率
    cursor.execute("""
    CREATE TABLE gold_kpi_inventory_turnover (
        kpi_key INTEGER PRIMARY KEY AUTOINCREMENT,
        year_month TEXT NOT NULL,
        location_key INTEGER,
        location_name TEXT,
        product_category TEXT,
        cogs REAL,
        avg_inventory_value REAL,
        inventory_turnover_ratio REAL,
        target_turnover REAL DEFAULT 12.0,
        achievement_rate REAL,
        evaluation TEXT,
        previous_month_ratio REAL,
        mom_change REAL,
        is_improving INTEGER,
        roic_contribution TEXT,
        action_recommendation TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # KPI2: 調達リードタイム遵守率
    cursor.execute("""
    CREATE TABLE gold_kpi_procurement_lead_time (
        kpi_key INTEGER PRIMARY KEY AUTOINCREMENT,
        year_month TEXT NOT NULL,
        supplier_key INTEGER,
        supplier_name TEXT,
        material_category TEXT,
        account_group TEXT,
        total_orders INTEGER,
        on_time_deliveries INTEGER,
        late_deliveries INTEGER,
        lead_time_adherence_rate REAL,
        avg_lead_time_days REAL,
        avg_lead_time_variance_days REAL,
        target_adherence_rate REAL DEFAULT 95.0,
        achievement_rate REAL,
        evaluation TEXT,
        previous_month_rate REAL,
        mom_change REAL,
        is_improving INTEGER,
        roic_contribution TEXT,
        action_recommendation TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # KPI3: 物流コスト売上高比率
    cursor.execute("""
    CREATE TABLE gold_kpi_logistics_cost_ratio (
        kpi_key INTEGER PRIMARY KEY AUTOINCREMENT,
        year_month TEXT NOT NULL,
        location_key INTEGER,
        location_name TEXT,
        total_sales REAL,
        total_logistics_cost REAL,
        inbound_cost REAL,
        outbound_cost REAL,
        warehouse_cost REAL,
        logistics_cost_ratio REAL,
        target_ratio REAL DEFAULT 5.0,
        achievement_rate REAL,
        evaluation TEXT,
        previous_month_ratio REAL,
        mom_change REAL,
        is_improving INTEGER,
        roic_contribution TEXT,
        action_recommendation TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # KPI4: 間接材調達コスト削減率
    cursor.execute("""
    CREATE TABLE gold_kpi_indirect_material_cost_reduction (
        kpi_key INTEGER PRIMARY KEY AUTOINCREMENT,
        year_month TEXT NOT NULL,
        material_category TEXT,
        account_group TEXT,
        supplier_key INTEGER,
        supplier_name TEXT,
        baseline_period TEXT,
        baseline_unit_price REAL,
        current_period TEXT,
        current_unit_price REAL,
        cost_reduction_rate REAL,
        quantity_procured REAL,
        total_savings REAL,
        target_reduction_rate REAL DEFAULT 3.0,
        achievement_rate REAL,
        evaluation TEXT,
        is_improving INTEGER,
        roic_contribution TEXT,
        action_recommendation TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # KPI5: キャッシュコンバージョンサイクル (CCC)
    cursor.execute("""
    CREATE TABLE gold_kpi_cash_conversion_cycle (
        kpi_key INTEGER PRIMARY KEY AUTOINCREMENT,
        year_month TEXT NOT NULL,
        location_key INTEGER,
        location_name TEXT,
        avg_inventory REAL,
        total_cogs REAL,
        avg_receivables REAL,
        total_sales REAL,
        avg_payables REAL,
        total_procurement REAL,
        dio REAL,
        dso REAL,
        dpo REAL,
        cash_conversion_cycle REAL,
        target_ccc REAL DEFAULT 60.0,
        achievement_rate REAL,
        evaluation TEXT,
        previous_month_ccc REAL,
        mom_change REAL,
        is_improving INTEGER,
        roic_contribution TEXT,
        action_recommendation TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    conn.commit()
    print_progress("✓ Goldテーブル作成完了")

# =============================================================================
# KPI計算SQL
# =============================================================================

def calculate_kpi1_inventory_turnover(conn):
    """KPI1: 在庫回転率を計算"""
    print_progress("KPI1: 在庫回転率を計算中...")
    
    sql = """
    INSERT INTO gold_kpi_inventory_turnover (
        year_month, location_key, location_name, product_category,
        cogs, avg_inventory_value, inventory_turnover_ratio,
        target_turnover, achievement_rate, evaluation,
        is_improving, roic_contribution, action_recommendation
    )
    WITH monthly_inventory AS (
        SELECT
            d.year_month,
            inv.location_key,
            l.location_name,
            p.item_group AS product_category,
            AVG(inv.inventory_value) AS avg_inventory_value
        FROM silver_fact_inventory_daily inv
        JOIN silver_dim_date d ON inv.date_key = d.date_key
        JOIN silver_dim_location l ON inv.location_key = l.location_key
        JOIN silver_dim_product p ON inv.product_key = p.product_key
        GROUP BY d.year_month, inv.location_key, l.location_name, p.item_group
    ),
    monthly_cogs AS (
        SELECT
            d.year_month,
            so.location_key,
            SUM(so.line_total_ex_tax) * 0.7 AS cogs
        FROM silver_fact_sales_order so
        JOIN silver_dim_date d ON so.date_key = d.date_key
        GROUP BY d.year_month, so.location_key
    )
    SELECT
        mi.year_month,
        mi.location_key,
        mi.location_name,
        mi.product_category,
        COALESCE(mc.cogs, 0) AS cogs,
        mi.avg_inventory_value,
        CASE
            WHEN mi.avg_inventory_value > 0 THEN (COALESCE(mc.cogs, 0) / mi.avg_inventory_value) * 12
            ELSE 0
        END AS inventory_turnover_ratio,
        12.0 AS target_turnover,
        CASE
            WHEN mi.avg_inventory_value > 0 THEN ((COALESCE(mc.cogs, 0) / mi.avg_inventory_value) * 12) / 12.0 * 100
            ELSE 0
        END AS achievement_rate,
        CASE
            WHEN mi.avg_inventory_value > 0 AND ((COALESCE(mc.cogs, 0) / mi.avg_inventory_value) * 12) >= 12.0 THEN '優良'
            WHEN mi.avg_inventory_value > 0 AND ((COALESCE(mc.cogs, 0) / mi.avg_inventory_value) * 12) >= 9.0 THEN '良好'
            WHEN mi.avg_inventory_value > 0 AND ((COALESCE(mc.cogs, 0) / mi.avg_inventory_value) * 12) >= 6.0 THEN '要改善'
            ELSE '要注意'
        END AS evaluation,
        0 AS is_improving,
        '在庫削減→運転資本減少→ROICアップ' AS roic_contribution,
        CASE
            WHEN mi.avg_inventory_value > 0 AND ((COALESCE(mc.cogs, 0) / mi.avg_inventory_value) * 12) < 6.0 
            THEN '在庫削減施策を検討: 安全在庫見直し、調達頻度アップ'
            ELSE '現状維持'
        END AS action_recommendation
    FROM monthly_inventory mi
    LEFT JOIN monthly_cogs mc ON mi.year_month = mc.year_month AND mi.location_key = mc.location_key
    """
    
    execute_sql(conn, sql, "在庫回転率")

def calculate_kpi2_procurement_lead_time(conn):
    """KPI2: 調達リードタイム遵守率を計算"""
    print_progress("KPI2: 調達リードタイム遵守率を計算中...")
    
    sql = """
    INSERT INTO gold_kpi_procurement_lead_time (
        year_month, supplier_key, supplier_name, material_category, account_group,
        total_orders, on_time_deliveries, late_deliveries,
        lead_time_adherence_rate, avg_lead_time_days, avg_lead_time_variance_days,
        target_adherence_rate, achievement_rate, evaluation,
        is_improving, roic_contribution, action_recommendation
    )
    SELECT
        d.year_month,
        pr.supplier_key,
        p.partner_name AS supplier_name,
        pr.material_category,
        pr.account_group,
        COUNT(*) AS total_orders,
        SUM(pr.is_on_time) AS on_time_deliveries,
        COUNT(*) - SUM(pr.is_on_time) AS late_deliveries,
        CAST(SUM(pr.is_on_time) AS REAL) / COUNT(*) * 100 AS lead_time_adherence_rate,
        AVG(pr.lead_time_days) AS avg_lead_time_days,
        AVG(pr.lead_time_variance_days) AS avg_lead_time_variance_days,
        95.0 AS target_adherence_rate,
        (CAST(SUM(pr.is_on_time) AS REAL) / COUNT(*) * 100) / 95.0 * 100 AS achievement_rate,
        CASE
            WHEN CAST(SUM(pr.is_on_time) AS REAL) / COUNT(*) * 100 >= 95.0 THEN '優良'
            WHEN CAST(SUM(pr.is_on_time) AS REAL) / COUNT(*) * 100 >= 90.0 THEN '良好'
            WHEN CAST(SUM(pr.is_on_time) AS REAL) / COUNT(*) * 100 >= 80.0 THEN '要改善'
            ELSE '要注意'
        END AS evaluation,
        0 AS is_improving,
        'リードタイム短縮→在庫削減→運転資本減少→ROICアップ' AS roic_contribution,
        CASE
            WHEN CAST(SUM(pr.is_on_time) AS REAL) / COUNT(*) * 100 < 80.0 
            THEN 'サプライヤー変更検討、調達計画見直し、安全在庫増加'
            ELSE '現状維持、継続モニタリング'
        END AS action_recommendation
    FROM silver_fact_procurement pr
    JOIN silver_dim_date d ON pr.date_key = d.date_key
    JOIN silver_dim_partner p ON pr.supplier_key = p.partner_key
    WHERE pr.actual_received_date IS NOT NULL
    GROUP BY d.year_month, pr.supplier_key, p.partner_name, pr.material_category, pr.account_group
    """
    
    execute_sql(conn, sql, "調達リードタイム遵守率")

def calculate_kpi3_logistics_cost_ratio(conn):
    """KPI3: 物流コスト売上高比率を計算"""
    print_progress("KPI3: 物流コスト売上高比率を計算中...")
    
    sql = """
    INSERT INTO gold_kpi_logistics_cost_ratio (
        year_month, location_key, location_name,
        total_sales, total_logistics_cost,
        inbound_cost, outbound_cost, warehouse_cost,
        logistics_cost_ratio, target_ratio, achievement_rate, evaluation,
        is_improving, roic_contribution, action_recommendation
    )
    WITH monthly_sales AS (
        SELECT
            d.year_month,
            so.location_key,
            l.location_name,
            SUM(so.line_total_ex_tax) AS total_sales
        FROM silver_fact_sales_order so
        JOIN silver_dim_date d ON so.date_key = d.date_key
        JOIN silver_dim_location l ON so.location_key = l.location_key
        GROUP BY d.year_month, so.location_key, l.location_name
    ),
    monthly_logistics_cost AS (
        SELECT
            d.year_month,
            tc.location_key,
            SUM(CASE WHEN tc.cost_type = 'inbound' THEN tc.cost_amount ELSE 0 END) AS inbound_cost,
            SUM(CASE WHEN tc.cost_type = 'outbound' THEN tc.cost_amount ELSE 0 END) AS outbound_cost,
            SUM(CASE WHEN tc.cost_type = 'warehouse' THEN tc.cost_amount ELSE 0 END) AS warehouse_cost,
            SUM(tc.cost_amount) AS total_logistics_cost
        FROM silver_fact_transportation_cost tc
        JOIN silver_dim_date d ON tc.date_key = d.date_key
        GROUP BY d.year_month, tc.location_key
    )
    SELECT
        ms.year_month,
        ms.location_key,
        ms.location_name,
        ms.total_sales,
        COALESCE(mlc.total_logistics_cost, 0) AS total_logistics_cost,
        COALESCE(mlc.inbound_cost, 0) AS inbound_cost,
        COALESCE(mlc.outbound_cost, 0) AS outbound_cost,
        COALESCE(mlc.warehouse_cost, 0) AS warehouse_cost,
        CASE
            WHEN ms.total_sales > 0 THEN COALESCE(mlc.total_logistics_cost, 0) / ms.total_sales * 100
            ELSE 0
        END AS logistics_cost_ratio,
        5.0 AS target_ratio,
        CASE
            WHEN ms.total_sales > 0 THEN 5.0 / (COALESCE(mlc.total_logistics_cost, 0) / ms.total_sales * 100) * 100
            ELSE 0
        END AS achievement_rate,
        CASE
            WHEN ms.total_sales > 0 AND (COALESCE(mlc.total_logistics_cost, 0) / ms.total_sales * 100) <= 5.0 THEN '優良'
            WHEN ms.total_sales > 0 AND (COALESCE(mlc.total_logistics_cost, 0) / ms.total_sales * 100) <= 7.0 THEN '良好'
            WHEN ms.total_sales > 0 AND (COALESCE(mlc.total_logistics_cost, 0) / ms.total_sales * 100) <= 10.0 THEN '要改善'
            ELSE '要注意'
        END AS evaluation,
        0 AS is_improving,
        '物流コスト削減→営業利益率改善→ROICアップ' AS roic_contribution,
        CASE
            WHEN ms.total_sales > 0 AND (COALESCE(mlc.total_logistics_cost, 0) / ms.total_sales * 100) > 10.0 
            THEN '輸送ルート最適化、キャリア見直し、在庫配置最適化を検討'
            ELSE '現状維持、コスト削減余地を継続検討'
        END AS action_recommendation
    FROM monthly_sales ms
    LEFT JOIN monthly_logistics_cost mlc ON ms.year_month = mlc.year_month AND ms.location_key = mlc.location_key
    """
    
    execute_sql(conn, sql, "物流コスト売上高比率")

def calculate_kpi4_indirect_material_cost_reduction(conn):
    """KPI4: 間接材調達コスト削減率を計算"""
    print_progress("KPI4: 間接材調達コスト削減率を計算中...")
    
    sql = """
    INSERT INTO gold_kpi_indirect_material_cost_reduction (
        year_month, material_category, account_group,
        supplier_key, supplier_name,
        baseline_period, baseline_unit_price,
        current_period, current_unit_price,
        cost_reduction_rate, quantity_procured, total_savings,
        target_reduction_rate, achievement_rate, evaluation,
        is_improving, roic_contribution, action_recommendation
    )
    WITH baseline AS (
        SELECT
            pr.material_key,
            pr.supplier_key,
            pr.material_category,
            pr.account_group,
            AVG(pr.unit_price_ex_tax) AS baseline_unit_price,
            '2022-01' AS baseline_period
        FROM silver_fact_procurement pr
        JOIN silver_dim_date d ON pr.date_key = d.date_key
        WHERE d.year_month = '2022-01'
          AND pr.account_group = 'MRO'
        GROUP BY pr.material_key, pr.supplier_key, pr.material_category, pr.account_group
    ),
    current_procurement AS (
        SELECT
            d.year_month,
            pr.material_key,
            pr.supplier_key,
            p.partner_name,
            pr.material_category,
            pr.account_group,
            AVG(pr.unit_price_ex_tax) AS current_unit_price,
            SUM(pr.quantity) AS quantity_procured
        FROM silver_fact_procurement pr
        JOIN silver_dim_date d ON pr.date_key = d.date_key
        JOIN silver_dim_partner p ON pr.supplier_key = p.partner_key
        WHERE pr.account_group = 'MRO'
        GROUP BY d.year_month, pr.material_key, pr.supplier_key, p.partner_name, pr.material_category, pr.account_group
    )
    SELECT
        cp.year_month,
        cp.material_category,
        cp.account_group,
        cp.supplier_key,
        cp.partner_name AS supplier_name,
        b.baseline_period,
        b.baseline_unit_price,
        cp.year_month AS current_period,
        cp.current_unit_price,
        CASE
            WHEN b.baseline_unit_price > 0 THEN (b.baseline_unit_price - cp.current_unit_price) / b.baseline_unit_price * 100
            ELSE 0
        END AS cost_reduction_rate,
        cp.quantity_procured,
        CASE
            WHEN b.baseline_unit_price > 0 THEN (b.baseline_unit_price - cp.current_unit_price) * cp.quantity_procured
            ELSE 0
        END AS total_savings,
        3.0 AS target_reduction_rate,
        CASE
            WHEN b.baseline_unit_price > 0 THEN ((b.baseline_unit_price - cp.current_unit_price) / b.baseline_unit_price * 100) / 3.0 * 100
            ELSE 0
        END AS achievement_rate,
        CASE
            WHEN b.baseline_unit_price > 0 AND ((b.baseline_unit_price - cp.current_unit_price) / b.baseline_unit_price * 100) >= 3.0 THEN '優良'
            WHEN b.baseline_unit_price > 0 AND ((b.baseline_unit_price - cp.current_unit_price) / b.baseline_unit_price * 100) >= 1.5 THEN '良好'
            WHEN b.baseline_unit_price > 0 AND ((b.baseline_unit_price - cp.current_unit_price) / b.baseline_unit_price * 100) >= 0 THEN '要改善'
            ELSE '要注意'
        END AS evaluation,
        CASE
            WHEN b.baseline_unit_price > 0 AND ((b.baseline_unit_price - cp.current_unit_price) / b.baseline_unit_price * 100) > 0 THEN 1
            ELSE 0
        END AS is_improving,
        '調達コスト削減→営業利益率改善→ROICアップ' AS roic_contribution,
        CASE
            WHEN b.baseline_unit_price > 0 AND ((b.baseline_unit_price - cp.current_unit_price) / b.baseline_unit_price * 100) < 0 
            THEN 'サプライヤー交渉、競合見積り、集約購買を実施'
            WHEN b.baseline_unit_price > 0 AND ((b.baseline_unit_price - cp.current_unit_price) / b.baseline_unit_price * 100) < 1.5
            THEN '更なるコスト削減余地を検討'
            ELSE '現状維持、継続的コスト管理'
        END AS action_recommendation
    FROM current_procurement cp
    JOIN baseline b ON cp.material_key = b.material_key AND cp.supplier_key = b.supplier_key
    """
    
    execute_sql(conn, sql, "間接材調達コスト削減率")

def calculate_kpi5_cash_conversion_cycle(conn):
    """KPI5: キャッシュコンバージョンサイクルを計算"""
    print_progress("KPI5: キャッシュコンバージョンサイクルを計算中...")
    
    sql = """
    INSERT INTO gold_kpi_cash_conversion_cycle (
        year_month, location_key, location_name,
        avg_inventory, total_cogs,
        avg_receivables, total_sales,
        avg_payables, total_procurement,
        dio, dso, dpo, cash_conversion_cycle,
        target_ccc, achievement_rate, evaluation,
        is_improving, roic_contribution, action_recommendation
    )
    WITH monthly_inventory AS (
        SELECT
            d.year_month,
            inv.location_key,
            l.location_name,
            AVG(inv.inventory_value) AS avg_inventory
        FROM silver_fact_inventory_daily inv
        JOIN silver_dim_date d ON inv.date_key = d.date_key
        JOIN silver_dim_location l ON inv.location_key = l.location_key
        GROUP BY d.year_month, inv.location_key, l.location_name
    ),
    monthly_sales AS (
        SELECT
            d.year_month,
            so.location_key,
            SUM(so.line_total_ex_tax) AS total_sales,
            SUM(so.line_total_ex_tax) * 0.7 AS total_cogs,
            SUM(so.line_total_ex_tax) * 0.3 AS avg_receivables
        FROM silver_fact_sales_order so
        JOIN silver_dim_date d ON so.date_key = d.date_key
        GROUP BY d.year_month, so.location_key
    ),
    monthly_procurement AS (
        SELECT
            d.year_month,
            pr.location_key,
            SUM(pr.line_total_ex_tax) AS total_procurement,
            SUM(pr.line_total_ex_tax) * 0.5 AS avg_payables
        FROM silver_fact_procurement pr
        JOIN silver_dim_date d ON pr.date_key = d.date_key
        GROUP BY d.year_month, pr.location_key
    )
    SELECT
        mi.year_month,
        mi.location_key,
        mi.location_name,
        mi.avg_inventory,
        COALESCE(ms.total_cogs, 0) AS total_cogs,
        COALESCE(ms.avg_receivables, 0) AS avg_receivables,
        COALESCE(ms.total_sales, 0) AS total_sales,
        COALESCE(mp.avg_payables, 0) AS avg_payables,
        COALESCE(mp.total_procurement, 0) AS total_procurement,
        CASE
            WHEN COALESCE(ms.total_cogs, 0) > 0 THEN (mi.avg_inventory / (COALESCE(ms.total_cogs, 0) / 30))
            ELSE 0
        END AS dio,
        CASE
            WHEN COALESCE(ms.total_sales, 0) > 0 THEN (COALESCE(ms.avg_receivables, 0) / (COALESCE(ms.total_sales, 0) / 30))
            ELSE 0
        END AS dso,
        CASE
            WHEN COALESCE(mp.total_procurement, 0) > 0 THEN (COALESCE(mp.avg_payables, 0) / (COALESCE(mp.total_procurement, 0) / 30))
            ELSE 0
        END AS dpo,
        (CASE WHEN COALESCE(ms.total_cogs, 0) > 0 THEN (mi.avg_inventory / (COALESCE(ms.total_cogs, 0) / 30)) ELSE 0 END) +
        (CASE WHEN COALESCE(ms.total_sales, 0) > 0 THEN (COALESCE(ms.avg_receivables, 0) / (COALESCE(ms.total_sales, 0) / 30)) ELSE 0 END) -
        (CASE WHEN COALESCE(mp.total_procurement, 0) > 0 THEN (COALESCE(mp.avg_payables, 0) / (COALESCE(mp.total_procurement, 0) / 30)) ELSE 0 END)
        AS cash_conversion_cycle,
        60.0 AS target_ccc,
        CASE
            WHEN ((CASE WHEN COALESCE(ms.total_cogs, 0) > 0 THEN (mi.avg_inventory / (COALESCE(ms.total_cogs, 0) / 30)) ELSE 0 END) +
                  (CASE WHEN COALESCE(ms.total_sales, 0) > 0 THEN (COALESCE(ms.avg_receivables, 0) / (COALESCE(ms.total_sales, 0) / 30)) ELSE 0 END) -
                  (CASE WHEN COALESCE(mp.total_procurement, 0) > 0 THEN (COALESCE(mp.avg_payables, 0) / (COALESCE(mp.total_procurement, 0) / 30)) ELSE 0 END)) > 0
            THEN 60.0 / ((CASE WHEN COALESCE(ms.total_cogs, 0) > 0 THEN (mi.avg_inventory / (COALESCE(ms.total_cogs, 0) / 30)) ELSE 0 END) +
                         (CASE WHEN COALESCE(ms.total_sales, 0) > 0 THEN (COALESCE(ms.avg_receivables, 0) / (COALESCE(ms.total_sales, 0) / 30)) ELSE 0 END) -
                         (CASE WHEN COALESCE(mp.total_procurement, 0) > 0 THEN (COALESCE(mp.avg_payables, 0) / (COALESCE(mp.total_procurement, 0) / 30)) ELSE 0 END)) * 100
            ELSE 0
        END AS achievement_rate,
        CASE
            WHEN ((CASE WHEN COALESCE(ms.total_cogs, 0) > 0 THEN (mi.avg_inventory / (COALESCE(ms.total_cogs, 0) / 30)) ELSE 0 END) +
                  (CASE WHEN COALESCE(ms.total_sales, 0) > 0 THEN (COALESCE(ms.avg_receivables, 0) / (COALESCE(ms.total_sales, 0) / 30)) ELSE 0 END) -
                  (CASE WHEN COALESCE(mp.total_procurement, 0) > 0 THEN (COALESCE(mp.avg_payables, 0) / (COALESCE(mp.total_procurement, 0) / 30)) ELSE 0 END)) <= 60.0 THEN '優良'
            WHEN ((CASE WHEN COALESCE(ms.total_cogs, 0) > 0 THEN (mi.avg_inventory / (COALESCE(ms.total_cogs, 0) / 30)) ELSE 0 END) +
                  (CASE WHEN COALESCE(ms.total_sales, 0) > 0 THEN (COALESCE(ms.avg_receivables, 0) / (COALESCE(ms.total_sales, 0) / 30)) ELSE 0 END) -
                  (CASE WHEN COALESCE(mp.total_procurement, 0) > 0 THEN (COALESCE(mp.avg_payables, 0) / (COALESCE(mp.total_procurement, 0) / 30)) ELSE 0 END)) <= 90.0 THEN '良好'
            WHEN ((CASE WHEN COALESCE(ms.total_cogs, 0) > 0 THEN (mi.avg_inventory / (COALESCE(ms.total_cogs, 0) / 30)) ELSE 0 END) +
                  (CASE WHEN COALESCE(ms.total_sales, 0) > 0 THEN (COALESCE(ms.avg_receivables, 0) / (COALESCE(ms.total_sales, 0) / 30)) ELSE 0 END) -
                  (CASE WHEN COALESCE(mp.total_procurement, 0) > 0 THEN (COALESCE(mp.avg_payables, 0) / (COALESCE(mp.total_procurement, 0) / 30)) ELSE 0 END)) <= 120.0 THEN '要改善'
            ELSE '要注意'
        END AS evaluation,
        0 AS is_improving,
        'CCC短縮→運転資本減少→ROICアップ' AS roic_contribution,
        CASE
            WHEN ((CASE WHEN COALESCE(ms.total_cogs, 0) > 0 THEN (mi.avg_inventory / (COALESCE(ms.total_cogs, 0) / 30)) ELSE 0 END) +
                  (CASE WHEN COALESCE(ms.total_sales, 0) > 0 THEN (COALESCE(ms.avg_receivables, 0) / (COALESCE(ms.total_sales, 0) / 30)) ELSE 0 END) -
                  (CASE WHEN COALESCE(mp.total_procurement, 0) > 0 THEN (COALESCE(mp.avg_payables, 0) / (COALESCE(mp.total_procurement, 0) / 30)) ELSE 0 END)) > 120.0 
            THEN '在庫削減、与信管理強化、支払条件交渉を総合的に実施'
            WHEN ((CASE WHEN COALESCE(ms.total_cogs, 0) > 0 THEN (mi.avg_inventory / (COALESCE(ms.total_cogs, 0) / 30)) ELSE 0 END) +
                  (CASE WHEN COALESCE(ms.total_sales, 0) > 0 THEN (COALESCE(ms.avg_receivables, 0) / (COALESCE(ms.total_sales, 0) / 30)) ELSE 0 END) -
                  (CASE WHEN COALESCE(mp.total_procurement, 0) > 0 THEN (COALESCE(mp.avg_payables, 0) / (COALESCE(mp.total_procurement, 0) / 30)) ELSE 0 END)) > 90.0
            THEN 'DIO/DSO改善余地を検討'
            ELSE '現状維持、継続モニタリング'
        END AS action_recommendation
    FROM monthly_inventory mi
    LEFT JOIN monthly_sales ms ON mi.year_month = ms.year_month AND mi.location_key = ms.location_key
    LEFT JOIN monthly_procurement mp ON mi.year_month = mp.year_month AND mi.location_key = mp.location_key
    """
    
    execute_sql(conn, sql, "キャッシュコンバージョンサイクル")

# =============================================================================
# メイン処理
# =============================================================================

def main():
    """メイン処理"""
    print("\n" + "="*70)
    print("  Silver → Gold KPI計算スクリプト")
    print("="*70 + "\n")
    
    # データベース接続
    if not DATABASE_PATH.exists():
        print(f"❌ エラー: データベースファイルが見つかりません")
        print(f"   {DATABASE_PATH}")
        print("\n先に load_bronze_to_silver.py を実行してください。")
        sys.exit(1)
    
    print_progress(f"データベースに接続: {DATABASE_PATH}")
    conn = sqlite3.connect(str(DATABASE_PATH))
    
    try:
        # Goldテーブル作成
        create_gold_tables(conn)
        
        # KPI計算
        print("\n--- KPI計算実行 ---")
        calculate_kpi1_inventory_turnover(conn)
        calculate_kpi2_procurement_lead_time(conn)
        calculate_kpi3_logistics_cost_ratio(conn)
        calculate_kpi4_indirect_material_cost_reduction(conn)
        calculate_kpi5_cash_conversion_cycle(conn)
        
        # 完了
        print("\n" + "="*70)
        print("✓ 5つのKPI計算が完了しました！")
        print(f"  データベース: {DATABASE_PATH}")
        print(f"  更新日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n次のステップ:")
        print("  1. DB Browser for SQLiteでデータベースを開く")
        print("  2. queries/フォルダ内のSQLクエリを実行して分析")
        print("  3. ExcelやPower BIからデータベースに接続")
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
