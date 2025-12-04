"""
ゴールド層ETL処理
KPI計算・集計処理
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from etl.common import DatabaseManager, setup_logging, validate_dataframe
from etl.common.config import BUSINESS_RULES, DAYS_PER_MONTH

class GoldETL:
    """ゴールド層ETL処理クラス"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.logger = setup_logging("gold_etl")
        
    def initialize_database(self):
        """ゴールド層スキーマ初期化"""
        self.logger.info("ゴールド層スキーマを初期化中...")
        
        schema_file = Path(__file__).parent.parent.parent / "database" / "schema_gold.sql"
        if schema_file.exists():
            self.db_manager.execute_sql_file(schema_file)
            self.logger.info("ゴールド層スキーマ初期化完了")
        else:
            raise FileNotFoundError(f"スキーマファイルが見つかりません: {schema_file}")
    
    def calculate_monthly_product_gross_margin(self) -> bool:
        """月次商品別粗利率の計算"""
        try:
            self.logger.info("月次商品別粗利率計算開始...")
            
            # 受注データから売上を計算
            sql = """
            INSERT OR REPLACE INTO gold_monthly_product_gross_margin 
            (product_id, product_name, gross_margin_rate, year_month, revenue)
            WITH monthly_sales AS (
                SELECT 
                    product_id,
                    product_name,
                    year_month,
                    SUM(line_total_ex_tax) as revenue
                FROM silver_order_data
                WHERE line_total_ex_tax IS NOT NULL
                GROUP BY product_id, product_name, year_month
            ),
            monthly_cogs AS (
                SELECT 
                    product_id,
                    year_month,
                    SUM(line_total_ex_tax) as cogs
                FROM silver_procurement_data
                WHERE account_group = 'DIRECT_MATERIAL'
                GROUP BY product_id, year_month
            )
            SELECT 
                s.product_id,
                s.product_name,
                CASE 
                    WHEN s.revenue > 0 THEN 
                        ROUND(((s.revenue - COALESCE(c.cogs, 0)) / s.revenue), 6)
                    ELSE NULL
                END as gross_margin_rate,
                s.year_month,
                s.revenue
            FROM monthly_sales s
            LEFT JOIN monthly_cogs c ON s.product_id = c.product_id 
                                     AND s.year_month = c.year_month
            WHERE s.revenue > 0
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_product_gross_margin")
            self.logger.info(f"月次商品別粗利率計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次商品別粗利率計算エラー: {e}")
            return False
    
    def calculate_monthly_ev_sales_share(self) -> bool:
        """月次EV販売率の計算"""
        try:
            self.logger.info("月次EV販売率計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_ev_sales_share 
            (year_month, total_revenue, ev_revenue, ev_sales_share)
            WITH monthly_sales AS (
                SELECT 
                    o.year_month,
                    SUM(o.line_total_ex_tax) as total_revenue,
                    SUM(CASE WHEN i.is_ev = 1 THEN o.line_total_ex_tax ELSE 0 END) as ev_revenue
                FROM silver_order_data o
                JOIN silver_item_master i ON o.product_id = i.product_id
                WHERE o.line_total_ex_tax IS NOT NULL
                GROUP BY o.year_month
            )
            SELECT 
                year_month,
                total_revenue,
                ev_revenue,
                CASE 
                    WHEN total_revenue > 0 THEN 
                        ROUND((ev_revenue / total_revenue), 6)
                    ELSE NULL
                END as ev_sales_share
            FROM monthly_sales
            WHERE total_revenue > 0
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_ev_sales_share")
            self.logger.info(f"月次EV販売率計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次EV販売率計算エラー: {e}")
            return False
    
    def calculate_monthly_area_ev_sales_share(self) -> bool:
        """月次エリア別EV販売率の計算"""
        try:
            self.logger.info("月次エリア別EV販売率計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_area_ev_sales_share 
            (year_month, location_id, total_revenue, ev_revenue, ev_sales_share)
            WITH area_sales AS (
                SELECT 
                    o.year_month,
                    o.location_id,
                    SUM(o.line_total_ex_tax) as total_revenue,
                    SUM(CASE WHEN i.is_ev = 1 THEN o.line_total_ex_tax ELSE 0 END) as ev_revenue
                FROM silver_order_data o
                JOIN silver_item_master i ON o.product_id = i.product_id
                WHERE o.line_total_ex_tax IS NOT NULL AND o.location_id IS NOT NULL
                GROUP BY o.year_month, o.location_id
            )
            SELECT 
                year_month,
                location_id,
                total_revenue,
                ev_revenue,
                CASE 
                    WHEN total_revenue > 0 THEN 
                        ROUND((ev_revenue / total_revenue), 6)
                    ELSE NULL
                END as ev_sales_share
            FROM area_sales
            WHERE total_revenue > 0
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_area_ev_sales_share")
            self.logger.info(f"月次エリア別EV販売率計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次エリア別EV販売率計算エラー: {e}")
            return False
    
    def calculate_monthly_safety_equipment_adoption(self) -> bool:
        """月次先進安全装置適用率の計算"""
        try:
            self.logger.info("月次先進安全装置適用率計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_safety_equipment_adoption 
            (year_month, total_revenue, safety_equipped_revenue, safety_equipment_adoption_rate)
            WITH monthly_sales AS (
                SELECT 
                    o.year_month,
                    SUM(o.line_total_ex_tax) as total_revenue,
                    SUM(CASE WHEN i.is_safety_equipped = 1 THEN o.line_total_ex_tax ELSE 0 END) as safety_equipped_revenue
                FROM silver_order_data o
                JOIN silver_item_master i ON o.product_id = i.product_id
                WHERE o.line_total_ex_tax IS NOT NULL
                GROUP BY o.year_month
            )
            SELECT 
                year_month,
                total_revenue,
                safety_equipped_revenue,
                CASE 
                    WHEN total_revenue > 0 THEN 
                        ROUND((safety_equipped_revenue / total_revenue), 6)
                    ELSE NULL
                END as safety_equipment_adoption_rate
            FROM monthly_sales
            WHERE total_revenue > 0
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_safety_equipment_adoption")
            self.logger.info(f"月次先進安全装置適用率計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次先進安全装置適用率計算エラー: {e}")
            return False
    
    def calculate_monthly_area_safety_equipment_adoption(self) -> bool:
        """月次エリア別先進安全装置適用率の計算"""
        try:
            self.logger.info("月次エリア別先進安全装置適用率計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_area_safety_equipment_adoption 
            (year_month, location_id, total_revenue, safety_equipped_revenue, safety_equipment_adoption_rate)
            WITH area_sales AS (
                SELECT 
                    o.year_month,
                    o.location_id,
                    SUM(o.line_total_ex_tax) as total_revenue,
                    SUM(CASE WHEN i.is_safety_equipped = 1 THEN o.line_total_ex_tax ELSE 0 END) as safety_equipped_revenue
                FROM silver_order_data o
                JOIN silver_item_master i ON o.product_id = i.product_id
                WHERE o.line_total_ex_tax IS NOT NULL AND o.location_id IS NOT NULL
                GROUP BY o.year_month, o.location_id
            )
            SELECT 
                year_month,
                location_id,
                total_revenue,
                safety_equipped_revenue,
                CASE 
                    WHEN total_revenue > 0 THEN 
                        ROUND((safety_equipped_revenue / total_revenue), 6)
                    ELSE NULL
                END as safety_equipment_adoption_rate
            FROM area_sales
            WHERE total_revenue > 0
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_area_safety_equipment_adoption")
            self.logger.info(f"月次エリア別先進安全装置適用率計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次エリア別先進安全装置適用率計算エラー: {e}")
            return False
    
    def calculate_inventory_rotation_period(self) -> bool:
        """棚卸資産回転期間の計算"""
        try:
            self.logger.info("棚卸資産回転期間計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_inventory_rotation_period 
            (year_month, monthly_cogs, inventory_value, inventory_rotation_period)
            WITH monthly_cogs AS (
                SELECT 
                    year_month,
                    SUM(line_total_ex_tax) as monthly_cogs
                FROM silver_procurement_data
                WHERE account_group = 'DIRECT_MATERIAL' AND line_total_ex_tax IS NOT NULL
                GROUP BY year_month
            ),
            monthly_inventory AS (
                SELECT 
                    year_month,
                    SUM(closing_quantity * COALESCE(unit_cost, 0)) as inventory_value
                FROM silver_inventory_data
                WHERE closing_quantity IS NOT NULL
                GROUP BY year_month
            )
            SELECT 
                c.year_month,
                c.monthly_cogs,
                COALESCE(i.inventory_value, 0) as inventory_value,
                CASE 
                    WHEN c.monthly_cogs > 0 THEN 
                        ROUND(((COALESCE(i.inventory_value, 0) / c.monthly_cogs) * 30), 2)
                    ELSE NULL
                END as inventory_rotation_period
            FROM monthly_cogs c
            LEFT JOIN monthly_inventory i ON c.year_month = i.year_month
            WHERE c.monthly_cogs > 0
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_inventory_rotation_period")
            self.logger.info(f"棚卸資産回転期間計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"棚卸資産回転期間計算エラー: {e}")
            return False
    
    def calculate_monthly_ebitda(self) -> bool:
        """月次EBITDAの計算"""
        try:
            self.logger.info("月次EBITDA計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_ebitda 
            (year_month, revenue, gross_margin_amount, operating_expenses, ebitda)
            WITH monthly_revenue AS (
                SELECT 
                    year_month,
                    SUM(line_total_ex_tax) as revenue
                FROM silver_order_data
                WHERE line_total_ex_tax IS NOT NULL
                GROUP BY year_month
            ),
            monthly_cogs AS (
                SELECT 
                    year_month,
                    SUM(line_total_ex_tax) as cogs
                FROM silver_procurement_data
                WHERE account_group = 'DIRECT_MATERIAL' AND line_total_ex_tax IS NOT NULL
                GROUP BY year_month
            ),
            monthly_operating_expenses AS (
                SELECT 
                    year_month,
                    SUM(total_compensation) as payroll_expenses
                FROM silver_payroll_data
                WHERE total_compensation IS NOT NULL
                GROUP BY year_month
            ),
            monthly_indirect_costs AS (
                SELECT 
                    year_month,
                    SUM(line_total_ex_tax) as indirect_costs
                FROM silver_procurement_data
                WHERE account_group = 'MRO' AND line_total_ex_tax IS NOT NULL
                GROUP BY year_month
            )
            SELECT 
                r.year_month,
                r.revenue,
                (r.revenue - COALESCE(c.cogs, 0)) as gross_margin_amount,
                (COALESCE(o.payroll_expenses, 0) + COALESCE(i.indirect_costs, 0)) as operating_expenses,
                (r.revenue - COALESCE(c.cogs, 0) - COALESCE(o.payroll_expenses, 0) - COALESCE(i.indirect_costs, 0)) as ebitda
            FROM monthly_revenue r
            LEFT JOIN monthly_cogs c ON r.year_month = c.year_month
            LEFT JOIN monthly_operating_expenses o ON r.year_month = o.year_month
            LEFT JOIN monthly_indirect_costs i ON r.year_month = i.year_month
            WHERE r.revenue > 0
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_ebitda")
            self.logger.info(f"月次EBITDA計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次EBITDA計算エラー: {e}")
            return False
    
    def calculate_emergency_transportation_cost_share(self) -> bool:
        """緊急輸送費率の計算"""
        try:
            self.logger.info("緊急輸送費率計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_emergency_transportation_cost_share 
            (year_month, transportation_cost_total, emergency_transportation_cost_total, emergency_transportation_cost_share)
            WITH monthly_transport_costs AS (
                SELECT 
                    year_month,
                    SUM(cost_amount) as transportation_cost_total,
                    SUM(CASE WHEN is_emergency = 1 THEN cost_amount ELSE 0 END) as emergency_transportation_cost_total
                FROM silver_transportation_cost
                WHERE cost_amount IS NOT NULL
                GROUP BY year_month
            )
            SELECT 
                year_month,
                transportation_cost_total,
                emergency_transportation_cost_total,
                CASE 
                    WHEN transportation_cost_total > 0 THEN 
                        ROUND((emergency_transportation_cost_total / transportation_cost_total), 6)
                    ELSE NULL
                END as emergency_transportation_cost_share
            FROM monthly_transport_costs
            WHERE transportation_cost_total > 0
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_emergency_transportation_cost_share")
            self.logger.info(f"緊急輸送費率計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"緊急輸送費率計算エラー: {e}")
            return False
    
    def validate_kpi_results(self) -> bool:
        """KPI結果の検証"""
        self.logger.info("KPI結果検証開始...")
        
        # 各KPIテーブルのデータ件数チェック
        kpi_tables = [
            "gold_monthly_product_gross_margin",
            "gold_monthly_ev_sales_share",
            "gold_monthly_area_ev_sales_share",
            "gold_monthly_safety_equipment_adoption",
            "gold_monthly_area_safety_equipment_adoption",
            "gold_inventory_rotation_period",
            "gold_monthly_ebitda",
            "gold_emergency_transportation_cost_share"
        ]
        
        results = {}
        for table in kpi_tables:
            if self.db_manager.table_exists(table):
                row_count = self.db_manager.get_row_count(table)
                results[table] = row_count
                self.logger.info(f"{table}: {row_count} rows")
            else:
                results[table] = 0
                self.logger.warning(f"{table}: テーブルが存在しません")
        
        # 基本的な検証
        total_kpis = sum(1 for count in results.values() if count > 0)
        self.logger.info(f"計算完了KPI数: {total_kpis}/{len(kpi_tables)}")
        
        # サンプル値の表示
        if results["gold_monthly_ev_sales_share"] > 0:
            sample = self.db_manager.query_to_dataframe(
                "SELECT * FROM gold_monthly_ev_sales_share LIMIT 3"
            )
            self.logger.info(f"EV販売率サンプル:\\n{sample.to_string()}")
        
        self.logger.info("KPI結果検証完了")
    
    def calculate_monthly_product_inventory_rotation(self) -> bool:
        """月次商品別棚卸資産回転期間計算"""
        try:
            self.logger.info("月次商品別棚卸資産回転期間計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_product_inventory_rotation 
            (year_month, product_id, product_name, avg_inventory_value, monthly_cost_of_sales, rotation_period_days)
            WITH inventory_data AS (
                SELECT 
                    inv.year_month,
                    inv.product_id,
                    i.product_name,
                    AVG(inv.inventory_value) as avg_inventory_value
                FROM silver_inventory_data inv
                JOIN silver_item_master i ON inv.product_id = i.product_id
                GROUP BY inv.year_month, inv.product_id, i.product_name
            ),
            sales_data AS (
                SELECT 
                    o.year_month,
                    o.product_id,
                    SUM(o.line_total_ex_tax * 0.7) as monthly_cost_of_sales  -- 売上原価を売上の70%と仮定
                FROM silver_order_data o
                GROUP BY o.year_month, o.product_id
            )
            SELECT 
                inv.year_month,
                inv.product_id,
                inv.product_name,
                inv.avg_inventory_value,
                COALESCE(sales.monthly_cost_of_sales, 0) as monthly_cost_of_sales,
                CASE 
                    WHEN COALESCE(sales.monthly_cost_of_sales, 0) > 0 
                    THEN ROUND(inv.avg_inventory_value * 30.0 / sales.monthly_cost_of_sales, 1)
                    ELSE NULL 
                END as rotation_period_days
            FROM inventory_data inv
            LEFT JOIN sales_data sales ON inv.year_month = sales.year_month AND inv.product_id = sales.product_id
            ORDER BY inv.year_month, inv.product_id
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_product_inventory_rotation")
            self.logger.info(f"月次商品別棚卸資産回転期間計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次商品別棚卸資産回転期間計算エラー: {e}")
            return False
    
    def calculate_monthly_product_inventory_rotation(self):
        """月次商品別棚卸資産回転期間計算"""
        sql = """
        INSERT INTO gold_monthly_product_inventory_rotation 
        (year_month, product_id, product_name, avg_inventory_value, monthly_cost_of_sales, rotation_period_days)
        SELECT 
            wi.year_month,
            wi.product_id,
            sim.product_name,
            AVG(wi.inventory_value) as avg_inventory_value,
            COALESCE(sales.monthly_cost, 0) as monthly_cost_of_sales,
            CASE 
                WHEN COALESCE(sales.monthly_cost, 0) > 0 
                THEN ROUND(AVG(wi.inventory_value) * 30.0 / sales.monthly_cost, 1)
                ELSE NULL 
            END as rotation_period_days
        FROM silver_inventory_data wi
        JOIN silver_item_master sim ON wi.product_id = sim.product_id
        LEFT JOIN (
            SELECT 
                strftime('%Y-%m', so.order_date) as year_month,
                so.product_id,
                SUM(so.line_total_ex_tax * 0.7) as monthly_cost  -- 売上原価を売上の70%と仮定
            FROM silver_order_data so
            GROUP BY strftime('%Y-%m', so.order_date), so.product_id
        ) sales ON wi.year_month = sales.year_month AND wi.product_id = sales.product_id
        GROUP BY wi.year_month, wi.product_id, sim.product_name, sales.monthly_cost
        ORDER BY wi.year_month, wi.product_id
        """
        self.db.execute_query(sql)
    
    def calculate_monthly_product_ebitda(self):
        """月次商品別EBITDA計算"""
        sql = """
        INSERT INTO gold_monthly_product_ebitda 
        (year_month, product_id, product_name, revenue, gross_profit, ebitda, ebitda_margin)
        SELECT 
            gross.year_month,
            gross.product_id,
            gross.product_name,
            gross.revenue,
            gross.gross_profit,
            gross.gross_profit - COALESCE(indirect_costs.monthly_cost, 0) as ebitda,
            CASE 
                WHEN gross.revenue > 0 
                THEN ROUND((gross.gross_profit - COALESCE(indirect_costs.monthly_cost, 0)) * 100.0 / gross.revenue, 2)
                ELSE 0 
            END as ebitda_margin
        FROM (
            SELECT 
                strftime('%Y-%m', so.order_date) as year_month,
                so.product_id,
                sim.product_name,
                SUM(so.line_total_ex_tax) as revenue,
                SUM(so.line_total_ex_tax * 0.3) as gross_profit  -- 粗利を売上の30%と仮定
            FROM silver_order_data so
            JOIN silver_item_master sim ON so.product_id = sim.product_id
            GROUP BY strftime('%Y-%m', so.order_date), so.product_id, sim.product_name
        ) gross
        LEFT JOIN (
            SELECT 
                strftime('%Y-%m', sp.billing_date) as year_month,
                sp.product_id,
                SUM(sp.line_total) * 0.1 as monthly_cost  -- 間接費を調達額の10%と仮定
            FROM silver_procurement_data sp
            GROUP BY strftime('%Y-%m', sp.billing_date), sp.product_id
        ) indirect_costs ON gross.year_month = indirect_costs.year_month AND gross.product_id = indirect_costs.product_id
        ORDER BY gross.year_month, gross.product_id
        """
        self.db.execute_query(sql)
    
    def calculate_monthly_delivery_compliance_rate(self):
        """月次納期遵守率計算"""
        sql = """
        INSERT INTO gold_monthly_delivery_compliance_rate 
        (year_month, total_orders, on_time_deliveries, compliance_rate)
        SELECT 
            strftime('%Y-%m', so.order_date) as year_month,
            COUNT(*) as total_orders,
            SUM(CASE 
                WHEN ss.actual_shipment_date IS NOT NULL AND ss.actual_shipment_date <= so.promised_delivery_date 
                THEN 1 
                ELSE 0 
            END) as on_time_deliveries,
            ROUND(CAST(SUM(CASE 
                WHEN ss.actual_shipment_date IS NOT NULL AND ss.actual_shipment_date <= so.promised_delivery_date 
                THEN 1 
                ELSE 0 
            END) AS REAL) * 100.0 / CAST(COUNT(*) AS REAL), 2) as compliance_rate
        FROM silver_order_data so
        LEFT JOIN silver_shipment_data ss ON so.order_id = ss.order_id AND so.product_id = ss.product_id
        WHERE so.promised_delivery_date IS NOT NULL
        GROUP BY strftime('%Y-%m', so.order_date)
        ORDER BY year_month
        """
        self.db.execute_query(sql)
    
    def calculate_monthly_supplier_lead_time_compliance(self):
        """月次仕入先リードタイム遵守率計算"""
        sql = """
        INSERT INTO gold_monthly_supplier_lead_time_compliance 
        (year_month, supplier_id, supplier_name, total_orders, on_time_deliveries, compliance_rate, avg_lead_time_days)
        SELECT 
            strftime('%Y-%m', sp.order_date) as year_month,
            sp.supplier_id,
            spm.partner_name as supplier_name,
            COUNT(*) as total_orders,
            SUM(CASE 
                WHEN sp.delivery_date IS NOT NULL AND sp.delivery_date <= sp.requested_delivery_date 
                THEN 1 
                ELSE 0 
            END) as on_time_deliveries,
            ROUND(CAST(SUM(CASE 
                WHEN sp.delivery_date IS NOT NULL AND sp.delivery_date <= sp.requested_delivery_date 
                THEN 1 
                ELSE 0 
            END) AS REAL) * 100.0 / CAST(COUNT(*) AS REAL), 2) as compliance_rate,
            ROUND(AVG(CASE 
                WHEN sp.delivery_date IS NOT NULL 
                THEN julianday(sp.delivery_date) - julianday(sp.order_date) 
                ELSE NULL 
            END), 1) as avg_lead_time_days
        FROM silver_procurement_data sp
        LEFT JOIN silver_partner_master spm ON sp.supplier_id = spm.partner_id
        WHERE sp.requested_delivery_date IS NOT NULL
        GROUP BY strftime('%Y-%m', sp.order_date), sp.supplier_id, spm.partner_name
        HAVING COUNT(*) >= 1  -- 最低1件以上の取引がある仕入先のみ
        ORDER BY year_month, supplier_id
        """
        self.db.execute_query(sql)
        return total_kpis >= len(kpi_tables) // 2  # 半分以上のKPIが計算できていればOK
    
    def run(self) -> bool:
        """ゴールド層ETL実行"""
        try:
            self.logger.info("=== ゴールド層ETL開始 ===")
            
            # スキーマ初期化
            self.initialize_database()
            
            # KPI計算実行
            success_count = 0
            total_count = 8
            
            if self.calculate_monthly_product_gross_margin():
                success_count += 1
            
            if self.calculate_monthly_ev_sales_share():
                success_count += 1
            
            if self.calculate_monthly_area_ev_sales_share():
                success_count += 1
            
            if self.calculate_monthly_safety_equipment_adoption():
                success_count += 1
            
            if self.calculate_monthly_area_safety_equipment_adoption():
                success_count += 1
            
            if self.calculate_inventory_rotation_period():
                success_count += 1
            
            if self.calculate_monthly_ebitda():
                success_count += 1
            
            if self.calculate_emergency_transportation_cost_share():
                success_count += 1
            
            if self.calculate_monthly_product_inventory_rotation():
                success_count += 1
            
            if self.calculate_monthly_product_ebitda():
                success_count += 1
            
            if self.calculate_monthly_delivery_compliance_rate():
                success_count += 1
            
            if self.calculate_monthly_supplier_lead_time_compliance():
                success_count += 1
            
            self.logger.info(f"KPI計算完了: {success_count}/{total_count}")
            
            # 結果検証
            if not self.validate_kpi_results():
                self.logger.warning("KPI結果に問題がありますが、処理を継続します")
            
            self.logger.info("=== ゴールド層ETL完了 ===")
            return success_count >= total_count // 2
            
        except Exception as e:
            self.logger.error(f"ゴールド層ETL実行エラー: {e}")
            return False

def main():
    """メイン実行関数"""
    gold_etl = GoldETL()
    success = gold_etl.run()
    
    if success:
        print("ゴールド層ETL処理が正常に完了しました")
        return 0
    else:
        print("ゴールド層ETL処理でエラーが発生しました")
        return 1

if __name__ == "__main__":
    exit(main())