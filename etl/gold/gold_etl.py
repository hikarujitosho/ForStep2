#!/usr/bin/env python3
"""
ゴールド層ETL処理（修正版）
KPI計算とレポート用データマート作成
"""

import os
import sys
import pandas as pd
from typing import Dict, Any, Optional

# プロジェクトルートを追加
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from etl.common.database import DatabaseManager
from etl.common.utils import setup_logging

class GoldETL:
    """ゴールド層ETL処理"""
    
    def __init__(self):
        """初期化"""
        self.logger = setup_logging('gold_etl')
        self.db_manager = DatabaseManager()
    
    def run(self) -> bool:
        """ゴールド層ETL実行"""
        try:
            self.logger.info("=== ゴールド層ETL開始 ===")
            
            # スキーマ初期化
            if not self.initialize_schema():
                return False
            
            # KPI計算実行
            if not self.calculate_all_kpis():
                return False
            
            # 結果検証
            self.validate_kpi_results()
            
            self.logger.info("=== ゴールド層ETL完了 ===")
            return True
            
        except Exception as e:
            self.logger.error(f"ゴールド層ETL実行エラー: {e}")
            return False
    
    def initialize_schema(self) -> bool:
        """ゴールド層スキーマ初期化"""
        try:
            self.logger.info("ゴールド層スキーマを初期化中...")
            
            schema_file = os.path.join(
                os.path.dirname(__file__), '..', '..', 
                'database', 'schema_gold.sql'
            )
            
            if os.path.exists(schema_file):
                with open(schema_file, 'r', encoding='utf-8') as f:
                    schema_content = f.read()
                
                # SQLファイルを個別のステートメントに分割
                statements = []
                current_statement = ""
                
                for line in schema_content.split('\n'):
                    line = line.strip()
                    if line.startswith('--') or not line:
                        continue
                    
                    current_statement += line + '\n'
                    
                    if line.endswith(';'):
                        statements.append(current_statement.strip())
                        current_statement = ""
                
                # 各ステートメントを個別に実行
                for statement in statements:
                    if statement.strip():
                        self.db_manager.execute_sql(statement)
                        
            else:
                self.logger.warning(f"スキーマファイルが見つかりません: {schema_file}")
            
            self.logger.info("ゴールド層スキーマ初期化完了")
            return True
            
        except Exception as e:
            self.logger.error(f"スキーマ初期化エラー: {e}")
            return False
    
    def calculate_all_kpis(self) -> bool:
        """全KPI計算実行"""
        try:
            self.logger.info("KPI計算開始...")
            
            success_count = 0
            total_count = 12
            
            # 既存の8つのKPI
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
            
            # 新しい4つのKPI
            if self.calculate_monthly_product_inventory_rotation():
                success_count += 1
            
            if self.calculate_monthly_product_ebitda():
                success_count += 1
            
            if self.calculate_monthly_delivery_compliance_rate():
                success_count += 1
            
            if self.calculate_monthly_supplier_lead_time_compliance():
                success_count += 1
            
            self.logger.info(f"KPI計算完了: {success_count}/{total_count}")
            return success_count >= total_count // 2
            
        except Exception as e:
            self.logger.error(f"KPI計算エラー: {e}")
            return False
    
    def calculate_monthly_product_gross_margin(self) -> bool:
        """月次商品別粗利率の計算"""
        try:
            self.logger.info("月次商品別粗利率計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_product_gross_margin 
            (year_month, product_id, product_name, revenue, cost, gross_profit, gross_margin)
            WITH monthly_product_sales AS (
                SELECT 
                    o.year_month,
                    o.product_id,
                    i.product_name,
                    SUM(o.line_total_ex_tax) as revenue,
                    COALESCE(SUM(p.line_total_ex_tax), SUM(o.line_total_ex_tax * 0.7)) as total_procurement_cost
                FROM silver_order_data o
                JOIN silver_item_master i ON o.product_id = i.product_id
                LEFT JOIN silver_procurement_data p ON o.product_id = p.product_id 
                    AND o.year_month = p.year_month
                WHERE o.line_total_ex_tax IS NOT NULL
                GROUP BY o.year_month, o.product_id, i.product_name
            )
            SELECT 
                year_month,
                product_id,
                product_name,
                revenue,
                total_procurement_cost as cost,
                revenue - total_procurement_cost as gross_profit,
                CASE 
                    WHEN revenue > 0 THEN 
                        ROUND((revenue - total_procurement_cost) * 100.0 / revenue, 2)
                    ELSE NULL
                END as gross_margin
            FROM monthly_product_sales
            WHERE revenue > 0
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
                        ROUND(CAST(ev_revenue AS REAL) * 100.0 / CAST(total_revenue AS REAL), 2)
                    ELSE 0
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
            (year_month, location_id, location_name, total_revenue, ev_revenue, ev_sales_share)
            WITH area_sales AS (
                SELECT 
                    o.year_month,
                    o.location_id,
                    l.location_name,
                    SUM(o.line_total_ex_tax) as total_revenue,
                    SUM(CASE WHEN i.is_ev = 1 THEN o.line_total_ex_tax ELSE 0 END) as ev_revenue
                FROM silver_order_data o
                JOIN silver_item_master i ON o.product_id = i.product_id
                JOIN silver_location_master l ON o.location_id = l.location_id
                WHERE o.line_total_ex_tax IS NOT NULL
                GROUP BY o.year_month, o.location_id, l.location_name
            )
            SELECT 
                year_month,
                location_id,
                location_name,
                total_revenue,
                ev_revenue,
                CASE 
                    WHEN total_revenue > 0 THEN 
                        ROUND(CAST(ev_revenue AS REAL) * 100.0 / CAST(total_revenue AS REAL), 2)
                    ELSE 0
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
                        ROUND(CAST(safety_equipped_revenue AS REAL) * 100.0 / CAST(total_revenue AS REAL), 2)
                    ELSE 0
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
            (year_month, location_id, location_name, total_revenue, safety_equipped_revenue, safety_equipment_adoption_rate)
            WITH area_sales AS (
                SELECT 
                    o.year_month,
                    o.location_id,
                    l.location_name,
                    SUM(o.line_total_ex_tax) as total_revenue,
                    SUM(CASE WHEN i.is_safety_equipped = 1 THEN o.line_total_ex_tax ELSE 0 END) as safety_equipped_revenue
                FROM silver_order_data o
                JOIN silver_item_master i ON o.product_id = i.product_id
                JOIN silver_location_master l ON o.location_id = l.location_id
                WHERE o.line_total_ex_tax IS NOT NULL
                GROUP BY o.year_month, o.location_id, l.location_name
            )
            SELECT 
                year_month,
                location_id,
                location_name,
                total_revenue,
                safety_equipped_revenue,
                CASE 
                    WHEN total_revenue > 0 THEN 
                        ROUND(CAST(safety_equipped_revenue AS REAL) * 100.0 / CAST(total_revenue AS REAL), 2)
                    ELSE 0
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
        """棚卸資産回転期間の計算 - 期末在庫金額 / 売上原価"""
        try:
            self.logger.info("棚卸資産回転期間計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_inventory_rotation_period 
            (year_month, avg_inventory_value, monthly_cost_of_sales, rotation_period_days)
            WITH inventory_summary AS (
                -- 期末在庫金額（月次在庫履歴から月末時点の在庫価値）
                SELECT 
                    inv.year_month,
                    SUM(inv.total_value) as total_inventory_value,
                    COUNT(DISTINCT inv.product_id) as product_count
                FROM silver_inventory_data inv
                GROUP BY inv.year_month
            ),
            actual_cost_of_sales AS (
                -- 実際の売上原価（調達伝票から直接材料費を集計）
                SELECT 
                    p.year_month,
                    SUM(p.line_total_ex_tax) as direct_material_cost
                FROM silver_procurement_data p
                WHERE p.material_type = 'direct'
                    AND p.line_total_ex_tax IS NOT NULL
                GROUP BY p.year_month
            ),
            monthly_sales AS (
                -- 月次売上
                SELECT 
                    o.year_month,
                    SUM(o.line_total_ex_tax) as total_sales
                FROM silver_order_data o
                WHERE o.line_total_ex_tax IS NOT NULL
                GROUP BY o.year_month
            ),
            cost_calculation AS (
                -- 売上原価の計算（実際の直接材料費 + 間接費配賦）
                SELECT 
                    ms.year_month,
                    ms.total_sales,
                    COALESCE(acs.direct_material_cost, 0) as direct_cost,
                    -- 間接材費を売上比例で配賦
                    COALESCE(acs.direct_material_cost, 0) + 
                    CASE 
                        WHEN ms.total_sales > 0 AND acs.direct_material_cost > 0
                        THEN acs.direct_material_cost * 0.2  -- 直接材に対して20%の間接費
                        ELSE ms.total_sales * 0.7  -- フォールバック（従来の固定比率）
                    END as total_cost_of_sales
                FROM monthly_sales ms
                LEFT JOIN actual_cost_of_sales acs ON ms.year_month = acs.year_month
            )
            SELECT 
                inv.year_month,
                inv.total_inventory_value as avg_inventory_value,
                COALESCE(cost.total_cost_of_sales, 0) as monthly_cost_of_sales,
                CASE 
                    WHEN COALESCE(cost.total_cost_of_sales, 0) > 0 
                    THEN ROUND(inv.total_inventory_value * 30.0 / cost.total_cost_of_sales, 1)
                    ELSE NULL 
                END as rotation_period_days
            FROM inventory_summary inv
            LEFT JOIN cost_calculation cost ON inv.year_month = cost.year_month
            WHERE inv.total_inventory_value > 0
            ORDER BY inv.year_month
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
            (year_month, revenue, gross_profit, operating_expenses, ebitda, ebitda_margin)
            WITH monthly_financials AS (
                SELECT 
                    o.year_month,
                    SUM(o.line_total_ex_tax) as revenue,
                    SUM(o.line_total_ex_tax) - COALESCE(SUM(p.line_total_ex_tax), SUM(o.line_total_ex_tax * 0.7)) as gross_profit,
                    COALESCE(hr.total_payroll, 0) + COALESCE(tc.total_transport_cost, 0) as operating_expenses
                FROM silver_order_data o
                LEFT JOIN silver_procurement_data p ON o.product_id = p.product_id AND o.year_month = p.year_month
                LEFT JOIN (
                    SELECT year_month, SUM(total_compensation) as total_payroll 
                    FROM silver_payroll_data 
                    GROUP BY year_month
                ) hr ON o.year_month = hr.year_month
                LEFT JOIN (
                    SELECT year_month, SUM(cost_amount) as total_transport_cost 
                    FROM silver_transportation_cost 
                    GROUP BY year_month
                ) tc ON o.year_month = tc.year_month
                GROUP BY o.year_month, hr.total_payroll, tc.total_transport_cost
            )
            SELECT 
                year_month,
                revenue,
                gross_profit,
                operating_expenses,
                gross_profit - operating_expenses as ebitda,
                CASE 
                    WHEN revenue > 0 THEN 
                        ROUND((gross_profit - operating_expenses) * 100.0 / revenue, 2)
                    ELSE 0
                END as ebitda_margin
            FROM monthly_financials
            WHERE revenue > 0
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
            (year_month, total_cost, emergency_cost, emergency_cost_share)
            WITH transport_summary AS (
                SELECT 
                    year_month,
                    SUM(cost_amount) as total_cost,
                    SUM(CASE WHEN is_emergency = 1 THEN cost_amount ELSE 0 END) as emergency_cost
                FROM silver_transportation_cost
                GROUP BY year_month
            )
            SELECT 
                year_month,
                total_cost,
                emergency_cost,
                CASE 
                    WHEN total_cost > 0 THEN 
                        ROUND(CAST(emergency_cost AS REAL) * 100.0 / CAST(total_cost AS REAL), 2)
                    ELSE 0
                END as emergency_cost_share
            FROM transport_summary
            WHERE total_cost > 0
            ORDER BY year_month
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_emergency_transportation_cost_share")
            self.logger.info(f"緊急輸送費率計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"緊急輸送費率計算エラー: {e}")
            return False
    
    def calculate_monthly_product_inventory_rotation(self) -> bool:
        """月次商品別棚卸資産回転期間計算 - 商品別期末在庫金額 / 商品別売上原価"""
        try:
            self.logger.info("月次商品別棚卸資産回転期間計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_product_inventory_rotation 
            (year_month, product_id, product_name, avg_inventory_value, monthly_cost_of_sales, rotation_period_days)
            WITH inventory_data AS (
                -- 商品別期末在庫金額
                SELECT 
                    inv.year_month,
                    inv.product_id,
                    i.product_name,
                    AVG(inv.total_value) as avg_inventory_value
                FROM silver_inventory_data inv
                JOIN silver_item_master i ON inv.product_id = i.product_id
                WHERE inv.total_value IS NOT NULL
                GROUP BY inv.year_month, inv.product_id, i.product_name
            ),
            product_procurement_cost AS (
                -- 商品別直接材料費
                SELECT 
                    p.year_month,
                    p.product_id,
                    SUM(p.line_total_ex_tax) as direct_material_cost
                FROM silver_procurement_data p
                WHERE p.material_type = 'direct'
                    AND p.line_total_ex_tax IS NOT NULL
                GROUP BY p.year_month, p.product_id
            ),
            product_sales AS (
                -- 商品別売上
                SELECT 
                    o.year_month,
                    o.product_id,
                    SUM(o.line_total_ex_tax) as total_sales
                FROM silver_order_data o
                WHERE o.line_total_ex_tax IS NOT NULL
                GROUP BY o.year_month, o.product_id
            ),
            product_cost_calculation AS (
                -- 商品別売上原価の計算
                SELECT 
                    ps.year_month,
                    ps.product_id,
                    ps.total_sales,
                    COALESCE(ppc.direct_material_cost, 0) as direct_cost,
                    -- 商品別売上原価 = 直接材料費 + 間接費配賦
                    CASE 
                        WHEN ppc.direct_material_cost > 0
                        THEN ppc.direct_material_cost * 1.2  -- 直接材に20%の間接費を上乗せ
                        ELSE ps.total_sales * 0.7  -- フォールバック（従来の固定比率）
                    END as monthly_cost_of_sales
                FROM product_sales ps
                LEFT JOIN product_procurement_cost ppc 
                    ON ps.year_month = ppc.year_month AND ps.product_id = ppc.product_id
            )
            SELECT 
                inv.year_month,
                inv.product_id,
                inv.product_name,
                inv.avg_inventory_value,
                COALESCE(cost.monthly_cost_of_sales, 0) as monthly_cost_of_sales,
                CASE 
                    WHEN COALESCE(cost.monthly_cost_of_sales, 0) > 0 AND inv.avg_inventory_value > 0
                    THEN ROUND(inv.avg_inventory_value * 30.0 / cost.monthly_cost_of_sales, 1)
                    ELSE NULL 
                END as rotation_period_days
            FROM inventory_data inv
            LEFT JOIN product_cost_calculation cost 
                ON inv.year_month = cost.year_month AND inv.product_id = cost.product_id
            WHERE inv.avg_inventory_value > 0
            ORDER BY inv.year_month, inv.product_id
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_product_inventory_rotation")
            self.logger.info(f"月次商品別棚卸資産回転期間計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次商品別棚卸資産回転期間計算エラー: {e}")
            return False
    
    def calculate_monthly_product_ebitda(self) -> bool:
        """完成品別月次EBITDA計算 - 提示されたロジックに準拠（売上原価=直接材のみ、販管費=間接材+給与）"""
        try:
            self.logger.info("完成品別月次EBITDA計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_product_ebitda 
            (year_month, product_id, product_name, revenue, gross_profit, ebitda, ebitda_margin)
            WITH product_sales AS (
                -- 完成品別売上の計算（受注伝票から）
                SELECT 
                    o.year_month,
                    o.product_id,
                    i.product_name,
                    SUM(o.line_total_ex_tax) as revenue
                FROM silver_order_data o
                JOIN silver_item_master i ON o.product_id = i.product_id
                WHERE o.line_total_ex_tax IS NOT NULL
                GROUP BY o.year_month, o.product_id, i.product_name
            ),
            direct_material_cost AS (
                -- 完成品別直接材料費（売上原価）
                SELECT 
                    p.year_month,
                    p.product_id,
                    SUM(p.line_total_ex_tax) as direct_cost
                FROM silver_procurement_data p
                WHERE p.material_type = 'direct'
                    AND p.product_id IS NOT NULL
                    AND p.line_total_ex_tax IS NOT NULL
                GROUP BY p.year_month, p.product_id
            ),
            monthly_totals AS (
                -- 月次合計の計算（販管費配賦用）
                SELECT 
                    s.year_month,
                    SUM(s.revenue) as total_monthly_revenue,
                    -- 間接材費の月次合計（販管費）
                    COALESCE(SUM(indirect.indirect_cost), 0) as total_indirect_cost,
                    -- 人件費の月次合計（販管費）
                    COALESCE(SUM(payroll.total_payroll), 0) as total_payroll_cost
                FROM product_sales s
                LEFT JOIN (
                    SELECT 
                        year_month,
                        SUM(line_total_ex_tax) as indirect_cost
                    FROM silver_procurement_data
                    WHERE material_type = 'indirect'
                        AND line_total_ex_tax IS NOT NULL
                    GROUP BY year_month
                ) indirect ON s.year_month = indirect.year_month
                LEFT JOIN (
                    SELECT 
                        year_month,
                        SUM(total_compensation) as total_payroll
                    FROM silver_payroll_data
                    WHERE total_compensation IS NOT NULL
                    GROUP BY year_month
                ) payroll ON s.year_month = payroll.year_month
                GROUP BY s.year_month, indirect.indirect_cost, payroll.total_payroll
            ),
            allocated_costs AS (
                -- 完成品別コスト配賦計算
                SELECT 
                    ps.year_month,
                    ps.product_id,
                    ps.product_name,
                    ps.revenue,
                    -- 売上原価 = 直接材料費のみ
                    COALESCE(dmc.direct_cost, 0) as cogs,
                    -- 販管費 = 間接材費（配賦）+ 人件費（配賦）
                    CASE 
                        WHEN mt.total_monthly_revenue > 0 
                        THEN ROUND(mt.total_indirect_cost * ps.revenue / mt.total_monthly_revenue, 2)
                        ELSE 0
                    END as allocated_indirect_cost,
                    CASE 
                        WHEN mt.total_monthly_revenue > 0 
                        THEN ROUND(mt.total_payroll_cost * ps.revenue / mt.total_monthly_revenue, 2)
                        ELSE 0
                    END as allocated_payroll_cost
                FROM product_sales ps
                LEFT JOIN direct_material_cost dmc 
                    ON ps.year_month = dmc.year_month 
                    AND ps.product_id = dmc.product_id
                JOIN monthly_totals mt ON ps.year_month = mt.year_month
            ),
            ebitda_calculation AS (
                SELECT 
                    ac.*,
                    -- 粗利益 = 売上 - 売上原価（直接材のみ）
                    ac.revenue - ac.cogs as gross_profit,
                    -- 販売管理費 = 間接材（配賦）+ 人件費（配賦）
                    ac.allocated_indirect_cost + ac.allocated_payroll_cost as total_sg_expense,
                    -- EBITDA = 売上 - 売上原価 - 販売管理費
                    ac.revenue - ac.cogs - (ac.allocated_indirect_cost + ac.allocated_payroll_cost) as ebitda
                FROM allocated_costs ac
            )
            SELECT 
                ec.year_month,
                ec.product_id,
                ec.product_name,
                ec.revenue,
                ec.gross_profit,
                ec.ebitda,
                -- EBITDAマージンの計算
                CASE 
                    WHEN ec.revenue > 0 
                    THEN ROUND(ec.ebitda * 100.0 / ec.revenue, 2)
                    ELSE 0 
                END as ebitda_margin
            FROM ebitda_calculation ec
            WHERE ec.revenue > 0
            ORDER BY ec.year_month, ec.product_id
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_product_ebitda")
            self.logger.info(f"完成品別月次EBITDA計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"完成品別月次EBITDA計算エラー: {e}")
            return False
    
    def calculate_monthly_delivery_compliance_rate(self) -> bool:
        """月次納期遵守率計算"""
        try:
            self.logger.info("月次納期遵守率計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_delivery_compliance_rate 
            (year_month, total_orders, on_time_deliveries, compliance_rate)
            WITH delivery_analysis AS (
                SELECT 
                    o.year_month,
                    COUNT(*) as total_orders,
                    SUM(CASE 
                        WHEN s.shipment_date IS NOT NULL 
                        AND s.shipment_date <= o.promised_delivery_date 
                        THEN 1 
                        ELSE 0 
                    END) as on_time_deliveries
                FROM silver_order_data o
                LEFT JOIN silver_shipment_data s ON o.order_id = s.order_id 
                WHERE o.promised_delivery_date IS NOT NULL
                GROUP BY o.year_month
            )
            SELECT 
                year_month,
                total_orders,
                on_time_deliveries,
                CASE 
                    WHEN total_orders > 0 
                    THEN ROUND(CAST(on_time_deliveries AS REAL) * 100.0 / CAST(total_orders AS REAL), 2)
                    ELSE 0 
                END as compliance_rate
            FROM delivery_analysis
            ORDER BY year_month
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_delivery_compliance_rate")
            self.logger.info(f"月次納期遵守率計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次納期遵守率計算エラー: {e}")
            return False
    
    def calculate_monthly_supplier_lead_time_compliance(self) -> bool:
        """月次仕入先リードタイム遵守率計算"""
        try:
            self.logger.info("月次仕入先リードタイム遵守率計算開始...")
            
            sql = """
            INSERT OR REPLACE INTO gold_monthly_supplier_lead_time_compliance 
            (year_month, supplier_id, supplier_name, total_orders, on_time_deliveries, compliance_rate, avg_lead_time_days)
            WITH supplier_performance AS (
                SELECT 
                    p.year_month,
                    p.supplier_id,
                    pm.partner_name as supplier_name,
                    COUNT(*) as total_orders,
                    SUM(CASE 
                        WHEN s.shipment_date IS NOT NULL 
                        AND s.shipment_date <= p.expected_delivery_date 
                        THEN 1 
                        ELSE 0 
                    END) as on_time_deliveries,
                    AVG(CASE 
                        WHEN s.shipment_date IS NOT NULL 
                        THEN julianday(s.shipment_date) - julianday(p.order_date) 
                        ELSE NULL 
                    END) as avg_lead_time_days
                FROM silver_procurement_data p
                LEFT JOIN silver_partner_master pm ON p.supplier_id = pm.partner_id
                LEFT JOIN silver_shipment_data s ON p.product_id = s.product_id AND p.year_month = s.year_month
                WHERE p.expected_delivery_date IS NOT NULL
                GROUP BY p.year_month, p.supplier_id, pm.partner_name
                HAVING COUNT(*) >= 1
            )
            SELECT 
                year_month,
                supplier_id,
                supplier_name,
                total_orders,
                on_time_deliveries,
                CASE 
                    WHEN total_orders > 0 
                    THEN ROUND(CAST(on_time_deliveries AS REAL) * 100.0 / CAST(total_orders AS REAL), 2)
                    ELSE 0 
                END as compliance_rate,
                ROUND(avg_lead_time_days, 1) as avg_lead_time_days
            FROM supplier_performance
            ORDER BY year_month, supplier_id
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("gold_monthly_supplier_lead_time_compliance")
            self.logger.info(f"月次仕入先リードタイム遵守率計算完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"月次仕入先リードタイム遵守率計算エラー: {e}")
            return False
    
    def validate_kpi_results(self) -> bool:
        """KPI結果の検証"""
        self.logger.info("KPI結果検証開始...")
        
        # 全KPIテーブル
        kpi_tables = [
            "gold_monthly_product_gross_margin",
            "gold_monthly_ev_sales_share",
            "gold_monthly_area_ev_sales_share",
            "gold_monthly_safety_equipment_adoption",
            "gold_monthly_area_safety_equipment_adoption",
            "gold_inventory_rotation_period",
            "gold_monthly_ebitda",
            "gold_emergency_transportation_cost_share",
            "gold_monthly_product_inventory_rotation",
            "gold_monthly_product_ebitda",
            "gold_monthly_delivery_compliance_rate",
            "gold_monthly_supplier_lead_time_compliance"
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
        
        total_kpis = sum(1 for count in results.values() if count > 0)
        self.logger.info(f"計算完了KPI数: {total_kpis}/{len(kpi_tables)}")
        
        # サンプル値の表示
        if results.get("gold_monthly_ev_sales_share", 0) > 0:
            sample = self.db_manager.query_to_dataframe(
                "SELECT * FROM gold_monthly_ev_sales_share LIMIT 3"
            )
            self.logger.info(f"EV販売率サンプル:\n{sample.to_string()}")
        
        self.logger.info("KPI結果検証完了")
        return True

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