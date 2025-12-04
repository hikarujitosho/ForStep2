"""
シルバー層ETL処理
ブロンズ層データのクレンジング・正規化・統合処理
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from etl.common import DatabaseManager, setup_logging, validate_dataframe, clean_dataframe
from etl.common.utils import convert_to_date, calculate_year_month, safe_divide

class SilverETL:
    """シルバー層ETL処理クラス"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.logger = setup_logging("silver_etl")
        
    def initialize_database(self):
        """シルバー層スキーマ初期化"""
        self.logger.info("シルバー層スキーマを初期化中...")
        
        # 個別にテーブル定義を実行
        table_sqls = [
            """
            CREATE TABLE IF NOT EXISTS silver_item_master (
                product_id VARCHAR PRIMARY KEY,
                product_name VARCHAR,
                brand_name VARCHAR,
                item_group VARCHAR,
                item_hierarchy VARCHAR,
                detail_category VARCHAR,
                transport_group VARCHAR,
                import_export_group VARCHAR,
                country_of_origin VARCHAR,
                is_ev BOOLEAN,
                is_safety_equipped BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_partner_master (
                partner_id VARCHAR PRIMARY KEY,
                partner_name VARCHAR,
                partner_type VARCHAR,
                partner_category VARCHAR,
                address VARCHAR,
                city VARCHAR,
                state_province VARCHAR,
                postal_code VARCHAR,
                country VARCHAR,
                contact_person VARCHAR,
                contact_email VARCHAR,
                contact_phone VARCHAR,
                payment_terms VARCHAR,
                currency VARCHAR,
                is_active BOOLEAN,
                region VARCHAR,
                valid_from DATE,
                valid_to DATE,
                source_system VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_location_master (
                location_id VARCHAR PRIMARY KEY,
                location_name VARCHAR,
                location_type VARCHAR,
                address VARCHAR,
                city VARCHAR,
                state_province VARCHAR,
                postal_code VARCHAR,
                country VARCHAR,
                contact_person VARCHAR,
                contact_phone VARCHAR,
                contact_email VARCHAR,
                is_active BOOLEAN,
                valid_from DATE,
                valid_to DATE,
                source_system VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_order_data (
                order_id VARCHAR,
                line_number VARCHAR,
                order_date DATE,
                product_id VARCHAR,
                product_name VARCHAR,
                customer_id VARCHAR,
                customer_name VARCHAR,
                location_id VARCHAR,
                quantity DECIMAL,
                unit_price_ex_tax DECIMAL,
                line_total_ex_tax DECIMAL,
                promised_delivery_date DATE,
                year_month VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (order_id, line_number)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_procurement_data (
                purchase_order_id VARCHAR,
                line_number VARCHAR,
                order_date DATE,
                product_id VARCHAR,
                product_name VARCHAR,
                supplier_id VARCHAR,
                supplier_name VARCHAR,
                location_id VARCHAR,
                account_group VARCHAR,
                quantity DECIMAL,
                unit_price_ex_tax DECIMAL,
                line_total_ex_tax DECIMAL,
                expected_delivery_date DATE,
                material_type VARCHAR,
                material_category VARCHAR,
                year_month VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (purchase_order_id, line_number)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_shipment_data (
                shipment_id VARCHAR,
                line_number VARCHAR,
                order_id VARCHAR,
                order_line_number VARCHAR,
                shipment_date DATE,
                product_id VARCHAR,
                product_name VARCHAR,
                customer_id VARCHAR,
                location_id VARCHAR,
                quantity_shipped DECIMAL,
                unit_price DECIMAL,
                line_total DECIMAL,
                year_month VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (shipment_id, line_number)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_transportation_cost (
                cost_id VARCHAR PRIMARY KEY,
                shipment_id VARCHAR,
                location_id VARCHAR,
                cost_type VARCHAR,
                cost_amount DECIMAL,
                currency VARCHAR,
                billing_date DATE,
                year_month VARCHAR,
                is_emergency BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_payroll_data (
                payroll_id VARCHAR PRIMARY KEY,
                employee_id VARCHAR,
                employee_name VARCHAR,
                department VARCHAR,
                position VARCHAR,
                payment_period VARCHAR,
                base_salary DECIMAL,
                overtime_pay DECIMAL,
                allowances DECIMAL,
                deductions DECIMAL,
                net_salary DECIMAL,
                payment_date DATE,
                currency VARCHAR,
                cost_center VARCHAR,
                year_month VARCHAR,
                total_compensation DECIMAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS silver_inventory_data (
                inventory_history_id VARCHAR PRIMARY KEY,
                location_id VARCHAR,
                product_id VARCHAR,
                product_name VARCHAR,
                year_month VARCHAR,
                opening_quantity DECIMAL,
                received_quantity DECIMAL,
                issued_quantity DECIMAL,
                closing_quantity DECIMAL,
                unit_cost DECIMAL,
                total_value DECIMAL,
                currency VARCHAR,
                inventory_category VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]
        
        for sql in table_sqls:
            self.db_manager.execute_sql(sql.strip())
        
        self.logger.info("シルバー層スキーマ初期化完了")
    
    def create_unified_item_master(self) -> bool:
        """統合品目マスタの作成"""
        try:
            self.logger.info("統合品目マスタ作成開始...")
            
            # ERP品目マスタを基準として統合
            sql = """
            INSERT OR REPLACE INTO silver_item_master 
            (product_id, product_name, brand_name, item_group, item_hierarchy, 
             detail_category, transport_group, import_export_group, country_of_origin,
             is_ev, is_safety_equipped)
            SELECT DISTINCT
                product_id,
                product_name,
                brand_name,
                item_group,
                item_hierarchy,
                detail_category,
                transport_group,
                import_export_group,
                country_of_origin,
                CASE WHEN item_hierarchy = 'EV' THEN 1 ELSE 0 END as is_ev,
                CASE WHEN detail_category = 'safety_equipped' THEN 1 ELSE 0 END as is_safety_equipped
            FROM bronze_erp_item_master
            WHERE product_id IS NOT NULL
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("silver_item_master")
            self.logger.info(f"統合品目マスタ作成完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"統合品目マスタ作成エラー: {e}")
            return False
    
    def create_unified_partner_master(self) -> bool:
        """統合取引先マスタの作成"""
        try:
            self.logger.info("統合取引先マスタ作成開始...")
            
            # 各システムの取引先マスタを統合
            systems = [
                ("bronze_erp_partner_master", "ERP"),
                ("bronze_p2p_partner_master", "P2P"),
                ("bronze_mes_partner_master", "MES"),
                ("bronze_tms_partner_master", "TMS")
            ]
            
            for table_name, system_name in systems:
                sql = f"""
                INSERT OR IGNORE INTO silver_partner_master 
                (partner_id, partner_name, partner_type, partner_category, 
                 address, city, state_province, postal_code, country,
                 contact_person, contact_email, contact_phone, payment_terms,
                 currency, is_active, region, valid_from, valid_to, source_system)
                SELECT 
                    partner_id,
                    partner_name,
                    partner_type,
                    partner_category,
                    address,
                    city,
                    state_province,
                    postal_code,
                    country,
                    contact_person,
                    contact_email,
                    contact_phone,
                    payment_terms,
                    currency,
                    CASE WHEN is_active = 'active' THEN 1 ELSE 0 END as is_active,
                    region,
                    valid_from,
                    valid_to,
                    '{system_name}' as source_system
                FROM {table_name}
                WHERE partner_id IS NOT NULL
                """
                
                self.db_manager.execute_sql(sql)
            
            row_count = self.db_manager.get_row_count("silver_partner_master")
            self.logger.info(f"統合取引先マスタ作成完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"統合取引先マスタ作成エラー: {e}")
            return False
    
    def create_unified_location_master(self) -> bool:
        """統合拠点マスタの作成"""
        try:
            self.logger.info("統合拠点マスタ作成開始...")
            
            # 各システムの拠点マスタを統合
            systems = [
                ("bronze_erp_location_master", "ERP"),
                ("bronze_mes_location_master", "MES"),
                ("bronze_tms_location_master", "TMS"),
                ("bronze_wms_location_master", "WMS")
            ]
            
            for table_name, system_name in systems:
                sql = f"""
                INSERT OR IGNORE INTO silver_location_master 
                (location_id, location_name, location_type, address, city,
                 state_province, postal_code, country, contact_person,
                 contact_phone, contact_email, is_active, valid_from, valid_to, source_system)
                SELECT 
                    location_id,
                    location_name,
                    location_type,
                    address,
                    city,
                    state_province,
                    postal_code,
                    country,
                    contact_person,
                    contact_phone,
                    contact_email,
                    CASE WHEN is_active = 'active' THEN 1 ELSE 0 END as is_active,
                    valid_from,
                    valid_to,
                    '{system_name}' as source_system
                FROM {table_name}
                WHERE location_id IS NOT NULL
                """
                
                self.db_manager.execute_sql(sql)
            
            row_count = self.db_manager.get_row_count("silver_location_master")
            self.logger.info(f"統合拠点マスタ作成完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"統合拠点マスタ作成エラー: {e}")
            return False
    
    def process_order_data(self) -> bool:
        """受注データの処理"""
        try:
            self.logger.info("受注データ処理開始...")
            
            sql = """
            INSERT OR REPLACE INTO silver_order_data 
            (order_id, line_number, order_date, product_id, product_name,
             customer_id, customer_name, location_id, quantity, unit_price_ex_tax,
             line_total_ex_tax, promised_delivery_date, year_month)
            SELECT 
                h.order_id,
                i.line_number,
                DATE(h.order_timestamp) as order_date,
                i.product_id,
                im.product_name,
                h.customer_id,
                pm.partner_name as customer_name,
                h.location_id,
                i.quantity,
                pc.selling_price_ex_tax as unit_price_ex_tax,
                (i.quantity * pc.selling_price_ex_tax) as line_total_ex_tax,
                DATE(i.promised_delivery_date) as promised_delivery_date,
                strftime('%Y-%m', h.order_timestamp) as year_month
            FROM bronze_erp_order_header h
            JOIN bronze_erp_order_item i ON h.order_id = i.order_id
            LEFT JOIN bronze_erp_item_master im ON i.product_id = im.product_id
            LEFT JOIN bronze_erp_partner_master pm ON h.customer_id = pm.partner_id
            LEFT JOIN bronze_erp_price_condition pc ON (
                i.product_id = pc.product_id 
                AND h.customer_id = pc.customer_id
                AND DATE(i.pricing_date) >= pc.valid_from 
                AND (DATE(i.pricing_date) <= pc.valid_to OR pc.valid_to IS NULL)
            )
            WHERE h.order_id IS NOT NULL AND i.line_number IS NOT NULL
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("silver_order_data")
            self.logger.info(f"受注データ処理完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"受注データ処理エラー: {e}")
            return False
    
    def process_procurement_data(self) -> bool:
        """調達データの処理"""
        try:
            self.logger.info("調達データ処理開始...")
            
            sql = """
            INSERT OR REPLACE INTO silver_procurement_data 
            (purchase_order_id, line_number, order_date, product_id, product_name,
             supplier_id, supplier_name, location_id, account_group, quantity,
             unit_price_ex_tax, line_total_ex_tax, expected_delivery_date, material_type,
             material_category, year_month)
            SELECT 
                h.purchase_order_id,
                i.line_number,
                h.order_date,
                COALESCE(i.product_id, i.material_id) as product_id,
                i.material_name as product_name,
                h.supplier_id,
                h.supplier_name,
                h.location_id,
                h.account_group,
                i.quantity,
                i.unit_price_ex_tax,
                i.line_subtotal_ex_tax as line_total_ex_tax,
                h.expected_delivery_date,
                i.material_type,
                i.material_category,
                strftime('%Y-%m', h.order_date) as year_month
            FROM bronze_p2p_purchase_header h
            JOIN bronze_p2p_purchase_item i ON h.purchase_order_id = i.purchase_order_id
            WHERE h.purchase_order_id IS NOT NULL AND i.line_number IS NOT NULL
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("silver_procurement_data")
            self.logger.info(f"調達データ処理完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"調達データ処理エラー: {e}")
            return False
    
    def process_shipment_data(self) -> bool:
        """出荷データの処理"""
        try:
            self.logger.info("出荷データ処理開始...")
            
            sql = """
            INSERT OR REPLACE INTO silver_shipment_data 
            (shipment_id, line_number, order_id, order_line_number, shipment_date,
             product_id, product_name, customer_id, location_id, quantity_shipped,
             unit_price, line_total, year_month)
            SELECT 
                h.shipment_id,
                i.line_number,
                i.order_id,
                i.line_number as order_line_number,
                DATE(h.shipment_timestamp) as shipment_date,
                i.product_id,
                im.product_name,
                h.customer_id,
                h.location_id,
                i.quantity as quantity_shipped,
                NULL as unit_price,
                NULL as line_total,
                strftime('%Y-%m', h.shipment_timestamp) as year_month
            FROM bronze_mes_shipment_header h
            JOIN bronze_mes_shipment_item i ON h.shipment_id = i.shipment_id
            LEFT JOIN silver_item_master im ON i.product_id = im.product_id
            WHERE h.shipment_id IS NOT NULL AND i.line_number IS NOT NULL
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("silver_shipment_data")
            self.logger.info(f"出荷データ処理完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"出荷データ処理エラー: {e}")
            return False
    
    def process_transportation_cost_data(self) -> bool:
        """輸送コストデータの処理"""
        try:
            self.logger.info("輸送コストデータ処理開始...")
            
            sql = """
            INSERT OR REPLACE INTO silver_transportation_cost 
            (cost_id, shipment_id, location_id, cost_type, cost_amount,
             currency, billing_date, year_month, is_emergency)
            SELECT 
                cost_id,
                shipment_id,
                location_id,
                cost_type,
                cost_amount,
                currency,
                billing_date,
                strftime('%Y-%m', billing_date) as year_month,
                CASE WHEN cost_type = 'expedite' THEN 1 ELSE 0 END as is_emergency
            FROM bronze_tms_transportation_cost
            WHERE cost_id IS NOT NULL
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("silver_transportation_cost")
            self.logger.info(f"輸送コストデータ処理完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"輸送コストデータ処理エラー: {e}")
            return False
    
    def process_payroll_data(self) -> bool:
        """給与データの処理"""
        try:
            self.logger.info("給与データ処理開始...")
            
            sql = """
            INSERT OR REPLACE INTO silver_payroll_data 
            (payroll_id, employee_id, employee_name, department, position,
             payment_period, base_salary, overtime_pay, allowances, deductions,
             net_salary, payment_date, currency, cost_center, year_month, total_compensation)
            SELECT 
                payroll_id,
                employee_id,
                employee_name,
                department,
                position,
                payment_period,
                base_salary,
                overtime_pay,
                allowances,
                deductions,
                net_salary,
                payment_date,
                currency,
                cost_center,
                payment_period as year_month,
                (base_salary + overtime_pay + allowances) as total_compensation
            FROM bronze_hr_payroll
            WHERE payroll_id IS NOT NULL
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("silver_payroll_data")
            self.logger.info(f"給与データ処理完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"給与データ処理エラー: {e}")
            return False
    
    def process_inventory_data(self) -> bool:
        """在庫データの処理"""
        try:
            self.logger.info("在庫データ処理開始...")
            
            sql = """
            INSERT OR REPLACE INTO silver_inventory_data 
            (inventory_history_id, location_id, product_id, product_name,
             year_month, opening_quantity, received_quantity, issued_quantity,
             closing_quantity, unit_cost, total_value, currency, inventory_category)
            SELECT 
                (product_id || '-' || location_id || '-' || year_month) as inventory_history_id,
                location_id,
                product_id,
                product_name,
                year_month,
                NULL as opening_quantity,
                NULL as received_quantity,
                NULL as issued_quantity,
                inventory_quantity as closing_quantity,
                NULL as unit_cost,
                NULL as total_value,
                'JPY' as currency,
                inventory_status as inventory_category
            FROM bronze_wms_monthly_inventory
            WHERE product_id IS NOT NULL AND location_id IS NOT NULL
            """
            
            self.db_manager.execute_sql(sql)
            row_count = self.db_manager.get_row_count("silver_inventory_data")
            self.logger.info(f"在庫データ処理完了: {row_count} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"在庫データ処理エラー: {e}")
            return False
    
    def validate_data_quality(self) -> bool:
        """データ品質チェック"""
        self.logger.info("シルバー層データ品質チェック開始...")
        
        tables = [
            "silver_item_master",
            "silver_partner_master", 
            "silver_location_master",
            "silver_order_data",
            "silver_procurement_data",
            "silver_shipment_data",
            "silver_transportation_cost",
            "silver_payroll_data",
            "silver_inventory_data"
        ]
        
        quality_issues = []
        
        for table in tables:
            if self.db_manager.table_exists(table):
                row_count = self.db_manager.get_row_count(table)
                if row_count == 0:
                    quality_issues.append(f"テーブル {table} にデータがありません")
                else:
                    self.logger.info(f"{table}: {row_count} rows")
            else:
                quality_issues.append(f"テーブル {table} が存在しません")
        
        # EV/安全装置判定の検証
        ev_count = self.db_manager.query_to_dataframe(
            "SELECT COUNT(*) as count FROM silver_item_master WHERE is_ev = 1"
        ).iloc[0]['count']
        
        safety_count = self.db_manager.query_to_dataframe(
            "SELECT COUNT(*) as count FROM silver_item_master WHERE is_safety_equipped = 1"
        ).iloc[0]['count']
        
        self.logger.info(f"EV車種数: {ev_count}")
        self.logger.info(f"安全装置搭載車種数: {safety_count}")
        
        if quality_issues:
            for issue in quality_issues:
                self.logger.warning(f"品質チェック警告: {issue}")
            return len(quality_issues) == 0
        
        self.logger.info("シルバー層データ品質チェック完了")
        return True
    
    def run(self) -> bool:
        """シルバー層ETL実行"""
        try:
            self.logger.info("=== シルバー層ETL開始 ===")
            
            # スキーマ初期化
            self.initialize_database()
            
            # マスタデータ統合
            if not self.create_unified_item_master():
                return False
            if not self.create_unified_partner_master():
                return False
            if not self.create_unified_location_master():
                return False
            
            # トランザクションデータ処理
            if not self.process_order_data():
                return False
            if not self.process_procurement_data():
                return False
            if not self.process_shipment_data():
                return False
            if not self.process_transportation_cost_data():
                return False
            if not self.process_payroll_data():
                return False
            if not self.process_inventory_data():
                return False
            
            # データ品質チェック
            if not self.validate_data_quality():
                self.logger.warning("データ品質に問題がありますが、処理を継続します")
            
            self.logger.info("=== シルバー層ETL完了 ===")
            return True
            
        except Exception as e:
            self.logger.error(f"シルバー層ETL実行エラー: {e}")
            return False

def main():
    """メイン実行関数"""
    silver_etl = SilverETL()
    success = silver_etl.run()
    
    if success:
        print("シルバー層ETL処理が正常に完了しました")
        return 0
    else:
        print("シルバー層ETL処理でエラーが発生しました")
        return 1

if __name__ == "__main__":
    exit(main())