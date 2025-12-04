"""
ブロンズ層ETL処理
CSVファイルをそのままSQLiteテーブルにロードする処理
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).parent.parent.parent))

from etl.common import DatabaseManager, setup_logging, validate_dataframe, clean_dataframe, BRONZE_DATA_PATH

class BronzeETL:
    """ブロンズ層ETL処理クラス"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.logger = setup_logging("bronze_etl")
        
        # CSVファイルとテーブル名のマッピング
        self.file_mappings = {
            # ERPシステム
            "ERP": {
                "受注伝票_header.csv": "bronze_erp_order_header",
                "受注伝票_item.csv": "bronze_erp_order_item",
                "品目マスタ.csv": "bronze_erp_item_master",
                "条件マスタ.csv": "bronze_erp_price_condition",
                "BOMマスタ.csv": "bronze_erp_bom_master",
                "取引先マスタ.csv": "bronze_erp_partner_master",
                "拠点マスタ.csv": "bronze_erp_location_master"
            },
            # P2Pシステム
            "P2P": {
                "調達伝票_header.csv": "bronze_p2p_purchase_header",
                "調達伝票_item.csv": "bronze_p2p_purchase_item",
                "BOMマスタ.csv": "bronze_p2p_bom_master",
                "取引先マスタ.csv": "bronze_p2p_partner_master"
            },
            # MESシステム
            "MES": {
                "出荷伝票_header.csv": "bronze_mes_shipment_header",
                "出荷伝票_item.csv": "bronze_mes_shipment_item",
                "取引先マスタ.csv": "bronze_mes_partner_master",
                "拠点マスタ.csv": "bronze_mes_location_master"
            },
            # TMSシステム
            "TMS": {
                "輸送コスト.csv": "bronze_tms_transportation_cost",
                "取引先マスタ.csv": "bronze_tms_partner_master",
                "拠点マスタ.csv": "bronze_tms_location_master"
            },
            # HRシステム
            "HR": {
                "給与テーブル.csv": "bronze_hr_payroll"
            },
            # WMSシステム
            "WMS": {
                "月次在庫履歴.csv": "bronze_wms_monthly_inventory",
                "現在在庫.csv": "bronze_wms_current_inventory",
                "拠点マスタ.csv": "bronze_wms_location_master"
            }
        }
    
    def initialize_database(self):
        """データベーススキーマを初期化"""
        self.logger.info("データベーススキーマを初期化中...")
        
        schema_files = [
            Path(__file__).parent.parent.parent / "database" / "schema_bronze_erp.sql",
            Path(__file__).parent.parent.parent / "database" / "schema_bronze_p2p.sql",
            Path(__file__).parent.parent.parent / "database" / "schema_bronze_mes.sql",
            Path(__file__).parent.parent.parent / "database" / "schema_bronze_others.sql"
        ]
        
        for schema_file in schema_files:
            if schema_file.exists():
                self.db_manager.execute_sql_file(schema_file)
            else:
                self.logger.warning(f"スキーマファイルが見つかりません: {schema_file}")
        
        self.logger.info("データベーススキーマ初期化完了")
    
    def load_system_data(self, system_name: str) -> bool:
        """指定システムの全CSVファイルをロード"""
        try:
            self.logger.info(f"{system_name}システムのデータロードを開始...")
            
            if system_name not in self.file_mappings:
                self.logger.error(f"未対応のシステム: {system_name}")
                return False
            
            system_path = BRONZE_DATA_PATH / system_name
            if not system_path.exists():
                self.logger.error(f"システムディレクトリが存在しません: {system_path}")
                return False
            
            success_count = 0
            total_count = len(self.file_mappings[system_name])
            
            for csv_filename, table_name in self.file_mappings[system_name].items():
                csv_path = system_path / csv_filename
                
                if not csv_path.exists():
                    self.logger.warning(f"CSVファイルが存在しません: {csv_path}")
                    continue
                
                if self.load_csv_file(csv_path, table_name):
                    success_count += 1
                    
            self.logger.info(f"{system_name}システム完了: {success_count}/{total_count} ファイル")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"{system_name}システムのデータロードでエラー: {e}")
            return False
    
    def load_csv_file(self, csv_path: Path, table_name: str) -> bool:
        """CSVファイルを指定テーブルにロード"""
        try:
            self.logger.info(f"CSVロード開始: {csv_path.name} -> {table_name}")
            
            # CSVファイル読み込み
            df = pd.read_csv(csv_path, encoding='utf-8')
            
            # データクリーニング
            df = clean_dataframe(df)
            
            # 基本検証
            if df.empty:
                self.logger.warning(f"空のデータファイル: {csv_path}")
                return False
            
            # データベースにロード
            self.db_manager.load_csv_to_table(csv_path, table_name)
            
            # ロード結果確認
            row_count = self.db_manager.get_row_count(table_name)
            self.logger.info(f"CSVロード完了: {table_name} ({row_count} rows)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"CSVロードエラー: {csv_path} -> {table_name}: {e}")
            return False
    
    def load_all_data(self) -> bool:
        """全システムのデータをロード"""
        self.logger.info("全システムデータロード開始...")
        
        systems = ["ERP", "P2P", "MES", "TMS", "HR", "WMS"]
        success_systems = []
        
        for system in systems:
            if self.load_system_data(system):
                success_systems.append(system)
        
        self.logger.info(f"データロード完了: {len(success_systems)}/{len(systems)} システム成功")
        self.logger.info(f"成功システム: {success_systems}")
        
        return len(success_systems) == len(systems)
    
    def validate_data_quality(self) -> bool:
        """データ品質チェック"""
        self.logger.info("データ品質チェック開始...")
        
        # 主要テーブルのデータ件数チェック
        key_tables = [
            "bronze_erp_item_master",
            "bronze_erp_order_header",
            "bronze_p2p_purchase_header",
            "bronze_mes_shipment_header",
            "bronze_wms_monthly_inventory"
        ]
        
        quality_issues = []
        
        for table in key_tables:
            if self.db_manager.table_exists(table):
                row_count = self.db_manager.get_row_count(table)
                if row_count == 0:
                    quality_issues.append(f"テーブル {table} にデータがありません")
                else:
                    self.logger.info(f"{table}: {row_count} rows")
            else:
                quality_issues.append(f"テーブル {table} が存在しません")
        
        if quality_issues:
            for issue in quality_issues:
                self.logger.warning(f"品質チェック警告: {issue}")
            return False
        
        self.logger.info("データ品質チェック完了")
        return True
    
    def run(self) -> bool:
        """ブロンズ層ETL実行"""
        try:
            self.logger.info("=== ブロンズ層ETL開始 ===")
            
            # データベース初期化
            self.initialize_database()
            
            # データロード
            if not self.load_all_data():
                self.logger.error("データロードに失敗しました")
                return False
            
            # データ品質チェック
            if not self.validate_data_quality():
                self.logger.warning("データ品質に問題がありますが、処理を継続します")
            
            self.logger.info("=== ブロンズ層ETL完了 ===")
            return True
            
        except Exception as e:
            self.logger.error(f"ブロンズ層ETL実行エラー: {e}")
            return False

def main():
    """メイン実行関数"""
    bronze_etl = BronzeETL()
    success = bronze_etl.run()
    
    if success:
        print("ブロンズ層ETL処理が正常に完了しました")
        return 0
    else:
        print("ブロンズ層ETL処理でエラーが発生しました")
        return 1

if __name__ == "__main__":
    exit(main())