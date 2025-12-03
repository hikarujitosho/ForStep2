"""
月次商品別粗利率を計算するためのデータレイク構築スクリプト

メダリオンアーキテクチャに従って、Bronze -> Silver -> Gold の3層構造を構築します。
- Bronze層: CSVファイルをそのまま格納
- Silver層: データ型変換とクレンジング
- Gold層: KPI計算（月次商品別粗利率）
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# パス設定
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
BRONZE_DIR = DATA_DIR / 'Bronze'
DB_PATH = DATA_DIR / 'datalake.db'


class DataLakeBuilder:
    """データレイク構築クラス"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def build_bronze_layer(self):
        """Bronze層を構築: CSVファイルをそのまま格納"""
        logger.info("=" * 60)
        logger.info("Bronze層の構築を開始")
        logger.info("=" * 60)
        
        # ERPシステムのテーブル
        erp_tables = [
            ('受注伝票_header', 'ERP'),
            ('受注伝票_item', 'ERP'),
            ('条件マスタ', 'ERP'),
            ('品目マスタ', 'ERP'),
            ('取引先マスタ', 'ERP'),
            ('BOMマスタ', 'ERP'),
        ]
        
        # P2Pシステムのテーブル
        p2p_tables = [
            ('調達伝票_header', 'P2P'),
            ('調達伝票_item', 'P2P'),
        ]
        
        all_tables = erp_tables + p2p_tables
        
        for table_name, system in all_tables:
            csv_path = BRONZE_DIR / system / f'{table_name}.csv'
            
            if not csv_path.exists():
                logger.warning(f"CSVファイルが見つかりません: {csv_path}")
                continue
            
            try:
                # CSVを読み込み
                df = pd.read_csv(csv_path)
                logger.info(f"読み込み: {table_name} ({len(df):,} 行)")
                
                # Bronze層にそのまま格納
                bronze_table_name = f'bronze_{table_name}'
                df.to_sql(bronze_table_name, self.conn, if_exists='replace', index=False)
                logger.info(f"格納完了: {bronze_table_name}")
                
            except Exception as e:
                logger.error(f"エラー: {table_name} の処理中に問題が発生しました - {e}")
                raise
        
        logger.info("Bronze層の構築が完了しました")
        logger.info("")
    
    def build_silver_layer(self):
        """Silver層を構築: データ型変換とクレンジング"""
        logger.info("=" * 60)
        logger.info("Silver層の構築を開始")
        logger.info("=" * 60)
        
        # 受注伝票_header
        self._process_order_header()
        
        # 受注伝票_item
        self._process_order_item()
        
        # 条件マスタ
        self._process_price_condition()
        
        # 品目マスタ
        self._process_product_master()
        
        # 取引先マスタ
        self._process_partner_master()
        
        # 調達伝票_header
        self._process_purchase_header()
        
        # 調達伝票_item
        self._process_purchase_item()
        
        # BOMマスタ
        self._process_bom_master()
        
        logger.info("Silver層の構築が完了しました")
        logger.info("")
    
    def _process_order_header(self):
        """受注伝票_headerの処理"""
        table_name = '受注伝票_header'
        logger.info(f"処理中: {table_name}")
        
        df = pd.read_sql(f'SELECT * FROM bronze_{table_name}', self.conn)
        
        # データ型変換
        df['order_timestamp'] = pd.to_datetime(df['order_timestamp'])
        
        # 格納
        df.to_sql(f'silver_{table_name}', self.conn, if_exists='replace', index=False)
        logger.info(f"完了: silver_{table_name} ({len(df):,} 行)")
    
    def _process_order_item(self):
        """受注伝票_itemの処理"""
        table_name = '受注伝票_item'
        logger.info(f"処理中: {table_name}")
        
        df = pd.read_sql(f'SELECT * FROM bronze_{table_name}', self.conn)
        
        # データ型変換
        df['promised_delivery_date'] = pd.to_datetime(df['promised_delivery_date'])
        df['pricing_date'] = pd.to_datetime(df['pricing_date']).dt.date
        df['quantity'] = pd.to_numeric(df['quantity'])
        
        # 格納
        df.to_sql(f'silver_{table_name}', self.conn, if_exists='replace', index=False)
        logger.info(f"完了: silver_{table_name} ({len(df):,} 行)")
    
    def _process_price_condition(self):
        """条件マスタの処理"""
        table_name = '条件マスタ'
        logger.info(f"処理中: {table_name}")
        
        df = pd.read_sql(f'SELECT * FROM bronze_{table_name}', self.conn)
        
        # データ型変換
        df['valid_from'] = pd.to_datetime(df['valid_from']).dt.date
        df['valid_to'] = pd.to_datetime(df['valid_to']).dt.date
        df['list_price_ex_tax'] = pd.to_numeric(df['list_price_ex_tax'])
        df['selling_price_ex_tax'] = pd.to_numeric(df['selling_price_ex_tax'])
        df['discount_rate'] = pd.to_numeric(df['discount_rate'])
        df['minimum_order_quantity'] = pd.to_numeric(df['minimum_order_quantity'])
        
        # 格納
        df.to_sql(f'silver_{table_name}', self.conn, if_exists='replace', index=False)
        logger.info(f"完了: silver_{table_name} ({len(df):,} 行)")
    
    def _process_product_master(self):
        """品目マスタの処理"""
        table_name = '品目マスタ'
        logger.info(f"処理中: {table_name}")
        
        df = pd.read_sql(f'SELECT * FROM bronze_{table_name}', self.conn)
        
        # データ型変換
        if 'base_unit_quantity' in df.columns:
            df['base_unit_quantity'] = pd.to_numeric(df['base_unit_quantity'], errors='coerce')
        
        # 格納
        df.to_sql(f'silver_{table_name}', self.conn, if_exists='replace', index=False)
        logger.info(f"完了: silver_{table_name} ({len(df):,} 行)")
    
    def _process_partner_master(self):
        """取引先マスタの処理"""
        table_name = '取引先マスタ'
        logger.info(f"処理中: {table_name}")
        
        df = pd.read_sql(f'SELECT * FROM bronze_{table_name}', self.conn)
        
        # データ型変換
        if 'valid_from' in df.columns:
            df['valid_from'] = pd.to_datetime(df['valid_from']).dt.date
        if 'valid_to' in df.columns:
            df['valid_to'] = pd.to_datetime(df['valid_to'], errors='coerce').dt.date
        if 'credit_limit' in df.columns:
            df['credit_limit'] = pd.to_numeric(df['credit_limit'], errors='coerce')
        
        # 格納
        df.to_sql(f'silver_{table_name}', self.conn, if_exists='replace', index=False)
        logger.info(f"完了: silver_{table_name} ({len(df):,} 行)")
    
    def _process_purchase_header(self):
        """調達伝票_headerの処理"""
        table_name = '調達伝票_header'
        logger.info(f"処理中: {table_name}")
        
        df = pd.read_sql(f'SELECT * FROM bronze_{table_name}', self.conn)
        
        # データ型変換
        df['order_date'] = pd.to_datetime(df['order_date']).dt.date
        if 'expected_delivery_date' in df.columns:
            df['expected_delivery_date'] = pd.to_datetime(df['expected_delivery_date']).dt.date
        if 'payment_date' in df.columns:
            df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce').dt.date
        
        # 数値型変換
        numeric_columns = ['order_subtotal_ex_tax', 'shipping_fee_ex_tax', 'tax_amount', 
                          'discount_amount_incl_tax', 'order_total_incl_tax', 'payment_amount']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 格納
        df.to_sql(f'silver_{table_name}', self.conn, if_exists='replace', index=False)
        logger.info(f"完了: silver_{table_name} ({len(df):,} 行)")
    
    def _process_purchase_item(self):
        """調達伝票_itemの処理"""
        table_name = '調達伝票_item'
        logger.info(f"処理中: {table_name}")
        
        df = pd.read_sql(f'SELECT * FROM bronze_{table_name}', self.conn)
        
        # データ型変換
        if 'ship_date' in df.columns:
            df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce').dt.date
        if 'received_date' in df.columns:
            df['received_date'] = pd.to_datetime(df['received_date'], errors='coerce').dt.date
        
        # 数値型変換
        numeric_columns = ['quantity', 'unit_price_ex_tax', 'line_subtotal_incl_tax', 
                          'line_subtotal_ex_tax', 'line_tax_amount', 'line_tax_rate',
                          'line_shipping_fee_incl_tax', 'line_discount_incl_tax', 
                          'line_total_incl_tax', 'reference_price_ex_tax',
                          'shipped_quantity', 'received_quantity']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 格納
        df.to_sql(f'silver_{table_name}', self.conn, if_exists='replace', index=False)
        logger.info(f"完了: silver_{table_name} ({len(df):,} 行)")
    
    def _process_bom_master(self):
        """BOMマスタの処理"""
        table_name = 'BOMマスタ'
        logger.info(f"処理中: {table_name}")
        
        df = pd.read_sql(f'SELECT * FROM bronze_{table_name}', self.conn)
        
        # データ型変換
        if 'component_quantity_per' in df.columns:
            df['component_quantity_per'] = pd.to_numeric(df['component_quantity_per'], errors='coerce')
        
        # 格納
        df.to_sql(f'silver_{table_name}', self.conn, if_exists='replace', index=False)
        logger.info(f"完了: silver_{table_name} ({len(df):,} 行)")
    
    def build_gold_layer(self):
        """Gold層を構築: 月次商品別粗利率を計算"""
        logger.info("=" * 60)
        logger.info("Gold層の構築を開始")
        logger.info("=" * 60)
        
        # 売上データの計算
        logger.info("売上データを計算中...")
        sales_query = """
        SELECT 
            strftime('%Y-%m', oh.order_timestamp) as year_month,
            oi.product_id,
            pm.product_name,
            SUM(oi.quantity) as total_quantity,
            SUM(oi.quantity * pc.selling_price_ex_tax) as total_sales
        FROM silver_受注伝票_header oh
        INNER JOIN silver_受注伝票_item oi 
            ON oh.order_id = oi.order_id
        INNER JOIN silver_条件マスタ pc 
            ON oi.product_id = pc.product_id 
            AND oh.customer_id = pc.customer_id
            AND oi.pricing_date BETWEEN pc.valid_from AND pc.valid_to
        LEFT JOIN silver_品目マスタ pm
            ON oi.product_id = pm.product_id
        GROUP BY year_month, oi.product_id, pm.product_name
        """
        
        sales_df = pd.read_sql(sales_query, self.conn)
        logger.info(f"売上データ: {len(sales_df):,} 行")
        
        # 売上原価データの計算
        logger.info("売上原価データを計算中...")
        cogs_query = """
        SELECT 
            strftime('%Y-%m', ph.order_date) as year_month,
            pi.product_id,
            SUM(pi.line_subtotal_ex_tax) as total_cogs
        FROM silver_調達伝票_header ph
        INNER JOIN silver_調達伝票_item pi 
            ON ph.purchase_order_id = pi.purchase_order_id
        WHERE pi.material_type = 'direct'
            AND pi.product_id IS NOT NULL
            AND pi.product_id != ''
        GROUP BY year_month, pi.product_id
        """
        
        cogs_df = pd.read_sql(cogs_query, self.conn)
        logger.info(f"売上原価データ: {len(cogs_df):,} 行")
        
        # 売上と売上原価を結合して粗利率を計算
        logger.info("粗利率を計算中...")
        result_df = sales_df.merge(
            cogs_df,
            on=['year_month', 'product_id'],
            how='left'
        )
        
        # 売上原価がない場合は0とする
        result_df['total_cogs'] = result_df['total_cogs'].fillna(0)
        
        # 粗利と粗利率を計算
        result_df['gross_profit'] = result_df['total_sales'] - result_df['total_cogs']
        result_df['gross_profit_margin'] = (result_df['gross_profit'] / result_df['total_sales'] * 100).round(2)
        
        # カラムの順序を整理
        result_df = result_df[[
            'year_month',
            'product_id',
            'product_name',
            'total_quantity',
            'total_sales',
            'total_cogs',
            'gross_profit',
            'gross_profit_margin'
        ]]
        
        # ソート
        result_df = result_df.sort_values(['year_month', 'product_id'])
        
        # Gold層に格納
        result_df.to_sql('gold_月次商品別粗利率', self.conn, if_exists='replace', index=False)
        logger.info(f"完了: gold_月次商品別粗利率 ({len(result_df):,} 行)")
        
        # CSV出力
        output_path = DATA_DIR / 'Gold' / '月次商品別粗利率.csv'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"CSV出力: {output_path}")
        
        logger.info("Gold層の構築が完了しました")
        logger.info("")
        
        return result_df
    
    def verify_data(self):
        """データ検証"""
        logger.info("=" * 60)
        logger.info("データ検証を開始")
        logger.info("=" * 60)
        
        # テーブル一覧を取得
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        
        logger.info(f"作成されたテーブル数: {len(tables)}")
        logger.info("")
        
        for table in tables:
            table_name = table[0]
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count = cursor.execute(count_query).fetchone()[0]
            logger.info(f"  {table_name}: {count:,} 行")
        
        logger.info("")
        logger.info("Gold層のサンプルデータ:")
        logger.info("-" * 60)
        
        sample_df = pd.read_sql(
            "SELECT * FROM gold_月次商品別粗利率 LIMIT 10", 
            self.conn
        )
        
        print(sample_df.to_string(index=False))
        logger.info("")
        logger.info("データ検証が完了しました")


def main():
    """メイン処理"""
    start_time = datetime.now()
    logger.info("")
    logger.info("*" * 60)
    logger.info("データレイク構築開始")
    logger.info(f"開始時刻: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("*" * 60)
    logger.info("")
    
    try:
        with DataLakeBuilder(DB_PATH) as builder:
            # Bronze層構築
            builder.build_bronze_layer()
            
            # Silver層構築
            builder.build_silver_layer()
            
            # Gold層構築
            result_df = builder.build_gold_layer()
            
            # データ検証
            builder.verify_data()
        
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        
        logger.info("")
        logger.info("*" * 60)
        logger.info("データレイク構築完了")
        logger.info(f"終了時刻: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"処理時間: {elapsed_time}")
        logger.info("*" * 60)
        logger.info("")
        logger.info(f"データベースファイル: {DB_PATH}")
        logger.info(f"CSV出力先: {DATA_DIR / 'Gold' / '月次商品別粗利率.csv'}")
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
