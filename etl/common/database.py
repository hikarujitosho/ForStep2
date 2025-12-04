import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging
from .config import DATABASE_PATH

logger = logging.getLogger(__name__)

class DatabaseManager:
    """SQLiteデータベース接続管理クラス"""
    
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DATABASE_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def get_connection(self) -> sqlite3.Connection:
        """データベース接続を取得"""
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    
    def execute_sql(self, sql: str, params: Optional[tuple] = None) -> None:
        """SQL実行"""
        with self.get_connection() as conn:
            try:
                if params:
                    conn.execute(sql, params)
                else:
                    conn.execute(sql)
                conn.commit()
                logger.info(f"SQL実行成功: {sql[:100]}...")
            except Exception as e:
                logger.error(f"SQL実行エラー: {e}")
                raise
    
    def execute_sql_file(self, sql_file_path: Path) -> None:
        """SQLファイルを実行"""
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        with self.get_connection() as conn:
            try:
                conn.executescript(sql_content)
                logger.info(f"SQLファイル実行成功: {sql_file_path}")
            except Exception as e:
                logger.error(f"SQLファイル実行エラー: {e}")
                raise
    
    def load_csv_to_table(self, csv_path: Path, table_name: str, if_exists: str = 'replace') -> None:
        """CSVファイルをテーブルにロード"""
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
            with self.get_connection() as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
                logger.info(f"CSVロード成功: {csv_path} -> {table_name} ({len(df)} rows)")
        except Exception as e:
            logger.error(f"CSVロードエラー: {csv_path} -> {table_name}: {e}")
            raise
    
    def query_to_dataframe(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """クエリ結果をDataFrameとして取得"""
        try:
            with self.get_connection() as conn:
                if params:
                    df = pd.read_sql_query(sql, conn, params=params)
                else:
                    df = pd.read_sql_query(sql, conn)
                logger.info(f"クエリ実行成功: {len(df)} rows returned")
                return df
        except Exception as e:
            logger.error(f"クエリエラー: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """テーブル情報を取得"""
        sql = "PRAGMA table_info(?)"
        with self.get_connection() as conn:
            cursor = conn.execute(sql, (table_name,))
            return [dict(row) for row in cursor.fetchall()]
    
    def table_exists(self, table_name: str) -> bool:
        """テーブルの存在確認"""
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        with self.get_connection() as conn:
            cursor = conn.execute(sql, (table_name,))
            return cursor.fetchone() is not None
    
    def get_row_count(self, table_name: str) -> int:
        """テーブルの行数を取得"""
        sql = f"SELECT COUNT(*) FROM {table_name}"
        with self.get_connection() as conn:
            cursor = conn.execute(sql)
            return cursor.fetchone()[0]