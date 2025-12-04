"""
データベーススキーマ作成スクリプト
"""
import sqlite3
from pathlib import Path

BASE_DIR = Path(r"C:\Users\PC\dev\ForStep2")
DATABASE_PATH = BASE_DIR / "data" / "kpi_database.db"
SCHEMA_PATH = BASE_DIR / "scripts" / "01_create_database_schema.sql"

print(f"データベースパス: {DATABASE_PATH}")
print(f"スキーマファイル: {SCHEMA_PATH}")

# データベース接続
DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
conn = sqlite3.connect(str(DATABASE_PATH))

# SQLスクリプト読み込み
with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
    schema_sql = f.read()

# スキーマ実行
print("スキーマ作成中...")
conn.executescript(schema_sql)
conn.commit()
conn.close()

print("✓ データベーススキーマの作成が完了しました")
print(f"データベース: {DATABASE_PATH}")
