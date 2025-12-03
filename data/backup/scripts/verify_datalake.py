"""
データレイク検証スクリプト - データベース構造とサンプルデータの確認
"""

import sqlite3
import pandas as pd
from pathlib import Path

# パス設定
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "datalake.db"

# データベース接続
conn = sqlite3.connect(DB_PATH)

print("=" * 80)
print("データレイク構造確認")
print("=" * 80)
print(f"データベース: {DB_PATH}")
print()

# テーブル一覧
tables = pd.read_sql_query("""
    SELECT name, type 
    FROM sqlite_master 
    WHERE type='table' 
    ORDER BY name
""", conn)

print(f"テーブル数: {len(tables)}")
print("-" * 80)

# 各層のテーブルを分類
bronze_tables = [t for t in tables['name'] if t.startswith('bronze_')]
silver_tables = [t for t in tables['name'] if t.startswith('silver_')]
gold_tables = [t for t in tables['name'] if t.startswith('gold_')]

print(f"\nBronze層: {len(bronze_tables)} テーブル")
for table in bronze_tables:
    count = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {table}", conn)['cnt'][0]
    print(f"  - {table}: {count:,} レコード")

print(f"\nSilver層: {len(silver_tables)} テーブル")
for table in silver_tables:
    count = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {table}", conn)['cnt'][0]
    print(f"  - {table}: {count:,} レコード")

print(f"\nGold層: {len(gold_tables)} テーブル")
for table in gold_tables:
    count = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {table}", conn)['cnt'][0]
    print(f"  - {table}: {count:,} レコード")

# Gold層の詳細確認
print("\n" + "=" * 80)
print("Gold層: 月次EV販売率 - 詳細データ")
print("=" * 80)

df = pd.read_sql_query("""
    SELECT 
        year_month as 年月,
        PRINTF('%,.0f', total_revenue) as 全売上金額,
        PRINTF('%,.0f', ev_revenue) as EV売上金額,
        PRINTF('%.2f%%', ev_sales_share) as EV販売率
    FROM gold_monthly_ev_sales_rate
    ORDER BY year_month
""", conn)

print(df.to_string(index=False))

# データ品質チェック
print("\n" + "=" * 80)
print("データ品質チェック")
print("=" * 80)

# 1. NULL値のチェック
null_check = pd.read_sql_query("""
    SELECT 
        COUNT(*) as 総レコード数,
        SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) as 全売上金額_NULL,
        SUM(CASE WHEN ev_revenue IS NULL THEN 1 ELSE 0 END) as EV売上金額_NULL,
        SUM(CASE WHEN ev_sales_share IS NULL THEN 1 ELSE 0 END) as EV販売率_NULL
    FROM gold_monthly_ev_sales_rate
""", conn)
print("\nNULL値チェック:")
print(null_check.to_string(index=False))

# 2. EV車種の確認
ev_items = pd.read_sql_query("""
    SELECT 
        product_id as 製品ID,
        product_name as 製品名,
        item_hierarchy as 車種区分,
        is_ev as EV判定
    FROM silver_items
    WHERE is_ev = 1
    ORDER BY product_id
""", conn)
print(f"\nEV車種: {len(ev_items)} 種類")
print(ev_items.to_string(index=False))

# 3. 非EV車種の確認
non_ev_items = pd.read_sql_query("""
    SELECT 
        product_id as 製品ID,
        product_name as 製品名,
        item_hierarchy as 車種区分,
        is_ev as EV判定
    FROM silver_items
    WHERE is_ev = 0
    ORDER BY product_id
""", conn)
print(f"\n非EV車種: {len(non_ev_items)} 種類")
print(non_ev_items.to_string(index=False))

# 4. 売上明細のサンプル
print("\n売上明細サンプル（最新5件）:")
sales_sample = pd.read_sql_query("""
    SELECT 
        order_id as 注文ID,
        year_month as 年月,
        product_name as 製品名,
        quantity as 数量,
        PRINTF('%,.0f', unit_price) as 単価,
        PRINTF('%,.0f', sales_amount) as 売上金額,
        CASE WHEN is_ev = 1 THEN 'EV' ELSE 'ICE' END as 車種区分
    FROM silver_sales_detail
    ORDER BY order_date DESC
    LIMIT 5
""", conn)
print(sales_sample.to_string(index=False))

conn.close()

print("\n" + "=" * 80)
print("検証完了")
print("=" * 80)
