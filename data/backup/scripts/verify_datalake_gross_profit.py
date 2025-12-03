"""
データレイク検証スクリプト
"""

import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / 'data' / 'datalake.db'

def main():
    conn = sqlite3.connect(DB_PATH)
    
    print('=' * 60)
    print('データベース統計')
    print('=' * 60)
    
    # テーブル数の確認
    bronze_tables = pd.read_sql(
        'SELECT name FROM sqlite_master WHERE type="table" AND name LIKE "bronze_%"', 
        conn
    )
    silver_tables = pd.read_sql(
        'SELECT name FROM sqlite_master WHERE type="table" AND name LIKE "silver_%"', 
        conn
    )
    gold_tables = pd.read_sql(
        'SELECT name FROM sqlite_master WHERE type="table" AND name LIKE "gold_%"', 
        conn
    )
    
    print(f'Bronze層テーブル数: {len(bronze_tables)}')
    print(f'Silver層テーブル数: {len(silver_tables)}')
    print(f'Gold層テーブル数: {len(gold_tables)}')
    print()
    
    print('=' * 60)
    print('月次商品別粗利率 統計')
    print('=' * 60)
    
    df = pd.read_sql('SELECT * FROM gold_月次商品別粗利率', conn)
    
    print(f'総レコード数: {len(df):,}')
    print(f'対象期間: {df["year_month"].min()} 〜 {df["year_month"].max()}')
    print(f'商品数: {df["product_id"].nunique()}')
    print()
    
    print('粗利率の統計:')
    print(f'  平均: {df["gross_profit_margin"].mean():.2f}%')
    print(f'  最小: {df["gross_profit_margin"].min():.2f}%')
    print(f'  最大: {df["gross_profit_margin"].max():.2f}%')
    print(f'  中央値: {df["gross_profit_margin"].median():.2f}%')
    print()
    
    print('=' * 60)
    print('商品別粗利率の上位10件（2024年11月）')
    print('=' * 60)
    
    latest_df = df[df['year_month'] == '2024-11'].sort_values(
        'gross_profit_margin', 
        ascending=False
    ).head(10)
    
    if len(latest_df) > 0:
        print(latest_df[['product_id', 'product_name', 'total_sales', 'gross_profit_margin']].to_string(index=False))
    else:
        print('2024年11月のデータがありません')
    print()
    
    print('=' * 60)
    print('商品別累計売上TOP10（全期間）')
    print('=' * 60)
    
    total_sales_df = df.groupby(['product_id', 'product_name']).agg({
        'total_sales': 'sum',
        'gross_profit': 'sum'
    }).reset_index()
    
    total_sales_df['avg_gross_profit_margin'] = (
        total_sales_df['gross_profit'] / total_sales_df['total_sales'] * 100
    ).round(2)
    
    top_sales = total_sales_df.sort_values('total_sales', ascending=False).head(10)
    print(top_sales[['product_id', 'product_name', 'total_sales', 'avg_gross_profit_margin']].to_string(index=False))
    print()
    
    conn.close()
    
    print('検証完了！')


if __name__ == '__main__':
    main()
