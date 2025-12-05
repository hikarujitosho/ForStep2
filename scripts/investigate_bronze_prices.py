"""
粗利率計算の考え方を整理:

【現在の計算方法】
売上 = 月次の全受注line_total_ex_taxの合計（複数台分の合計金額）
コスト = 月次の全調達line_total_ex_taxの合計（全部品の合計金額）
粗利率 = (売上 - コスト) / 売上 × 100

例: FIT 2022-01
- 売上: ¥5,403,178,174 (約54億円)
- コスト: ¥735,637,300 (約7.4億円)
- 粗利率: 86.39%

【問題点】
1. 売上54億円は現実的には多すぎる（Hondaの月次売上としても）
2. 1台あたりに換算すると販売価格約665万円は高すぎる

【確認すべきこと】
1. Bronze層の selling_price_ex_tax が正しいか
2. 数量 quantity が正しいか
3. 期待される1台あたりの価格帯（200-500万円）に合っているか
"""

import sqlite3
import pandas as pd

conn = sqlite3.connect('database/data_lake.db')

# Bronze層の価格マスタを確認
print("=" * 120)
print("Bronze層の価格マスタ確認")
print("=" * 120)

price_query = """
SELECT 
    pc.product_id,
    im.product_name,
    pc.selling_price_ex_tax,
    pc.valid_from,
    pc.valid_to,
    pc.customer_id
FROM bronze_erp_price_condition pc
LEFT JOIN bronze_erp_item_master im ON pc.product_id = im.product_id
WHERE im.product_name IS NOT NULL
ORDER BY im.product_name, pc.valid_from
LIMIT 30
"""

price_df = pd.read_sql_query(price_query, conn)
print(price_df.to_string(index=False))

# 商品マスタの確認
print("\n" + "=" * 120)
print("商品マスタ確認")
print("=" * 120)

item_query = """
SELECT *
FROM bronze_erp_item_master
ORDER BY product_name
"""

item_df = pd.read_sql_query(item_query, conn)
print(item_df.to_string(index=False))

# 受注明細の数量を確認
print("\n" + "=" * 120)
print("受注明細の数量分布")
print("=" * 120)

qty_query = """
SELECT 
    i.product_id,
    im.product_name,
    COUNT(*) as order_count,
    SUM(i.quantity) as total_quantity,
    AVG(i.quantity) as avg_quantity,
    MIN(i.quantity) as min_quantity,
    MAX(i.quantity) as max_quantity
FROM bronze_erp_order_item i
LEFT JOIN bronze_erp_item_master im ON i.product_id = im.product_id
GROUP BY i.product_id, im.product_name
ORDER BY im.product_name
"""

qty_df = pd.read_sql_query(qty_query, conn)
print(qty_df.to_string(index=False))

conn.close()
