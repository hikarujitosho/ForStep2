# データレイク利用ガイド

## 概要
このデータレイクは、複数の基幹システムから出力されたCSVデータを統合し、BIツールで可視化するためのKPIを算出するSQLiteデータベースです。
メダリオンアーキテクチャ（Bronze → Silver → Gold）に従った三層構造で構築されています。

## アーキテクチャ

### Bronze層（生データ格納層）
CSVファイルをそのままSQLiteテーブルに格納します。データの加工は行いません。

**格納テーブル:**
- `bronze_erp_受注伝票_header` - 受注伝票ヘッダー (935レコード)
- `bronze_erp_受注伝票_item` - 受注伝票明細 (1,909レコード)
- `bronze_erp_条件マスタ` - 価格条件マスタ (474レコード)
- `bronze_erp_品目マスタ` - 品目マスタ (15レコード)
- `bronze_p2p_調達伝票_header` - 調達伝票ヘッダー (4,186レコード)
- `bronze_p2p_調達伝票_item` - 調達伝票明細 (12,044レコード)

### Silver層（データクレンジング層）
Bronze層のデータに対して以下の処理を実施します:
- NULL値の処理（COALESCE関数による空文字列への変換）
- 文字列のトリム（前後の空白除去）
- データ型変換（数値フィールドをREAL型に変換）
- データ品質チェック（主キーのNULLチェック）

**処理テーブル:**
- `silver_受注伝票_header`
- `silver_受注伝票_item`
- `silver_条件マスタ`
- `silver_品目マスタ`
- `silver_調達伝票_header`
- `silver_調達伝票_item`

### Gold層（KPI算出層）
Silver層のデータを使用してビジネスKPIを算出します。

**現在利用可能なKPI:**

#### 月次商品別粗利率 (`gold_月次商品別粗利率`)
- **計算式:** (売上 - 売上原価) / 売上 × 100
- **レコード数:** 540件
- **カラム:**
  - `product_id` - 製品ID
  - `product_name` - 製品名
  - `year_month` - 集計年月 (YYYY-MM形式)
  - `revenue` - 売上金額（税抜）
  - `cogs` - 売上原価（直接材のみ）
  - `gross_profit` - 粗利益
  - `gross_margin_rate` - 粗利率（%）

**算出ロジック:**
1. **売上計算（車種別）:** `受注伝票_item.quantity × 条件マスタ.selling_price_ex_tax`
   - 受注日の条件マスタから取引先別・製品別の販売価格を参照
   - pricing_dateがvalid_from〜valid_toの範囲内のレコードを使用
   - 製品ID（product_id）で車種別に集計

2. **売上原価計算（車種別）:** 調達伝票の`material_type='direct'`（直接材）の`line_subtotal_ex_tax`を集計
   - 完成車ごとに紐付けられた直接材の調達額を合計
   - 調達伝票_itemのproduct_idで車種別に集計

3. **月次集計:** 受注伝票の`order_timestamp`を基準に年月・車種別でグルーピング

## データベースファイル

- **ファイル名:** `datalake.db`
- **配置場所:** ワークスペースルート (`c:\Users\PC\dev\ForStep2\datalake.db`)
- **データベース形式:** SQLite 3

## データレイク構築手順

### 1. 前提条件
- Python 3.11以上
- pandas ライブラリ
- 必要なCSVファイルが`data/Bronze/`配下に配置されていること

### 2. 構築コマンド
```powershell
C:/Users/PC/dev/ForStep2/.venv/Scripts/python.exe build_datalake_gross_margin.py
```

### 3. 実行結果
- SQLiteデータベース `datalake.db` が生成されます
- Gold層のKPIデータが `data/Gold/月次商品別粗利率.csv` にエクスポートされます

### 4. 実行ログの見方
```
[2025-12-03 22:49:46] === Bronze層の構築開始 ===
[2025-12-03 22:49:46] ✓ bronze_erp_受注伝票_header を作成 (935 レコード)
...
[2025-12-03 22:49:46] === Silver層の構築開始 ===
[2025-12-03 22:49:46] ✓ silver_受注伝票_header を作成
...
[2025-12-03 22:49:46] === Gold層の構築開始 ===
[2025-12-03 22:49:46] ✓ gold_月次商品別粗利率 を作成 (540 レコード)
...
✅ データレイク構築が正常に完了しました！
```

## SQLiteデータベースへの接続方法

### コマンドラインから接続
```powershell
sqlite3 datalake.db
```

### Pythonから接続
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('datalake.db')

# Gold層のKPIを取得
df = pd.read_sql_query("SELECT * FROM gold_月次商品別粗利率", conn)
print(df.head())

conn.close()
```

### SQL例
```sql
-- 2025年12月の製品別粗利率
SELECT product_id, product_name, gross_margin_rate
FROM gold_月次商品別粗利率
WHERE year_month = '2025-12'
ORDER BY gross_margin_rate DESC;

-- 粗利率が低い製品トップ10
SELECT product_id, product_name, year_month, gross_margin_rate
FROM gold_月次商品別粗利率
ORDER BY gross_margin_rate ASC
LIMIT 10;

-- 月別の平均粗利率
SELECT year_month, 
       AVG(gross_margin_rate) as avg_margin_rate,
       COUNT(*) as product_count
FROM gold_月次商品別粗利率
GROUP BY year_month
ORDER BY year_month DESC;
```

## BIツール接続設定

### Tableau
1. 新しいデータソースを作成
2. 「SQLite」を選択
3. `datalake.db` ファイルを指定
4. `gold_月次商品別粗利率` テーブルを選択

### Power BI
1. 「データを取得」→「その他」→「ODBC」
2. SQLite ODBC Driverを使用
3. データベースファイルパスに `datalake.db` を指定
4. `gold_月次商品別粗利率` テーブルをインポート

### Metabase
1. 「Admin」→「Databases」→「Add database」
2. Database type: SQLite
3. Database file: `c:\Users\PC\dev\ForStep2\datalake.db`
4. 保存してテーブルをスキャン

## 出力ファイル

### CSVファイル
- **ファイル:** `data/Gold/月次商品別粗利率.csv`
- **エンコーディング:** UTF-8 with BOM
- **レコード数:** 540件
- **用途:** BIツールへの直接インポートやExcelでの分析

## データ品質に関する注意事項

現在の実装における特記事項:
- 一部の製品で`cogs`（売上原価）が0となっているケースがあります。これは調達伝票に該当する直接材のレコードが存在しないためです。
- `gross_margin_rate`が100%となっている製品は、売上原価が計上されていない製品です。
- マイナスの粗利率が発生している製品は、売上よりも原価が高い製品です。

## トラブルシューティング

### エラー: "no such table"
- Bronze層のCSVファイルが正しく読み込まれているか確認してください
- `data/Bronze/ERP/`と`data/Bronze/P2P/`にCSVファイルが存在するか確認してください

### エラー: "database is locked"
- 他のプログラムがデータベースファイルを開いていないか確認してください
- SQLiteブラウザなどのツールを閉じてから再実行してください

### CSVファイルが見つからない
- CSVファイルのパスが正しいか確認してください
- ファイル名に日本語が含まれる場合、エンコーディングの問題がないか確認してください

## 今後の拡張

このデータレイクは以下のKPIにも対応可能です（データ定義書に基づく）:
- 月次EBITDA
- 完成品別月次EBITDA
- 棚卸資産回転期間
- 完成品別棚卸資産回転期間
- 完成品出荷リードタイム遵守率
- 取引先別部品入荷リードタイム遵守率
- 緊急輸送費率
- 月次EV販売率
- 月次エリア別EV販売率
- 月次先進安全装置適用率

各KPIを追加する場合は、`build_datalake_gross_margin.py`の`create_gold_layer()`関数にSQLを追加してください。

## 関連ドキュメント

- `data_definition/` - データ定義書（各テーブルの詳細仕様）
- `KPI_calculation_logic/kpi_calculation_logic.md` - KPI計算ロジックの詳細
- `DATALAKE_README.md` - データレイク全体の構成説明

## バージョン情報

- **作成日:** 2025年12月3日
- **Python:** 3.11.9
- **pandas:** 2.3.3
- **SQLite:** 3.x
