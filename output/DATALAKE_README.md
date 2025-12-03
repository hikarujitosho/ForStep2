# データレイク構築完了レポート - 月次EV販売率

## 概要
メダリオンアーキテクチャに基づいたデータレイク（SQLite）を構築し、月次EV販売率KPIを算出しました。

## データベース情報
- **ファイル名**: `datalake.db`
- **場所**: `C:\Users\PC\dev\ForStep2\datalake.db`
- **アーキテクチャ**: Bronze → Silver → Gold の3層構造

## 構築されたテーブル

### Bronze層（生データ）: 5テーブル
Bronze層は既存CSVファイルをそのままSQLiteにロードした層です。

1. **bronze_order_header** (935レコード) - 受注伝票ヘッダー
2. **bronze_order_item** (1,909レコード) - 受注伝票明細
3. **bronze_item_master** (15レコード) - 品目マスタ
4. **bronze_price_condition** (474レコード) - 価格条件マスタ
5. **bronze_partner_master** (31レコード) - 取引先マスタ

### Silver層（正規化データ）: 4テーブル
Silver層はBronze層のデータをクレンジング・正規化・結合した層です。

1. **silver_orders** (1,909レコード) - 正規化された受注データ
   - 年月の抽出（year_month列）
   - 日付の標準化
   
2. **silver_items** (15レコード) - 正規化された品目マスタ
   - EV判定フラグの追加（is_ev列）
   - item_hierarchy='EV'の場合、is_ev=1
   
3. **silver_price_conditions** (474レコード) - 正規化された価格条件
   - データ型の適正化（数値型への変換）
   
4. **silver_sales_detail** (1,909レコード) - 売上明細
   - 受注×品目×価格を結合
   - 売上金額の算出（数量×単価）
   - EV判定フラグの継承

### Gold層（KPI集計）: 1テーブル
Gold層は分析用のKPIを保持する層です。

1. **gold_monthly_ev_sales_rate** (48レコード) - 月次EV販売率
   - **year_month**: 年月（YYYY-MM形式）
   - **total_revenue**: 全完成車売上金額（円）
   - **ev_revenue**: EV完成車売上金額（円）
   - **ev_sales_share**: EV販売率（%、小数点6桁）

## 算出ロジック

### 月次EV販売率（金額ベース）
```
EV販売率 = (EV売上金額 / 全売上金額) × 100
```

#### 詳細な計算フロー
1. **受注明細の取得**: 受注伝票から製品・数量・年月を取得
2. **EV判定**: 品目マスタのitem_hierarchy列で'EV'を判定
3. **価格取得**: 条件マスタから販売価格を取得
4. **売上金額計算**: 数量 × 単価
5. **月次集計**: 年月ごとに全売上とEV売上を集計
6. **販売率算出**: EV売上 ÷ 全売上 × 100

## 実行方法

### 1. データレイクの構築
```powershell
C:/Users/PC/dev/ForStep2/.venv/Scripts/python.exe build_datalake_ev_sales.py
```

このスクリプトは以下を実行します:
- Bronze層: CSVファイルをSQLiteにロード
- Silver層: データの正規化と結合
- Gold層: 月次EV販売率の算出
- CSV出力: `data/Gold/月次EV販売率_金額ベース.csv`

### 2. データレイクの検証
```powershell
C:/Users/PC/dev/ForStep2/.venv/Scripts/python.exe verify_datalake.py
```

このスクリプトは以下を確認します:
- テーブル構造とレコード数
- Gold層の全データ表示
- データ品質チェック（NULL値）
- EV車種と非EV車種の一覧
- 売上明細のサンプル表示

### 3. SQLで直接クエリ
```powershell
sqlite3 datalake.db
```

#### サンプルクエリ

**月次EV販売率の取得:**
```sql
SELECT 
    year_month,
    total_revenue,
    ev_revenue,
    ev_sales_share
FROM gold_monthly_ev_sales_rate
ORDER BY year_month;
```

**特定月のEV売上詳細:**
```sql
SELECT 
    product_name,
    SUM(quantity) as 販売台数,
    SUM(sales_amount) as 売上金額
FROM silver_sales_detail
WHERE year_month = '2025-12' AND is_ev = 1
GROUP BY product_name;
```

**年別のEV販売率推移:**
```sql
SELECT 
    SUBSTR(year_month, 1, 4) as 年,
    ROUND(AVG(ev_sales_share), 2) as 平均EV販売率,
    ROUND(MIN(ev_sales_share), 2) as 最小EV販売率,
    ROUND(MAX(ev_sales_share), 2) as 最大EV販売率
FROM gold_monthly_ev_sales_rate
GROUP BY SUBSTR(year_month, 1, 4)
ORDER BY 年;
```

## 算出結果サマリー

### 統計情報（2022年1月～2025年12月）
- **集計月数**: 48ヶ月
- **平均EV販売率**: 35.74%
- **最小EV販売率**: 11.62%（2024年6月）
- **最大EV販売率**: 68.35%（2024年3月）

### EV車種（4種類）
- ENP-ENP1: e:NP1
- HDE-ZC1: Honda e
- NOE-JG4: N-ONE e:
- PRO-ZC5: Prologue

### 非EV車種（11種類）
- ACD-CV1: ACCORD
- CRV-RT5: CR-V
- FIT-GR3: FIT
- FRD-GB5: FREED
- ODY-RC1: ODYSSEY
- PLT-YF3: PILOT
- PSP-YF7: PASSPORT
- RDG-YF6: RIDGELINE
- SWN-RP6: STEP WGN
- VZL-RV3: VEZEL
- ZRV-RZ3: ZR-V

## 出力ファイル

### CSV出力
**ファイル**: `data/Gold/月次EV販売率_金額ベース.csv`

**列構成**:
- 年月: YYYY-MM形式
- 全売上金額: 全完成車の売上金額（円）
- EV売上金額: EV完成車の売上金額（円）
- EV販売率: パーセンテージ（小数点以下6桁）

## BIツールへの接続方法

### Power BI
1. データソースで「SQLite」を選択
2. データベースファイル: `C:\Users\PC\dev\ForStep2\datalake.db`
3. テーブル選択: `gold_monthly_ev_sales_rate`

### Tableau
1. 「その他のデータベース」→「SQLite」を選択
2. データベースファイルを指定
3. カスタムSQLまたはテーブルを選択

### Python/pandas
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('datalake.db')
df = pd.read_sql_query('''
    SELECT * FROM gold_monthly_ev_sales_rate
''', conn)
conn.close()
```

## データ更新方法

### 新しいデータが追加された場合
1. Bronze層のCSVファイルを更新
2. 構築スクリプトを再実行
   ```powershell
   C:/Users/PC/dev/ForStep2/.venv/Scripts/python.exe build_datalake_ev_sales.py
   ```
3. データベース全体が再構築され、最新のKPIが算出されます

## 拡張性

このデータレイク構造は以下のKPIを追加する準備が整っています:

- 月次エリア別EV販売率
- 月次先進安全装置適用率
- 月次商品別粗利率
- 棚卸資産回転期間
- 月次EBITDA
- 完成品出荷リードタイム遵守率

追加のKPIについては、同様の構造でGold層テーブルを追加することで実装可能です。

## トラブルシューティング

### エラー: "database is locked"
→ 既にデータベースファイルが開かれている可能性があります。すべての接続を閉じてください。

### エラー: "no such table"
→ 構築スクリプトが正常に完了していない可能性があります。スクリプトを再実行してください。

### データが古い
→ 構築スクリプトを再実行してデータベースを更新してください。

## ファイル一覧
- `datalake.db`: SQLiteデータベース（Bronze/Silver/Gold層）
- `build_datalake_ev_sales.py`: データレイク構築スクリプト
- `verify_datalake.py`: データレイク検証スクリプト
- `data/Gold/月次EV販売率_金額ベース.csv`: KPI算出結果（CSV）
