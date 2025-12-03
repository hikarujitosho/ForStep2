# データレイク構築ガイド - 月次商品別粗利率

## 概要

このプロジェクトは、複数の基幹システム（ERP、P2P）から出力されたCSVデータを用いて、メダリオンアーキテクチャに基づくデータレイクを構築し、「月次商品別粗利率」KPIを算出します。

## アーキテクチャ

### メダリオンアーキテクチャ（3層構造）

```
┌─────────────────────────────────────────────────────────┐
│ Bronze層 (Raw Data)                                      │
│ - CSVファイルをそのまま格納                              │
│ - データ型は文字列のまま                                  │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│ Silver層 (Cleaned & Typed Data)                         │
│ - データ型の変換（日付型、数値型など）                    │
│ - データクレンジング（NULL処理、異常値チェック）          │
│ - テーブル間の結合や集計は行わない                        │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│ Gold層 (KPI / Analytics Ready)                          │
│ - KPI計算結果を格納                                       │
│ - BIツールから直接参照可能                                │
└─────────────────────────────────────────────────────────┘
```

## ディレクトリ構造

```
ForStep2/
├── data/
│   ├── Bronze/              # 元データ（CSV）
│   │   ├── ERP/             # ERPシステムのデータ
│   │   └── P2P/             # P2Pシステムのデータ
│   ├── Gold/                # KPI算出結果（CSV出力）
│   └── datalake.db          # SQLiteデータベース
│
├── data_definition/         # データ定義書
│   ├── data_definition_bronze_ERP.md
│   ├── data_definition_bronze_P2P.md
│   └── ...
│
├── KPI_calculation_logic/   # KPI計算ロジック定義
│   └── kpi_calculation_logic.md
│
├── build_datalake_gross_profit.py          # データレイク構築スクリプト
└── verify_datalake_gross_profit.py         # データレイク検証スクリプト
```

## 使用データ

### 必要なCSVファイル

#### ERPシステム（受発注）
- `受注伝票_header.csv` - 受注のヘッダー情報
- `受注伝票_item.csv` - 受注の明細情報
- `条件マスタ.csv` - 販売価格情報
- `品目マスタ.csv` - 商品マスター
- `取引先マスタ.csv` - 取引先マスター
- `BOMマスタ.csv` - 部品表

#### P2Pシステム（間接材購買）
- `調達伝票_header.csv` - 調達のヘッダー情報
- `調達伝票_item.csv` - 調達の明細情報（売上原価計算に使用）

## KPI計算ロジック

### 月次商品別粗利率

**計算式:**
```
粗利率 = (売上 - 売上原価) / 売上 × 100
```

**計算詳細:**

1. **売上の計算**
   ```sql
   売上 = Σ(受注数量 × 販売価格)
   ```
   - 受注伝票_itemから受注数量を取得
   - 条件マスタから販売価格を取得
   - 結合条件:
     - `product_id`（商品ID）
     - `customer_id`（取引先ID）
     - `pricing_date BETWEEN valid_from AND valid_to`（価格有効期間）

2. **売上原価の計算**
   ```sql
   売上原価 = Σ(調達金額) WHERE material_type = 'direct'
   ```
   - 調達伝票_itemから直接材（`material_type='direct'`）の調達金額を集計
   - `product_id`で商品別に集計

3. **粗利と粗利率の計算**
   ```
   粗利 = 売上 - 売上原価
   粗利率 = 粗利 / 売上 × 100
   ```

## セットアップ

### 前提条件

- Python 3.11以上
- 仮想環境が作成済み（`.venv`）

### パッケージのインストール

既存のrequirements.txtには必要なパッケージが含まれていますが、データレイク構築に必要なのは：
- `pandas` - データ処理
- `sqlite3` - データベース（Python標準ライブラリ）

```bash
# 既存の仮想環境を使用
.venv\Scripts\activate

# パッケージは既にインストール済み
```

## 実行方法

### 1. データレイクの構築

```bash
python build_datalake_gross_profit.py
```

**処理内容:**
1. Bronze層の構築: CSVをSQLiteに格納
2. Silver層の構築: データ型変換とクレンジング
3. Gold層の構築: 月次商品別粗利率の計算
4. CSV出力: `data/Gold/月次商品別粗利率.csv`

**実行時間:** 約1〜2秒

### 2. データレイクの検証

```bash
python verify_datalake_gross_profit.py
```

**出力内容:**
- テーブル数の統計
- KPIレコード数
- 粗利率の統計（平均、最小、最大、中央値）
- 最新月の商品別粗利率TOP10
- 全期間の累計売上TOP10

## データベース構造

### Bronze層テーブル（8テーブル）

- `bronze_受注伝票_header`
- `bronze_受注伝票_item`
- `bronze_条件マスタ`
- `bronze_品目マスタ`
- `bronze_取引先マスタ`
- `bronze_BOMマスタ`
- `bronze_調達伝票_header`
- `bronze_調達伝票_item`

### Silver層テーブル（8テーブル）

- `silver_受注伝票_header`
- `silver_受注伝票_item`
- `silver_条件マスタ`
- `silver_品目マスタ`
- `silver_取引先マスタ`
- `silver_BOMマスタ`
- `silver_調達伝票_header`
- `silver_調達伝票_item`

### Gold層テーブル（1テーブル）

- `gold_月次商品別粗利率`

**スキーマ:**
| カラム名 | 型 | 説明 |
|---------|-----|------|
| year_month | TEXT | 年月（YYYY-MM形式） |
| product_id | TEXT | 商品ID |
| product_name | TEXT | 商品名 |
| total_quantity | REAL | 販売数量 |
| total_sales | REAL | 売上金額 |
| total_cogs | REAL | 売上原価 |
| gross_profit | REAL | 粗利 |
| gross_profit_margin | REAL | 粗利率（%） |

## BIツールへの接続

### SQLiteデータベースへの接続

**接続情報:**
- データベースファイル: `data/datalake.db`
- 推奨接続方法: ODBC/JDBC/直接SQLite接続

### 参照すべきテーブル

BIツールからは以下のテーブルを参照してください：

1. **Gold層のKPIテーブル**
   - `gold_月次商品別粗利率` - 月次商品別粗利率

2. **Silver層のマスターテーブル（必要に応じて）**
   - `silver_品目マスタ` - 商品の詳細情報
   - `silver_取引先マスタ` - 取引先の詳細情報

### サンプルクエリ

```sql
-- 最新月の粗利率TOP10
SELECT 
    product_id,
    product_name,
    total_sales,
    gross_profit_margin
FROM gold_月次商品別粗利率
WHERE year_month = '2024-11'
ORDER BY gross_profit_margin DESC
LIMIT 10;

-- 商品別の累計売上と平均粗利率
SELECT 
    product_id,
    product_name,
    SUM(total_sales) as cumulative_sales,
    AVG(gross_profit_margin) as avg_margin
FROM gold_月次商品別粗利率
GROUP BY product_id, product_name
ORDER BY cumulative_sales DESC;

-- 月次トレンド分析
SELECT 
    year_month,
    SUM(total_sales) as monthly_sales,
    AVG(gross_profit_margin) as avg_margin
FROM gold_月次商品別粗利率
GROUP BY year_month
ORDER BY year_month;
```

## データ更新

### 増分更新

CSVファイルが更新された場合、再度スクリプトを実行してください：

```bash
python build_datalake_gross_profit.py
```

既存のテーブルは上書き（`if_exists='replace'`）されます。

### 定期実行（オプション）

Windowsタスクスケジューラーや、cronジョブで定期実行を設定できます。

**例: Windowsタスクスケジューラー**
```
プログラム: C:\Users\PC\dev\ForStep2\.venv\Scripts\python.exe
引数: C:\Users\PC\dev\ForStep2\build_datalake_gross_profit.py
```

## トラブルシューティング

### エラー: CSVファイルが見つからない

CSVファイルが正しいパスに配置されているか確認してください：
- `data/Bronze/ERP/` 配下
- `data/Bronze/P2P/` 配下

### エラー: データ型変換エラー

Silver層でのデータ型変換でエラーが発生した場合、Bronze層のデータ品質を確認してください。

### パフォーマンスの問題

データ量が増加した場合は、以下を検討してください：
1. SQLiteのインデックス追加
2. PostgreSQLやMySQL等への移行
3. データパーティショニング

## 今後の拡張

### 追加予定のKPI

- 月次EBITDA
- 完成品別月次EBITDA
- 棚卸資産回転期間
- 完成品出荷リードタイム遵守率
- 取引先別部品入荷リードタイム遵守率
- 緊急輸送費率
- 月次EV販売率
- 月次エリア別EV販売率
- 月次エリア別先進安全装置適用率

各KPIの詳細は `KPI_calculation_logic/kpi_calculation_logic.md` を参照してください。

## ライセンス

社内利用のみ

## お問い合わせ

データ定義や計算ロジックについて質問がある場合は、以下を参照してください：
- データ定義書: `data_definition/`
- KPI計算ロジック: `KPI_calculation_logic/kpi_calculation_logic.md`
