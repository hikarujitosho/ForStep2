# ローカル環境でのデータ分析基盤構築ガイド

**対象者:** 非エンジニア（データアナリスト、業務担当者）  
**作成日:** 2025年12月5日  
**所要時間:** 初回セットアップ 約30分

---

## 📋 目次

1. [概要](#概要)
2. [必要なもの](#必要なもの)
3. [初回セットアップ](#初回セットアップ)
4. [データの更新方法](#データの更新方法)
5. [データの見方・分析方法](#データの見方分析方法)
6. [トラブルシューティング](#トラブルシューティング)

---

## 概要

このパッケージは、CSV形式の業務データからKPIを自動計算し、SQLiteデータベースに格納するローカル分析基盤です。

### できること

✅ CSVファイルを読み込んでデータベース化  
✅ 5つのKPIを自動計算  
✅ Excel・Power BIでデータを分析  
✅ 過去データとの比較分析

### データの流れ

```
【Bronze】CSV生データ
    ↓ (load_bronze_to_silver.py)
【Silver】標準化されたデータ（12テーブル）
    ↓ (calculate_gold_kpis.py)
【Gold】KPIデータ（5テーブル）
    ↓
【分析】Excel / Power BI で可視化
```

---

## 必要なもの

### ✅ 事前準備（既にPCにあるもの）

- Windows 10/11
- Excelまたは Power BI Desktop（オプション）

### ⚠️ インストールが必要なもの

#### 1. Python 3.8以降

**確認方法:**
```
コマンドプロンプトを開いて以下を実行
> python --version
```

**まだない場合:**
1. https://www.python.org/downloads/ にアクセス
2. 「Download Python 3.12.x」をクリック
3. インストール時に「Add Python to PATH」にチェック✓

#### 2. 必要なPythonライブラリ

以下のコマンドを実行：
```bash
pip install pandas openpyxl
```

---

## 初回セットアップ

### ステップ1: フォルダ構成の確認

以下のような構成になっていることを確認：

```
C:\Users\PC\dev\ForStep2\
├── data\
│   └── Bronze\
│       └── pre24\           ← CSVファイルの場所
│           ├── ERP\
│           ├── P2P\
│           ├── WMS\
│           ├── MES\
│           └── TMS\
│
└── output\
    └── test251205 1000jitosho\
        └── sqlite_local_setup\  ← このフォルダ
            ├── scripts\         ← Pythonスクリプト
            ├── database\        ← SQLiteデータベース格納先
            ├── queries\         ← 分析用SQLクエリ
            └── README.md        ← このファイル
```

### ステップ2: 初回データロード

**Windows PowerShellまたはコマンドプロンプトを開く**

```powershell
# フォルダ移動
cd "C:\Users\PC\dev\ForStep2\output\test251205 1000jitosho\sqlite_local_setup"

# ステップ1: CSVからSilverテーブル作成（5-10分）
python scripts\load_bronze_to_silver.py

# ステップ2: KPI計算（1-2分）
python scripts\calculate_gold_kpis.py
```

### ステップ3: 完了確認

`database\analytics.db` ファイルが作成されていればOK！

---

## データの更新方法

### 毎月のデータ更新手順

1. **新しいCSVファイルを配置**
   - `C:\Users\PC\dev\ForStep2\data\Bronze\pre24\` に最新CSVを上書き

2. **更新バッチを実行**
   ```powershell
   cd "C:\Users\PC\dev\ForStep2\output\test251205 1000jitosho\sqlite_local_setup"
   .\run_etl.bat
   ```

3. **完了メッセージを確認**
   ```
   ✓ Silverテーブル更新完了
   ✓ Goldテーブル更新完了
   更新日時: 2025-12-05 10:30:00
   ```

---

## データの見方・分析方法

### 方法1: DB Browser for SQLite（推奨）

**無料のSQLiteビューワー**

1. ダウンロード: https://sqlitebrowser.org/
2. インストール後、`database\analytics.db` を開く
3. 「Browse Data」タブでテーブル一覧表示

### 方法2: Excel で直接接続

**Excel 2016以降**

1. Excelを開く
2. 「データ」→「データの取得」→「データベースから」→「SQLiteデータベース」
3. `database\analytics.db` を選択
4. 必要なテーブルを選択してロード

**おすすめテーブル:**
- `gold_kpi_dashboard`: 全KPI一覧
- `gold_kpi_inventory_turnover`: 在庫回転率詳細
- `gold_location_kpi_summary`: 拠点別サマリー

### 方法3: Power BI Desktop

1. Power BIを開く
2. 「データを取得」→「その他」→「ODBC」
3. SQLite ODBC Driverを使用して接続

### 方法4: SQLクエリで分析

`queries\` フォルダ内のSQLファイルを使用：

```sql
-- 拠点別の在庫回転日数トレンド
SELECT 
    l.location_name,
    k.year_month,
    k.inventory_turnover_days,
    k.inventory_health_status
FROM gold_kpi_inventory_turnover k
JOIN silver_dim_location l ON k.location_key = l.location_key
WHERE k.year_month >= '2024-01'
ORDER BY k.year_month, l.location_name;
```

---

## 主要KPIの見方

### KPI1: 在庫回転率

**テーブル:** `gold_kpi_inventory_turnover`

**重要カラム:**
- `inventory_turnover_days`: 在庫が何日で売れるか
- `inventory_health_status`: OPTIMAL/NORMAL/OVERSTOCK/DEAD_STOCK

**良い状態:**
- 30日以内 → OPTIMAL
- デッドストック比率 < 10%

### KPI2: 調達リードタイム遵守率

**テーブル:** `gold_kpi_procurement_lead_time`

**重要カラム:**
- `lead_time_adherence_rate`: 期日内納品率（0.95 = 95%）
- `supplier_performance_grade`: A/B/C/Dランク

**良い状態:**
- 遵守率 > 95% (A評価)
- 遅延金額 < 総発注額の5%

### KPI3: 物流コスト対売上高比率

**テーブル:** `gold_kpi_logistics_cost_ratio`

**重要カラム:**
- `logistics_cost_to_sales_ratio`: 物流コスト÷売上
- `expedite_cost_ratio`: 緊急配送費比率

**良い状態:**
- コスト比率 < 5%
- 緊急配送費比率 < 10%

### KPI4: 間接材調達コスト削減率

**テーブル:** `gold_kpi_indirect_material_cost_reduction`

**重要カラム:**
- `cost_reduction_rate`: 削減率（0.10 = 10%削減）
- `total_cost_savings`: 削減金額（円）

**良い状態:**
- 削減率 > 5%
- MRO比率が安定

### KPI5: キャッシュ・コンバージョン・サイクル

**テーブル:** `gold_kpi_cash_conversion_cycle`

**重要カラム:**
- `ccc_days`: 現金が拘束される日数
- `ccc_improvement_flag`: 前月比改善フラグ

**良い状態:**
- CCC < 60日
- 前月比で短縮傾向

---

## よく使う分析クエリ

### 📊 月次KPIトレンド分析

```sql
SELECT 
    year_month,
    AVG(inventory_turnover_days) AS avg_inventory_days,
    AVG(lead_time_adherence_rate) AS avg_lead_time_rate,
    AVG(logistics_cost_to_sales_ratio) AS avg_logistics_cost,
    AVG(cost_reduction_rate) AS avg_cost_reduction,
    AVG(ccc_days) AS avg_ccc_days
FROM gold_kpi_dashboard
GROUP BY year_month
ORDER BY year_month;
```

### 📍 拠点別パフォーマンス比較

```sql
SELECT 
    location_name,
    overall_performance_score,
    avg_inventory_turnover_days,
    avg_lead_time_adherence_rate,
    total_cost_savings
FROM gold_location_kpi_summary
WHERE year_month = '2024-12'
ORDER BY overall_performance_score DESC;
```

### 🏢 サプライヤー評価

```sql
SELECT 
    partner_name,
    supplier_performance_grade,
    lead_time_adherence_rate,
    total_cost_savings,
    overall_supplier_status
FROM gold_supplier_performance
WHERE year_month = '2024-12'
ORDER BY lead_time_adherence_rate DESC;
```

---

## トラブルシューティング

### ❌ エラー: `ModuleNotFoundError: No module named 'pandas'`

**原因:** Pandasがインストールされていない

**解決策:**
```bash
pip install pandas openpyxl
```

### ❌ エラー: `FileNotFoundError: CSV file not found`

**原因:** CSVファイルのパスが間違っている

**解決策:**
1. `scripts\load_bronze_to_silver.py` の6-7行目を確認
2. `BRONZE_DATA_PATH` を正しいパスに修正

### ❌ エラー: `database is locked`

**原因:** データベースファイルが他のプログラムで開かれている

**解決策:**
1. DB Browser / Excelを閉じる
2. もう一度スクリプトを実行

### ❌ データが更新されない

**確認事項:**
1. CSVファイルの日付が最新か？
2. `database\analytics.db` のファイルサイズが増えているか？
3. エラーメッセージが出ていないか？

**リセット方法:**
```bash
# データベースを削除して再構築
del database\analytics.db
python scripts\load_bronze_to_silver.py
python scripts\calculate_gold_kpis.py
```

---

## サポート・問い合わせ

### 📧 連絡先

- **技術サポート:** データエンジニアリングチーム
- **業務サポート:** 物流・購買部門 データ分析チーム

### 📚 参考資料

- 詳細設計書: `../docs/medallion_architecture_design.md`
- SQL定義: `../sql/` フォルダ内

---

## 更新履歴

| 日付 | バージョン | 変更内容 |
|------|-----------|---------|
| 2025-12-05 | 1.0 | 初版作成 |

---

**このREADMEで分からないことがあれば、遠慮なく問い合わせください！**
