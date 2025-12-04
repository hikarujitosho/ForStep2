# データ分析基盤セットアップガイド
## 間接材調達コスト削減率 KPIモニタリング

---

## 📋 目次
1. [概要](#概要)
2. [必要な環境](#必要な環境)
3. [セットアップ手順](#セットアップ手順)
4. [データベース構築](#データベース構築)
5. [KPI確認方法](#kpi確認方法)
6. [トラブルシューティング](#トラブルシューティング)

---

## 📌 概要

このガイドでは、非エンジニアの方でもローカル環境でデータ分析基盤を構築し、「間接材調達コスト削減率」KPIをモニタリングできるようにします。

### システム構成
```
Bronze (CSV)  →  Silver (SQLite)  →  Gold (SQLite)
   生データ         前処理済み         KPI集計
```

### KPI定義
- **間接材調達コスト削減率**: 前年同月比での間接材調達額の削減率を算出
- **分析軸**: 全社/サプライヤー別/資材カテゴリ別

---

## 💻 必要な環境

### 1. Python 3.8以降
Pythonがインストールされているか確認してください。

**確認方法（PowerShellで実行）:**
```powershell
python --version
```

**インストールされていない場合:**
1. [Python公式サイト](https://www.python.org/downloads/)からダウンロード
2. インストール時に「Add Python to PATH」にチェックを入れる

### 2. 必要なPythonパッケージ
以下のパッケージが必要です:
- pandas
- sqlite3 (Python標準ライブラリ)

---

## 🚀 セットアップ手順

### ステップ1: フォルダ構成の確認

以下のフォルダ構成になっていることを確認してください:

```
C:\Users\PC\dev\ForStep2\
├── data\
│   ├── Bronze\
│   │   └── P2P\
│   │       ├── 調達伝票_header.csv
│   │       ├── 調達伝票_item.csv
│   │       └── 取引先マスタ.csv
│   └── (kpi_database.db は自動作成されます)
├── scripts\
│   ├── 01_create_database_schema.sql
│   └── 02_etl_bronze_to_gold.py
├── logs\
│   └── (ログファイルが自動作成されます)
└── data_definition\
    └── KPI_間接材調達コスト削減率_テーブル設計書.md
```

### ステップ2: 必要なパッケージのインストール

PowerShellを開いて、以下のコマンドを実行してください:

```powershell
# プロジェクトディレクトリに移動
cd C:\Users\PC\dev\ForStep2

# pandasをインストール
pip install pandas
```

### ステップ3: データベーススキーマの作成

PowerShellで以下のコマンドを実行:

```powershell
# SQLiteでデータベースを作成し、スキーマを適用
sqlite3 data\kpi_database.db < scripts\01_create_database_schema.sql
```

**sqlite3コマンドが使えない場合:**
1. [SQLite公式サイト](https://www.sqlite.org/download.html)からダウンロード
2. または次のステップで自動的にデータベースが作成されます

---

## 🔧 データベース構築

### ETLスクリプトの実行

PowerShellで以下のコマンドを実行してください:

```powershell
# プロジェクトディレクトリに移動
cd C:\Users\PC\dev\ForStep2

# ETLスクリプトを実行
python scripts\02_etl_bronze_to_gold.py
```

### 実行結果の例
```
============================================================
ETL処理開始
開始時刻: 2025-12-05 10:30:00
============================================================

============================================================
CSVファイルの存在確認
============================================================
✓ header: C:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_header.csv
✓ item: C:\Users\PC\dev\ForStep2\data\Bronze\P2P\調達伝票_item.csv
✓ supplier: C:\Users\PC\dev\ForStep2\data\Bronze\P2P\取引先マスタ.csv
すべてのCSVファイルが存在します

============================================================
CSVファイルの読み込み
============================================================
読み込み中: 調達伝票_header.csv
  レコード数: 1,234
読み込み中: 調達伝票_item.csv
  レコード数: 5,678
...

============================================================
ETL処理完了
終了時刻: 2025-12-05 10:30:45
処理時間: 45.23秒
============================================================
```

---

## 📊 KPI確認方法

### 方法1: SQLiteコマンドラインで確認

```powershell
# データベースに接続
sqlite3 C:\Users\PC\dev\ForStep2\data\kpi_database.db

# 直近12ヶ月の全社削減率トレンド
SELECT * FROM v_kpi_overall_trend;

# サプライヤー別削減貢献ランキング
SELECT * FROM v_kpi_supplier_ranking WHERE year_month = '2025-11';

# カテゴリ別削減率サマリー
SELECT * FROM v_kpi_category_summary WHERE year_month = '2025-11';

# 終了
.quit
```

### 方法2: Pythonで確認

`scripts`フォルダに以下のスクリプトを作成してください:

**ファイル名: `03_view_kpi.py`**

```python
import sqlite3
import pandas as pd

# データベース接続
conn = sqlite3.connect(r"C:\Users\PC\dev\ForStep2\data\kpi_database.db")

# 全社削減率トレンド
print("=" * 60)
print("全社削減率トレンド（直近12ヶ月）")
print("=" * 60)
df = pd.read_sql_query("SELECT * FROM v_kpi_overall_trend", conn)
print(df.to_string(index=False))
print()

# サプライヤー別ランキング（直近月）
print("=" * 60)
print("サプライヤー別削減貢献ランキング（TOP10）")
print("=" * 60)
df = pd.read_sql_query("""
    SELECT * FROM v_kpi_supplier_ranking 
    WHERE year_month = (SELECT MAX(year_month) FROM v_kpi_supplier_ranking)
    LIMIT 10
""", conn)
print(df.to_string(index=False))
print()

# データ品質チェック
print("=" * 60)
print("データ品質チェック")
print("=" * 60)
df = pd.read_sql_query("SELECT * FROM v_data_completeness_check LIMIT 5", conn)
print(df.to_string(index=False))

conn.close()
```

**実行方法:**
```powershell
python scripts\03_view_kpi.py
```

### 方法3: Excel/Power BIでの可視化

1. Excel または Power BI を開く
2. 「データの取得」から「SQLite」を選択
3. データベースファイルを指定: `C:\Users\PC\dev\ForStep2\data\kpi_database.db`
4. 以下のビューを選択:
   - `v_kpi_overall_trend` - 全社トレンド
   - `v_kpi_supplier_ranking` - サプライヤーランキング
   - `v_kpi_category_summary` - カテゴリ別サマリー

---

## 📈 主要なビューとテーブル

### Goldテーブル

1. **gold_indirect_material_cost_monthly**
   - 月次の間接材調達コスト集計
   - サプライヤー別、カテゴリ別、拠点別に集計

2. **gold_indirect_material_cost_reduction_rate**
   - 前年同月比での削減率を計算
   - 複数の分析軸（全社/サプライヤー/カテゴリ）

### 便利なビュー

1. **v_kpi_overall_trend**
   - 全社レベルの削減率トレンド（直近12ヶ月）

2. **v_kpi_supplier_ranking**
   - サプライヤー別削減貢献度ランキング

3. **v_kpi_category_summary**
   - 資材カテゴリ別削減率サマリー

4. **v_quality_check_abnormal_price**
   - 異常な単価を検出

5. **v_quality_check_abnormal_change**
   - 前年同月比で異常な変動（±50%以上）を検出

---

## 🔍 トラブルシューティング

### エラー1: `ModuleNotFoundError: No module named 'pandas'`

**原因:** pandasがインストールされていません

**解決方法:**
```powershell
pip install pandas
```

### エラー2: `FileNotFoundError: CSVファイルが見つかりません`

**原因:** CSVファイルのパスが正しくありません

**解決方法:**
1. CSVファイルが以下の場所にあることを確認:
   ```
   C:\Users\PC\dev\ForStep2\data\Bronze\P2P\
   ```
2. ファイル名が正確に一致していることを確認（全角/半角、スペースなど）

### エラー3: `sqlite3.OperationalError: unable to open database file`

**原因:** データベースファイルのパスが正しくないか、権限がありません

**解決方法:**
1. `data`フォルダが存在することを確認
2. 管理者権限でPowerShellを実行
3. パスに日本語や特殊文字が含まれていないか確認

### エラー4: 前年データがNULLになる

**原因:** 前年のデータが存在しません

**解決方法:**
- これは正常な動作です
- データが1年分蓄積されると、前年同月比が計算されます
- 最初は`previous_year_amount`がNULLとなります

### エラー5: 文字化けが発生する

**原因:** 文字エンコーディングの問題

**解決方法:**
```powershell
# PowerShellの文字エンコーディングをUTF-8に設定
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
chcp 65001
```

---

## 📅 定期的なデータ更新

月次でデータを更新する手順:

### 1. 新しいCSVデータを配置
```
C:\Users\PC\dev\ForStep2\data\Bronze\P2P\
```
に最新のCSVファイルを上書き保存

### 2. ETLスクリプトを再実行
```powershell
cd C:\Users\PC\dev\ForStep2
python scripts\02_etl_bronze_to_gold.py
```

### 3. KPIを確認
```powershell
python scripts\03_view_kpi.py
```

---

## 📝 サンプルSQLクエリ

### 特定月のサプライヤー別削減額
```sql
SELECT 
    axis_value AS サプライヤー名,
    current_amount AS 当月調達額,
    previous_year_amount AS 前年同月調達額,
    amount_difference AS 削減額,
    cost_reduction_rate AS 削減率
FROM gold_indirect_material_cost_reduction_rate
WHERE analysis_axis = 'supplier'
  AND year_month = '2025-11'
ORDER BY amount_difference DESC;
```

### カテゴリ別の月次推移
```sql
SELECT 
    material_category,
    year_month,
    total_order_amount
FROM gold_indirect_material_cost_monthly
WHERE material_category = 'Office Equipment'
ORDER BY year_month DESC;
```

### データ品質チェック（異常値検出）
```sql
-- 異常な単価
SELECT * FROM v_quality_check_abnormal_price;

-- 異常な変動
SELECT * FROM v_quality_check_abnormal_change;
```

---

## 🎯 次のステップ

1. **ダッシュボード作成**
   - Power BIやTableauでビジュアライゼーション
   - Excelピボットテーブルでの分析

2. **アラート設定**
   - 削減率が負の値（コスト増加）の場合に通知
   - 異常な変動を検出した場合に通知

3. **他のKPIの追加**
   - 棚卸資産回転率
   - 調達リードタイム遵守率
   - 物流コスト売上高比率
   - 間接材在庫月数

---

## ❓ よくある質問

### Q1: データベースファイルはどこに保存されますか？
**A:** `C:\Users\PC\dev\ForStep2\data\kpi_database.db` に自動作成されます。

### Q2: CSVデータを更新したらどうすればいいですか？
**A:** ETLスクリプト (`02_etl_bronze_to_gold.py`) を再実行してください。既存データは上書きされます。

### Q3: エラーログはどこで確認できますか？
**A:** `C:\Users\PC\dev\ForStep2\logs\` フォルダに自動作成されます。

### Q4: Excelで直接開けますか？
**A:** SQLiteデータベースはExcelで直接開けません。ODBCドライバーまたはPower Queryを使用してください。

### Q5: データのバックアップは必要ですか？
**A:** はい。定期的に `kpi_database.db` ファイルをバックアップしてください。

---

## 📞 サポート

問題が解決しない場合は、以下の情報を含めてお問い合わせください:
1. エラーメッセージの全文
2. 実行したコマンド
3. ログファイルの内容 (`logs\` フォルダ内)
4. Python のバージョン (`python --version`)
5. pandas のバージョン (`pip show pandas`)

---

**最終更新日:** 2025年12月5日
**バージョン:** 1.0
