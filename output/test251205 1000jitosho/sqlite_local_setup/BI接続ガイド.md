# BIツール接続ガイド

このガイドでは、SQLiteデータベース（analytics.db）を各種BIツールに接続する方法を説明します。

---

## 目次

1. [Excel 2016以降](#1-excel-2016以降)
2. [Power BI Desktop](#2-power-bi-desktop)
3. [Tableau Desktop](#3-tableau-desktop)
4. [Google Data Studio](#4-google-data-studio)
5. [DB Browser for SQLite（簡易BI）](#5-db-browser-for-sqlite簡易bi)

---

## 1. Excel 2016以降

### 接続手順

#### ステップ1: ODBCドライバーのインストール

1. **SQLite ODBC Driverをダウンロード**
   - URL: http://www.ch-werner.de/sqliteodbc/
   - `sqliteodbc_w64.exe`（64bit版）をダウンロード
   - インストーラーを実行

#### ステップ2: Excelでデータベースに接続

1. **Excelを起動**
2. **データタブ → データの取得 → その他のデータソースから → ODBCから**
3. **データソース名（DSN）の選択画面で「なし」を選択**
4. **接続文字列を入力**:
   ```
   DRIVER=SQLite3 ODBC Driver;Database=C:\Users\PC\dev\ForStep2\output\test251205 1000jitosho\sqlite_local_setup\database\analytics.db;
   ```
5. **接続をクリック**
6. **ナビゲーターでテーブルを選択**:
   - `gold_kpi_inventory_turnover`（在庫回転率）
   - `gold_kpi_procurement_lead_time`（調達リードタイム）
   - `gold_kpi_logistics_cost_ratio`（物流コスト）
   - `gold_kpi_indirect_material_cost_reduction`（間接材コスト削減）
   - `gold_kpi_cash_conversion_cycle`（CCC）
7. **読み込みをクリック**

#### ステップ3: ピボットテーブルで分析

```excel
推奨ピボットテーブル構成:

【在庫回転率の月次推移】
- 行: year_month
- 値: AVG(inventory_turnover_ratio)
- グラフ: 折れ線グラフ

【拠点別KPI比較】
- 行: location_name
- 列: KPI項目
- 値: KPI値
- グラフ: 横棒グラフ
```

### トラブルシューティング

**エラー: "ドライバーが見つかりません"**
- SQLite ODBC Driverが正しくインストールされているか確認
- 64bit版Excelには64bit版ドライバーが必要

**エラー: "データベースが開けません"**
- ファイルパスが正しいか確認（バックスラッシュをエスケープしない）
- ファイルがロックされていないか確認

---

## 2. Power BI Desktop

### 接続手順

#### ステップ1: Power BIでデータベースに接続

1. **Power BI Desktopを起動**
2. **ホームタブ → データを取得 → その他**
3. **ODBC を選択**
4. **データソース名（DSN）で「なし」を選択**
5. **詳細オプションを展開し、接続文字列を入力**:
   ```
   DRIVER=SQLite3 ODBC Driver;Database=C:\Users\PC\dev\ForStep2\output\test251205 1000jitosho\sqlite_local_setup\database\analytics.db;
   ```
6. **OK → ナビゲーターでテーブルを選択**
7. **変換データ または 読み込み**

#### ステップ2: リレーションシップの設定

Power Queryエディターで以下のリレーションシップを設定:

```
gold_kpi_inventory_turnover.location_key → silver_dim_location.location_key
gold_kpi_procurement_lead_time.supplier_key → silver_dim_partner.partner_key
```

#### ステップ3: ビジュアライゼーションの作成

**推奨ビジュアル構成**:

1. **KPIカード（5つのKPI）**
   - フィールド: `inventory_turnover_ratio`, `lead_time_adherence_rate`, etc.
   - 集計: 平均値
   - 条件付き書式: 目標達成率で色分け

2. **月次トレンド（折れ線グラフ）**
   - X軸: `year_month`
   - Y軸: 各KPI値
   - 凡例: KPI名

3. **拠点別比較（マトリックス）**
   - 行: `location_name`
   - 値: 各KPI値
   - 条件付き書式: ヒートマップ

4. **サプライヤーランキング（横棒グラフ）**
   - Y軸: `supplier_name`
   - X軸: `lead_time_adherence_rate`
   - 色: `evaluation`

#### サンプルDAXメジャー

```dax
// KPI1: 在庫回転率の平均
平均在庫回転率 = AVERAGE(gold_kpi_inventory_turnover[inventory_turnover_ratio])

// KPI1: 目標達成フラグ
在庫回転率達成 = IF([平均在庫回転率] >= 12, "達成", "未達")

// KPI5: CCC総合評価
CCC評価 = 
SWITCH(
    TRUE(),
    AVERAGE(gold_kpi_cash_conversion_cycle[cash_conversion_cycle]) <= 60, "優良",
    AVERAGE(gold_kpi_cash_conversion_cycle[cash_conversion_cycle]) <= 90, "良好",
    AVERAGE(gold_kpi_cash_conversion_cycle[cash_conversion_cycle]) <= 120, "要改善",
    "要注意"
)

// 前月比
在庫回転率_前月比 = 
VAR CurrentMonth = MAX(gold_kpi_inventory_turnover[year_month])
VAR PreviousMonth = 
    CALCULATE(
        AVERAGE(gold_kpi_inventory_turnover[inventory_turnover_ratio]),
        gold_kpi_inventory_turnover[year_month] = 
            EOMONTH(DATE(LEFT(CurrentMonth,4), RIGHT(CurrentMonth,2), 1), -1)
    )
RETURN
    [平均在庫回転率] - PreviousMonth
```

---

## 3. Tableau Desktop

### 接続手順

1. **Tableau Desktopを起動**
2. **左ペインで「その他」→「SQLite」を選択**
3. **データベースファイルを選択**:
   ```
   C:\Users\PC\dev\ForStep2\output\test251205 1000jitosho\sqlite_local_setup\database\analytics.db
   ```
4. **テーブルをドラッグ＆ドロップしてデータソースを構築**
5. **シートに移動してビジュアライゼーションを作成**

### 推奨ワークブック構成

**ダッシュボード1: KPIサマリー**
- 5つのKPIカード（最新月）
- 月次トレンドライン（全KPI）
- 目標達成率ゲージ

**ダッシュボード2: 拠点分析**
- 拠点別ヒートマップ（KPI × 拠点）
- 地理的分布図（都道府県別）
- ドリルダウン詳細表

**ダッシュボード3: サプライヤー分析**
- サプライヤーランキング（横棒グラフ）
- リードタイム分布図（ヒストグラム）
- コスト削減貢献度（バブルチャート）

---

## 4. Google Data Studio

### 接続手順（間接的な方法）

Google Data StudioはSQLiteに直接接続できないため、以下の方法で連携します:

#### 方法1: CSVエクスポート経由

1. **DB Browser for SQLiteでクエリを実行**
2. **結果をCSVエクスポート**
3. **Google Sheetsにインポート**
4. **Google Data StudioでGoogle Sheetsに接続**

#### 方法2: Google Cloud SQLに移行

1. **SQLiteデータをCloud SQLにインポート**
2. **Google Data StudioでCloud SQLに接続**

---

## 5. DB Browser for SQLite（簡易BI）

### グラフ作成機能

DB Browser for SQLiteには簡易的なグラフ作成機能があります。

#### ステップ1: クエリを実行

```sql
SELECT
    year_month,
    ROUND(AVG(inventory_turnover_ratio), 2) AS 在庫回転率
FROM gold_kpi_inventory_turnover
GROUP BY year_month
ORDER BY year_month;
```

#### ステップ2: 結果をグラフ化

1. **クエリ結果の右上にある「グラフ」アイコンをクリック**
2. **グラフタイプを選択（折れ線、棒グラフなど）**
3. **X軸: year_month**
4. **Y軸: 在庫回転率**
5. **保存またはエクスポート**

### プロット機能

**Plot Dock** タブを使用すると、より高度なグラフ作成が可能:

1. **ツールバー → View → Plot Dock**
2. **テーブルを選択: `gold_kpi_inventory_turnover`**
3. **X軸フィールド: `year_month`**
4. **Y軸フィールド: `inventory_turnover_ratio`**
5. **グラフタイプ: Line Plot**
6. **色分け: `evaluation`**

---

## データ更新の自動化

### 1. スケジュールタスクの設定（Windows）

#### バッチファイル作成: `auto_update.bat`

```batch
@echo off
cd /d "C:\Users\PC\dev\ForStep2\output\test251205 1000jitosho\sqlite_local_setup"
call run_etl.bat
echo データ更新完了: %date% %time% >> update_log.txt
```

#### タスクスケジューラで自動実行

1. **タスクスケジューラを起動**
2. **基本タスクの作成**
3. **トリガー: 毎日 朝6:00**
4. **操作: プログラムの開始 → `auto_update.bat`**
5. **完了**

### 2. Power BIの自動更新

Power BI Desktopでデータソースを設定後:

1. **ファイル → オプションと設定 → データソース設定**
2. **スケジュール更新を有効化**
3. **更新頻度: 毎日**

---

## サンプルダッシュボード構成例

### エグゼクティブダッシュボード（経営層向け）

```
┌─────────────────────────────────────────────────────────┐
│ 【ROIC改善KPIダッシュボード】              2024年12月    │
├─────────────────────────────────────────────────────────┤
│ KPI1: 在庫回転率    │ KPI2: リードタイム  │ KPI3: 物流コスト │
│   12.5回/年 (達成) │   93.2% (要改善)   │   5.8% (良好)   │
├─────────────────────────────────────────────────────────┤
│ KPI4: 間接材削減率  │ KPI5: CCC          │ ROIC貢献度     │
│   2.8% (要改善)    │   72日 (良好)      │   +0.8%       │
├─────────────────────────────────────────────────────────┤
│                   月次トレンド（折れ線グラフ）            │
│                   ↗↗→↗↘（改善傾向）               │
├─────────────────────────────────────────────────────────┤
│ 拠点別総合評価        │ 要改善アクション件数             │
│ - 優秀拠点: 3拠点     │ - 優先度HIGH: 5件               │
│ - 要改善拠点: 2拠点   │ - 優先度MEDIUM: 12件            │
└─────────────────────────────────────────────────────────┘
```

### オペレーショナルダッシュボード（現場管理者向け）

```
┌─────────────────────────────────────────────────────────┐
│ 【拠点別詳細KPIダッシュボード】      拠点: 横浜工場      │
├─────────────────────────────────────────────────────────┤
│ 在庫回転率: 10.2回  │ 前月比: -1.5回     │ 目標: 12回     │
│ アクション: 安全在庫見直し、調達頻度アップ              │
├─────────────────────────────────────────────────────────┤
│ サプライヤー別リードタイム遵守率ランキング               │
│ 1. ABC商事: 98% (優良)                                  │
│ 2. XYZ工業: 95% (優良)                                  │
│ 3. 丸山産業: 82% (要改善) ← 改善要請済                  │
├─────────────────────────────────────────────────────────┤
│ 物流コスト内訳（円グラフ）                              │
│ - 入庫コスト: 35%                                       │
│ - 出庫コスト: 45%                                       │
│ - 倉庫コスト: 20%                                       │
└─────────────────────────────────────────────────────────┘
```

---

## トラブルシューティング

### 共通エラー

**エラー: "database is locked"**
- 他のアプリケーションがデータベースを開いている
- DB Browser for SQLiteを閉じる
- ETLスクリプトが実行中でないか確認

**エラー: "no such table"**
- テーブル名の前に `gold_` プレフィックスが必要
- `run_etl.bat` を実行してテーブルを作成

**パフォーマンスが遅い**
- SQLiteは大規模データには向かない（100万行以上は注意）
- インデックスを追加（create_gold_tables.pyに含まれる）
- 集約テーブルを活用

---

## まとめ

このガイドに従って、Excel、Power BI、Tableauなどの一般的なBIツールでSQLiteデータベースを活用できます。

**推奨アプローチ**:
- **非エンジニア**: Excel + ピボットテーブル
- **データアナリスト**: Power BI Desktop
- **データサイエンティスト**: Python (pandas) + Jupyter Notebook

**サポートが必要な場合**:
- SQLクエリサンプル: `queries/` フォルダを参照
- トラブルシューティング: `README.md` の「よくある質問」を確認
