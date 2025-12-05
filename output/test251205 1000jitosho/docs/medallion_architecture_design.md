# メダリオンアーキテクチャ設計書

**自動車製造業 物流・間接材購買部門 データ基盤**  
**作成日:** 2025年12月5日  
**バージョン:** 1.0

---

## 目次

1. [概要](#概要)
2. [アーキテクチャ概要](#アーキテクチャ概要)
3. [Silverレイヤー設計](#silverレイヤー設計)
4. [Goldレイヤー設計](#goldレイヤー設計)
5. [データリネージ](#データリネージ)
6. [KPI詳細仕様](#kpi詳細仕様)
7. [運用ガイド](#運用ガイド)
8. [付録](#付録)

---

## 概要

### 目的

本データ基盤は、自動車製造業の物流部門および間接材購買部門におけるROIC（投下資本利益率）の改善を目的としています。メダリオンアーキテクチャ（Bronze → Silver → Gold）を採用し、5つの重要KPIをモニタリングします。

### ROIC改善へのアプローチ

```
ROIC = NOPAT（税引後営業利益） ÷ 投下資本

【NOPAT向上施策】
├─ KPI3: 物流コスト対売上高比率 → 物流コスト削減
└─ KPI4: 間接材調達コスト削減率 → 購買コスト削減

【投下資本削減施策】
├─ KPI1: 在庫回転率 → 在庫圧縮
├─ KPI2: 調達リードタイム遵守率 → 安全在庫削減
└─ KPI5: キャッシュ・コンバージョン・サイクル → 運転資本最適化
```

### 対象データソース（Bronzeレイヤー）

| システム | データソース | 更新頻度 |
|---------|------------|---------|
| **ERP** | product_master, location_master, partner_master, pricing_conditions, bom_master, sales_order_header/item | 日次 |
| **P2P** | procurement_header/item, partner_master, bom_master | 日次 |
| **WMS** | monthly_inventory, current_inventory, location_master | 月次/リアルタイム |
| **MES** | shipment_header/item, location_master, partner_master | 日次 |
| **TMS** | transportation_cost, location_master, partner_master | 月次 |

---

## アーキテクチャ概要

### メダリオンアーキテクチャの構成

```
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (生データ)                    │
│  ERP / P2P / WMS / MES / TMS からの生データ                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (標準化・統合)                  │
│  【ディメンション】5テーブル                                  │
│  【ファクト】5テーブル                                        │
│  【集計中間】2テーブル                                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                GOLD LAYER (ビジネスKPI)                       │
│  【KPIテーブル】5テーブル                                     │
│  【分析ビュー】4ビュー                                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
                    [BI/分析ツール]
```

### ディレクトリ構造

```
C:\Users\PC\dev\ForStep2\output\test251205 1000jitosho\
├── docs\
│   └── medallion_architecture_design.md (本ドキュメント)
├── sql\
│   ├── silver\
│   │   ├── 01_create_dimensions.sql
│   │   ├── 02_create_facts.sql
│   │   └── 03_create_aggregations.sql
│   └── gold\
│       ├── 01_create_kpi_tables_part1.sql
│       ├── 02_create_kpi_tables_part2.sql
│       └── 03_create_views.sql
```

---

## Silverレイヤー設計

### テーブル一覧

#### ディメンションテーブル（5個）

| テーブル名 | 説明 | 主キー | レコード数目安 |
|-----------|------|-------|---------------|
| `dim_product` | 製品マスタ | product_key | ~20件 |
| `dim_location` | 拠点マスタ | location_key | ~5件 |
| `dim_partner` | 取引先マスタ | partner_key | ~40件 |
| `dim_material` | 材料マスタ | material_key | ~100件 |
| `dim_date` | 日付ディメンション | date_key | 1,461件（4年分） |

#### ファクトテーブル（5個）

| テーブル名 | 説明 | 粒度 | 更新頻度 |
|-----------|------|------|---------|
| `fact_inventory_daily` | 日次在庫ファクト | 日次×製品×拠点 | 日次 |
| `fact_procurement` | 調達ファクト | 発注明細行 | 日次 |
| `fact_sales_order` | 受注ファクト | 受注明細行 | 日次 |
| `fact_shipment` | 出荷ファクト | 出荷明細行 | 日次 |
| `fact_transportation_cost` | 輸送コストファクト | コスト明細行 | 月次 |

#### 集計中間テーブル（2個）

| テーブル名 | 説明 | 集計単位 |
|-----------|------|---------|
| `agg_inventory_monthly` | 月次在庫集計 | 月×製品×拠点 |
| `agg_procurement_performance` | 調達パフォーマンス集計 | 月×サプライヤー×材料タイプ |

### 主要テーブル定義例

#### dim_product（製品マスタ）

```sql
CREATE TABLE silver.dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name NVARCHAR(200),
    brand_name VARCHAR(100),
    item_group VARCHAR(100),           -- compact_car, suv, minivan等
    item_hierarchy VARCHAR(50),         -- ICE, EV, HEV
    detail_category VARCHAR(100),
    country_of_origin VARCHAR(3),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);
```

**分析軸として重要なカラム:**
- `item_group`: 車種別分析
- `item_hierarchy`: パワートレイン別分析（ICE/EV/HEV）

#### fact_procurement（調達ファクト）

```sql
CREATE TABLE silver.fact_procurement (
    procurement_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    purchase_order_id VARCHAR(50) NOT NULL,
    line_number INT NOT NULL,
    order_date DATE,
    order_date_key INT,
    expected_delivery_date DATE,
    actual_received_date DATE,
    supplier_key INT,
    location_key INT,
    material_key INT,
    quantity DECIMAL(18,4),
    unit_price_ex_tax DECIMAL(18,4),
    reference_price_ex_tax DECIMAL(18,4),      -- 基準価格
    price_variance DECIMAL(18,4),               -- 価格差異
    material_type VARCHAR(50),                  -- direct / indirect
    is_mro BIT,                                 -- MRO判定
    lead_time_days INT,                         -- リードタイム
    lead_time_variance_days INT,                -- リードタイム差異
    is_on_time BIT,                             -- 期日内納品フラグ
    payment_date DATE,
    FOREIGN KEY (order_date_key) REFERENCES silver.dim_date(date_key),
    FOREIGN KEY (supplier_key) REFERENCES silver.dim_partner(partner_key)
);
```

**KPI算出に重要なカラム:**
- `is_on_time`: リードタイム遵守率算出
- `price_variance`: コスト削減額算出
- `is_mro`: 間接材分析
- `payment_date`: CCC算出（DPO計算）

---

## Goldレイヤー設計

### KPIテーブル一覧

| KPI | テーブル名 | ROIC貢献 | 主要指標 |
|-----|-----------|---------|---------|
| **KPI1** | `kpi_inventory_turnover` | 投下資本削減 | 在庫回転率、在庫回転日数 |
| **KPI2** | `kpi_procurement_lead_time` | 投下資本削減 & NOPAT向上 | リードタイム遵守率、サプライヤー評価 |
| **KPI3** | `kpi_logistics_cost_ratio` | NOPAT向上 | 物流コスト対売上高比率 |
| **KPI4** | `kpi_indirect_material_cost_reduction` | NOPAT向上 | コスト削減率、MRO比率 |
| **KPI5** | `kpi_cash_conversion_cycle` | 投下資本削減 | CCC日数（DIO+DSO-DPO） |

### 分析ビュー一覧

| ビュー名 | 用途 |
|---------|------|
| `vw_kpi_dashboard` | 全KPI統合ダッシュボード |
| `vw_location_kpi_summary` | 拠点別KPI総合評価 |
| `vw_supplier_performance` | サプライヤー総合評価 |
| `vw_roic_improvement_trend` | ROIC改善トレンド分析 |

---

## データリネージ

### Bronze → Silver → Gold のデータフロー

#### KPI1: 在庫回転率

```
Bronze (WMS/monthly_inventory) 
    → Silver (fact_inventory_daily)
    → Gold (kpi_inventory_turnover)

Bronze (ERP/sales_order_item) 
    → Silver (fact_sales_order) 
    → Gold (kpi_inventory_turnover) [COGS算出]
```

**算出ロジック:**
```
在庫回転率 = 売上原価（COGS） / 平均在庫金額
在庫回転日数 = 365日 / 在庫回転率
```

#### KPI2: 調達リードタイム遵守率

```
Bronze (P2P/procurement_header + procurement_item)
    → Silver (fact_procurement) [lead_time計算]
    → Gold (kpi_procurement_lead_time)
```

**算出ロジック:**
```
遵守率 = 期日内納品件数 / 総発注件数
リードタイム差異 = 実績受領日 - 期待納期
```

#### KPI3: 物流コスト対売上高比率

```
Bronze (TMS/transportation_cost)
    → Silver (fact_transportation_cost)
    → Gold (kpi_logistics_cost_ratio)

Bronze (MES/shipment_item)
    → Silver (fact_shipment)
    → Gold (kpi_logistics_cost_ratio) [出荷数量]

Bronze (ERP/sales_order_item)
    → Silver (fact_sales_order)
    → Gold (kpi_logistics_cost_ratio) [売上金額]
```

**算出ロジック:**
```
物流コスト比率 = 総物流費用 / 売上高
総物流費用 = 輸送費 + 緊急配送費 + その他
```

#### KPI4: 間接材調達コスト削減率

```
Bronze (P2P/procurement_item) [reference_price vs unit_price]
    → Silver (fact_procurement) [price_variance計算]
    → Gold (kpi_indirect_material_cost_reduction)
```

**算出ロジック:**
```
コスト削減率 = (基準価格 - 実購入価格) / 基準価格
総削減額 = Σ(価格差異 × 数量)
MRO比率 = MRO調達額 / 総調達額
```

#### KPI5: キャッシュ・コンバージョン・サイクル

```
Bronze (WMS/monthly_inventory) 
    → Silver (fact_inventory_daily) 
    → Gold (kpi_cash_conversion_cycle) [DIO算出]

Bronze (ERP/sales_order_item)
    → Silver (fact_sales_order)
    → Gold (kpi_cash_conversion_cycle) [DSO算出]

Bronze (P2P/procurement_item + payment_date)
    → Silver (fact_procurement)
    → Gold (kpi_cash_conversion_cycle) [DPO算出]
```

**算出ロジック:**
```
DIO (在庫回転日数) = 平均在庫 / COGS × 365
DSO (売掛金回収日数) = 平均売掛金 / 売上 × 365
DPO (買掛金支払日数) = 平均買掛金 / 仕入 × 365
CCC = DIO + DSO - DPO
```

---

## KPI詳細仕様

### KPI1: 在庫回転率（Inventory Turnover Ratio）

#### ビジネス定義
製品在庫が平均何日で販売されるかを示す指標。値が低いほど在庫効率が良い。

#### 算出式
```sql
inventory_turnover_ratio = cogs_amount / avg_inventory_value
inventory_turnover_days = 365 / inventory_turnover_ratio
```

#### 評価基準
| ステータス | 在庫回転日数 | 評価 |
|-----------|------------|------|
| OPTIMAL | < 30日 | 最適な在庫レベル |
| NORMAL | 30-60日 | 標準的な在庫レベル |
| OVERSTOCK | 60-90日 | 過剰在庫の可能性 |
| DEAD_STOCK | > 90日 | デッドストック化 |

#### モニタリング項目
- 月次在庫回転日数（全体・拠点別・製品別）
- デッドストック比率（90日以上滞留）
- 製品カテゴリ別回転率
- 期首在庫 vs 期末在庫トレンド

#### 改善分析軸
- 拠点別（location_id）
- 製品グループ別（item_group）
- パワートレイン別（item_hierarchy: ICE/EV/HEV）
- 時系列トレンド（year_month）

### KPI2: 調達リードタイム遵守率

#### ビジネス定義
サプライヤーが約束した納期を守った発注の割合。高いほど安全在庫を削減可能。

#### 算出式
```sql
lead_time_adherence_rate = on_time_orders / total_orders
supplier_performance_grade = 
    CASE 
        WHEN rate >= 0.95 THEN 'A'
        WHEN rate >= 0.85 THEN 'B'
        WHEN rate >= 0.70 THEN 'C'
        ELSE 'D'
    END
```

#### 評価基準
| グレード | 遵守率 | サプライヤー評価 |
|---------|-------|-----------------|
| A | ≥ 95% | 戦略的パートナー |
| B | 85-95% | 優良サプライヤー |
| C | 70-85% | 条件付きサプライヤー |
| D | < 70% | 改善要請 |

#### モニタリング項目
- サプライヤー別遵守率
- 材料カテゴリ別遵守率
- 平均リードタイム（計画 vs 実績）
- 遅延発注の影響金額
- 緊急配送費用発生率

#### 改善分析軸
- サプライヤー別（supplier_id, account_group）
- 材料タイプ別（material_type: direct/indirect）
- 材料カテゴリ別（material_category）
- 拠点別（location_id）
- 発注金額帯別

### KPI3: 物流コスト対売上高比率

#### ビジネス定義
売上高に対する物流費用の割合。低いほど物流効率が良い。

#### 算出式
```sql
logistics_cost_to_sales_ratio = total_logistics_cost / total_sales_amount
total_logistics_cost = freight_cost + expedite_cost + handling_cost
expedite_cost_ratio = expedite_cost / total_logistics_cost
```

#### 評価基準
| グレード | コスト比率 | 評価 |
|---------|-----------|------|
| EXCELLENT | < 3% | 優秀 |
| GOOD | 3-5% | 良好 |
| AVERAGE | 5-8% | 平均的 |
| POOR | > 8% | 改善必要 |

#### モニタリング項目
- 月次物流コスト比率トレンド
- 輸送モード別コスト（road, air, sea）
- 緊急配送費用比率
- 配送遅延率
- 拠点間輸送効率

#### 改善分析軸
- 輸送モード別（transportation_mode）
- コストタイプ別（cost_type: freight, expedite）
- 拠点別（location_id）
- 運送会社別（carrier_name）
- 配送ステータス別（delivery_status）

### KPI4: 間接材調達コスト削減率

#### ビジネス定義
基準価格に対する実購入価格の削減率。MRO（間接材）購買の効率性を示す。

#### 算出式
```sql
cost_reduction_rate = (total_reference_value - total_actual_value) / total_reference_value
total_cost_savings = Σ((reference_price - unit_price) × quantity)
mro_spend_ratio = mro_procurement_value / total_procurement_value
```

#### 評価基準
| グレード | 削減率 | 評価 |
|---------|-------|------|
| EXCELLENT | > 10% | 優秀 |
| GOOD | 5-10% | 良好 |
| AVERAGE | 0-5% | 平均的 |
| POOR | < 0% | 改善必要（価格上昇） |

#### モニタリング項目
- 月次コスト削減金額
- サプライヤー別価格競争力
- 基準価格対比実績（正負分布）
- MRO品目の集約率
- スポット購買比率

#### 改善分析軸
- 材料カテゴリ別（material_category）
- サプライヤー別（supplier_id）
- 拠点別（location_id）
- 部門別（department_code）
- プロジェクト別（project_code）

### KPI5: キャッシュ・コンバージョン・サイクル（CCC）

#### ビジネス定義
現金が事業サイクルで拘束される日数。短いほど運転資本効率が良い。

#### 算出式
```sql
DIO = (avg_inventory / cogs) × 365
DSO = (avg_accounts_receivable / revenue) × 365
DPO = (avg_accounts_payable / purchases) × 365
CCC = DIO + DSO - DPO
working_capital = inventory + accounts_receivable - accounts_payable
```

#### 評価基準
| グレード | CCC日数 | 評価 |
|---------|--------|------|
| EXCELLENT | < 30日 | 優秀 |
| GOOD | 30-60日 | 良好 |
| AVERAGE | 60-90日 | 平均的 |
| POOR | > 90日 | 改善必要 |

#### モニタリング項目
- 月次CCC日数トレンド
- DIO/DSO/DPO個別モニタリング
- 運転資本金額
- 前月比改善/悪化
- 拠点別運転資本効率

#### 改善分析軸
- 時系列トレンド（year_month）
- 拠点別（location_id）
- 製品別（product_id）
- 顧客別（customer_id）
- サプライヤー別（supplier_id）

---

## 運用ガイド

### データ更新スケジュール

| レイヤー | テーブル種別 | 更新頻度 | 更新タイミング | 依存関係 |
|---------|------------|---------|--------------|---------|
| **Bronze** | 生データ | 日次 | 02:00 | - |
| **Silver** | ディメンション | 日次 | 03:00 | Bronze完了後 |
| **Silver** | ファクト | 日次 | 04:00 | ディメンション完了後 |
| **Silver** | 集計中間 | 月次 | 月初1日 05:00 | ファクト完了後 |
| **Gold** | KPIテーブル | 月次 | 月初1日 06:00 | Silver集計完了後 |

### SQL実行順序

#### 初回セットアップ

```bash
# 1. Silverレイヤー
sqlcmd -S server -d database -i "sql\silver\01_create_dimensions.sql"
sqlcmd -S server -d database -i "sql\silver\02_create_facts.sql"
sqlcmd -S server -d database -i "sql\silver\03_create_aggregations.sql"

# 2. Goldレイヤー
sqlcmd -S server -d database -i "sql\gold\01_create_kpi_tables_part1.sql"
sqlcmd -S server -d database -i "sql\gold\02_create_kpi_tables_part2.sql"
sqlcmd -S server -d database -i "sql\gold\03_create_views.sql"
```

#### 日次更新（ファクトテーブル）

```sql
-- Bronzeデータ取り込み後に実行
TRUNCATE TABLE silver.fact_inventory_daily;
TRUNCATE TABLE silver.fact_procurement;
TRUNCATE TABLE silver.fact_sales_order;
TRUNCATE TABLE silver.fact_shipment;
TRUNCATE TABLE silver.fact_transportation_cost;

-- 再投入
EXEC [bronze].[sp_load_silver_facts];
```

#### 月次更新（KPIテーブル）

```sql
-- 月初1日に実行
TRUNCATE TABLE gold.kpi_inventory_turnover;
TRUNCATE TABLE gold.kpi_procurement_lead_time;
TRUNCATE TABLE gold.kpi_logistics_cost_ratio;
TRUNCATE TABLE gold.kpi_indirect_material_cost_reduction;
TRUNCATE TABLE gold.kpi_cash_conversion_cycle;

-- 再計算
EXEC [silver].[sp_calculate_gold_kpis];
```

### データ品質チェック

#### 必須チェック項目

```sql
-- 1. NULL値チェック
SELECT 'fact_procurement' AS table_name, COUNT(*) AS null_count
FROM silver.fact_procurement
WHERE supplier_key IS NULL OR order_date IS NULL;

-- 2. 重複チェック
SELECT purchase_order_id, line_number, COUNT(*)
FROM silver.fact_procurement
GROUP BY purchase_order_id, line_number
HAVING COUNT(*) > 1;

-- 3. 参照整合性チェック
SELECT f.procurement_key
FROM silver.fact_procurement f
LEFT JOIN silver.dim_partner p ON f.supplier_key = p.partner_key
WHERE p.partner_key IS NULL;

-- 4. データ範囲チェック
SELECT MIN(order_date), MAX(order_date), COUNT(*)
FROM silver.fact_procurement
WHERE order_date > GETDATE() OR order_date < '2022-01-01';
```

### パフォーマンス最適化

#### インデックスメンテナンス

```sql
-- インデックスの再構築（週次）
ALTER INDEX ALL ON silver.fact_procurement REBUILD;
ALTER INDEX ALL ON silver.fact_inventory_daily REBUILD;
ALTER INDEX ALL ON gold.kpi_inventory_turnover REBUILD;

-- 統計情報の更新（日次）
UPDATE STATISTICS silver.fact_procurement;
UPDATE STATISTICS silver.fact_inventory_daily;
```

#### パーティショニング戦略

```sql
-- 大規模データの場合、日付でパーティショニング
CREATE PARTITION FUNCTION pf_monthly_date (DATE)
AS RANGE RIGHT FOR VALUES 
('2022-01-01', '2022-02-01', '2022-03-01', ...);

CREATE PARTITION SCHEME ps_monthly_date
AS PARTITION pf_monthly_date
ALL TO ([PRIMARY]);

-- テーブル作成時にパーティション指定
CREATE TABLE silver.fact_procurement (
    ...
) ON ps_monthly_date(order_date);
```

### トラブルシューティング

#### よくある問題と対処法

**問題1: KPI値が異常に高い/低い**
```sql
-- 原因調査
SELECT 
    year_month,
    avg_inventory_value,
    cogs_amount,
    inventory_turnover_days
FROM gold.kpi_inventory_turnover
WHERE inventory_turnover_days > 180 OR inventory_turnover_days < 5;

-- 元データ確認
SELECT product_key, AVG(inventory_value)
FROM silver.fact_inventory_daily
WHERE snapshot_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY product_key;
```

**問題2: データ投入エラー**
```sql
-- エラーログ確認
SELECT * FROM [dbo].[ETL_Error_Log]
WHERE execution_date = CAST(GETDATE() AS DATE)
ORDER BY error_timestamp DESC;

-- 外部キー違反確認
SELECT f.supplier_key
FROM bronze.p2p_procurement_header f
WHERE NOT EXISTS (SELECT 1 FROM silver.dim_partner p WHERE f.supplier_id = p.partner_id);
```

---

## 付録

### テーブルサイズ見積もり

| テーブル | 行数（年間） | 1行サイズ | 年間容量 | 備考 |
|---------|------------|----------|---------|------|
| fact_inventory_daily | 36,500 | 120 bytes | ~4 MB | 20製品×5拠点×365日 |
| fact_procurement | 150,000 | 250 bytes | ~35 MB | 年間14,407件（実績ベース） |
| fact_sales_order | 15,000 | 180 bytes | ~3 MB | 年間1,464件（実績ベース） |
| fact_shipment | 25,000 | 200 bytes | ~5 MB | 年間2,262件（実績ベース） |
| kpi_inventory_turnover | 1,200 | 150 bytes | ~180 KB | 20製品×5拠点×12月 |

### 用語集

| 用語 | 説明 |
|------|------|
| **ROIC** | Return On Invested Capital（投下資本利益率） = NOPAT / 投下資本 |
| **NOPAT** | Net Operating Profit After Tax（税引後営業利益） |
| **COGS** | Cost of Goods Sold（売上原価） |
| **DIO** | Days Inventory Outstanding（在庫回転日数） |
| **DSO** | Days Sales Outstanding（売掛金回収日数） |
| **DPO** | Days Payable Outstanding（買掛金支払日数） |
| **CCC** | Cash Conversion Cycle（キャッシュ・コンバージョン・サイクル） |
| **MRO** | Maintenance, Repair, and Operations（間接材） |
| **ICE** | Internal Combustion Engine（内燃機関） |
| **EV** | Electric Vehicle（電気自動車） |
| **HEV** | Hybrid Electric Vehicle（ハイブリッド車） |

### 変更履歴

| バージョン | 日付 | 変更内容 | 作成者 |
|-----------|------|---------|-------|
| 1.0 | 2025-12-05 | 初版作成 | データ基盤チーム |

---

## 問い合わせ先

データ基盤に関する問い合わせ:
- **技術担当:** データエンジニアリングチーム
- **業務担当:** 物流・購買部門 データ分析チーム

---

**以上**
