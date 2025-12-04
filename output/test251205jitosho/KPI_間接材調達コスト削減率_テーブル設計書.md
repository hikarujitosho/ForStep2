# KPI: 間接材調達コスト削減率 - テーブル設計書
---

## 概要
本設計書は、「間接材調達コスト削減率」KPIをモニタリングするためのメダリオンアーキテクチャにおけるSilver層およびGold層のテーブル定義とその作成方法を示します。

**KPI定義:**
```
間接材調達コスト削減率 = ((前年同月単価 - 当月単価) ÷ 前年同月単価) × 100
または
総調達額削減率 = ((前年度間接材総額 - 当年度間接材総額) ÷ 前年度間接材総額) × 100
```

---

## データレイヤー構成

```
Bronze (生データ)
  ├─ P2P/調達伝票_header.csv
  ├─ P2P/調達伝票_item.csv
  └─ P2P/取引先マスタ.csv
         ↓
Silver (クレンジング・正規化)
  ├─ silver_procurement_fact
  ├─ silver_supplier_dim
  └─ silver_material_dim
         ↓
Gold (KPI集計)
  ├─ gold_indirect_material_cost_monthly
  └─ gold_indirect_material_cost_reduction_rate
```

---

## Silver層テーブル定義

### 1. silver_procurement_fact (調達ファクトテーブル)

**概要:** Bronze層の調達伝票データをクレンジング・正規化した調達トランザクションのファクトテーブルです。間接材のみを抽出し、KPI算出および詳細分析の基礎データとなります。

| カラム名 | 型 | 制約 | 説明 |
|---------|-----|------|------|
| procurement_fact_id | VARCHAR | PK | サロゲートキー（purchase_order_id + line_number から生成） |
| purchase_order_id | VARCHAR | FK | 調達発注ID（元データのキー保持） |
| line_number | INTEGER |  | 明細行番号 |
| order_date | DATE | NOT NULL | 発注日（YYYY-MM-DD） |
| order_year_month | VARCHAR | NOT NULL | 発注年月（YYYY-MM形式、集計用） |
| supplier_key | VARCHAR | FK | サプライヤーディメンションキー |
| material_key | VARCHAR | FK | 資材ディメンションキー |
| location_id | VARCHAR |  | 発注拠点コード |
| material_id | VARCHAR |  | 資材ID（元データ保持） |
| material_name | VARCHAR |  | 資材名 |
| material_category | VARCHAR |  | 資材カテゴリ（例: Office Equipment, Factory Consumables） |
| unspsc_code | VARCHAR |  | UNSPSC分類コード |
| cost_center | VARCHAR |  | コストセンターコード |
| department_code | VARCHAR |  | 部門コード |
| quantity | DECIMAL(18,4) | NOT NULL | 発注数量 |
| unit_price_ex_tax | DECIMAL(18,4) | NOT NULL | 単価（税抜） |
| line_subtotal_ex_tax | DECIMAL(18,2) | NOT NULL | 明細小計（税抜） |
| line_total_incl_tax | DECIMAL(18,2) |  | 明細合計（税込） |
| received_date | DATE |  | 実際納品日 |
| received_quantity | DECIMAL(18,4) |  | 受領済数量 |
| order_status | VARCHAR |  | 発注ステータス |
| currency | VARCHAR |  | 通貨コード（ISO 4217） |
| created_at | TIMESTAMP | NOT NULL | レコード作成日時 |
| updated_at | TIMESTAMP | NOT NULL | レコード更新日時 |

**作成SQL:**
```sql
CREATE TABLE silver_procurement_fact (
    procurement_fact_id VARCHAR(100) PRIMARY KEY,
    purchase_order_id VARCHAR(50) NOT NULL,
    line_number INTEGER NOT NULL,
    order_date DATE NOT NULL,
    order_year_month VARCHAR(7) NOT NULL,
    supplier_key VARCHAR(50),
    material_key VARCHAR(100),
    location_id VARCHAR(50),
    material_id VARCHAR(50),
    material_name VARCHAR(200),
    material_category VARCHAR(100),
    unspsc_code VARCHAR(20),
    cost_center VARCHAR(50),
    department_code VARCHAR(50),
    quantity DECIMAL(18,4) NOT NULL,
    unit_price_ex_tax DECIMAL(18,4) NOT NULL,
    line_subtotal_ex_tax DECIMAL(18,2) NOT NULL,
    line_total_incl_tax DECIMAL(18,2),
    received_date DATE,
    received_quantity DECIMAL(18,4),
    order_status VARCHAR(50),
    currency VARCHAR(3) DEFAULT 'JPY',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス作成
CREATE INDEX idx_procurement_order_ym ON silver_procurement_fact(order_year_month);
CREATE INDEX idx_procurement_supplier ON silver_procurement_fact(supplier_key);
CREATE INDEX idx_procurement_material ON silver_procurement_fact(material_key);
CREATE INDEX idx_procurement_category ON silver_procurement_fact(material_category);
CREATE INDEX idx_procurement_location ON silver_procurement_fact(location_id);
CREATE INDEX idx_procurement_cost_center ON silver_procurement_fact(cost_center);
```

**データ投入SQL:**
```sql
INSERT INTO silver_procurement_fact (
    procurement_fact_id,
    purchase_order_id,
    line_number,
    order_date,
    order_year_month,
    supplier_key,
    material_key,
    location_id,
    material_id,
    material_name,
    material_category,
    unspsc_code,
    cost_center,
    department_code,
    quantity,
    unit_price_ex_tax,
    line_subtotal_ex_tax,
    line_total_incl_tax,
    received_date,
    received_quantity,
    order_status,
    currency
)
SELECT 
    -- サロゲートキー生成
    CONCAT(item.purchase_order_id, '-', LPAD(item.line_number::TEXT, 5, '0')) AS procurement_fact_id,
    item.purchase_order_id,
    item.line_number,
    header.order_date,
    -- 発注年月を YYYY-MM 形式で生成
    TO_CHAR(header.order_date, 'YYYY-MM') AS order_year_month,
    -- サプライヤーキー（supplier_id をそのまま使用）
    header.supplier_id AS supplier_key,
    -- 資材キー（material_id をそのまま使用）
    item.material_id AS material_key,
    header.location_id,
    item.material_id,
    item.material_name,
    item.material_category,
    item.unspsc_code,
    item.cost_center,
    item.department_code,
    item.quantity,
    item.unit_price_ex_tax,
    item.line_subtotal_ex_tax,
    item.line_total_incl_tax,
    item.received_date,
    item.received_quantity,
    header.order_status,
    COALESCE(header.currency, 'JPY') AS currency
FROM 
    bronze_p2p.調達伝票_item AS item
INNER JOIN 
    bronze_p2p.調達伝票_header AS header
    ON item.purchase_order_id = header.purchase_order_id
WHERE 
    -- 間接材のみを抽出
    item.material_type = 'indirect'
    -- キャンセル済みは除外
    AND header.order_status <> 'cancelled'
    -- 単価・数量が正常値のみ
    AND item.unit_price_ex_tax > 0
    AND item.quantity > 0;
```

---

### 2. silver_supplier_dim (サプライヤーディメンションテーブル)

**概要:** サプライヤー（取引先）の属性情報を正規化したディメンションテーブルです。

| カラム名 | 型 | 制約 | 説明 |
|---------|-----|------|------|
| supplier_key | VARCHAR | PK | サプライヤーディメンションキー（partner_id） |
| partner_id | VARCHAR | UNIQUE | 取引先ID（元データキー） |
| partner_name | VARCHAR | NOT NULL | 取引先名称 |
| partner_type | VARCHAR |  | 取引先区分（supplier固定） |
| partner_category | VARCHAR |  | 取引先カテゴリ（tier1_supplier等） |
| account_group | VARCHAR |  | アカウントグループ |
| country | VARCHAR |  | 国コード |
| region | VARCHAR |  | 地域区分 |
| payment_terms | VARCHAR |  | 支払条件 |
| is_active | VARCHAR |  | 有効フラグ |
| created_at | TIMESTAMP | NOT NULL | レコード作成日時 |
| updated_at | TIMESTAMP | NOT NULL | レコード更新日時 |

**作成SQL:**
```sql
CREATE TABLE silver_supplier_dim (
    supplier_key VARCHAR(50) PRIMARY KEY,
    partner_id VARCHAR(50) UNIQUE NOT NULL,
    partner_name VARCHAR(200) NOT NULL,
    partner_type VARCHAR(50),
    partner_category VARCHAR(100),
    account_group VARCHAR(50),
    country VARCHAR(10),
    region VARCHAR(50),
    payment_terms VARCHAR(50),
    is_active VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス作成
CREATE INDEX idx_supplier_name ON silver_supplier_dim(partner_name);
CREATE INDEX idx_supplier_category ON silver_supplier_dim(partner_category);
CREATE INDEX idx_supplier_active ON silver_supplier_dim(is_active);
```

**データ投入SQL:**
```sql
INSERT INTO silver_supplier_dim (
    supplier_key,
    partner_id,
    partner_name,
    partner_type,
    partner_category,
    account_group,
    country,
    region,
    payment_terms,
    is_active
)
SELECT 
    partner_id AS supplier_key,
    partner_id,
    partner_name,
    partner_type,
    partner_category,
    account_group,
    country,
    region,
    payment_terms,
    is_active
FROM 
    bronze_p2p.取引先マスタ
WHERE 
    partner_type = 'supplier';
```

---

### 3. silver_material_dim (資材ディメンションテーブル)

**概要:** 間接材の属性情報を集約したディメンションテーブルです。調達明細から一意の資材を抽出して作成します。

| カラム名 | 型 | 制約 | 説明 |
|---------|-----|------|------|
| material_key | VARCHAR | PK | 資材ディメンションキー（material_id） |
| material_id | VARCHAR | UNIQUE | 資材ID（元データキー） |
| material_name | VARCHAR | NOT NULL | 資材名 |
| material_category | VARCHAR |  | 資材カテゴリ |
| material_type | VARCHAR |  | 資材タイプ（indirect固定） |
| unspsc_code | VARCHAR |  | UNSPSC分類コード |
| created_at | TIMESTAMP | NOT NULL | レコード作成日時 |
| updated_at | TIMESTAMP | NOT NULL | レコード更新日時 |

**作成SQL:**
```sql
CREATE TABLE silver_material_dim (
    material_key VARCHAR(100) PRIMARY KEY,
    material_id VARCHAR(50) UNIQUE NOT NULL,
    material_name VARCHAR(200) NOT NULL,
    material_category VARCHAR(100),
    material_type VARCHAR(20) DEFAULT 'indirect',
    unspsc_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス作成
CREATE INDEX idx_material_category ON silver_material_dim(material_category);
CREATE INDEX idx_material_unspsc ON silver_material_dim(unspsc_code);
```

**データ投入SQL:**
```sql
INSERT INTO silver_material_dim (
    material_key,
    material_id,
    material_name,
    material_category,
    material_type,
    unspsc_code
)
SELECT DISTINCT
    material_id AS material_key,
    material_id,
    material_name,
    material_category,
    material_type,
    unspsc_code
FROM 
    bronze_p2p.調達伝票_item
WHERE 
    material_type = 'indirect'
ORDER BY material_id;
```

---

## Gold層テーブル定義

### 1. gold_indirect_material_cost_monthly (月次間接材調達コスト集計テーブル)

**概要:** 月次・カテゴリ・サプライヤー別に間接材の調達コストを集計したテーブルです。KPI算出の基礎データとなります。

| カラム名 | 型 | 制約 | 説明 |
|---------|-----|------|------|
| cost_summary_id | VARCHAR | PK | サロゲートキー |
| year_month | VARCHAR | NOT NULL | 対象年月（YYYY-MM） |
| supplier_key | VARCHAR | FK | サプライヤーキー |
| supplier_name | VARCHAR |  | サプライヤー名（非正規化） |
| material_category | VARCHAR |  | 資材カテゴリ |
| location_id | VARCHAR |  | 拠点コード |
| cost_center | VARCHAR |  | コストセンター |
| total_order_amount | DECIMAL(18,2) | NOT NULL | 調達総額（税抜） |
| total_quantity | DECIMAL(18,4) |  | 発注総数量 |
| order_count | INTEGER |  | 発注回数 |
| avg_unit_price | DECIMAL(18,4) |  | 平均単価 |
| unique_material_count | INTEGER |  | 調達品目数 |
| created_at | TIMESTAMP | NOT NULL | レコード作成日時 |

**作成SQL:**
```sql
CREATE TABLE gold_indirect_material_cost_monthly (
    cost_summary_id VARCHAR(200) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    supplier_key VARCHAR(50),
    supplier_name VARCHAR(200),
    material_category VARCHAR(100),
    location_id VARCHAR(50),
    cost_center VARCHAR(50),
    total_order_amount DECIMAL(18,2) NOT NULL,
    total_quantity DECIMAL(18,4),
    order_count INTEGER,
    avg_unit_price DECIMAL(18,4),
    unique_material_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス作成
CREATE INDEX idx_cost_monthly_ym ON gold_indirect_material_cost_monthly(year_month);
CREATE INDEX idx_cost_monthly_supplier ON gold_indirect_material_cost_monthly(supplier_key);
CREATE INDEX idx_cost_monthly_category ON gold_indirect_material_cost_monthly(material_category);
CREATE INDEX idx_cost_monthly_location ON gold_indirect_material_cost_monthly(location_id);
```

**データ投入SQL:**
```sql
INSERT INTO gold_indirect_material_cost_monthly (
    cost_summary_id,
    year_month,
    supplier_key,
    supplier_name,
    material_category,
    location_id,
    cost_center,
    total_order_amount,
    total_quantity,
    order_count,
    avg_unit_price,
    unique_material_count
)
SELECT 
    -- サロゲートキー生成
    CONCAT(
        pf.order_year_month, '-',
        COALESCE(pf.supplier_key, 'UNKNOWN'), '-',
        COALESCE(pf.material_category, 'UNKNOWN'), '-',
        COALESCE(pf.location_id, 'UNKNOWN'), '-',
        COALESCE(pf.cost_center, 'UNKNOWN')
    ) AS cost_summary_id,
    pf.order_year_month AS year_month,
    pf.supplier_key,
    sd.partner_name AS supplier_name,
    pf.material_category,
    pf.location_id,
    pf.cost_center,
    -- 調達総額
    SUM(pf.line_subtotal_ex_tax) AS total_order_amount,
    -- 発注総数量
    SUM(pf.quantity) AS total_quantity,
    -- 発注回数
    COUNT(DISTINCT pf.purchase_order_id) AS order_count,
    -- 平均単価（加重平均）
    SUM(pf.line_subtotal_ex_tax) / NULLIF(SUM(pf.quantity), 0) AS avg_unit_price,
    -- 調達品目数
    COUNT(DISTINCT pf.material_id) AS unique_material_count
FROM 
    silver_procurement_fact AS pf
LEFT JOIN 
    silver_supplier_dim AS sd
    ON pf.supplier_key = sd.supplier_key
GROUP BY 
    pf.order_year_month,
    pf.supplier_key,
    sd.partner_name,
    pf.material_category,
    pf.location_id,
    pf.cost_center;
```

---

### 2. gold_indirect_material_cost_reduction_rate (間接材調達コスト削減率テーブル)

**概要:** 前年同月比での間接材調達コスト削減率を算出したKPIテーブルです。複数の分析軸での削減率を提供します。

| カラム名 | 型 | 制約 | 説明 |
|---------|-----|------|------|
| kpi_id | VARCHAR | PK | サロゲートキー |
| year_month | VARCHAR | NOT NULL | 対象年月（YYYY-MM） |
| analysis_axis | VARCHAR | NOT NULL | 分析軸（overall/supplier/category/location/cost_center） |
| axis_value | VARCHAR |  | 分析軸の値（サプライヤー名等） |
| axis_key | VARCHAR |  | 分析軸のキー |
| current_amount | DECIMAL(18,2) | NOT NULL | 当月調達額（税抜） |
| previous_year_amount | DECIMAL(18,2) |  | 前年同月調達額（税抜） |
| amount_difference | DECIMAL(18,2) |  | 金額差異（前年同月 - 当月） |
| cost_reduction_rate | DECIMAL(10,4) |  | コスト削減率（%）※負の値は増加 |
| current_avg_unit_price | DECIMAL(18,4) |  | 当月平均単価 |
| previous_year_avg_unit_price | DECIMAL(18,4) |  | 前年同月平均単価 |
| unit_price_reduction_rate | DECIMAL(10,4) |  | 単価削減率（%） |
| created_at | TIMESTAMP | NOT NULL | レコード作成日時 |

**作成SQL:**
```sql
CREATE TABLE gold_indirect_material_cost_reduction_rate (
    kpi_id VARCHAR(200) PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    analysis_axis VARCHAR(50) NOT NULL,
    axis_value VARCHAR(200),
    axis_key VARCHAR(100),
    current_amount DECIMAL(18,2) NOT NULL,
    previous_year_amount DECIMAL(18,2),
    amount_difference DECIMAL(18,2),
    cost_reduction_rate DECIMAL(10,4),
    current_avg_unit_price DECIMAL(18,4),
    previous_year_avg_unit_price DECIMAL(18,4),
    unit_price_reduction_rate DECIMAL(10,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス作成
CREATE INDEX idx_reduction_ym ON gold_indirect_material_cost_reduction_rate(year_month);
CREATE INDEX idx_reduction_axis ON gold_indirect_material_cost_reduction_rate(analysis_axis);
CREATE INDEX idx_reduction_axis_value ON gold_indirect_material_cost_reduction_rate(axis_value);
```

**データ投入SQL (全社レベル):**
```sql
-- 分析軸: 全社レベル (overall)
INSERT INTO gold_indirect_material_cost_reduction_rate (
    kpi_id,
    year_month,
    analysis_axis,
    axis_value,
    axis_key,
    current_amount,
    previous_year_amount,
    amount_difference,
    cost_reduction_rate,
    current_avg_unit_price,
    previous_year_avg_unit_price,
    unit_price_reduction_rate
)
WITH current_month AS (
    SELECT 
        year_month,
        SUM(total_order_amount) AS total_amount,
        SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
    FROM gold_indirect_material_cost_monthly
    GROUP BY year_month
),
previous_year AS (
    SELECT 
        TO_CHAR(TO_DATE(year_month || '-01', 'YYYY-MM-DD') + INTERVAL '1 year', 'YYYY-MM') AS current_ym,
        SUM(total_order_amount) AS total_amount,
        SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
    FROM gold_indirect_material_cost_monthly
    GROUP BY TO_CHAR(TO_DATE(year_month || '-01', 'YYYY-MM-DD') + INTERVAL '1 year', 'YYYY-MM')
)
SELECT 
    CONCAT('OVERALL-', cm.year_month) AS kpi_id,
    cm.year_month,
    'overall' AS analysis_axis,
    '全社' AS axis_value,
    'OVERALL' AS axis_key,
    cm.total_amount AS current_amount,
    py.total_amount AS previous_year_amount,
    py.total_amount - cm.total_amount AS amount_difference,
    -- コスト削減率（%）
    CASE 
        WHEN py.total_amount > 0 THEN 
            ((py.total_amount - cm.total_amount) / py.total_amount) * 100
        ELSE NULL 
    END AS cost_reduction_rate,
    cm.avg_price AS current_avg_unit_price,
    py.avg_price AS previous_year_avg_unit_price,
    -- 単価削減率（%）
    CASE 
        WHEN py.avg_price > 0 THEN 
            ((py.avg_price - cm.avg_price) / py.avg_price) * 100
        ELSE NULL 
    END AS unit_price_reduction_rate
FROM 
    current_month cm
LEFT JOIN 
    previous_year py
    ON cm.year_month = py.current_ym;
```

**データ投入SQL (サプライヤー別):**
```sql
-- 分析軸: サプライヤー別 (supplier)
INSERT INTO gold_indirect_material_cost_reduction_rate (
    kpi_id,
    year_month,
    analysis_axis,
    axis_value,
    axis_key,
    current_amount,
    previous_year_amount,
    amount_difference,
    cost_reduction_rate,
    current_avg_unit_price,
    previous_year_avg_unit_price,
    unit_price_reduction_rate
)
WITH current_month AS (
    SELECT 
        year_month,
        supplier_key,
        supplier_name,
        SUM(total_order_amount) AS total_amount,
        SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
    FROM gold_indirect_material_cost_monthly
    GROUP BY year_month, supplier_key, supplier_name
),
previous_year AS (
    SELECT 
        TO_CHAR(TO_DATE(year_month || '-01', 'YYYY-MM-DD') + INTERVAL '1 year', 'YYYY-MM') AS current_ym,
        supplier_key,
        SUM(total_order_amount) AS total_amount,
        SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
    FROM gold_indirect_material_cost_monthly
    GROUP BY TO_CHAR(TO_DATE(year_month || '-01', 'YYYY-MM-DD') + INTERVAL '1 year', 'YYYY-MM'), supplier_key
)
SELECT 
    CONCAT('SUPPLIER-', cm.year_month, '-', cm.supplier_key) AS kpi_id,
    cm.year_month,
    'supplier' AS analysis_axis,
    cm.supplier_name AS axis_value,
    cm.supplier_key AS axis_key,
    cm.total_amount AS current_amount,
    py.total_amount AS previous_year_amount,
    py.total_amount - cm.total_amount AS amount_difference,
    CASE 
        WHEN py.total_amount > 0 THEN 
            ((py.total_amount - cm.total_amount) / py.total_amount) * 100
        ELSE NULL 
    END AS cost_reduction_rate,
    cm.avg_price AS current_avg_unit_price,
    py.avg_price AS previous_year_avg_unit_price,
    CASE 
        WHEN py.avg_price > 0 THEN 
            ((py.avg_price - cm.avg_price) / py.avg_price) * 100
        ELSE NULL 
    END AS unit_price_reduction_rate
FROM 
    current_month cm
LEFT JOIN 
    previous_year py
    ON cm.year_month = py.current_ym
    AND cm.supplier_key = py.supplier_key
WHERE cm.supplier_key IS NOT NULL;
```

**データ投入SQL (カテゴリ別):**
```sql
-- 分析軸: 資材カテゴリ別 (category)
INSERT INTO gold_indirect_material_cost_reduction_rate (
    kpi_id,
    year_month,
    analysis_axis,
    axis_value,
    axis_key,
    current_amount,
    previous_year_amount,
    amount_difference,
    cost_reduction_rate,
    current_avg_unit_price,
    previous_year_avg_unit_price,
    unit_price_reduction_rate
)
WITH current_month AS (
    SELECT 
        year_month,
        material_category,
        SUM(total_order_amount) AS total_amount,
        SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
    FROM gold_indirect_material_cost_monthly
    GROUP BY year_month, material_category
),
previous_year AS (
    SELECT 
        TO_CHAR(TO_DATE(year_month || '-01', 'YYYY-MM-DD') + INTERVAL '1 year', 'YYYY-MM') AS current_ym,
        material_category,
        SUM(total_order_amount) AS total_amount,
        SUM(total_order_amount) / NULLIF(SUM(total_quantity), 0) AS avg_price
    FROM gold_indirect_material_cost_monthly
    GROUP BY TO_CHAR(TO_DATE(year_month || '-01', 'YYYY-MM-DD') + INTERVAL '1 year', 'YYYY-MM'), material_category
)
SELECT 
    CONCAT('CATEGORY-', cm.year_month, '-', COALESCE(cm.material_category, 'UNKNOWN')) AS kpi_id,
    cm.year_month,
    'category' AS analysis_axis,
    cm.material_category AS axis_value,
    cm.material_category AS axis_key,
    cm.total_amount AS current_amount,
    py.total_amount AS previous_year_amount,
    py.total_amount - cm.total_amount AS amount_difference,
    CASE 
        WHEN py.total_amount > 0 THEN 
            ((py.total_amount - cm.total_amount) / py.total_amount) * 100
        ELSE NULL 
    END AS cost_reduction_rate,
    cm.avg_price AS current_avg_unit_price,
    py.avg_price AS previous_year_avg_unit_price,
    CASE 
        WHEN py.avg_price > 0 THEN 
            ((py.avg_price - cm.avg_price) / py.avg_price) * 100
        ELSE NULL 
    END AS unit_price_reduction_rate
FROM 
    current_month cm
LEFT JOIN 
    previous_year py
    ON cm.year_month = py.current_ym
    AND cm.material_category = py.material_category;
```

---

## データフロー概要

### ETLパイプライン構成

```
1. Bronze → Silver (データクレンジング)
   - 間接材フィルタリング (material_type = 'indirect')
   - データ型変換・正規化
   - 異常値除外（単価・数量が0以下等）
   - キャンセル済み発注の除外
   - 年月カラムの生成

2. Silver → Gold (集計処理)
   - 月次・分析軸別の集計
   - 前年同月データとの突合
   - KPI計算（削減率）
   - 複数分析軸でのKPI展開

3. Gold レイヤーでのKPI活用
   - ダッシュボード可視化
   - トレンド分析
   - アラート設定（削減率が負の場合等）
```

### 更新頻度

- **Silver層**: 日次バッチ更新（夜間）
- **Gold層**: 月次更新（月初3営業日以内）+ 月中途のプレビュー更新（週次）

### データ品質チェック

```sql
-- 品質チェッククエリ例

-- 1. 異常な単価を検出
SELECT * FROM silver_procurement_fact
WHERE unit_price_ex_tax > 10000000 OR unit_price_ex_tax < 0;

-- 2. 前年同月比での異常な変動を検出（±50%以上）
SELECT * FROM gold_indirect_material_cost_reduction_rate
WHERE ABS(cost_reduction_rate) > 50;

-- 3. NULL値の確認
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN previous_year_amount IS NULL THEN 1 ELSE 0 END) AS missing_previous_year
FROM gold_indirect_material_cost_reduction_rate
WHERE year_month >= TO_CHAR(CURRENT_DATE, 'YYYY-MM');
```

---

## KPI活用例

### ダッシュボード表示例

```sql
-- 直近12ヶ月の全社削減率トレンド
SELECT 
    year_month,
    current_amount,
    previous_year_amount,
    cost_reduction_rate,
    unit_price_reduction_rate
FROM 
    gold_indirect_material_cost_reduction_rate
WHERE 
    analysis_axis = 'overall'
    AND year_month >= TO_CHAR(CURRENT_DATE - INTERVAL '12 months', 'YYYY-MM')
ORDER BY 
    year_month DESC;

-- サプライヤー別削減貢献ランキング（直近月）
SELECT 
    axis_value AS supplier_name,
    current_amount,
    amount_difference AS cost_saved,
    cost_reduction_rate
FROM 
    gold_indirect_material_cost_reduction_rate
WHERE 
    analysis_axis = 'supplier'
    AND year_month = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
    AND amount_difference > 0
ORDER BY 
    amount_difference DESC
LIMIT 10;
```

---

## 補足: Python/Sparkでの実装例

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Spark セッション初期化
spark = SparkSession.builder.appName("IndirectMaterialCostKPI").getOrCreate()

# Bronze層データ読み込み
df_header = spark.read.csv("Bronze/P2P/調達伝票_header.csv", header=True, inferSchema=True)
df_item = spark.read.csv("Bronze/P2P/調達伝票_item.csv", header=True, inferSchema=True)
df_supplier = spark.read.csv("Bronze/P2P/取引先マスタ.csv", header=True, inferSchema=True)

# Silver層: 調達ファクト作成
df_silver_procurement = df_item.filter(
    (col("material_type") == "indirect") &
    (col("unit_price_ex_tax") > 0) &
    (col("quantity") > 0)
).join(
    df_header.filter(col("order_status") != "cancelled"),
    on="purchase_order_id",
    how="inner"
).select(
    concat(col("purchase_order_id"), lit("-"), lpad(col("line_number").cast("string"), 5, "0")).alias("procurement_fact_id"),
    col("purchase_order_id"),
    col("line_number"),
    col("order_date"),
    date_format(col("order_date"), "yyyy-MM").alias("order_year_month"),
    col("supplier_id").alias("supplier_key"),
    col("material_id").alias("material_key"),
    col("location_id"),
    col("material_id"),
    col("material_name"),
    col("material_category"),
    col("unspsc_code"),
    col("cost_center"),
    col("department_code"),
    col("quantity"),
    col("unit_price_ex_tax"),
    col("line_subtotal_ex_tax"),
    col("line_total_incl_tax"),
    col("received_date"),
    col("received_quantity"),
    col("order_status"),
    coalesce(col("currency"), lit("JPY")).alias("currency"),
    current_timestamp().alias("created_at"),
    current_timestamp().alias("updated_at")
)

# Silver層書き込み
df_silver_procurement.write.mode("overwrite").parquet("Silver/procurement_fact")

# Gold層: 月次コスト集計
df_gold_monthly = df_silver_procurement.groupBy(
    "order_year_month",
    "supplier_key",
    "material_category",
    "location_id",
    "cost_center"
).agg(
    sum("line_subtotal_ex_tax").alias("total_order_amount"),
    sum("quantity").alias("total_quantity"),
    countDistinct("purchase_order_id").alias("order_count"),
    (sum("line_subtotal_ex_tax") / sum("quantity")).alias("avg_unit_price"),
    countDistinct("material_id").alias("unique_material_count")
)

df_gold_monthly.write.mode("overwrite").parquet("Gold/indirect_material_cost_monthly")

print("ETL処理完了")
```

---

## まとめ

本設計書により、Bronze層の生データからSilver層での正規化、Gold層でのKPI算出までの一連のデータフローが定義されました。

**主要ポイント:**
1. **Silver層**: クレンジング済みのファクトテーブルとディメンションテーブルで柔軟な分析が可能
2. **Gold層**: 複数の分析軸（全社/サプライヤー/カテゴリ/拠点/コストセンター）でKPIを展開
3. **拡張性**: 新たな分析軸の追加が容易な設計
4. **データ品質**: 異常値検出とNULL処理を組み込んだ堅牢な設計

このテーブル設計により、間接材調達コスト削減率のモニタリングと改善分析が実現できます。
