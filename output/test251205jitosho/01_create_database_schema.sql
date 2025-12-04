-- ================================================
-- SQLite データベーススキーマ定義
-- KPI: 間接材調達コスト削減率
-- ================================================

-- ================================================
-- Silver層テーブル
-- ================================================

-- 1. silver_procurement_fact (調達ファクトテーブル)
DROP TABLE IF EXISTS silver_procurement_fact;
CREATE TABLE silver_procurement_fact (
    procurement_fact_id TEXT PRIMARY KEY,
    purchase_order_id TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    order_date TEXT NOT NULL,
    order_year_month TEXT NOT NULL,
    supplier_key TEXT,
    material_key TEXT,
    location_id TEXT,
    material_id TEXT,
    material_name TEXT,
    material_category TEXT,
    unspsc_code TEXT,
    cost_center TEXT,
    department_code TEXT,
    quantity REAL NOT NULL,
    unit_price_ex_tax REAL NOT NULL,
    line_subtotal_ex_tax REAL NOT NULL,
    line_total_incl_tax REAL,
    received_date TEXT,
    received_quantity REAL,
    order_status TEXT,
    currency TEXT DEFAULT 'JPY',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- インデックス作成
CREATE INDEX idx_procurement_order_ym ON silver_procurement_fact(order_year_month);
CREATE INDEX idx_procurement_supplier ON silver_procurement_fact(supplier_key);
CREATE INDEX idx_procurement_material ON silver_procurement_fact(material_key);
CREATE INDEX idx_procurement_category ON silver_procurement_fact(material_category);
CREATE INDEX idx_procurement_location ON silver_procurement_fact(location_id);
CREATE INDEX idx_procurement_cost_center ON silver_procurement_fact(cost_center);

-- 2. silver_supplier_dim (サプライヤーディメンションテーブル)
DROP TABLE IF EXISTS silver_supplier_dim;
CREATE TABLE silver_supplier_dim (
    supplier_key TEXT PRIMARY KEY,
    partner_id TEXT UNIQUE NOT NULL,
    partner_name TEXT NOT NULL,
    partner_type TEXT,
    partner_category TEXT,
    account_group TEXT,
    country TEXT,
    region TEXT,
    payment_terms TEXT,
    is_active TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- インデックス作成
CREATE INDEX idx_supplier_name ON silver_supplier_dim(partner_name);
CREATE INDEX idx_supplier_category ON silver_supplier_dim(partner_category);
CREATE INDEX idx_supplier_active ON silver_supplier_dim(is_active);

-- 3. silver_material_dim (資材ディメンションテーブル)
DROP TABLE IF EXISTS silver_material_dim;
CREATE TABLE silver_material_dim (
    material_key TEXT PRIMARY KEY,
    material_id TEXT UNIQUE NOT NULL,
    material_name TEXT NOT NULL,
    material_category TEXT,
    material_type TEXT DEFAULT 'indirect',
    unspsc_code TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- インデックス作成
CREATE INDEX idx_material_category ON silver_material_dim(material_category);
CREATE INDEX idx_material_unspsc ON silver_material_dim(unspsc_code);

-- ================================================
-- Gold層テーブル
-- ================================================

-- 1. gold_indirect_material_cost_monthly (月次間接材調達コスト集計テーブル)
DROP TABLE IF EXISTS gold_indirect_material_cost_monthly;
CREATE TABLE gold_indirect_material_cost_monthly (
    cost_summary_id TEXT PRIMARY KEY,
    year_month TEXT NOT NULL,
    supplier_key TEXT,
    supplier_name TEXT,
    material_category TEXT,
    location_id TEXT,
    cost_center TEXT,
    total_order_amount REAL NOT NULL,
    total_quantity REAL,
    order_count INTEGER,
    avg_unit_price REAL,
    unique_material_count INTEGER,
    created_at TEXT DEFAULT (datetime('now'))
);

-- インデックス作成
CREATE INDEX idx_cost_monthly_ym ON gold_indirect_material_cost_monthly(year_month);
CREATE INDEX idx_cost_monthly_supplier ON gold_indirect_material_cost_monthly(supplier_key);
CREATE INDEX idx_cost_monthly_category ON gold_indirect_material_cost_monthly(material_category);
CREATE INDEX idx_cost_monthly_location ON gold_indirect_material_cost_monthly(location_id);

-- 2. gold_indirect_material_cost_reduction_rate (間接材調達コスト削減率テーブル)
DROP TABLE IF EXISTS gold_indirect_material_cost_reduction_rate;
CREATE TABLE gold_indirect_material_cost_reduction_rate (
    kpi_id TEXT PRIMARY KEY,
    year_month TEXT NOT NULL,
    analysis_axis TEXT NOT NULL,
    axis_value TEXT,
    axis_key TEXT,
    current_amount REAL NOT NULL,
    previous_year_amount REAL,
    amount_difference REAL,
    cost_reduction_rate REAL,
    current_avg_unit_price REAL,
    previous_year_avg_unit_price REAL,
    unit_price_reduction_rate REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- インデックス作成
CREATE INDEX idx_reduction_ym ON gold_indirect_material_cost_reduction_rate(year_month);
CREATE INDEX idx_reduction_axis ON gold_indirect_material_cost_reduction_rate(analysis_axis);
CREATE INDEX idx_reduction_axis_value ON gold_indirect_material_cost_reduction_rate(axis_value);

-- ================================================
-- データ品質チェック用ビュー
-- ================================================

-- 異常な単価を検出するビュー
DROP VIEW IF EXISTS v_quality_check_abnormal_price;
CREATE VIEW v_quality_check_abnormal_price AS
SELECT 
    procurement_fact_id,
    purchase_order_id,
    material_name,
    unit_price_ex_tax,
    order_date,
    'Abnormal Price' as issue_type
FROM silver_procurement_fact
WHERE unit_price_ex_tax > 10000000 OR unit_price_ex_tax < 0;

-- 前年同月比での異常な変動を検出するビュー
DROP VIEW IF EXISTS v_quality_check_abnormal_change;
CREATE VIEW v_quality_check_abnormal_change AS
SELECT 
    kpi_id,
    year_month,
    analysis_axis,
    axis_value,
    cost_reduction_rate,
    'Abnormal Change (>50%)' as issue_type
FROM gold_indirect_material_cost_reduction_rate
WHERE ABS(cost_reduction_rate) > 50;

-- データ完全性チェックビュー
DROP VIEW IF EXISTS v_data_completeness_check;
CREATE VIEW v_data_completeness_check AS
SELECT 
    year_month,
    COUNT(*) as total_records,
    SUM(CASE WHEN previous_year_amount IS NULL THEN 1 ELSE 0 END) as missing_previous_year_count,
    ROUND(100.0 * SUM(CASE WHEN previous_year_amount IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as completeness_rate
FROM gold_indirect_material_cost_reduction_rate
GROUP BY year_month
ORDER BY year_month DESC;

-- ================================================
-- KPI参照用ビュー
-- ================================================

-- 直近12ヶ月の全社削減率トレンド
DROP VIEW IF EXISTS v_kpi_overall_trend;
CREATE VIEW v_kpi_overall_trend AS
SELECT 
    year_month,
    current_amount,
    previous_year_amount,
    amount_difference,
    cost_reduction_rate,
    unit_price_reduction_rate
FROM 
    gold_indirect_material_cost_reduction_rate
WHERE 
    analysis_axis = 'overall'
ORDER BY 
    year_month DESC
LIMIT 12;

-- サプライヤー別削減貢献ランキング
DROP VIEW IF EXISTS v_kpi_supplier_ranking;
CREATE VIEW v_kpi_supplier_ranking AS
SELECT 
    year_month,
    axis_value AS supplier_name,
    current_amount,
    amount_difference AS cost_saved,
    cost_reduction_rate,
    RANK() OVER (PARTITION BY year_month ORDER BY amount_difference DESC) as rank
FROM 
    gold_indirect_material_cost_reduction_rate
WHERE 
    analysis_axis = 'supplier'
    AND amount_difference > 0
ORDER BY 
    year_month DESC, amount_difference DESC;

-- カテゴリ別削減率サマリー
DROP VIEW IF EXISTS v_kpi_category_summary;
CREATE VIEW v_kpi_category_summary AS
SELECT 
    year_month,
    axis_value AS material_category,
    current_amount,
    previous_year_amount,
    cost_reduction_rate,
    unit_price_reduction_rate
FROM 
    gold_indirect_material_cost_reduction_rate
WHERE 
    analysis_axis = 'category'
ORDER BY 
    year_month DESC, cost_reduction_rate DESC;

-- スキーマ作成完了
SELECT 'データベーススキーマの作成が完了しました。' as message;
