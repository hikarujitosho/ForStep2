-- Gold層テーブル定義
-- ゴールド層KPI集計テーブル

-- 月次商品別粗利率テーブル
CREATE TABLE IF NOT EXISTS gold_monthly_product_gross_margin (
    product_id VARCHAR,
    product_name VARCHAR,
    year_month VARCHAR,
    revenue DECIMAL,
    cost DECIMAL,
    gross_profit DECIMAL,
    gross_margin DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, year_month)
);

-- 月次EV販売率テーブル
CREATE TABLE IF NOT EXISTS gold_monthly_ev_sales_share (
    year_month VARCHAR PRIMARY KEY,
    total_revenue DECIMAL,
    ev_revenue DECIMAL,
    ev_sales_share DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 月次エリア別EV販売率テーブル
CREATE TABLE IF NOT EXISTS gold_monthly_area_ev_sales_share (
    year_month VARCHAR,
    location_id VARCHAR,
    location_name VARCHAR,
    total_revenue DECIMAL,
    ev_revenue DECIMAL,
    ev_sales_share DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year_month, location_id)
);

-- 月次先進安全装置適用率テーブル
CREATE TABLE IF NOT EXISTS gold_monthly_safety_equipment_adoption (
    year_month VARCHAR PRIMARY KEY,
    total_revenue DECIMAL,
    safety_equipped_revenue DECIMAL,
    safety_equipment_adoption_rate DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 月次エリア別先進安全装置適用率テーブル
CREATE TABLE IF NOT EXISTS gold_monthly_area_safety_equipment_adoption (
    year_month VARCHAR,
    location_id VARCHAR,
    location_name VARCHAR,
    total_revenue DECIMAL,
    safety_equipped_revenue DECIMAL,
    safety_equipment_adoption_rate DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year_month, location_id)
);

-- 棚卸資産回転期間テーブル
CREATE TABLE IF NOT EXISTS gold_inventory_rotation_period (
    year_month VARCHAR PRIMARY KEY,
    avg_inventory_value DECIMAL,
    monthly_cost_of_sales DECIMAL,
    rotation_period_days DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 完成品別棚卸資産回転期間テーブル
CREATE TABLE IF NOT EXISTS gold_product_inventory_rotation_period (
    product_id VARCHAR,
    product_name VARCHAR,
    year_month VARCHAR,
    monthly_cogs DECIMAL,
    inventory_value DECIMAL,
    inventory_rotation_period DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, year_month)
);

-- 月次EBITDAテーブル
CREATE TABLE IF NOT EXISTS gold_monthly_ebitda (
    year_month VARCHAR PRIMARY KEY,
    revenue DECIMAL,
    gross_profit DECIMAL,
    operating_expenses DECIMAL,
    ebitda DECIMAL,
    ebitda_margin DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 完成品別月次EBITDAテーブル
CREATE TABLE IF NOT EXISTS gold_product_monthly_ebitda (
    product_id VARCHAR,
    product_name VARCHAR,
    year_month VARCHAR,
    revenue DECIMAL,
    gross_margin_amount DECIMAL,
    operating_expenses DECIMAL,
    ebitda DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, year_month)
);

-- 完成品出荷リードタイム遵守率テーブル
CREATE TABLE IF NOT EXISTS gold_product_delivery_compliance (
    product_id VARCHAR,
    product_name VARCHAR,
    year_month VARCHAR,
    orders_received DECIMAL,
    orders_shipped DECIMAL,
    on_time_delivery_rate DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, year_month)
);

-- 取引先別部品入荷リードタイム遵守率テーブル
CREATE TABLE IF NOT EXISTS gold_supplier_lead_time_compliance (
    supplier_id VARCHAR,
    supplier_name VARCHAR,
    part_id VARCHAR,
    part_name VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    year_month VARCHAR,
    purchase_orders_count DECIMAL,
    receipts_count DECIMAL,
    inbound_lead_time_compliance_rate DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (supplier_id, part_id, year_month)
);

-- 緊急輸送費率テーブル
CREATE TABLE IF NOT EXISTS gold_emergency_transportation_cost_share (
    year_month VARCHAR PRIMARY KEY,
    total_cost DECIMAL,
    emergency_cost DECIMAL,
    emergency_cost_share DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 月次商品別棚卸資産回転期間テーブル
CREATE TABLE IF NOT EXISTS gold_monthly_product_inventory_rotation (
    year_month VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    avg_inventory_value DECIMAL,
    monthly_cost_of_sales DECIMAL,
    rotation_period_days DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year_month, product_id)
);

-- 月次商品別EBITDAテーブル
CREATE TABLE IF NOT EXISTS gold_monthly_product_ebitda (
    year_month VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    revenue DECIMAL,
    gross_profit DECIMAL,
    ebitda DECIMAL,
    ebitda_margin DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year_month, product_id)
);

-- 月次納期遵守率テーブル
CREATE TABLE IF NOT EXISTS gold_monthly_delivery_compliance_rate (
    year_month VARCHAR PRIMARY KEY,
    total_orders INTEGER,
    on_time_deliveries INTEGER,
    compliance_rate DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 月次仕入先リードタイム遵守率テーブル
CREATE TABLE IF NOT EXISTS gold_monthly_supplier_lead_time_compliance (
    year_month VARCHAR,
    supplier_id VARCHAR,
    supplier_name VARCHAR,
    total_orders INTEGER,
    on_time_deliveries INTEGER,
    compliance_rate DECIMAL,
    avg_lead_time_days DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year_month, supplier_id)
);