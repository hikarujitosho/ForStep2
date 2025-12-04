-- Bronze層テーブル定義
-- TMS（輸送管理）、HR（人事）、WMS（倉庫管理）システム

-- 輸送コスト（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_tms_transportation_cost (
    cost_id VARCHAR PRIMARY KEY,
    shipment_id VARCHAR,
    location_id VARCHAR,
    cost_type VARCHAR,
    cost_amount DECIMAL,
    currency VARCHAR,
    billing_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 取引先マスタ（ブロンズ層） - TMS版
CREATE TABLE IF NOT EXISTS bronze_tms_partner_master (
    partner_id VARCHAR PRIMARY KEY,
    partner_name VARCHAR,
    partner_type VARCHAR,
    partner_category VARCHAR,
    tax_id VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state_province VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    contact_person VARCHAR,
    contact_email VARCHAR,
    contact_phone VARCHAR,
    payment_terms VARCHAR,
    credit_limit DECIMAL,
    currency VARCHAR,
    is_active VARCHAR,
    account_group VARCHAR,
    region VARCHAR,
    valid_from DATE,
    valid_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 拠点マスタ（ブロンズ層） - TMS版
CREATE TABLE IF NOT EXISTS bronze_tms_location_master (
    location_id VARCHAR PRIMARY KEY,
    location_name VARCHAR,
    location_type VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state_province VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    contact_person VARCHAR,
    contact_phone VARCHAR,
    contact_email VARCHAR,
    is_active VARCHAR,
    valid_from DATE,
    valid_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 給与テーブル（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_hr_payroll (
    payroll_id VARCHAR PRIMARY KEY,
    employee_id VARCHAR,
    employee_name VARCHAR,
    department VARCHAR,
    position VARCHAR,
    payment_period VARCHAR,
    base_salary DECIMAL,
    overtime_pay DECIMAL,
    allowances DECIMAL,
    deductions DECIMAL,
    net_salary DECIMAL,
    payment_date DATE,
    currency VARCHAR,
    employment_type VARCHAR,
    cost_center VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 月次在庫履歴（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_wms_monthly_inventory (
    inventory_history_id VARCHAR PRIMARY KEY,
    location_id VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    year_month VARCHAR,
    opening_quantity DECIMAL,
    received_quantity DECIMAL,
    issued_quantity DECIMAL,
    closing_quantity DECIMAL,
    unit_cost DECIMAL,
    total_value DECIMAL,
    currency VARCHAR,
    inventory_category VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 現在在庫（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_wms_current_inventory (
    inventory_id VARCHAR PRIMARY KEY,
    location_id VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    available_quantity DECIMAL,
    reserved_quantity DECIMAL,
    total_quantity DECIMAL,
    unit_cost DECIMAL,
    total_value DECIMAL,
    currency VARCHAR,
    last_updated TIMESTAMP,
    inventory_status VARCHAR,
    lot_number VARCHAR,
    serial_number VARCHAR,
    expiry_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 拠点マスタ（ブロンズ層） - WMS版
CREATE TABLE IF NOT EXISTS bronze_wms_location_master (
    location_id VARCHAR PRIMARY KEY,
    location_name VARCHAR,
    location_type VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state_province VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    contact_person VARCHAR,
    contact_phone VARCHAR,
    contact_email VARCHAR,
    is_active VARCHAR,
    valid_from DATE,
    valid_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);