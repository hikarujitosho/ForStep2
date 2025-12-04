-- Silver層テーブル定義
-- データクレンジング・正規化後の統合テーブル

-- 統合品目マスタ（Silver層）
CREATE TABLE IF NOT EXISTS silver_item_master (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR,
    brand_name VARCHAR,
    item_group VARCHAR,
    item_hierarchy VARCHAR,
    detail_category VARCHAR,
    transport_group VARCHAR,
    import_export_group VARCHAR,
    country_of_origin VARCHAR,
    -- 判定フラグ
    is_ev BOOLEAN,
    is_safety_equipped BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 統合取引先マスタ（Silver層）
CREATE TABLE IF NOT EXISTS silver_partner_master (
    partner_id VARCHAR PRIMARY KEY,
    partner_name VARCHAR,
    partner_type VARCHAR,
    partner_category VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state_province VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    contact_person VARCHAR,
    contact_email VARCHAR,
    contact_phone VARCHAR,
    payment_terms VARCHAR,
    currency VARCHAR,
    is_active BOOLEAN,
    region VARCHAR,
    valid_from DATE,
    valid_to DATE,
    source_system VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 統合拠点マスタ（Silver層）
CREATE TABLE IF NOT EXISTS silver_location_master (
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
    is_active BOOLEAN,
    valid_from DATE,
    valid_to DATE,
    source_system VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 受注データ（Silver層）
CREATE TABLE IF NOT EXISTS silver_order_data (
    order_id VARCHAR,
    line_number VARCHAR,
    order_date DATE,
    product_id VARCHAR,
    product_name VARCHAR,
    customer_id VARCHAR,
    customer_name VARCHAR,
    location_id VARCHAR,
    quantity DECIMAL,
    unit_price_ex_tax DECIMAL,
    line_total_ex_tax DECIMAL,
    promised_delivery_date DATE,
    year_month VARCHAR,
    PRIMARY KEY (order_id, line_number),
    FOREIGN KEY (product_id) REFERENCES silver_item_master(product_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 調達データ（Silver層）
CREATE TABLE IF NOT EXISTS silver_procurement_data (
    purchase_order_id VARCHAR,
    line_number VARCHAR,
    order_date DATE,
    product_id VARCHAR,
    product_name VARCHAR,
    supplier_id VARCHAR,
    supplier_name VARCHAR,
    location_id VARCHAR,
    account_group VARCHAR,
    quantity DECIMAL,
    unit_price_ex_tax DECIMAL,
    line_total_ex_tax DECIMAL,
    expected_delivery_date DATE,
    year_month VARCHAR,
    PRIMARY KEY (purchase_order_id, line_number),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 出荷データ（Silver層）
CREATE TABLE IF NOT EXISTS silver_shipment_data (
    shipment_id VARCHAR,
    line_number VARCHAR,
    order_id VARCHAR,
    order_line_number VARCHAR,
    shipment_date DATE,
    product_id VARCHAR,
    product_name VARCHAR,
    customer_id VARCHAR,
    location_id VARCHAR,
    quantity_shipped DECIMAL,
    unit_price DECIMAL,
    line_total DECIMAL,
    year_month VARCHAR,
    PRIMARY KEY (shipment_id, line_number),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 輸送コストデータ（Silver層）
CREATE TABLE IF NOT EXISTS silver_transportation_cost (
    cost_id VARCHAR PRIMARY KEY,
    shipment_id VARCHAR,
    location_id VARCHAR,
    cost_type VARCHAR,
    cost_amount DECIMAL,
    currency VARCHAR,
    billing_date DATE,
    year_month VARCHAR,
    is_emergency BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 給与データ（Silver層）
CREATE TABLE IF NOT EXISTS silver_payroll_data (
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
    cost_center VARCHAR,
    year_month VARCHAR,
    total_compensation DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 在庫データ（Silver層）
CREATE TABLE IF NOT EXISTS silver_inventory_data (
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