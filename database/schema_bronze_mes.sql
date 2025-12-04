-- Bronze層テーブル定義
-- MESシステム（製造実行）

-- 出荷伝票_header（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_mes_shipment_header (
    shipment_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    shipment_date DATE,
    delivery_date DATE,
    customer_id VARCHAR,
    customer_name VARCHAR,
    location_id VARCHAR,
    shipment_status VARCHAR,
    tracking_number VARCHAR,
    shipping_method VARCHAR,
    carrier VARCHAR,
    total_weight DECIMAL,
    total_volume DECIMAL,
    shipping_cost DECIMAL,
    insurance_cost DECIMAL,
    currency VARCHAR,
    notes VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 出荷伝票_item（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_mes_shipment_item (
    shipment_id VARCHAR,
    line_number VARCHAR,
    order_line_number VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    quantity_shipped DECIMAL,
    unit_of_measure VARCHAR,
    unit_price DECIMAL,
    line_total DECIMAL,
    lot_number VARCHAR,
    serial_number VARCHAR,
    manufacturing_date DATE,
    expiry_date DATE,
    quality_status VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (shipment_id, line_number)
);

-- 取引先マスタ（ブロンズ層） - MES版
CREATE TABLE IF NOT EXISTS bronze_mes_partner_master (
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

-- 拠点マスタ（ブロンズ層） - MES版
CREATE TABLE IF NOT EXISTS bronze_mes_location_master (
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