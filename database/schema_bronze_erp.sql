-- Bronze層テーブル定義
-- ERPシステム

-- 受注伝票_header（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_erp_order_header (
    order_id VARCHAR PRIMARY KEY,
    order_timestamp TIMESTAMP,
    location_id VARCHAR,
    customer_id VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 受注伝票_item（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_erp_order_item (
    order_id VARCHAR,
    line_number VARCHAR,
    product_id VARCHAR,
    quantity DECIMAL,
    promised_delivery_date TIMESTAMP,
    pricing_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, line_number)
);

-- 品目マスタ（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_erp_item_master (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR,
    base_unit_quantity DECIMAL,
    brand_name VARCHAR,
    item_group VARCHAR,
    item_hierarchy VARCHAR,
    detail_category VARCHAR,
    tax_classification VARCHAR,
    transport_group VARCHAR,
    import_export_group VARCHAR,
    country_of_origin VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 条件マスタ（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_erp_price_condition (
    price_condition_id VARCHAR PRIMARY KEY,
    product_id VARCHAR,
    product_name VARCHAR,
    customer_id VARCHAR,
    customer_name VARCHAR,
    list_price_ex_tax DECIMAL,
    selling_price_ex_tax DECIMAL,
    discount_rate DECIMAL,
    price_type VARCHAR,
    minimum_order_quantity DECIMAL,
    currency VARCHAR,
    valid_from DATE,
    valid_to DATE,
    remarks VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- BOMマスタ（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_erp_bom_master (
    bom_id VARCHAR PRIMARY KEY,
    bom_name VARCHAR,
    product_id VARCHAR,
    site_id VARCHAR,
    production_process_id VARCHAR,
    component_product_id VARCHAR,
    component_quantity_per DECIMAL,
    component_quantity_uom VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 取引先マスタ（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_erp_partner_master (
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

-- 拠点マスタ（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_erp_location_master (
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