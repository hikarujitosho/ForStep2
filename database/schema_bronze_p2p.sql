-- Bronze層テーブル定義
-- P2Pシステム（調達）

-- 調達伝票_header（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_p2p_purchase_header (
    purchase_order_id VARCHAR PRIMARY KEY,
    order_date DATE,
    expected_delivery_date DATE,
    supplier_id VARCHAR,
    supplier_name VARCHAR,
    account_group VARCHAR,
    location_id VARCHAR,
    purchase_order_number VARCHAR,
    currency VARCHAR,
    order_subtotal_ex_tax DECIMAL,
    shipping_fee_ex_tax DECIMAL,
    tax_amount DECIMAL,
    discount_amount_incl_tax DECIMAL,
    order_total_incl_tax DECIMAL,
    order_status VARCHAR,
    approver VARCHAR,
    payment_method VARCHAR,
    payment_confirmation_id VARCHAR,
    payment_date DATE,
    payment_amount DECIMAL,
    purchase_order_date DATE,
    order_subtotal_incl_tax DECIMAL,
    order_total_tax_amount DECIMAL,
    order_shipping_fee_incl_tax DECIMAL,
    order_discount_incl_tax DECIMAL,
    payment_terms VARCHAR,
    delivery_address VARCHAR,
    billing_address VARCHAR,
    order_approval_status VARCHAR,
    approver_id VARCHAR,
    approval_date DATE,
    order_notes VARCHAR,
    cost_center VARCHAR,
    project_code VARCHAR,
    department_code VARCHAR,
    account_user VARCHAR,
    user_email VARCHAR,
    amount_changed BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 調達伝票_item（ブロンズ層）
CREATE TABLE IF NOT EXISTS bronze_p2p_purchase_item (
    purchase_order_id VARCHAR,
    line_number VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    quantity DECIMAL,
    unit_price_ex_tax DECIMAL,
    line_total_ex_tax DECIMAL,
    tax_rate DECIMAL,
    tax_amount DECIMAL,
    line_total_incl_tax DECIMAL,
    delivery_date DATE,
    delivery_location VARCHAR,
    item_category VARCHAR,
    specification VARCHAR,
    supplier_part_number VARCHAR,
    manufacturer_part_number VARCHAR,
    unit_of_measure VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (purchase_order_id, line_number)
);

-- BOMマスタ（ブロンズ層） - P2P版
CREATE TABLE IF NOT EXISTS bronze_p2p_bom_master (
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

-- 取引先マスタ（ブロンズ層） - P2P版
CREATE TABLE IF NOT EXISTS bronze_p2p_partner_master (
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