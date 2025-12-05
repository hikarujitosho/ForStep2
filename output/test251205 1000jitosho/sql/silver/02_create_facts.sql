-- ================================================================================
-- Silverレイヤー: ファクトテーブル作成
-- メダリオンアーキテクチャ - 自動車製造業 物流・間接材購買部門
-- 作成日: 2025-12-05
-- ================================================================================

-- ================================================================================
-- 1. fact_inventory_daily (日次在庫ファクト)
-- ================================================================================
CREATE TABLE silver.fact_inventory_daily (
    inventory_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    location_key INT NOT NULL,
    inventory_quantity DECIMAL(18,4),
    inventory_value DECIMAL(18,2),  -- 数量 × 単価
    inventory_status VARCHAR(50),  -- available, reserved, damaged
    unit_cost DECIMAL(18,4),  -- 標準原価
    source_system VARCHAR(20) DEFAULT 'WMS',
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (date_key) REFERENCES silver.dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES silver.dim_product(product_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key)
);

-- インデックス
CREATE INDEX idx_inventory_date_product ON silver.fact_inventory_daily(snapshot_date, product_key);
CREATE INDEX idx_inventory_location ON silver.fact_inventory_daily(location_key, snapshot_date);
CREATE UNIQUE INDEX idx_inventory_unique ON silver.fact_inventory_daily(snapshot_date, product_key, location_key);

-- データ投入
INSERT INTO silver.fact_inventory_daily (
    snapshot_date, date_key, product_key, location_key,
    inventory_quantity, inventory_status, unit_cost, inventory_value
)
SELECT
    CAST(i.snapshot_date AS DATE) AS snapshot_date,
    d.date_key,
    p.product_key,
    l.location_key,
    i.inventory_quantity,
    i.inventory_status,
    COALESCE(pc.selling_price_ex_tax, 0) AS unit_cost,
    i.inventory_quantity * COALESCE(pc.selling_price_ex_tax, 0) AS inventory_value
FROM bronze.wms_monthly_inventory i
INNER JOIN silver.dim_product p ON i.product_id = p.product_id
INNER JOIN silver.dim_location l ON i.location_id = l.location_id
INNER JOIN silver.dim_date d ON CAST(i.snapshot_date AS DATE) = d.full_date
LEFT JOIN (
    SELECT product_id, AVG(selling_price_ex_tax) AS selling_price_ex_tax
    FROM bronze.erp_pricing_conditions
    GROUP BY product_id
) pc ON i.product_id = pc.product_id
WHERE i.inventory_quantity IS NOT NULL;

GO

-- ================================================================================
-- 2. fact_procurement (調達ファクト)
-- ================================================================================
CREATE TABLE silver.fact_procurement (
    procurement_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    purchase_order_id VARCHAR(50) NOT NULL,
    line_number INT NOT NULL,
    order_date DATE,
    order_date_key INT,
    expected_delivery_date DATE,
    actual_received_date DATE,
    ship_date DATE,
    supplier_key INT,
    location_key INT,
    material_key INT,
    product_key INT,
    quantity DECIMAL(18,4),
    unit_price_ex_tax DECIMAL(18,4),
    reference_price_ex_tax DECIMAL(18,4),
    price_variance DECIMAL(18,4),  -- reference - actual
    line_total_ex_tax DECIMAL(18,2),
    line_total_incl_tax DECIMAL(18,2),
    shipping_fee DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    material_category VARCHAR(100),
    material_type VARCHAR(50),  -- direct / indirect
    account_group VARCHAR(100),  -- MRO判定用
    order_status VARCHAR(50),
    shipping_status VARCHAR(50),
    receiving_status VARCHAR(50),
    carrier_name NVARCHAR(100),
    payment_method VARCHAR(50),
    payment_date DATE,
    lead_time_days INT,  -- received_date - order_date
    lead_time_variance_days INT,  -- received_date - expected_delivery_date
    is_on_time BIT,  -- received_date <= expected_delivery_date
    is_mro BIT,  -- account_groupがMROか
    cost_center VARCHAR(50),
    department_code VARCHAR(50),
    project_code VARCHAR(50),
    currency VARCHAR(3),
    source_system VARCHAR(20) DEFAULT 'P2P',
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (order_date_key) REFERENCES silver.dim_date(date_key),
    FOREIGN KEY (supplier_key) REFERENCES silver.dim_partner(partner_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key),
    FOREIGN KEY (material_key) REFERENCES silver.dim_material(material_key),
    FOREIGN KEY (product_key) REFERENCES silver.dim_product(product_key)
);

-- インデックス
CREATE INDEX idx_procurement_order_date ON silver.fact_procurement(order_date);
CREATE INDEX idx_procurement_supplier ON silver.fact_procurement(supplier_key, order_date);
CREATE INDEX idx_procurement_material_type ON silver.fact_procurement(material_type);
CREATE INDEX idx_procurement_mro ON silver.fact_procurement(is_mro);
CREATE UNIQUE INDEX idx_procurement_unique ON silver.fact_procurement(purchase_order_id, line_number);

-- データ投入
INSERT INTO silver.fact_procurement (
    purchase_order_id, line_number, order_date, order_date_key,
    expected_delivery_date, actual_received_date, ship_date,
    supplier_key, location_key, material_key, product_key,
    quantity, unit_price_ex_tax, reference_price_ex_tax, price_variance,
    line_total_ex_tax, line_total_incl_tax, shipping_fee, discount_amount,
    material_category, material_type, account_group, order_status,
    shipping_status, receiving_status, carrier_name, payment_method,
    payment_date, lead_time_days, lead_time_variance_days, is_on_time,
    is_mro, cost_center, department_code, project_code, currency
)
SELECT
    h.purchase_order_id,
    i.line_number,
    CAST(h.order_date AS DATE),
    d1.date_key,
    CAST(h.expected_delivery_date AS DATE),
    CAST(i.received_date AS DATE),
    CAST(i.ship_date AS DATE),
    s.partner_key,
    l.location_key,
    m.material_key,
    p.product_key,
    i.quantity,
    i.unit_price_ex_tax,
    i.reference_price_ex_tax,
    i.reference_price_ex_tax - i.unit_price_ex_tax,
    i.line_subtotal_ex_tax,
    i.line_total_incl_tax,
    i.line_shipping_fee_incl_tax,
    i.line_discount_incl_tax,
    i.material_category,
    i.material_type,
    h.account_group,
    h.order_status,
    i.shipping_status,
    i.receiving_status,
    i.carrier_name,
    h.payment_method,
    CAST(h.payment_date AS DATE),
    DATEDIFF(DAY, CAST(h.order_date AS DATE), CAST(i.received_date AS DATE)),
    DATEDIFF(DAY, CAST(h.expected_delivery_date AS DATE), CAST(i.received_date AS DATE)),
    CASE WHEN CAST(i.received_date AS DATE) <= CAST(h.expected_delivery_date AS DATE) THEN 1 ELSE 0 END,
    CASE WHEN h.account_group = 'MRO' THEN 1 ELSE 0 END,
    i.cost_center,
    i.department_code,
    i.project_code,
    h.currency
FROM bronze.p2p_procurement_item i
INNER JOIN bronze.p2p_procurement_header h ON i.purchase_order_id = h.purchase_order_id
LEFT JOIN silver.dim_partner s ON h.supplier_id = s.partner_id
LEFT JOIN silver.dim_location l ON h.location_id = l.location_id
LEFT JOIN silver.dim_material m ON i.material_id = m.material_id
LEFT JOIN silver.dim_product p ON i.product_id = p.product_id
LEFT JOIN silver.dim_date d1 ON CAST(h.order_date AS DATE) = d1.full_date;

GO

-- ================================================================================
-- 3. fact_sales_order (受注ファクト)
-- ================================================================================
CREATE TABLE silver.fact_sales_order (
    sales_order_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    line_number INT NOT NULL,
    order_timestamp DATETIME2,
    order_date DATE,
    order_date_key INT,
    promised_delivery_date DATE,
    pricing_date DATE,
    customer_key INT,
    location_key INT,
    product_key INT,
    quantity DECIMAL(18,4),
    list_price_ex_tax DECIMAL(18,4),
    selling_price_ex_tax DECIMAL(18,4),
    discount_rate DECIMAL(5,4),
    line_total_ex_tax DECIMAL(18,2),
    line_total_incl_tax DECIMAL(18,2),
    currency VARCHAR(3),
    source_system VARCHAR(20) DEFAULT 'ERP',
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (order_date_key) REFERENCES silver.dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES silver.dim_partner(partner_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key),
    FOREIGN KEY (product_key) REFERENCES silver.dim_product(product_key)
);

-- インデックス
CREATE INDEX idx_sales_order_date ON silver.fact_sales_order(order_date);
CREATE INDEX idx_sales_customer ON silver.fact_sales_order(customer_key, order_date);
CREATE UNIQUE INDEX idx_sales_unique ON silver.fact_sales_order(order_id, line_number);

-- データ投入
INSERT INTO silver.fact_sales_order (
    order_id, line_number, order_timestamp, order_date, order_date_key,
    promised_delivery_date, pricing_date, customer_key, location_key,
    product_key, quantity, list_price_ex_tax, selling_price_ex_tax,
    discount_rate, line_total_ex_tax, line_total_incl_tax, currency
)
SELECT
    h.order_id,
    i.line_number,
    CAST(h.order_timestamp AS DATETIME2),
    CAST(h.order_timestamp AS DATE),
    d.date_key,
    CAST(i.promised_delivery_date AS DATE),
    CAST(i.pricing_date AS DATE),
    c.partner_key,
    l.location_key,
    p.product_key,
    i.quantity,
    pc.list_price_ex_tax,
    pc.selling_price_ex_tax,
    pc.discount_rate,
    i.quantity * pc.selling_price_ex_tax,
    i.quantity * pc.selling_price_ex_tax * 1.1,  -- 簡易的に10%の税率を適用
    pc.currency
FROM bronze.erp_sales_order_item i
INNER JOIN bronze.erp_sales_order_header h ON i.order_id = h.order_id
LEFT JOIN silver.dim_partner c ON h.customer_id = c.partner_id
LEFT JOIN silver.dim_location l ON h.location_id = l.location_id
INNER JOIN silver.dim_product p ON i.product_id = p.product_id
LEFT JOIN silver.dim_date d ON CAST(h.order_timestamp AS DATE) = d.full_date
LEFT JOIN bronze.erp_pricing_conditions pc 
    ON i.product_id = pc.product_id 
    AND h.customer_id = pc.customer_id
    AND CAST(i.pricing_date AS DATE) BETWEEN CAST(pc.valid_from AS DATE) AND CAST(pc.valid_to AS DATE);

GO

-- ================================================================================
-- 4. fact_shipment (出荷ファクト)
-- ================================================================================
CREATE TABLE silver.fact_shipment (
    shipment_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    shipment_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50),
    line_number INT,
    shipment_timestamp DATETIME2,
    shipment_date DATE,
    shipment_date_key INT,
    planned_ship_date DATE,
    expected_ship_date DATE,
    actual_arrival_timestamp DATETIME2,
    actual_arrival_date DATE,
    customer_key INT,
    location_key INT,
    product_key INT,
    quantity DECIMAL(18,4),
    carrier_name NVARCHAR(100),
    transportation_mode VARCHAR(50),  -- road, air, sea
    delivery_status VARCHAR(50),  -- delivered, in_transit, delayed
    is_delayed BIT,
    delivery_lead_time_days INT,
    source_system VARCHAR(20) DEFAULT 'MES',
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (shipment_date_key) REFERENCES silver.dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES silver.dim_partner(partner_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key),
    FOREIGN KEY (product_key) REFERENCES silver.dim_product(product_key)
);

-- インデックス
CREATE INDEX idx_shipment_date ON silver.fact_shipment(shipment_date);
CREATE INDEX idx_shipment_status ON silver.fact_shipment(delivery_status);
CREATE INDEX idx_shipment_mode ON silver.fact_shipment(transportation_mode);
CREATE UNIQUE INDEX idx_shipment_unique ON silver.fact_shipment(shipment_id, order_id, line_number);

-- データ投入
INSERT INTO silver.fact_shipment (
    shipment_id, order_id, line_number, shipment_timestamp, shipment_date,
    shipment_date_key, planned_ship_date, expected_ship_date,
    actual_arrival_timestamp, actual_arrival_date, customer_key, location_key,
    product_key, quantity, carrier_name, transportation_mode, delivery_status,
    is_delayed, delivery_lead_time_days
)
SELECT
    i.shipment_id,
    i.order_id,
    i.line_number,
    CAST(h.shipment_timestamp AS DATETIME2),
    CAST(h.shipment_timestamp AS DATE),
    d.date_key,
    CAST(i.planned_ship_date AS DATE),
    CAST(i.expected_ship_date AS DATE),
    CAST(i.actual_arrival_timestamp AS DATETIME2),
    CAST(i.actual_arrival_timestamp AS DATE),
    c.partner_key,
    l.location_key,
    p.product_key,
    i.quantity,
    i.carrier_name,
    i.transportation_mode,
    i.delivery_status,
    CASE WHEN i.delivery_status = 'delayed' THEN 1 ELSE 0 END,
    DATEDIFF(DAY, CAST(h.shipment_timestamp AS DATE), CAST(i.actual_arrival_timestamp AS DATE))
FROM bronze.mes_shipment_item i
INNER JOIN bronze.mes_shipment_header h ON i.shipment_id = h.shipment_id
LEFT JOIN silver.dim_partner c ON h.customer_id = c.partner_id
LEFT JOIN silver.dim_location l ON h.location_id = l.location_id
INNER JOIN silver.dim_product p ON i.product_id = p.product_id
LEFT JOIN silver.dim_date d ON CAST(h.shipment_timestamp AS DATE) = d.full_date;

GO

-- ================================================================================
-- 5. fact_transportation_cost (輸送コストファクト)
-- ================================================================================
CREATE TABLE silver.fact_transportation_cost (
    transport_cost_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    cost_id VARCHAR(50) NOT NULL UNIQUE,
    shipment_id VARCHAR(50),
    billing_date DATE,
    billing_date_key INT,
    location_key INT,
    cost_type VARCHAR(50),  -- freight, expedite, handling
    cost_amount DECIMAL(18,2),
    currency VARCHAR(3),
    source_system VARCHAR(20) DEFAULT 'TMS',
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (billing_date_key) REFERENCES silver.dim_date(date_key),
    FOREIGN KEY (location_key) REFERENCES silver.dim_location(location_key)
);

-- インデックス
CREATE INDEX idx_transport_cost_date ON silver.fact_transportation_cost(billing_date);
CREATE INDEX idx_transport_cost_type ON silver.fact_transportation_cost(cost_type);
CREATE INDEX idx_transport_shipment ON silver.fact_transportation_cost(shipment_id);

-- データ投入
INSERT INTO silver.fact_transportation_cost (
    cost_id, shipment_id, billing_date, billing_date_key,
    location_key, cost_type, cost_amount, currency
)
SELECT
    t.cost_id,
    t.shipment_id,
    CAST(t.billing_date AS DATE),
    d.date_key,
    l.location_key,
    t.cost_type,
    t.cost_amount,
    t.currency
FROM bronze.tms_transportation_cost t
LEFT JOIN silver.dim_location l ON t.location_id = l.location_id
LEFT JOIN silver.dim_date d ON CAST(t.billing_date AS DATE) = d.full_date;

GO

-- ================================================================================
-- ファクトテーブル作成完了
-- ================================================================================
PRINT 'Silver層ファクトテーブルの作成が完了しました。';
PRINT '- fact_inventory_daily: 日次在庫ファクト';
PRINT '- fact_procurement: 調達ファクト';
PRINT '- fact_sales_order: 受注ファクト';
PRINT '- fact_shipment: 出荷ファクト';
PRINT '- fact_transportation_cost: 輸送コストファクト';
