-- ================================================================================
-- Silverレイヤー: ディメンションテーブル作成
-- メダリオンアーキテクチャ - 自動車製造業 物流・間接材購買部門
-- 作成日: 2025-12-05
-- ================================================================================

-- ================================================================================
-- 1. dim_product (製品マスタ)
-- ================================================================================
CREATE TABLE silver.dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name NVARCHAR(200),
    brand_name VARCHAR(100),
    item_group VARCHAR(100),
    item_hierarchy VARCHAR(50),  -- ICE/EV/HEV
    detail_category VARCHAR(100),
    tax_classification VARCHAR(50),
    transport_group VARCHAR(50),
    import_export_group VARCHAR(50),
    country_of_origin VARCHAR(3),
    base_unit_quantity DECIMAL(18,4),
    is_active BIT DEFAULT 1,
    source_system VARCHAR(20) DEFAULT 'ERP',
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    data_quality_flag VARCHAR(20) DEFAULT 'VALID'  -- VALID/WARNING/ERROR
);

-- インデックス
CREATE INDEX idx_product_group ON silver.dim_product(item_group);
CREATE INDEX idx_product_hierarchy ON silver.dim_product(item_hierarchy);

-- データ投入
INSERT INTO silver.dim_product (
    product_id, product_name, brand_name, item_group, item_hierarchy,
    detail_category, tax_classification, transport_group, 
    import_export_group, country_of_origin, base_unit_quantity
)
SELECT DISTINCT
    product_id,
    product_name,
    brand_name,
    item_group,
    item_hierarchy,
    detail_category,
    tax_classification,
    transport_group,
    import_export_group,
    country_of_origin,
    CAST(base_unit_quantity AS DECIMAL(18,4))
FROM bronze.erp_product_master
WHERE product_id IS NOT NULL;

GO

-- ================================================================================
-- 2. dim_location (拠点マスタ)
-- ================================================================================
CREATE TABLE silver.dim_location (
    location_key INT IDENTITY(1,1) PRIMARY KEY,
    location_id VARCHAR(20) NOT NULL UNIQUE,
    location_name NVARCHAR(200),
    location_type VARCHAR(50),  -- manufacturing_plant, warehouse, distribution_center
    address NVARCHAR(500),
    city NVARCHAR(100),
    state_province NVARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(3),
    contact_person NVARCHAR(100),
    contact_phone VARCHAR(50),
    contact_email VARCHAR(100),
    is_active BIT DEFAULT 1,
    valid_from DATE,
    valid_to DATE,
    source_system VARCHAR(20) DEFAULT 'ERP',
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- インデックス
CREATE INDEX idx_location_type ON silver.dim_location(location_type);
CREATE INDEX idx_location_active ON silver.dim_location(is_active, valid_from, valid_to);

-- データ投入（複数システムから統合）
INSERT INTO silver.dim_location (
    location_id, location_name, location_type, address, city, 
    state_province, postal_code, country, contact_person, 
    contact_phone, contact_email, is_active, valid_from, valid_to
)
SELECT DISTINCT
    location_id,
    location_name,
    location_type,
    address,
    city,
    state_province,
    postal_code,
    country,
    contact_person,
    contact_phone,
    contact_email,
    CASE WHEN is_active = 'active' THEN 1 ELSE 0 END,
    CAST(valid_from AS DATE),
    CAST(valid_to AS DATE)
FROM (
    -- ERPとWMSとMESとTMSから統合
    SELECT * FROM bronze.erp_location_master
    UNION
    SELECT * FROM bronze.wms_location_master
    UNION
    SELECT * FROM bronze.mes_location_master
    UNION
    SELECT * FROM bronze.tms_location_master
) AS unified_locations
WHERE location_id IS NOT NULL;

GO

-- ================================================================================
-- 3. dim_partner (取引先マスタ)
-- ================================================================================
CREATE TABLE silver.dim_partner (
    partner_key INT IDENTITY(1,1) PRIMARY KEY,
    partner_id VARCHAR(50) NOT NULL UNIQUE,
    partner_name NVARCHAR(300),
    partner_type VARCHAR(50),  -- supplier, customer, carrier
    partner_category VARCHAR(100),  -- tier1_supplier, mro_supplier, dealer
    account_group VARCHAR(100),
    tax_id VARCHAR(50),
    address NVARCHAR(500),
    city NVARCHAR(100),
    state_province NVARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(3),
    contact_person NVARCHAR(100),
    contact_email VARCHAR(100),
    contact_phone VARCHAR(50),
    payment_terms VARCHAR(50),
    credit_limit DECIMAL(18,2),
    currency VARCHAR(3),
    region VARCHAR(50),
    is_active BIT DEFAULT 1,
    valid_from DATE,
    valid_to DATE,
    source_system VARCHAR(20),
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- インデックス
CREATE INDEX idx_partner_type ON silver.dim_partner(partner_type);
CREATE INDEX idx_partner_category ON silver.dim_partner(partner_category);
CREATE INDEX idx_partner_account_group ON silver.dim_partner(account_group);

-- データ投入
INSERT INTO silver.dim_partner (
    partner_id, partner_name, partner_type, partner_category, 
    account_group, tax_id, address, city, state_province, 
    postal_code, country, contact_person, contact_email, 
    contact_phone, payment_terms, credit_limit, currency, 
    region, is_active, valid_from, valid_to, source_system
)
SELECT DISTINCT
    partner_id,
    partner_name,
    partner_type,
    partner_category,
    account_group,
    tax_id,
    address,
    city,
    state_province,
    postal_code,
    country,
    contact_person,
    contact_email,
    contact_phone,
    payment_terms,
    credit_limit,
    currency,
    region,
    CASE WHEN is_active = 'active' THEN 1 ELSE 0 END,
    CAST(valid_from AS DATE),
    CAST(valid_to AS DATE),
    'P2P'
FROM (
    SELECT * FROM bronze.p2p_partner_master
    UNION
    SELECT * FROM bronze.erp_partner_master
    UNION
    SELECT * FROM bronze.mes_partner_master
    UNION
    SELECT * FROM bronze.tms_partner_master
) AS unified_partners
WHERE partner_id IS NOT NULL;

GO

-- ================================================================================
-- 4. dim_material (材料マスタ)
-- ================================================================================
CREATE TABLE silver.dim_material (
    material_key INT IDENTITY(1,1) PRIMARY KEY,
    material_id VARCHAR(50) NOT NULL UNIQUE,
    material_name NVARCHAR(300),
    material_category VARCHAR(100),  -- Raw Materials, Electronic Components, Engine Parts
    material_type VARCHAR(50),  -- direct, indirect
    unspsc_code VARCHAR(20),
    related_product_id VARCHAR(50),
    is_active BIT DEFAULT 1,
    source_system VARCHAR(20) DEFAULT 'P2P',
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- インデックス
CREATE INDEX idx_material_category ON silver.dim_material(material_category);
CREATE INDEX idx_material_type ON silver.dim_material(material_type);
CREATE INDEX idx_material_product ON silver.dim_material(related_product_id);

-- データ投入
INSERT INTO silver.dim_material (
    material_id, material_name, material_category, 
    material_type, unspsc_code, related_product_id
)
SELECT DISTINCT
    material_id,
    material_name,
    material_category,
    material_type,
    unspsc_code,
    product_id
FROM bronze.p2p_procurement_item
WHERE material_id IS NOT NULL;

GO

-- ================================================================================
-- 5. dim_date (日付ディメンション)
-- ================================================================================
CREATE TABLE silver.dim_date (
    date_key INT PRIMARY KEY,  -- YYYYMMDD形式
    full_date DATE NOT NULL UNIQUE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    week_of_year INT,
    day_of_month INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BIT,
    is_holiday BIT DEFAULT 0,
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT,
    year_month VARCHAR(7),  -- YYYY-MM
    created_at DATETIME2 DEFAULT GETDATE()
);

-- インデックス
CREATE INDEX idx_date_year_month ON silver.dim_date(year, month);
CREATE INDEX idx_date_fiscal ON silver.dim_date(fiscal_year, fiscal_quarter);

-- データ投入（2022-01-01から2025-12-31までの日付マスタを生成）
WITH date_range AS (
    SELECT CAST('2022-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM date_range
    WHERE dt < '2025-12-31'
)
INSERT INTO silver.dim_date (
    date_key, full_date, year, quarter, month, month_name,
    week_of_year, day_of_month, day_of_week, day_name, 
    is_weekend, fiscal_year, fiscal_quarter, fiscal_month, year_month
)
SELECT
    CAST(FORMAT(dt, 'yyyyMMdd') AS INT),
    dt,
    YEAR(dt),
    DATEPART(QUARTER, dt),
    MONTH(dt),
    DATENAME(MONTH, dt),
    DATEPART(WEEK, dt),
    DAY(dt),
    DATEPART(WEEKDAY, dt),
    DATENAME(WEEKDAY, dt),
    CASE WHEN DATEPART(WEEKDAY, dt) IN (1,7) THEN 1 ELSE 0 END,
    -- 日本の会計年度（4月開始）
    CASE 
        WHEN MONTH(dt) >= 4 THEN YEAR(dt)
        ELSE YEAR(dt) - 1
    END,
    CASE 
        WHEN MONTH(dt) >= 4 THEN (MONTH(dt) - 4) / 3 + 1
        ELSE (MONTH(dt) + 8) / 3 + 1
    END,
    CASE 
        WHEN MONTH(dt) >= 4 THEN MONTH(dt) - 3
        ELSE MONTH(dt) + 9
    END,
    FORMAT(dt, 'yyyy-MM')
FROM date_range
OPTION (MAXRECURSION 0);

GO

-- ================================================================================
-- ディメンションテーブル作成完了
-- ================================================================================
PRINT 'Silver層ディメンションテーブルの作成が完了しました。';
PRINT '- dim_product: 製品マスタ';
PRINT '- dim_location: 拠点マスタ';
PRINT '- dim_partner: 取引先マスタ';
PRINT '- dim_material: 材料マスタ';
PRINT '- dim_date: 日付ディメンション';
